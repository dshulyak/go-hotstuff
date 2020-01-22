package hotstuff

import (
	"bytes"
	"fmt"

	"github.com/dshulyak/go-hotstuff/types"
	"go.uber.org/zap"
)

type Signer interface {
	Sign([]byte, []byte) []byte
}

type Verifier interface {
	VerifyCert(*types.Certificate) bool
	VerifySingle(uint64, []byte, []byte) bool
	Merge(*types.Certificate, *types.Vote)
}

func newConsensus(
	logger *zap.Logger,
	store *BlockStore,
	signer Signer,
	verifier Verifier,
	id uint64,
	replicas []uint64,
) *consensus {
	logger = logger.Named(fmt.Sprintf("replica=%d", id))
	view, err := store.GetView()
	if err != nil {
		logger.Fatal("failed to load view", zap.Error(err))
	}
	voted, err := store.GetVoted()
	if err != nil {
		logger.Fatal("failed to load voted view", zap.Error(err))
	}
	prepare, err := store.GetTagHeader(PrepareTag)
	if err != nil {
		logger.Fatal("failed to load prepare header", zap.Error(err))
	}
	locked, err := store.GetTagHeader(LockedTag)
	if err != nil {
		logger.Fatal("failed to load locked header", zap.Error(err))
	}
	commit, err := store.GetTagHeader(DecideTag)
	if err != nil {
		logger.Fatal("failed to load commit header", zap.Error(err))
	}
	prepareCert, err := store.GetTagCert(PrepareTag)
	if err != nil {
		logger.Fatal("failed to load prepare certificate", zap.Error(err))
	}
	return &consensus{
		log:         logger,
		store:       store,
		signer:      signer,
		verifier:    verifier,
		id:          id,
		replicas:    replicas,
		timeouts:    NewTimeouts(verifier, 2*len(replicas)/3+1),
		votes:       NewVotes(verifier, 2*len(replicas)/3+1),
		prepare:     prepare,
		locked:      locked,
		commit:      commit,
		prepareCert: prepareCert,
		view:        view,
		voted:       voted,
	}
}

type consensus struct {
	log, vlog *zap.Logger

	store *BlockStore

	// signer and verifier are done this way to remove all public key references from consensus logic
	// uint64 refers to validator's public key in sorted set of validators keys
	signer   Signer
	verifier Verifier

	// timeout controlled by consensus. doubles the number of expected ticks for every gap since last block
	// was signed by quorum.
	ticks, timeout int

	// TODO maybe it wasn't great idea to remove public key from consensus
	id       uint64
	replicas []uint64 // all replicas, including current

	votes    *Votes
	timeouts *Timeouts

	view                    uint64 // current view.
	voted                   uint64 // last voted view. must be persisted to ensure no accidental double vote on the same view
	prepare, locked, commit *types.Header
	prepareCert             *types.Certificate // prepareCert always updated to highest known certificate

	// maybe instead Progress should be returned by each public method
	Progress Progress

	waitingData bool // if waitingData true then node must create a proposal when it receives data
}

func (c *consensus) Tick() {
	c.ticks++
	if c.ticks == c.timeout {
		c.onTimeout()
	}
}

func (c *consensus) Send(root []byte, data *types.Data) {
	if c.waitingData {
		c.waitingData = false
		header := &types.Header{
			View:     c.view,
			Parent:   c.prepare.Hash(),
			DataRoot: root,
		}
		proposal := &types.Proposal{
			Header:     header,
			Data:       data,
			ParentCert: c.prepareCert,
			Sig:        c.signer.Sign(nil, header.Hash()),
		}
		c.vlog.Debug("sending proposal",
			zap.Binary("hash", header.Hash()),
			zap.Binary("parent", header.Parent),
			zap.Uint64("signer", c.id))

		c.sendMsg(NewProposalMsg(proposal))
	}
}

func (c *consensus) Step(msg *types.Message) {
	switch m := msg.GetType().(type) {
	case *types.Message_Proposal:
		c.onProposal(m.Proposal)
	case *types.Message_Vote:
		c.onVote(m.Vote)
	case *types.Message_Newview:
		c.onNewView(m.Newview)
	}
}

func (c *consensus) sendMsg(msg *types.Message, ids ...uint64) {
	c.Progress.AddMessage(msg, ids...)
	if ids == nil {
		c.Step(msg)
	}
	for _, id := range ids {
		if c.id == id {
			c.Step(msg)
		}
	}
}

func (c *consensus) Start() {
	c.nextRound(false)
}

func (c *consensus) onTimeout() {
	c.view++
	err := c.store.SaveView(c.view)
	if err != nil {
		c.vlog.Fatal("can't update view", zap.Error(err))
	}
	c.nextRound(true)
}

func (c *consensus) resetTimeout() {
	c.ticks = 0
	c.timeout = c.newTimeout()
}

func (c *consensus) nextRound(timedout bool) {
	c.vlog = c.log.With(zap.Uint64("CURRENT VIEW", c.view))

	c.resetTimeout()
	c.waitingData = false
	c.votes.Reset()

	c.vlog.Debug("entered new view",
		zap.Int("view timeout", c.timeout),
		zap.Bool("timedout", timedout))

	// timeouts will be collected for two rounds, before leadership and the round when replica is a leader
	if c.id == c.getLeader(c.view-1) {
		c.timeouts.Reset()
	}
	// if this replica is a leader for next view start collecting new view messages
	if c.id == c.getLeader(c.view+1) {
		c.vlog.Debug("replica is a leader for next view. collecting new-view messages")

		c.timeouts.Reset()
		c.timeouts.Start(c.view)
	}

	if timedout && c.view-1 > c.voted {
		c.sendNewView()
	} else if c.id == c.getLeader(c.view) {
		c.vlog.Debug("replica is a leader for this view. waiting for data")
		c.waitingData = true
		c.Progress.WaitingData = true
	}
}

func (c *consensus) onProposal(msg *types.Proposal) {
	log := c.vlog.With(
		zap.String("msg", "proposal"),
		zap.Uint64("header view", msg.Header.View),
		zap.Uint64("prepare view", c.prepare.View),
		zap.Binary("hash", msg.Header.Hash()),
		zap.Binary("parent", msg.Header.Parent))

	log.Debug("received proposal")
	if bytes.Compare(msg.Header.Parent, msg.ParentCert.Block) != 0 {
		return
	}

	if !c.verifier.VerifyCert(msg.ParentCert) {
		log.Debug("certificate is invalid")
		return
	}
	parent, err := c.store.GetHeader(msg.ParentCert.Block)
	if err != nil {
		log.Debug("header for parent is not found", zap.Error(err))
		// TODO if certified block is not found we need to sync with another node
		return
	}

	leader := c.getLeader(msg.Header.View)
	if !c.verifier.VerifySingle(leader, msg.Header.Hash(), msg.Sig) {
		log.Debug("proposal is not signed correctly", zap.Uint64("signer", leader))
		return
	}

	// TODO this needs to be incorporated into updatePrepare routine after timeout certificate is added
	update := parent.View > c.prepare.View
	c.updatePrepare(parent, msg.ParentCert)
	if update {
		c.view = msg.Header.View
		c.nextRound(false)
	}
	c.update(msg.Header, msg.ParentCert)

	// this condition is not in the spec
	// but if 2f+1 replicas will sign byzantine block with view set to MaxUint64
	// protocol won't be able to make progress anymore
	// TODO this needs to be enabled when timeout certificate will be introduced

	if msg.Header.View != c.view {
		log.Debug("proposal view doesn't match local view")
		return
	}

	// TODO after basic validation, state machine needs to validate Data included in the proposal
	// add Data to Progress and wait for a validation from state machine
	// everything after this comment should be done after receiving ack from app state machine

	if msg.Header.View > c.voted && c.safeNode(msg.Header, msg.ParentCert) {
		log.Debug("proposal is safe to vote on")
		c.persistProposal(msg)
		c.sendVote(msg.Header)
	}
}

func (c *consensus) persistProposal(msg *types.Proposal) {
	hash := msg.Header.Hash()
	// TODO header and data for proposal must be tracked, as they can be pruned from store
	// e.g. add a separate bucket for tracking non-finalized blocks
	// remove from that bucket when block is commited
	// in background run a thread that will clear blocks that are in that bucket with a height <= commited hight
	err := c.store.SaveHeader(msg.Header)
	if err != nil {
		c.vlog.Fatal("can't save a header", zap.Error(err))
	}
	err = c.store.SaveData(hash, msg.Data)
	if err != nil {
		c.vlog.Fatal("can't save a block data", zap.Error(err))
	}

	err = c.store.SaveCertificate(msg.ParentCert)
	if err != nil {
		c.vlog.Fatal("can't save a certificate", zap.Error(err))
	}
}

func (c *consensus) sendNewView() {
	// send new-view to the leader of this round.

	leader := c.getLeader(c.view)
	nview := &types.NewView{
		Voter: c.id,
		View:  c.view - 1,
		Cert:  c.prepareCert,
		// TODO prehash encoded uint
		Sig: c.signer.Sign(nil, HashSum(EncodeUint64(c.view-1))),
	}
	c.voted = c.view - 1
	err := c.store.SaveVoted(c.voted)
	if err != nil {
		c.vlog.Fatal("can't save voted", zap.Error(err))
	}

	c.vlog.Debug("sending new-view", zap.Uint64("previous view", nview.View), zap.Uint64("leader", leader))
	c.sendMsg(NewViewMsg(nview), leader)
}

func (c *consensus) sendVote(header *types.Header) {
	hash := header.Hash()

	c.voted = header.View
	err := c.store.SaveVoted(c.voted)
	if err != nil {
		c.vlog.Fatal("can't save voted", zap.Error(err))
	}

	c.votes.Start(header)

	vote := &types.Vote{
		Block: hash,
		View:  header.View,
		Voter: c.id,
		Sig:   c.signer.Sign(nil, hash),
	}

	c.sendMsg(NewVoteMsg(vote), c.getLeader(vote.View+1))
}

func (c *consensus) update(header *types.Header, cert *types.Certificate) {
	// TODO if any node is missing in the chain we should switch to sync mode
	parent, err := c.store.GetHeader(header.Parent)
	if err != nil {
		return
	}
	gparent, err := c.store.GetHeader(parent.Parent)
	if err != nil {
		return
	}
	ggparent, err := c.store.GetHeader(gparent.Parent)
	if err != nil {
		return
	}

	// TODO 1-2 chain blocks should be emitted also so that transaction pool can be adjusted accordingly

	// 2-chain locked, gaps are allowed
	if gparent.View > c.locked.View {
		c.vlog.Debug("new block locked", zap.Uint64("view", gparent.View), zap.Binary("hash", gparent.Hash()))
		c.locked = gparent
		err := c.store.SetTag(LockedTag, gparent.Hash())
		if err != nil {
			c.vlog.Fatal("can't set locked tag", zap.Error(err))
		}
	}
	// 3-chain commited must be without gaps
	if parent.View-gparent.View == 1 && gparent.View-ggparent.View == 1 && ggparent.View > c.commit.View {
		c.vlog.Info("new block commited", zap.Uint64("view", ggparent.View), zap.Binary("hash", ggparent.Hash()))
		c.commit = ggparent
		err := c.store.SetTag(DecideTag, ggparent.Hash())
		if err != nil {
			c.vlog.Fatal("can't set decided tag", zap.Error(err))
		}
		c.Progress.AddHeader(c.commit, true)
	}
}

func (c *consensus) updatePrepare(header *types.Header, cert *types.Certificate) {
	if header.View > c.prepare.View {
		c.vlog.Debug("new block certified",
			zap.Uint64("view", header.View),
			zap.Binary("hash", header.Hash()),
		)
		c.prepare = header
		c.prepareCert = cert
		err := c.store.SetTag(PrepareTag, header.Hash())
		if err != nil {
			c.vlog.Fatal("failed to set prepare tag", zap.Error(err))
		}
		c.view = header.View + 1
		err = c.store.SaveView(c.view)
		if err != nil {
			c.vlog.Fatal("failed to store view", zap.Error(err))
		}
		c.Progress.AddHeader(header, false)
	}
}

func (c *consensus) safeNode(header *types.Header, cert *types.Certificate) bool {
	// is safe if header extends locked or cert height is higher then the lock height
	parent, err := c.store.GetHeader(header.Parent)
	if err != nil {
		// TODO this could be out of order, and require synchronization
		return false
	}
	if bytes.Compare(header.Parent, c.locked.Hash()) == 0 {
		return true
	}
	if bytes.Compare(header.Parent, c.prepare.Hash()) == 0 {
		return true
	}

	// safe to vote since majority overwrote a lock
	// i think, only possible if leader didn't wait for 2f+1 new views from a prev rounds
	if parent.View > c.locked.View {
		return true
	}
	return false
}

func (c *consensus) getLeader(view uint64) uint64 {
	// TODO change to hash(view) % replicas
	return c.replicas[view%uint64(len(c.replicas))]
}

func (c *consensus) newTimeout() int {
	// double for each gap between current round and last round where quorum was collected
	return int(1 << (c.view - c.prepare.View))
}

func (c *consensus) onVote(vote *types.Vote) {
	// next leader is reponsible for aggregating votes from the previous round
	log := c.vlog.With(
		zap.String("msg", "vote"),
		zap.Uint64("voter", vote.Voter),
		zap.Uint64("view", c.view),
		zap.Binary("hash", vote.Block),
	)
	if c.id != c.getLeader(vote.View+1) {
		return
	}
	log.Debug("received vote")
	if !c.votes.Collect(vote) {
		// do nothing if there is no majority
		return
	}
	// update leaf and certificate to collected and prepare for proposal
	if err := c.store.SaveCertificate(c.votes.Cert); err != nil {
		c.log.Fatal("can't save new certificate",
			zap.Binary("cert for block", c.votes.Cert.Block),
		)
	}
	c.updatePrepare(c.votes.Header, c.votes.Cert)
	c.nextRound(false)
}

func (c *consensus) onNewView(msg *types.NewView) {
	if c.id != c.getLeader(msg.View+1) {
		return
	}
	c.vlog.Debug("received new-view",
		zap.Uint64("local view", c.view),
		zap.Uint64("timedout view", msg.View),
		zap.Binary("certificate for", msg.Cert.Block),
		zap.Uint64("from", msg.Voter),
	)

	header, err := c.store.GetHeader(msg.Cert.Block)
	if err != nil {
		c.vlog.Debug("can't find block", zap.Binary("block", msg.Cert.Block))
		return
	}

	/*TODO there should be two types of certificates
		regular progress certificate, collected from votes
		timeout certificate, collected from signature on new-views
		without timeout certificate consensus will have to wait additional time to make progress

	Example:
		after collecting new-view for view 4 with highest cert view 2
		replicas that are aware of highest cert view 1 will reset its view to 3 (based on received progress certificate)
		but they will not vote for this round as they already "voted" on timeout for round 4
		they will not vote in round 3 and 4 since they already have voted=4
		only after entering round 5 they will send new-view, and respond to a proposal for round 6, because
		round won't be reset since all replicas have highest available qc
	*/
	c.updatePrepare(header, msg.Cert)
	if !c.timeouts.Collect(msg) {
		return
	}
	// if all new-views received before replica became a leader it must enter new round and then wait for data
	c.vlog.Debug("collected enough new-views to propose a block",
		zap.Uint64("timedout view", msg.View))

	if c.view == msg.View {
		c.nextRound(false)
	} else {
		c.timeouts.Reset()
		c.waitingData = true
		c.Progress.WaitingData = true
	}
}
