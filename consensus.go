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
	VerifyAggregated([]byte, *types.AggregatedSignature) bool
	Verify(uint64, []byte, []byte) bool
	Merge(*types.AggregatedSignature, uint64, []byte)
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
	// TODO should it be persisted? can it help with synchronization after restart?
	timeoutCert *types.TimeoutCertificate

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

func (c *consensus) Send(state, root []byte, data *types.Data) {
	if c.waitingData {
		c.waitingData = false
		header := &types.Header{
			View:       c.view,
			ParentView: c.prepare.View,
			Parent:     c.prepare.Hash(),
			DataRoot:   root,
			StateRoot:  state,
		}
		proposal := &types.Proposal{
			Header:     header,
			Data:       data,
			ParentCert: c.prepareCert,
			Timeout:    c.timeoutCert,
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
	case *types.Message_Sync:
		c.onSync(m.Sync)
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

	// release timeout certificate once it is useless
	if c.timeoutCert != nil && c.timeoutCert.View < c.view-1 {
		c.timeoutCert = nil
	}

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

	if msg.ParentCert == nil {
		return
	}

	if bytes.Compare(msg.Header.Parent, msg.ParentCert.Block) != 0 {
		return
	}

	if !c.verifier.VerifyAggregated(msg.Header.Parent, msg.ParentCert.Sig) {
		log.Debug("certificate is invalid")
		return
	}
	parent, err := c.store.GetHeader(msg.Header.Parent)
	if err != nil {
		log.Debug("header for parent is not found", zap.Error(err))
		// TODO if certified block is not found we need to sync with another node
		c.Progress.AddNotFound(msg.Header.ParentView, msg.Header.Parent)
		return
	}

	if msg.Timeout != nil {
		if !c.verifier.VerifyAggregated(HashSum(EncodeUint64(msg.Header.View-1)), msg.Timeout.Sig) {
			return
		}
	}

	leader := c.getLeader(msg.Header.View)
	if !c.verifier.Verify(leader, msg.Header.Hash(), msg.Sig) {
		log.Debug("proposal is not signed correctly", zap.Uint64("signer", leader))
		return
	}

	c.updatePrepare(parent, msg.ParentCert)
	c.syncView(parent, msg.ParentCert, msg.Timeout)
	c.update(parent, msg.ParentCert)

	if msg.Header.View != c.view {
		log.Debug("proposal view doesn't match local view")
		return
	}

	// TODO after basic validation, state machine needs to validate Data included in the proposal
	// add Data to Progress and wait for a validation from state machine
	// everything after this comment should be done after receiving ack from app state machine
	c.persistProposal(msg)
	if msg.Header.View > c.voted && c.safeNode(msg.Header, msg.ParentCert) {
		log.Debug("proposal is safe to vote on")
		c.sendVote(msg.Header)
	}
}

func (c *consensus) persistProposal(msg *types.Proposal) {
	hash := msg.Header.Hash()
	// TODO header and data for proposal must be tracked, as they can be pruned from store
	// e.g. add a separate bucket for tracking non-finalized blocks
	// remove from that bucket when block is commited
	// in background run a thread that will clear blocks that are in that bucket with a height <= commited height
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

func (c *consensus) update(parent *types.Header, cert *types.Certificate) {
	// TODO if any node is missing in the chain we should switch to sync mode
	gparent, err := c.store.GetHeader(parent.Parent)
	if err != nil {
		return
	}
	ggparent, err := c.store.GetHeader(gparent.Parent)
	if err != nil {
		return
	}

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
		c.Progress.AddHeader(header, false)
	}
}

func (c *consensus) syncView(header *types.Header, cert *types.Certificate, tcert *types.TimeoutCertificate) {
	if tcert == nil && header.View >= c.view {
		c.view = header.View + 1
		if err := c.store.SaveView(c.view); err != nil {
			c.vlog.Fatal("failed to store view", zap.Error(err))
		}
		c.nextRound(false)
	} else if tcert != nil && tcert.View >= c.view {
		c.view = tcert.View + 1
		if err := c.store.SaveView(c.view); err != nil {
			c.vlog.Fatal("failed to store view", zap.Error(err))
		}
		c.nextRound(false)
	}
}

func (c *consensus) onSync(sync *types.Sync) {
	for _, block := range sync.Blocks {
		if !c.syncBlock(block) {
			return
		}
	}
}

// syncBlock returns false if block is invalid.
func (c *consensus) syncBlock(block *types.Block) bool {
	if block.Header == nil || block.Cert == nil || block.Data == nil {
		return false
	}
	if block.Header.View <= c.commit.View {
		return true
	}
	if !c.verifier.VerifyAggregated(block.Header.Hash(), block.Cert.Sig) {
		return false
	}
	log := c.log.With(
		zap.Uint64("block view", block.Header.View),
		zap.Binary("block hash", block.Header.Hash()),
	)
	log.Debug("syncing block")

	if err := c.store.SaveBlock(block); err != nil {
		log.Fatal("can't save block")
	}

	c.updatePrepare(block.Header, block.Cert)
	c.update(block.Header, block.Cert)
	return true
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
	c.syncView(c.votes.Header, c.votes.Cert, nil)
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

	c.updatePrepare(header, msg.Cert)
	if !c.timeouts.Collect(msg) {
		return
	}

	c.vlog.Debug("collected enough new-views to propose a block",
		zap.Uint64("timedout view", msg.View))

	// if all new-views received before replica became a leader it must enter new round and then wait for data
	view := c.view

	c.timeoutCert = c.timeouts.Cert
	c.syncView(c.prepare, c.prepareCert, c.timeouts.Cert)
	if view == msg.View {
		c.nextRound(false)
	} else {
		c.waitingData = true
		c.Progress.WaitingData = true
	}
}
