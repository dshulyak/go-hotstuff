package hotstuff

import (
	"bytes"

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

type consensus struct {
	logger *zap.Logger
	store  *BlockStore

	// signer and verifier are done this way to remove all public key references from consensus logic
	// uint64 refers to validator's public key in sorted set of validators keys
	signer   Signer
	verifier Verifier

	// timeout controlled by consensus by changing number of expected ticks
	ticks, timeout int

	id       uint64
	replicas []uint64 // all replicas, including current

	votes    *Votes
	timeouts *Timeouts

	view                    uint64 // current view.
	voted                   uint64 // last voted view. must be persisted to ensure no accidental double vote on the same view
	prepare, locked, commit *types.Header
	prepareCert             *types.Certificate // prepareCert always updated to highest known certificate

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
		c.Progress.AddMessage(NewProposalMsg(proposal))
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

func (c *consensus) onTimeout() {
	c.view++
	err := c.store.SaveView(c.view)
	if err != nil {
		c.logger.Fatal("can't update view", zap.Error(err))
	}
	c.nextRound(true)
}

func (c *consensus) nextRound(timedout bool) {
	c.ticks = 0
	c.waitingData = false
	c.timeout = c.newTimeout()
	c.votes.Reset()

	// maintain timeouts transient state for 2 rounds. round before this node is a leader
	// and a round after.
	if c.id == c.getLeader(c.view-1) {
		c.timeouts.Reset()
	}
	// if this replica is a leader for next view start collecting new view messages
	if c.id == c.getLeader(c.view+1) {
		c.timeouts.Start(c.view)
	}

	leader := c.getLeader(c.view)
	if timedout && c.view-1 > c.voted {

		nview := &types.NewView{
			View: c.view - 1,
			Cert: c.prepareCert,
			Sig:  c.signer.Sign(nil, EncodeUint64(c.view-1)),
		}
		c.voted = c.view - 1
		err := c.store.SaveVoted(c.voted)
		if err != nil {
			c.logger.Fatal("can't save voted", zap.Error(err))
		}
		c.Progress.AddMessage(NewViewMsg(nview), leader)
	} else if leader == c.id {
		c.waitingData = true
		c.Progress.WaitingData = true
	}
}

func (c *consensus) onProposal(msg *types.Proposal) {
	if bytes.Compare(msg.Header.Parent, msg.ParentCert.Block) != 0 {
		return
	}
	// verify that parent certificate has enough unique votes
	if !c.verifier.VerifyCert(msg.ParentCert) {
		return
	}
	parent, err := c.store.GetHeader(msg.ParentCert.Block)
	if err != nil {
		return
	}
	c.updatePrepare(parent, msg.ParentCert)

	// this condition is not in the spec
	// but if 2f+1 replicas will sign byzantine block with view set to MaxUint64
	// protocol won't be able to make progress anymore
	// and i can't find a condition that prevents it
	if msg.Header.View != c.view {
		return
	}
	if bytes.Compare(msg.Header.Parent, msg.ParentCert.Block) != 0 {
		return
	}
	leader := c.getLeader(msg.Header.View)
	// TODO log verification errors
	// validate that a proposal received from an expected leader for this round
	if !c.verifier.VerifySingle(leader, msg.Header.Hash(), msg.Sig) {
		return
	}

	// TODO after basic validation state machine needs to validate Data included in the proposal
	// add Data to Progress and wait for a validation from state machine
	// everything after this comment should be done after receiving ack from app state machine

	if msg.Header.View > c.voted && c.safeNode(msg.Header, msg.ParentCert) {
		hash := msg.Header.Hash()
		// TODO header and data for proposal must be tracked, as they can be pruned from store
		// e.g. add a separate bucket for tracking non-finalized blocks
		// remove from that bucket when block is commited
		// in background run a thread that will clear blocks that are in that bucket with a height <= commited hight
		err := c.store.SaveHeader(msg.Header)
		if err != nil {
			c.logger.Fatal("can't save a header", zap.Error(err))
		}
		err = c.store.SaveData(hash, msg.Data)
		if err != nil {
			c.logger.Fatal("can't save a block data", zap.Error(err))
		}

		err = c.store.SaveCertificate(msg.ParentCert)
		if err != nil {
			c.logger.Fatal("can't save a certificate", zap.Error(err))
		}

		c.voted = msg.Header.View
		err = c.store.SaveVoted(c.voted)
		if err != nil {
			c.logger.Fatal("can't save voted", zap.Error(err))
		}

		c.votes.Start(msg.Header)

		vote := &types.Vote{
			Block: hash,
			Voter: c.id,
			Sig:   c.signer.Sign(nil, hash),
		}
		c.Progress.AddMessage(NewVoteMsg(vote), c.getLeader(c.view+1))
	}
	c.update(msg.Header, msg.ParentCert)
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

	// 2-chain locked, gaps are allowed
	if gparent.View > c.locked.View {
		c.locked = gparent
		err := c.store.SetTag(LockedTag, gparent.Hash())
		if err != nil {
			c.logger.Fatal("can't set locked tag", zap.Error(err))
		}
	}
	// 3-chain commited must be without gaps
	if gparent.View-parent.View == 1 && ggparent.View-gparent.View == 1 {
		c.commit = ggparent
		err := c.store.SetTag(DecideTag, ggparent.Hash())
		if err != nil {
			c.logger.Fatal("can't set decided tag", zap.Error(err))
		}
		c.Progress.AddHeader(c.commit)
	}
}

func (c *consensus) updatePrepare(header *types.Header, cert *types.Certificate) {
	if header.View > c.prepare.View {
		c.prepare = header
		c.prepareCert = cert
		err := c.store.SetTag(PrepareTag, header.Hash())
		if err != nil {
			c.logger.Fatal("failed to set prepare tag", zap.Error(err))
		}
		c.view = cert.View + 1
		err = c.store.SaveView(c.view)
		if err != nil {
			c.logger.Fatal("failed to store view", zap.Error(err))
		}
	}
}

func (c *consensus) safeNode(header *types.Header, cert *types.Certificate) bool {
	// is safe if header extends locked or cert height is higher then the lock height
	parent, err := c.store.GetHeader(header.Parent)
	if err != nil {
		// TODO this could be out of order, and require synchronization
		return false
	}
	// safe to vote since majority overwrote a lock
	// i think, only possible if leader didn't wait for 2f+1 new views from a prev rounds
	if parent.View > c.locked.View {
		return true
	}
	// TODO check that header extends locked
	return false
}

func (c *consensus) getLeader(view uint64) uint64 {
	// TODO change to hash(view) % replicas
	return c.replicas[view%uint64(len(c.replicas))]
}

func (c *consensus) newTimeout() int {
	// double for each gap between current round and last round where quorum was collected
	return int(2 * (c.view - c.prepare.View))
}

func (c *consensus) onVote(vote *types.Vote) {
	// next leader is reponsible for aggregating votes from the previous round
	if c.id != c.getLeader(c.view+1) {
		return
	}
	if !c.votes.Collect(vote) {
		// do nothing if there is no majority
		return
	}
	// update leaf and certificate to collected and prepare for proposal
	c.updatePrepare(c.votes.Header, c.votes.Cert)
	c.nextRound(false)
}

func (c *consensus) onNewView(msg *types.NewView) {
	if c.id != c.getLeader(msg.View+1) {
		return
	}
	header, err := c.store.GetHeader(msg.Cert.Block)
	if err != nil {
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
	c.waitingData = true
	c.Progress.WaitingData = true
}
