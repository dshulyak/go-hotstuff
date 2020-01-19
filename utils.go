package hotstuff

import (
	"bytes"

	"github.com/dshulyak/go-hotstuff/types"
)

type Timeouts struct {
	verifier Verifier
	view     uint64
	majority int
	received map[uint64]struct{}
}

func (t *Timeouts) Start(view uint64) {
	t.view = view
}

func (t *Timeouts) Reset() {
	t.view = 0
	for k := range t.received {
		delete(t.received, k)
	}
}

func (t *Timeouts) Collect(nview *types.NewView) bool {
	// 0 is a genesis
	if t.view == 0 {
		return false
	}
	_, exist := t.received[nview.Voter]
	if exist {
		return false
	}
	if !t.verifier.VerifySingle(nview.Voter, EncodeUint64(nview.View), nview.Sig) {
		return false
	}
	t.received[nview.Voter] = struct{}{}
	return len(t.received) >= t.majority
}

// Votes keeps track of votes for a block in a round.
// Must be reset at the start of every round.
type Votes struct {
	verifier Verifier
	majority int
	// block => certificate
	votes  map[uint64]struct{}
	Cert   *types.Certificate
	Header *types.Header
}

// Start must be invoked when node receives proposal from a valid leader.
func (v *Votes) Start(header *types.Header) {
	v.Cert = &types.Certificate{
		Block: header.Hash(),
	}
	v.Header = header
}

func (v *Votes) Reset() {
	for k := range v.votes {
		delete(v.votes, k)
	}
	v.Cert = nil
	v.Header = nil
}

func (v *Votes) Collect(vote *types.Vote) bool {
	if v.Cert == nil {
		// received vote before proposal from a valid leader
		return false
	}
	if bytes.Compare(v.Cert.Block, vote.Block) != 0 {
		// vote for a block not from a valid leader
		return false
	}
	_, exist := v.votes[vote.Voter]
	if exist {
		// duplicate
		return false
	}
	if !v.verifier.VerifySingle(vote.Voter, v.Cert.Block, vote.Sig) {
		// invalid signature
		return false
	}
	v.votes[vote.Voter] = struct{}{}
	v.verifier.Merge(v.Cert, vote)
	return len(v.votes) >= v.majority
}
