package hotstuff

import (
	"bytes"

	"github.com/dshulyak/go-hotstuff/types"
	"golang.org/x/crypto/blake2s"
)

func NewTimeouts(verifier Verifier, majority int) *Timeouts {
	return &Timeouts{
		verifier: verifier,
		majority: majority,
		received: map[uint64]struct{}{},
	}
}

type Timeouts struct {
	verifier Verifier
	view     uint64
	majority int
	received map[uint64]struct{}
	Cert     *types.TimeoutCertificate
}

func (t *Timeouts) Start(view uint64) {
	if t.view != 0 {
		panic("timeouts must be reset before starting collection for another view")
	}
	t.view = view
	t.Cert = &types.TimeoutCertificate{
		View: view,
		Sig:  &types.AggregatedSignature{},
	}
}

func (t *Timeouts) Reset() {
	t.view = 0
	for k := range t.received {
		delete(t.received, k)
	}
	t.Cert = nil
}

func (t *Timeouts) Collect(nview *types.NewView) bool {
	// 0 is a genesis
	if t.view == 0 {
		return false
	}
	if t.view != nview.View {
		return false
	}
	_, exist := t.received[nview.Voter]
	if exist {
		return false
	}
	if !t.verifier.Verify(nview.Voter, HashSum(EncodeUint64(nview.View)), nview.Sig) {
		return false
	}
	t.received[nview.Voter] = struct{}{}
	t.verifier.Merge(t.Cert.Sig, nview.Voter, nview.Sig)
	return len(t.received) >= t.majority
}

func NewVotes(verifier Verifier, majority int) *Votes {
	return &Votes{
		verifier: verifier,
		majority: majority,
		votes:    map[uint64]struct{}{},
	}
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
		Sig:   &types.AggregatedSignature{},
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
	if !v.verifier.Verify(vote.Voter, v.Cert.Block, vote.Sig) {
		// invalid signature
		return false
	}
	v.votes[vote.Voter] = struct{}{}
	v.verifier.Merge(v.Cert.Sig, vote.Voter, vote.Sig)
	return len(v.votes) >= v.majority
}

func ImportGenesis(store *BlockStore, genesis *types.Block) error {
	hash := genesis.Header.Hash()
	exist, err := store.GetHeader(hash)
	if err == nil && exist != nil {
		return nil
	}
	header := genesis.Header
	if err := store.SaveHeader(genesis.Header); err != nil {
		return err
	}
	if err := store.SaveCertificate(genesis.Cert); err != nil {
		return err
	}
	if err := store.SaveData(hash, genesis.Data); err != nil {
		return err
	}
	if err := store.SaveView(header.View + 1); err != nil {
		return err
	}
	if err := store.SaveVoted(header.View); err != nil {
		return err
	}
	if err := store.SetTag(PrepareTag, hash); err != nil {
		return err
	}
	if err := store.SetTag(LockedTag, hash); err != nil {
		return err
	}
	if err := store.SetTag(DecideTag, hash); err != nil {
		return err
	}
	return nil
}

func HashSum(buf []byte) []byte {
	digest, _ := blake2s.New256(nil)
	digest.Write(buf)
	return digest.Sum(make([]byte, 0, digest.Size()))
}
