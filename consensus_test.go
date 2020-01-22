package hotstuff

import (
	"crypto/rand"
	"testing"

	"github.com/dshulyak/go-hotstuff/crypto"
	"github.com/dshulyak/go-hotstuff/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func randGenesis() *types.Block {
	header := &types.Header{
		DataRoot: randRoot(),
	}
	return &types.Block{
		Header: header,
		Cert: &types.Certificate{
			Block: header.Hash(),
			Sig:   &types.AggregatedSignature{},
		},
		Data: &types.Data{},
	}
}

func randRoot() []byte {
	root := make([]byte, 32)
	rand.Read(root)
	return root
}

type testNode struct {
	ID        uint64
	Consensus *consensus
	Store     *BlockStore
	Signer    Signer
	Verifier  Verifier
}

func setupTestNodes(tb testing.TB, n int) map[uint64]*testNode {
	genesis := randGenesis()
	logger, err := zap.NewDevelopment()
	require.NoError(tb, err)

	pubs, privs := crypto.GenerateKeys(n)
	replicas := make([]uint64, 0, n)

	nodes := map[uint64]*testNode{}
	for id := range pubs {
		replicas = append(replicas, id)
		db := NewMemDB()
		store := NewBlockStore(db)
		signer := crypto.NewEd25519Signer(privs[id])
		verifier := crypto.NewEd25519Verifier(2*len(pubs)/3+1, pubs)
		//cons := newConsensus(nil, store, signer, verifier, id, replicas)
		sig := signer.Sign(nil, genesis.Header.Hash())
		genesis.Cert.Sig.Voters = append(genesis.Cert.Sig.Voters, id)
		genesis.Cert.Sig.Sigs = append(genesis.Cert.Sig.Sigs, sig)
		nodes[id] = &testNode{
			ID:       id,
			Signer:   signer,
			Verifier: verifier,
			Store:    store,
		}
	}
	for _, n := range nodes {
		require.NoError(tb, ImportGenesis(n.Store, genesis))
		n.Consensus = newConsensus(logger, n.Store, n.Signer, n.Verifier, n.ID, replicas)
	}

	return nodes
}

func TestConsensusErrorFreeQuorumProgress(t *testing.T) {
	var (
		nodes   = setupTestNodes(t, 4)
		waiting uint64
	)

	// one of the nodes will observe that it must be a leader for this round and will be waiting for data
	for id, n := range nodes {
		n.Consensus.Start()
		if n.Consensus.Progress.WaitingData {
			if waiting != 0 {
				require.Fail(t, "both %d and %d are waiting for data", id, waiting)
			}
			waiting = id
		}
		n.Consensus.Progress.Reset()
	}
	require.NotZero(t, waiting)
	root := randRoot()
	nodes[waiting].Consensus.Send(nil, root, &types.Data{})

	// it should prepare one proposal to be broadcasted
	msgs := nodes[waiting].Consensus.Progress.Messages
	require.Len(t, msgs, 2) // proposal and a vote

	msg := msgs[0]
	require.True(t, msg.Broadcast())
	require.NotNil(t, msg.Message.GetProposal())
	nodes[waiting].Consensus.Progress.Reset()

	// every node will verify proposal and respond with votes to the next leader
	// note that we send same message to every node including one who prep'ed proposal
	votes := make([]MsgTo, 0, len(nodes))
	votes = append(votes, msgs[1])
	for _, n := range nodes {
		n.Consensus.Step(msg.Message)
		for _, msg := range n.Consensus.Progress.Messages {
			votes = append(votes, msg)
		}
		n.Consensus.Progress.Reset()
	}

	// all votes must be delivered to the same node
	require.Len(t, votes[0].Recipients, 1)
	leader := votes[0].Recipients[0]
	for _, v := range votes {
		require.Equal(t, leader, v.Recipients[0])
		nodes[leader].Consensus.Step(v.Message)
	}
	// once collected replica will transition to new view and will wait for a data for new block
	require.True(t, nodes[leader].Consensus.Progress.WaitingData, "replica %d must be waiting for data", leader)
}

func TestConsensusTimeoutsProgress(t *testing.T) {
	var (
		nodes = setupTestNodes(t, 4)
	)

	for _, n := range nodes {
		n.Consensus.Start()
		n.Consensus.Progress.Reset()
	}

	// two ticks should be enough to timeout nodes
	newviews := make([]MsgTo, 0, len(nodes))
	for _, n := range nodes {
		n.Consensus.Tick()
		n.Consensus.Tick()
		// each node will send new-view message to leader of the round 2
		require.Len(t, n.Consensus.Progress.Messages, 1)
		newviews = append(newviews, n.Consensus.Progress.Messages[0])
		n.Consensus.Progress.Reset()
	}

	for _, nv := range newviews {
		for _, r := range nv.Recipients {
			nodes[r].Consensus.Step(nv.Message)
		}
	}
	// after receiving new-views one node must be waiting for data
	waiting := uint64(0)
	for i, n := range nodes {
		if n.Consensus.Progress.WaitingData {
			waiting = i
		}
	}
	require.NotZero(t, waiting)
	nodes[waiting].Consensus.Send(nil, randRoot(), &types.Data{})
}

type noopSignerVerifier struct {
}

func (noop noopSignerVerifier) Sign(dst []byte, _ []byte) []byte {
	return dst
}

func (noop noopSignerVerifier) VerifyCert(*types.Certificate) bool {
	return true
}

func (noop noopSignerVerifier) VerifySingle(uint64, []byte, []byte) bool {
	return true
}

func (noop noopSignerVerifier) Merge(*types.Certificate, *types.Vote) {}
