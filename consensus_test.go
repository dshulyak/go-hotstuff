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
	*consensus
	ID       uint64
	Store    *BlockStore
	Signer   Signer
	Verifier Verifier
}

func setupTestNodes(tb testing.TB, n int) []*testNode {
	genesis := randGenesis()
	logger, err := zap.NewDevelopment()
	require.NoError(tb, err)

	pubs, privs, err := crypto.GenerateKeys(nil, n)
	replicas := make([]uint64, 0, n)

	nodes := []*testNode{}
	for id := range pubs {
		uid := uint64(id)
		replicas = append(replicas, uid)
		db := NewMemDB()
		store := NewBlockStore(db)
		signer := crypto.NewBLS12381Signer(privs[uid])
		verifier := crypto.NewBLS12381Verifier(2*len(pubs)/3+1, pubs)
		sig := signer.Sign(nil, genesis.Header.Hash())
		verifier.Merge(genesis.Cert.Sig, uid, sig)
		nodes = append(nodes, &testNode{
			ID:       uid,
			Signer:   signer,
			Verifier: verifier,
			Store:    store,
		})
	}

	for _, n := range nodes {
		require.NoError(tb, ImportGenesis(n.Store, genesis))
		n.consensus = newConsensus(logger, n.Store, n.Signer, n.Verifier, n.ID, replicas)
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
		n.Start()
		if n.Progress.WaitingData {
			if waiting != 0 {
				require.Fail(t, "both %d and %d are waiting for data", id, waiting)
			}
			waiting = uint64(id)
		}
		n.Progress.Reset()
	}
	root := randRoot()
	nodes[waiting].Send(nil, root, &types.Data{})

	// it should prepare one proposal to be broadcasted
	msgs := nodes[waiting].Progress.Messages
	require.Len(t, msgs, 2) // proposal and a vote

	msg := msgs[0]
	require.True(t, msg.Broadcast())
	require.NotNil(t, msg.Message.GetProposal())
	nodes[waiting].Progress.Reset()

	// every node will verify proposal and respond with votes to the next leader
	// note that we send same message to every node including one who prep'ed proposal
	votes := make([]MsgTo, 0, len(nodes))
	votes = append(votes, msgs[1])
	for _, n := range nodes {
		n.Step(msg.Message)
		for _, msg := range n.Progress.Messages {
			votes = append(votes, msg)
		}
		n.Progress.Reset()
	}

	// all votes must be delivered to the same node
	require.Len(t, votes[0].Recipients, 1)
	leader := votes[0].Recipients[0]
	for _, v := range votes {
		require.Equal(t, leader, v.Recipients[0])
		nodes[leader].Step(v.Message)
	}
	// once collected replica will transition to new view and will wait for a data for new block
	require.True(t, nodes[leader].Progress.WaitingData, "replica %d must be waiting for data", leader)
}

func TestConsensusTimeoutsProgress(t *testing.T) {
	var (
		nodes = setupTestNodes(t, 4)
	)

	for _, n := range nodes {
		n.Start()
		n.Progress.Reset()
	}

	// two ticks should be enough to timeout nodes
	newviews := make([]MsgTo, 0, len(nodes))
	for _, n := range nodes {
		n.Tick()
		n.Tick()
		// each node will send new-view message to leader of the round 2
		require.Len(t, n.Progress.Messages, 1)
		newviews = append(newviews, n.Progress.Messages[0])
		n.Progress.Reset()
	}

	for _, nv := range newviews {
		for _, r := range nv.Recipients {
			nodes[r].Step(nv.Message)
		}
	}
	// after receiving new-views one node must be waiting for data
	waiting := uint64(0)
	for i, n := range nodes {
		if n.Progress.WaitingData {
			waiting = uint64(i)
		}
	}
	nodes[waiting].Send(nil, randRoot(), &types.Data{})
}

func propagateOneBlock(nodes []*testNode) {
	var (
		stack1, stack2 []MsgTo
	)
	for _, n := range nodes {
		if n.Progress.WaitingData {
			n.Send(nil, randRoot(), &types.Data{})
			for _, msg := range n.Progress.Messages {
				stack1 = append(stack1, msg)
			}
		}
		n.Progress.Reset()
	}

	for _, n := range nodes {
		for _, msg := range stack1 {
			n.Step(msg.Message)
		}
		for _, msg := range n.Progress.Messages {
			stack2 = append(stack2, msg)
		}
		n.Progress.Reset()
	}
	for _, n := range nodes {
		for _, msg := range stack2 {
			n.Step(msg.Message)
		}
	}
}

func TestConsensusProgressAfterSync(t *testing.T) {
	var (
		nodes = setupTestNodes(t, 4)
	)

	for _, n := range nodes {
		n.Start()
	}

	propagateOneBlock(nodes[1:])
	propagateOneBlock(nodes[:3]) // won't make progress, 1st node is missing a block

	first := nodes[0]
	require.Len(t, first.Progress.NotFound, 1)
	missing := first.Progress.NotFound[0]
	first.Progress.Reset()

	second := nodes[1]
	block, err := second.Store.GetBlock(missing.Hash)
	require.NoError(t, err)
	require.NotNil(t, block)
	first.Step(NewSyncMsg(block))
	require.Len(t, first.Progress.Events, 1)
	event := first.Progress.Events[0]
	require.False(t, event.Finalized)
	require.Equal(t, missing.Hash, event.Header.Hash())
}
