package hotstuff

import (
	"context"
	"crypto/ed25519"
	"flag"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/dshulyak/go-hotstuff/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	seed = flag.Int64("seed", time.Now().UnixNano(), "seed for tests")
)

func createNodes(tb testing.TB, n int, interval time.Duration) []*Node {
	genesis := randGenesis()
	rng := rand.New(rand.NewSource(*seed))

	logger, err := zap.NewDevelopment()
	require.NoError(tb, err)

	replicas := []Replica{}
	privs := []ed25519.PrivateKey{}
	for i := 1; i <= n; i++ {
		pub, priv, err := ed25519.GenerateKey(rng)
		require.NoError(tb, err)
		replicas = append(replicas, Replica{ID: pub})
		privs = append(privs, priv)
		genesis.Cert.Voters = append(genesis.Cert.Voters, uint64(i))
		genesis.Cert.Sigs = append(genesis.Cert.Sigs, ed25519.Sign(priv, genesis.Header.Hash()))
	}

	nodes := make([]*Node, n)
	for i, priv := range privs {
		db := NewMemDB()
		store := NewBlockStore(db)
		require.NoError(tb, ImportGenesis(store, genesis))

		node := NewNode(logger, store, priv, Config{
			Replicas: replicas,
			ID:       replicas[i].ID,
			Interval: interval,
		})
		nodes[i] = node
	}
	return nodes
}

// testChainConsistency will fail if commited chains have different blocks at certain height.
func testChainConsistency(tb testing.TB, nodes []*Node) {
	headers := map[uint64][]byte{}
	for _, n := range nodes {
		iter := NewChainIterator(n.Store())
		iter.Next()
		for ; iter.Valid(); iter.Next() {
			hash, exist := headers[iter.Header().View]
			if !exist {
				headers[iter.Header().View] = iter.Header().Hash()
			} else {
				require.Equal(tb, hash, iter.Header().Hash())
			}
		}
	}
}

func nodeEventLoop(ctx context.Context, n *Node, networkC chan<- []MsgTo, headersC chan<- []*types.Header) {
	n.Start()
	for {
		select {
		case <-ctx.Done():
			n.Close()
			return
		case headers := <-n.Blocks():
			select {
			case <-ctx.Done():
			case headersC <- headers:
			}
		case msgs := <-n.Messages():
			select {
			case <-ctx.Done():
			case networkC <- msgs:
			}
		case <-n.Ready():
			n.Send(ctx, Data{
				Root: randRoot(),
				Data: &types.Data{},
			})
		}

	}
}

func broadcastMsgs(ctx context.Context, nodes []*Node, networkC <-chan []MsgTo, filter func(MsgTo) bool) {
	for {
		select {
		case <-ctx.Done():
			return
		case msgs := <-networkC:
			for _, msg := range msgs {
				msg := msg
				if filter != nil && filter(msg) {
					continue
				}
				for _, n := range nodes {
					n.Step(ctx, msg.Message)
				}

			}
		}

	}
}

func waitViewCommited(view uint64, headersC <-chan []*types.Header) {
	var (
		current uint64 = 0
	)
	for headers := range headersC {
		for _, h := range headers {
			if h.View > current {
				current = h.View
			}
			if current >= view {
				return
			}
		}
	}
}

func testChainConsistencyAfterProgress(t *testing.T, view uint64, filter func(MsgTo) bool) {
	nodes := createNodes(t, 10, 20*time.Millisecond)

	networkC := make(chan []MsgTo, 100)
	headersC := make(chan []*types.Header, 100)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		wg sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		broadcastMsgs(ctx, nodes, networkC, filter)
		wg.Done()
	}()

	for _, n := range nodes {
		wg.Add(1)
		n := n
		go func() {
			nodeEventLoop(ctx, n, networkC, headersC)
			wg.Done()
		}()
	}

	waitViewCommited(view, headersC)
	cancel()
	wg.Wait()
	testChainConsistency(t, nodes)
}

func TestNodesProgressWithoutErrors(t *testing.T) {
	testChainConsistencyAfterProgress(t, 100, nil)
}

func TestNodesProgressMessagesDropped(t *testing.T) {
	// TODO this test is very random. there should be periods of asynchrony, not constant possibility of messages
	// being dropped, otherwise chances of establishing 3-chain are very low

	testChainConsistencyAfterProgress(t, 3, func(msg MsgTo) bool {
		return rand.Intn(100) < 10
	})
}

func TestNodesProposalDropped(t *testing.T) {
	count := 5
	testChainConsistencyAfterProgress(t, 10, func(msg MsgTo) bool {
		if msg.Message.GetProposal() != nil {
			count--
			return count == 0
		}
		return false
	})
}
