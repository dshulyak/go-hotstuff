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

func nodeProgress(ctx context.Context, n *Node, broadcast func(context.Context, []MsgTo), max int) error {
	count := 0
	n.Start()
	defer n.Close()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msgs := <-n.Messages():
			go broadcast(ctx, msgs)
		case headers := <-n.Blocks():
			count += len(headers)
			if count >= max {
				return nil
			}
		case <-n.Ready():
			n.Send(ctx, Data{
				Root: randRoot(),
				Data: &types.Data{},
			})
		}
	}
}

func TestNodesProgressWithoutErrors(t *testing.T) {
	nodes := createNodes(t, 4, 20*time.Millisecond)
	broadcast := func(ctx context.Context, msgs []MsgTo) {
		for _, msg := range msgs {
			for _, n := range nodes {
				n.Step(ctx, msg.Message)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		errors = make(chan error, len(nodes))
		wg     sync.WaitGroup
	)
	for _, n := range nodes {
		wg.Add(1)
		n := n
		go func() {
			errors <- nodeProgress(ctx, n, broadcast, 100)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(errors)
	}()
	for err := range errors {
		require.NoError(t, err)
	}

	// TODO compare histories after test
}

func TestNodesProgressMessagesDropped(t *testing.T) {
	rng := rand.New(rand.NewSource(*seed))

	nodes := createNodes(t, 4, 20*time.Millisecond)
	broadcast := func(ctx context.Context, msgs []MsgTo) {
		if rng.Intn(100) < 10 {
			return
		}
		for _, msg := range msgs {
			for _, n := range nodes {
				n.Step(ctx, msg.Message)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		errors = make(chan error, len(nodes))
		wg     sync.WaitGroup
	)
	for _, n := range nodes {
		wg.Add(1)
		n := n
		go func() {
			errors <- nodeProgress(ctx, n, broadcast, 3)
			wg.Done()
		}()
	}
	go func() {
		wg.Wait()
		close(errors)
	}()
	for err := range errors {
		require.NoError(t, err)
	}

	// TODO compare histories after test
}
