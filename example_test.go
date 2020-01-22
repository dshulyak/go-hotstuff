package hotstuff

import (
	"context"
	"crypto/ed25519"
	"time"

	"github.com/dshulyak/go-hotstuff/types"
	"go.uber.org/zap"
)

func ExampleFull() {

	logger, err := zap.NewProduction()
	must(err)

	db := NewMemDB()
	store := NewBlockStore(db)

	pub, pkey, err := ed25519.GenerateKey(nil)
	must(err)

	conf := Config{
		Interval: 100 * time.Millisecond, // estimated max network delay
		ID:       pub,
		Replicas: []Replica{},
	}

	node := NewNode(logger, store, pkey, conf)
	node.Start()

	// any message from the network
	node.Step(context.Background(), &types.Message{})

	select {
	case <-node.Ready():
		node.Send(context.Background(), Data{
			State: []byte{},
			Root:  []byte{},
			Data:  &types.Data{},
		})
	case msgs := <-node.Messages():
		_ = msgs
		// broadcast message or send it to a peer if specified
	case blocks := <-node.Blocks():
		_ = blocks
		// each block will appear up to two times
		// first time non-finalized, for speculative execution
		// second time finalized, execution can be persisted on disk
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
