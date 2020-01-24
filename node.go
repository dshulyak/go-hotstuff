package hotstuff

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/dshulyak/go-hotstuff/crypto"
	"github.com/dshulyak/go-hotstuff/types"
	"go.uber.org/zap"
)

var (
	ErrStopped   = errors.New("node was stopped")
	ErrInterrupt = errors.New("request interrupted")
)

type Data struct {
	State []byte
	Root  []byte
	Data  *types.Data
}

type Replica struct {
	ID crypto.PublicKey
	IP net.IP
}

type Config struct {
	Interval time.Duration
	ID       crypto.PublicKey
	Replicas []Replica
}

func NewNode(logger *zap.Logger, store *BlockStore, priv crypto.PrivateKey, conf Config) *Node {
	signer := crypto.NewBLS12381Signer(priv)
	replicas := conf.Replicas
	// FIXME introduce type ID []byte or [32]byte and use it instead of uint64 for replica id everywhere
	pubs := make([]crypto.PublicKey, 0, len(conf.Replicas))
	ids := []uint64{}
	rid := uint64(0)
	for i, r := range replicas {
		id := uint64(i)
		ids = append(ids, id)

		pubs = append(pubs, r.ID)

		if conf.ID == r.ID {
			rid = id
		}
	}
	verifier := crypto.NewBLS12381Verifier(2*len(pubs)/3+1, pubs)
	consensus := newConsensus(logger, store, signer, verifier, rid, ids)
	n := &Node{
		logger:      logger,
		conf:        conf,
		consensus:   consensus,
		store:       store,
		received:    make(chan *types.Message, 1),
		send:        make(chan Data, 1),
		deliver:     make(chan []MsgTo, 1),
		blocks:      make(chan []BlockEvent, 1),
		waitingData: make(chan struct{}, 1),
		missing:     make(chan []BlockRef, 1),
		quit:        make(chan struct{}),
		done:        make(chan struct{}),
		start:       make(chan struct{}),
	}
	go n.run()
	return n
}

type Node struct {
	logger *zap.Logger
	conf   Config

	consensus *consensus
	store     *BlockStore

	received    chan *types.Message
	deliver     chan []MsgTo
	send        chan Data
	blocks      chan []BlockEvent
	missing     chan []BlockRef
	waitingData chan struct{}
	quit        chan struct{}
	done        chan struct{}
	start       chan struct{}
}

func (n *Node) Store() *BlockStore {
	return n.store
}

// Send data after receiving Ready signal.
// Note that sending data doesn't guarantee that data will be commited
// or even proposed.
func (n *Node) Send(ctx context.Context, data Data) error {
	select {
	case <-ctx.Done():
		return ErrInterrupt
	case <-n.quit:
		return ErrStopped
	case n.send <- data:
	}
	return nil
}

// Step should be called every time when new message is received from any peer.
func (n *Node) Step(ctx context.Context, msg *types.Message) error {
	select {
	case <-ctx.Done():
		return ErrInterrupt
	case <-n.quit:
		return ErrStopped
	case n.received <- msg:
	}
	return nil
}

// Ready emit signals whenever node a leader and can make a proposal.
func (n *Node) Ready() <-chan struct{} {
	return n.waitingData
}

// Blocks will emit headers of the commited blocks.
func (n *Node) Blocks() <-chan []BlockEvent {
	return n.blocks
}

func (n *Node) Messages() <-chan []MsgTo {
	return n.deliver
}

func (n *Node) Missing() <-chan []BlockRef {
	return n.missing
}

// Start will panic if called more then one time.
func (n *Node) Start() {
	close(n.start)
}

func (n *Node) Close() {
	close(n.quit)
	<-n.done
}

func (n *Node) run() {
	n.logger.Debug("started event loop")
	var (
		ticker = time.NewTicker(n.conf.Interval)

		toSend   []MsgTo
		toUpdate []BlockEvent
		toSync   []BlockRef

		blocks      chan []BlockEvent
		messages    chan []MsgTo
		waitingData chan struct{}
		missing     chan []BlockRef
	)

	for {
		// wait until all existing progress will be consumed
		if missing == nil && waitingData == nil && blocks == nil && messages == nil {
			progress := n.consensus.Progress
			if len(progress.Messages) > 0 {
				toSend = progress.Messages
				messages = n.deliver
			}
			if len(progress.Events) > 0 {
				toUpdate = progress.Events
				blocks = n.blocks
			}
			if progress.WaitingData {
				waitingData = n.waitingData
			}
			if len(progress.NotFound) > 0 {
				missing = n.missing
				toSync = progress.NotFound
			}
			n.consensus.Progress.Reset()
		}

		select {
		case <-n.start:
			n.start = nil
			n.consensus.Start()
		case msg := <-n.received:
			n.consensus.Step(msg)
		case data := <-n.send:
			n.consensus.Send(data.State, data.Root, data.Data)
		case <-ticker.C:
			n.consensus.Tick()
		case waitingData <- struct{}{}:
			waitingData = nil
		case missing <- toSync:
			missing = nil
		case messages <- toSend:
			toSend = nil
			messages = nil
		case blocks <- toUpdate:
			toUpdate = nil
			blocks = nil
		case <-n.quit:
			ticker.Stop()
			close(n.done)
			n.logger.Debug("exited event loop")
			return
		}
	}
}
