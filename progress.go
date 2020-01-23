package hotstuff

import "github.com/dshulyak/go-hotstuff/types"

type MsgTo struct {
	Recipients []uint64 // nil recipient list is a broadcast
	Message    *types.Message
}

func (m *MsgTo) Broadcast() bool {
	return m.Recipients == nil
}

type BlockRef struct {
	Hash []byte
	View uint64
}

type BlockEvent struct {
	Header    *types.Header
	Finalized bool
}

// Progress is an endpoint for interaction with consensus module.
// Consensus is expected to interact with:
// - transaction pool (notify that node can propose a new block)
// - state machine (block verification and executing new commited blocks)
// - network (sending and receiving messages)
type Progress struct {
	Messages []MsgTo
	Events   []BlockEvent
	NotFound []BlockRef
	// TODO waiting data signal must include a version we built on top, we will need to get state root based on this version
	WaitingData bool
}

func (p *Progress) Empty() bool {
	return len(p.Messages) == 0 && len(p.Events) == 0 && !p.WaitingData
}

func (p *Progress) AddMessage(msg *types.Message, recipients ...uint64) {
	p.Messages = append(p.Messages, MsgTo{Recipients: recipients, Message: msg})
}

func (p *Progress) AddHeader(header *types.Header, finalized bool) {
	p.Events = append(p.Events, BlockEvent{Header: header, Finalized: finalized})
}

func (p *Progress) AddNotFound(view uint64, hash []byte) {
	p.NotFound = append(p.NotFound, BlockRef{View: view, Hash: hash})
}

func (p *Progress) Reset() {
	p.WaitingData = false
	p.Messages = nil
	p.Events = nil
}

func NewVoteMsg(vote *types.Vote) *types.Message {
	return &types.Message{Type: &types.Message_Vote{Vote: vote}}
}

func NewProposalMsg(proposal *types.Proposal) *types.Message {
	return &types.Message{Type: &types.Message_Proposal{Proposal: proposal}}
}

func NewViewMsg(newview *types.NewView) *types.Message {
	return &types.Message{Type: &types.Message_Newview{Newview: newview}}
}

func NewSyncMsg(blocks ...*types.Block) *types.Message {
	return &types.Message{Type: &types.Message_Sync{Sync: &types.Sync{Blocks: blocks}}}
}
