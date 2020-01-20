package hotstuff

import "github.com/dshulyak/go-hotstuff/types"

type MsgTo struct {
	Recipients []uint64 // nil recipient list is a broadcast
	Message    *types.Message
}

func (m *MsgTo) Broadcast() bool {
	return m.Recipients == nil
}

// Progress is an endpoint for interaction with consensus module.
// Consensus is expected to interact with:
// - transaction pool (notify that node can propose a new block)
// - state machine (block verification and executing new commited blocks)
// - network (sending and receiving messages)
type Progress struct {
	Messages    []MsgTo
	Headers     []*types.Header
	WaitingData bool
}

func (p *Progress) AddMessage(msg *types.Message, recipients ...uint64) {
	p.Messages = append(p.Messages, MsgTo{Recipients: recipients, Message: msg})
}

func (p *Progress) AddHeader(header *types.Header) {
	p.Headers = append(p.Headers, header)
}

func (p *Progress) Reset() {
	p.WaitingData = false
	p.Messages = nil
	p.Headers = nil
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
