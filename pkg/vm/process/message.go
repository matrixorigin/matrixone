// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"sync"
)

const ALLCN = "ALLCN"
const CURRENTCN = "CURRENTCN"

type MsgType int32

const (
	MsgMinValueSigned   MsgType = 0
	MsgMinValueUnsigned MsgType = 1
	MsgMaxValueSigned   MsgType = 2
	MsgMaxValueUnsigned MsgType = 3
	MsgPipelineStart    MsgType = 4
	MsgPipelineStop     MsgType = 5
	MsgRuntimeFilter    MsgType = 6
	MsgHashMap          MsgType = 7
	MaxMessage          MsgType = 1024
)

func (m MsgType) MessageName() string {
	switch m {
	case MsgMinValueSigned:
		return "MsgMinValueSigned"
	case MsgMinValueUnsigned:
		return "MsgMinValueUnsigned"
	case MsgMaxValueSigned:
		return "MsgMaxValueSigned"
	case MsgMaxValueUnsigned:
		return "MsgMaxValueUnsigned"
	}
	return "unknown message type"
}

func NewMessageBoard() *MessageBoard {
	m := &MessageBoard{
		Messages: make([]*Message, 0, 1024),
		RwMutex:  &sync.RWMutex{},
		Mutex:    &sync.Mutex{},
	}
	m.Cond = sync.NewCond(m.Mutex)
	return m
}

type MessageAddress struct {
	cnAddr     string
	operatorID int32
	parallelID int32
}

type Message interface {
	Serialize() []byte
	Deserialize([]byte) Message
	NeedBlock() bool
	GetMsgTag() int32
	GetReceiverAddr() MessageAddress
}

type MessageBoard struct {
	Messages []*Message
	RwMutex  *sync.RWMutex // for nonblock message
	Mutex    *sync.Mutex   // for block message
	Cond     *sync.Cond    //for block message
}

type MessageReceiver struct {
	offset   int32
	tags     []int32
	received []int32
	addr     *MessageAddress
	mb       *MessageBoard
}

func (proc *Process) SendMessage(m Message) {
	mb := proc.MessageBoard
	mb.RwMutex.Lock()
	defer mb.RwMutex.Unlock()
	if m.GetReceiverAddr().cnAddr == CURRENTCN { // message for current CN
		mb.Messages = append(mb.Messages, &m)
		if m.NeedBlock() {
			// broadcast for block message
			mb.Cond.Broadcast()
		}
	} else {
		//todo: send message to other CN, need to lookup cnlist
		panic("unsupported message yet!")
	}
}

func (proc *Process) NewMessageReceiver(tags []int32, addr *MessageAddress) *MessageReceiver {
	return &MessageReceiver{
		tags: tags,
		addr: addr,
		mb:   proc.MessageBoard,
	}
}

func (mr *MessageReceiver) receiveMessageNonBlock(result []Message) {
	mr.mb.RwMutex.RLock()
	defer mr.mb.RwMutex.RUnlock()
	lenMessages := int32(len(mr.mb.Messages))
	for ; mr.offset < lenMessages; mr.offset++ {
		message := *mr.mb.Messages[mr.offset]
		if !MatchAddress(message, mr.addr) {
			continue
		}
		for i := range mr.tags {
			if mr.tags[i] == message.GetMsgTag() {
				result = append(result, message)
				mr.received = append(mr.received, mr.offset)
				break
			}
		}
	}
}

func (mr *MessageReceiver) Free() {
	if len(mr.received) == 0 {
		return
	}
	mr.mb.RwMutex.Lock()
	defer mr.mb.RwMutex.Unlock()
	for i := range mr.received {
		mr.mb.Messages[i] = nil
	}
	mr.received = nil
}

func (mr *MessageReceiver) ReceiveMessage(needBlock bool) []Message {
	result := make([]Message, 0)
	if !needBlock {
		mr.receiveMessageNonBlock(result)
		return result
	}
	for len(result) == 0 {
		mr.mb.Cond.L.Lock()
		mr.mb.Cond.Wait()
		mr.mb.Cond.L.Unlock()
		mr.receiveMessageNonBlock(result)
	}
	return result
}

func MatchAddress(m Message, raddr *MessageAddress) bool {
	mAddr := m.GetReceiverAddr()
	if mAddr.operatorID != raddr.operatorID && mAddr.operatorID != -1 {
		return false
	}
	if mAddr.parallelID != raddr.parallelID && mAddr.parallelID != -1 {
		return false
	}
	return true
}

func AddrBroadCastOnCurrentCN() MessageAddress {
	return MessageAddress{
		cnAddr:     CURRENTCN,
		operatorID: -1,
		parallelID: -1,
	}
}

func AddrBroadCastOnALLCN() MessageAddress {
	return MessageAddress{
		cnAddr:     ALLCN,
		operatorID: -1,
		parallelID: -1,
	}
}

var _ Message = new(TopValueMessage)

type TopValueMessage struct {
	topvalue int64
	tag      int32
	signed   bool
	min      bool
}

func (t TopValueMessage) Serialize() []byte {
	panic("top value message only broadcasts on current CN, don't need to serialize")
}

func (t TopValueMessage) Deserialize([]byte) Message {
	panic("top value message only broadcasts on current CN, don't need to deserialize")
}

func (t TopValueMessage) NeedBlock() bool {
	return false
}

func (t TopValueMessage) GetMsgTag() int32 {
	return t.tag
}

func (t TopValueMessage) GetReceiverAddr() MessageAddress {
	return AddrBroadCastOnCurrentCN()
}
