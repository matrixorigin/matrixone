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

var messageBlockTable []bool

func init() {
	messageBlockTable = make([]bool, MaxMessage)
	messageBlockTable[MsgMinValueSigned] = false
	messageBlockTable[MsgMinValueUnsigned] = false
	messageBlockTable[MsgMaxValueSigned] = false
	messageBlockTable[MsgMaxValueUnsigned] = false
	messageBlockTable[MsgPipelineStart] = false
	messageBlockTable[MsgPipelineStop] = false
	messageBlockTable[MsgRuntimeFilter] = false
	messageBlockTable[MsgHashMap] = false
}

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
	pipelineID int32
	parallelID int32
}

type Message struct {
	tag      int32
	msgType  MsgType
	sender   *MessageAddress
	receiver *MessageAddress
	data     any
}

type MessageBoard struct {
	Messages []*Message
	RwMutex  *sync.RWMutex // for nonblock message
	Mutex    *sync.Mutex   // for block message
	Cond     *sync.Cond    //for block message
}

type MessageReceiver struct {
	offset  int32
	tag     int32
	msgType MsgType
	addr    *MessageAddress
	mb      *MessageBoard
}

func DeepCopyMessageAddr(m *MessageAddress) *MessageAddress {
	if m == nil {
		return nil
	}
	return &MessageAddress{
		cnAddr:     m.cnAddr,
		pipelineID: m.pipelineID,
		parallelID: m.parallelID,
	}
}

func DeepCopyMessage(m *Message) *Message {
	return &Message{
		tag:      m.tag,
		msgType:  m.msgType,
		sender:   DeepCopyMessageAddr(m.sender),
		receiver: DeepCopyMessageAddr(m.receiver),
		data:     m.data,
	}
}

func NewMessage(tag int32, msgType MsgType, data any, sender *MessageAddress, receiver *MessageAddress) *Message {
	return &Message{
		tag:      tag,
		msgType:  msgType,
		sender:   sender,
		receiver: receiver,
		data:     data,
	}
}

func (proc *Process) SendMessage(m *Message) {
	mb := proc.MessageBoard
	mb.RwMutex.Lock()
	defer mb.RwMutex.Unlock()
	if m.receiver == nil || m.receiver.cnAddr == CURRENTCN { // message for current CN
		mb.Messages = append(mb.Messages, m)
		if messageBlockTable[m.msgType] {
			// broadcast for block message
			mb.Cond.Broadcast()
		}
	} else {
		//todo: send message to other CN, need to lookup cnlist
		panic("unsupported message yet!")
	}
}

func (proc *Process) AddrBroadCastToCurrentCN() *MessageAddress {
	return nil
}

func (proc *Process) AddrBroadCastToALLCN() *MessageAddress {
	return &MessageAddress{cnAddr: ALLCN}
}

func (proc *Process) AddrBroadCastToOneCN(addr string) *MessageAddress {
	return &MessageAddress{cnAddr: addr}
}

func (proc *Process) AddrBroadCastToOneParallel(addr string, pipelineid int32, parallelid int32) *MessageAddress {
	return &MessageAddress{
		cnAddr:     addr,
		pipelineID: pipelineid,
		parallelID: parallelid,
	}
}

func (proc *Process) NewMessageReceiver(tag int32, msgType MsgType, addr *MessageAddress) *MessageReceiver {
	return &MessageReceiver{
		tag:     tag,
		msgType: msgType,
		addr:    addr,
		mb:      proc.MessageBoard,
	}
}

func (mr *MessageReceiver) receiverMessageNonBlock(result []*Message) {
	mr.mb.RwMutex.RLock()
	defer mr.mb.RwMutex.RUnlock()
	lenMessages := int32(len(mr.mb.Messages))
	for ; mr.offset < lenMessages; mr.offset++ {
		message := mr.mb.Messages[mr.offset]
		if message.tag != mr.tag || message.msgType != mr.msgType {
			continue
		}
		if message.MatchAddress(mr.addr) {
			result = append(result, DeepCopyMessage(message))
		}
	}
	return
}

func (mr *MessageReceiver) ReceiverMessage() []*Message {
	result := make([]*Message, 0)
	if !messageBlockTable[mr.msgType] {
		mr.receiverMessageNonBlock(result)
		return result
	}
	for len(result) == 0 {
		mr.mb.Cond.L.Lock()
		mr.mb.Cond.Wait()
		mr.mb.Cond.L.Unlock()
		mr.receiverMessageNonBlock(result)
	}
	return result
}

func (m *Message) MatchAddress(raddr *MessageAddress) bool {
	mAddr := m.receiver
	if mAddr == nil {
		return true //it's a broadcast message on current CN
	}
	if raddr != nil && mAddr.pipelineID == raddr.pipelineID && mAddr.parallelID == raddr.parallelID {
		return true //it's a unicast message
	}
	return false
}
