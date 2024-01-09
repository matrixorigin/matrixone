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
	"time"
)

type MsgType int32

const (
	MsgMinValueSigned   MsgType = 1
	MsgMinValueUnsigned MsgType = 2
	MsgMaxValueSigned   MsgType = 3
	MsgMaxValueUnsigned MsgType = 4
	MsgPipelineStart    MsgType = 5
	MsgPipelineStop     MsgType = 6
	MsgRuntimeFilter    MsgType = 7
	MsgHashMap          MsgType = 8
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
	return &MessageBoard{
		Messages: make([]*Message, 0, 1024),
		Mutex:    &sync.RWMutex{},
	}
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
	Mutex    *sync.RWMutex
}

type MessageReceiver struct {
	block   bool
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
	mb.Mutex.Lock()
	defer mb.Mutex.Unlock()
	if m.receiver == nil { // broadcast to current CN
		mb.Messages = append(mb.Messages, m)
	} else {
		//todo: send message to other CN
		panic("unsupported message yet!")
	}
}

func (proc *Process) AddrBroadCastToCurrentCN() *MessageAddress {
	return nil
}

func (proc *Process) AddrBroadCastToALLCN() *MessageAddress {
	return &MessageAddress{cnAddr: ""}
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

func (proc *Process) NewMessageReceiver(tag int32, msgType MsgType, addr *MessageAddress, block bool) *MessageReceiver {
	return &MessageReceiver{
		block:   block,
		tag:     tag,
		msgType: msgType,
		addr:    addr,
		mb:      proc.MessageBoard,
	}
}

func (mr *MessageReceiver) receiverMessageNonBlock() []*Message {
	result := make([]*Message, 0)
	mr.mb.Mutex.RLock()
	defer mr.mb.Mutex.RUnlock()
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
	return result
}

func (mr *MessageReceiver) ReceiverMessage() []*Message {
	waittime := 1
RETRY:
	result := mr.receiverMessageNonBlock()
	if !mr.block || len(result) > 0 {
		return result
	}
	time.Sleep(time.Millisecond * time.Duration(waittime))
	waittime = waittime * 2
	if waittime > 512 {
		waittime = 512
	}
	goto RETRY
}

func (m *Message) MatchAddress(addr *MessageAddress) bool {
	mAddr := m.receiver
	if mAddr == nil || len(mAddr.cnAddr) == 0 {
		return true //it's a broadcast message
	}
	if addr != nil && mAddr.pipelineID == addr.pipelineID && mAddr.parallelID == addr.parallelID {
		return true //it's a unicast message
	}
	return false
}
