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

package message

import (
	"bytes"
	"context"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

const messageTimeout = 300 * time.Second
const ALLCN = "ALLCN"
const CURRENTCN = "CURRENTCN"

type MsgType int32

const (
	MsgTopValue      MsgType = 0
	MsgPipelineStart MsgType = 1
	MsgPipelineStop  MsgType = 2
	MsgRuntimeFilter MsgType = 3
	MsgJoinMap       MsgType = 4
	MaxMessage       MsgType = 1024
)

func (m MsgType) MessageName() string {
	switch m {
	case MsgTopValue:
		return "MsgTopValue"
	case MsgRuntimeFilter:
		return "MsgRuntimeFilter"
	}
	return "unknown message type"
}

type MessageAddress struct {
	CnAddr     string
	OperatorID int32
	ParallelID int32
}

type Message interface {
	Serialize() []byte
	Deserialize([]byte) Message
	NeedBlock() bool
	GetMsgTag() int32
	GetReceiverAddr() MessageAddress
	DebugString() string
	Destroy()
}

type MessageCenter struct {
	StmtIDToBoard map[uuid.UUID]*MessageBoard
	RwMutex       *sync.Mutex
}

type MessageBoard struct {
	reset         bool // for debug purpose
	closed        bool
	drained       bool
	multiCN       bool
	stmtId        uuid.UUID
	messageCenter *MessageCenter
	messages      []*Message
	waiters       []chan bool
	rwMutex       *sync.RWMutex
}

func NewMessageBoard() *MessageBoard {
	m := &MessageBoard{
		messages: make([]*Message, 0, 16),
		waiters:  make([]chan bool, 0, 16),
		rwMutex:  &sync.RWMutex{},
	}
	runtime.SetFinalizer(m, (*MessageBoard).finalize)
	return m
}

func (m *MessageBoard) finalize() {
	m.cleanupQueuedMessages()
}

func (m *MessageBoard) DebugString() string {
	if m == nil || m.rwMutex == nil {
		return "messageBoard is nil\n"
	}
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	buf := bytes.NewBuffer(make([]byte, 0, 400))
	if m.reset {
		buf.WriteString("messageBoard has been reseted!\n")
	}
	if m.closed {
		buf.WriteString("messageBoard has been closed!\n")
	}
	if m.multiCN {
		buf.WriteString("messageBoard on MultiCN\n")
	} else {
		buf.WriteString("messageBoard on single CN\n")
	}
	buf.WriteString("messageBoard length: " + strconv.Itoa(len(m.messages)) + "\n")
	for i := range m.messages {
		message := *m.messages[i]
		buf.WriteString("message " + strconv.Itoa(i) + ": " + message.DebugString() + "\n")
	}
	return buf.String()
}

func (m *MessageBoard) SetMultiCN(center *MessageCenter, stmtId uuid.UUID) *MessageBoard {
	center.RwMutex.Lock()
	defer center.RwMutex.Unlock()
	mb, ok := center.StmtIDToBoard[stmtId]
	if ok {
		return mb
	}
	m.rwMutex.Lock()
	m.multiCN = true
	m.stmtId = stmtId
	m.messageCenter = center
	m.rwMutex.Unlock()
	center.StmtIDToBoard[stmtId] = m
	return m
}

func (m *MessageBoard) BeforeRunonce() {
	// call this before runonce
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	if !m.closed {
		m.reset = false
	}
}

func (m *MessageBoard) Reset() *MessageBoard {
	m.rwMutex.RLock()
	multiCN := m.multiCN
	center := m.messageCenter
	stmtID := m.stmtId
	m.rwMutex.RUnlock()
	if multiCN {
		center.RwMutex.Lock()
		delete(center.StmtIDToBoard, stmtID)
		center.RwMutex.Unlock()
		// other pipeline could still access thie messageBoard
		// so reset current message board to a new one
		return NewMessageBoard()
	}
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	if m.closed {
		return NewMessageBoard()
	}
	m.cleanupQueuedMessagesLocked()
	m.multiCN = false
	m.reset = true
	return m
}

// Close prevents publication and wakes blocked receivers without destroying
// queued ownership. It is safe while consumers are still unwinding; the
// terminal owner must call CloseAndDrain after every producer and consumer is
// quiescent. True identifies the call that first closed the board.
func (m *MessageBoard) Close() bool {
	return m.close(false)
}

// CloseAndDrain is the terminal MessageBoard boundary for one execution
// attempt. Callers invoke it only after all scope and remote-notifier producers
// and consumers are quiescent. It removes a multi-CN registration, destroys
// every queued ownership-bearing message, and prevents a late producer from
// republishing into the closed generation. The operation is idempotent; true
// identifies the call that performed the drain, including a drain after an
// earlier Close.
func (m *MessageBoard) CloseAndDrain() bool {
	return m.close(true)
}

func (m *MessageBoard) close(drain bool) bool {
	if m == nil || m.rwMutex == nil {
		return false
	}
	m.rwMutex.RLock()
	multiCN := m.multiCN
	center := m.messageCenter
	stmtID := m.stmtId
	m.rwMutex.RUnlock()
	if multiCN && center != nil {
		center.RwMutex.Lock()
		if center.StmtIDToBoard[stmtID] == m {
			delete(center.StmtIDToBoard, stmtID)
		}
		center.RwMutex.Unlock()
	}

	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	firstClose := !m.closed
	if firstClose {
		m.closed = true
		m.reset = true
		for _, waiter := range m.waiters {
			if waiter == nil {
				continue
			}
			select {
			case waiter <- true:
			default:
			}
		}
	}
	if !drain {
		return firstClose
	}
	if m.drained {
		return false
	}
	m.drained = true
	m.cleanupQueuedMessagesLocked()
	return true
}

func (m *MessageBoard) cleanupQueuedMessages() {
	if m == nil || m.rwMutex == nil {
		return
	}
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	m.cleanupQueuedMessagesLocked()
}

func (m *MessageBoard) cleanupQueuedMessagesLocked() {
	for i := range m.messages {
		if m.messages[i] == nil {
			continue
		}
		message := *m.messages[i]
		message.Destroy()
		m.messages[i] = nil
	}
	m.messages = m.messages[:0]
	m.waiters = m.waiters[:0]
}

type MessageReceiver struct {
	debug    bool // for unit test
	offset   int32
	tags     []int32
	received []int32
	addr     *MessageAddress
	mb       *MessageBoard
	waiter   chan bool
}

func NewMessageReceiver(tags []int32, addr MessageAddress, mb *MessageBoard) *MessageReceiver {
	return &MessageReceiver{
		tags: tags,
		addr: &addr,
		mb:   mb,
	}
}

func SendMessage(m Message, mb *MessageBoard) {
	if m.GetReceiverAddr().CnAddr == CURRENTCN { // message for current CN
		mb.rwMutex.Lock()
		if mb.closed {
			mb.rwMutex.Unlock()
			m.Destroy()
			return
		}
		mb.messages = append(mb.messages, &m)
		if m.NeedBlock() {
			// broadcast for block message
			for _, ch := range mb.waiters {
				if ch != nil && len(ch) == 0 {
					ch <- true
				}
			}
		}
		mb.rwMutex.Unlock()
	} else {
		//todo: send message to other CN, need to lookup cnlist
		panic("unsupported message yet!")
	}
}

func (mr *MessageReceiver) receiveMessageNonBlock() ([]Message, bool) {
	mr.mb.rwMutex.RLock()
	defer mr.mb.rwMutex.RUnlock()
	var result []Message
	lenMessages := int32(len(mr.mb.messages))
	for ; mr.offset < lenMessages; mr.offset++ {
		if mr.mb.messages[mr.offset] == nil {
			continue
		}
		message := *mr.mb.messages[mr.offset]
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
	return result, mr.mb.closed
}

func (mr *MessageReceiver) ReceiveMessage(needBlock bool, ctx context.Context) ([]Message, bool, error) {
	result, closed := mr.receiveMessageNonBlock()
	if !needBlock || len(result) > 0 {
		return result, false, nil
	}
	if closed {
		return result, false, moerr.NewInternalErrorNoCtx(
			"message board is closed",
		)
	}
	if mr.waiter == nil {
		mr.waiter = make(chan bool, 1)
		mr.mb.rwMutex.Lock()
		if mr.mb.closed {
			closed = true
		} else {
			mr.mb.waiters = append(mr.mb.waiters, mr.waiter)
		}
		mr.mb.rwMutex.Unlock()
		if closed {
			return result, false, moerr.NewInternalErrorNoCtx(
				"message board is closed",
			)
		}
	}
	for {
		result, closed = mr.receiveMessageNonBlock()
		if len(result) > 0 {
			break
		}
		if closed {
			return result, false, moerr.NewInternalErrorNoCtx(
				"message board is closed",
			)
		}
		timeout := messageTimeout
		if mr.debug {
			timeout = 1 * time.Second
		}
		timeoutCtx, timeoutCancel := context.WithTimeoutCause(context.Background(), timeout, moerr.CauseReceiveMessage)
		select {
		case <-timeoutCtx.Done():
			timeoutCancel()
			logutil.Warnf("waiting messsage timeout, waiting for tag %v, messageBoard debug message %v", mr.tags, mr.mb.DebugString())
		case <-mr.waiter:
			timeoutCancel()
		case <-ctx.Done():
			timeoutCancel()
			return result, true, nil
		}
	}
	return result, false, nil
}

func MatchAddress(m Message, raddr *MessageAddress) bool {
	mAddr := m.GetReceiverAddr()
	if mAddr.OperatorID != raddr.OperatorID && mAddr.OperatorID != -1 {
		return false
	}
	if mAddr.ParallelID != raddr.ParallelID && mAddr.ParallelID != -1 {
		return false
	}
	return true
}

func AddrBroadCastOnCurrentCN() MessageAddress {
	return MessageAddress{
		CnAddr:     CURRENTCN,
		OperatorID: -1,
		ParallelID: -1,
	}
}

func AddrBroadCastOnALLCN() MessageAddress {
	return MessageAddress{
		CnAddr:     ALLCN,
		OperatorID: -1,
		ParallelID: -1,
	}
}
