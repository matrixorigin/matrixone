// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// eventType alias uint8, which indicates the type of event.
type eventType uint8

// String returns the string of event type.
func (t eventType) String() string {
	switch t {
	case TypeKill:
		return "Kill"
	case TypeSetVar:
		return "SetVar"
	case TypeQuit:
		return "Quit"
	case TypeUpgrade:
		return "Upgrade"
	}
	return "Unknown"
}

const (
	// TypeKill indicates the kill query statement.
	TypeKill eventType = 1
	// TypeSetVar indicates the set variable statement.
	TypeSetVar eventType = 2
	// TypeQuit indicates the exit cmd.
	TypeQuit eventType = 3
	// TypeUpgrade indicates the "upgrade account all" statement.
	TypeUpgrade eventType = 4
)

// IEvent is the event interface.
type IEvent interface {
	// notify notifies the event is finished.
	notify()
	// wait waits until is event is finished.
	wait()
}

// baseEvent describes the base event information which happens in tunnel data flow.
type baseEvent struct {
	// typ is the event type.
	typ eventType
	// waitC is used to control the event waiter.
	waitC chan struct{}
}

// notify implements the IEvent interface.
func (e *baseEvent) notify() {
	e.waitC <- struct{}{}
}

// wait implements the IEvent interface.
func (e *baseEvent) wait() {
	<-e.waitC
}

// sendReq sends an event to event channel.
func sendReq(e IEvent, c chan<- IEvent) {
	c <- e
}

// sendResp receives an event response from the channel.
func sendResp(r []byte, c chan<- []byte) {
	c <- r
}

// makeEvent parses an event from message bytes. If we got no
// supported event, just return nil. If the second return value
// is true, means that the message has been consumed completely,
// and do not need to send to dst anymore.
func makeEvent(msg []byte, b *msgBuf) (IEvent, bool) {
	if msg == nil || len(msg) < preRecvLen {
		return nil, false
	}
	if isCmdQuery(msg) {
		sql := getStatement(msg)
		stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 0)
		if err != nil {
			return nil, false
		}
		if len(stmts) != 1 {
			return nil, false
		}
		switch s := stmts[0].(type) {
		case *tree.Kill:
			return makeKillEvent(sql, s.ConnectionId), true
		case *tree.SetVar:
			// This event should be sent to dst, so return false,
			return makeSetVarEvent(sql), false
		case *tree.UpgradeStatement:
			return makeUpgradeEvent(sql), true
		default:
			return nil, false
		}
	} else if b.connCacheEnabled && isCmdQuit(msg) {
		// The quit event should not be sent to server. It will be
		// handled in the event handler. According to the config,
		// the quit command will be sent to server or not.
		return makeQuitEvent(), true
	}
	return nil, false
}

// killEvent is the event that "kill query" or "kill connection" statement is captured.
// We need to send this statement to a specified CN server which has
// the connection ID on it.
type killEvent struct {
	baseEvent
	// stmt is the statement that will be sent to server.
	stmt string
	// The ID of connection that needs to be killed.
	connID uint32
}

// makeKillEvent creates a event with TypeKill type.
func makeKillEvent(stmt string, connID uint64) IEvent {
	e := &killEvent{
		stmt:   stmt,
		connID: uint32(connID),
	}
	e.typ = TypeKill
	return e
}

// notify implements the IEvent interface.
func (e *killEvent) notify() {}

// wait implements the IEvent interface.
func (e *killEvent) wait() {}

// setVarEvent is the event that set session variable or set user variable.
// We need to check if the execution of this statement is successful, and
// then keep the variable and its value to clientConn.
type setVarEvent struct {
	baseEvent
	// stmt is the statement that will be sent to server.
	stmt string
}

// makeSetVarEvent creates an event with TypeSetVar type.
func makeSetVarEvent(stmt string) IEvent {
	e := &setVarEvent{
		baseEvent: baseEvent{
			waitC: make(chan struct{}),
		},
		stmt: stmt,
	}
	e.typ = TypeSetVar
	return e
}

type quitEvent struct {
	baseEvent
}

// makeQuitEvent creates an event with TypeExit type.
func makeQuitEvent() IEvent {
	e := &quitEvent{
		baseEvent: baseEvent{
			waitC: make(chan struct{}),
		},
	}
	e.typ = TypeQuit
	return e
}

type upgradeEvent struct {
	baseEvent
	stmt string
}

func makeUpgradeEvent(stmt string) IEvent {
	e := &upgradeEvent{
		baseEvent: baseEvent{
			waitC: make(chan struct{}),
		},
		stmt: stmt,
	}
	e.typ = TypeUpgrade
	return e
}
