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
	case TypeKillQuery:
		return "KillQuery"
	case TypeSetVar:
		return "SetVar"
	case TypePrepare:
		return "Prepare"
	case TypeInitDB:
		return "Use"
	}
	return "Unknown"
}

const (
	// TypeMin is the minimal event type.
	TypeMin eventType = 0
	// TypeKillQuery indicates the kill query statement.
	TypeKillQuery eventType = 1
	// TypeSetVar indicates the set variable statement.
	TypeSetVar eventType = 2
	// TypePrepare indicates the prepare statement.
	TypePrepare eventType = 5
	// TypeInitDB indicates the use database statement.
	TypeInitDB eventType = 6
)

// IEvent is the event interface.
type IEvent interface {
	// eventType returns the type of the event.
	eventType() eventType
}

// baseEvent describes the base event information which happens in tunnel data flow.
type baseEvent struct {
	// typ is the event type.
	typ eventType
}

// eventType implements the IEvent interface.
func (e *baseEvent) eventType() eventType {
	return TypeMin
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
			return makeKillQueryEvent(sql, s.ConnectionId), true
		case *tree.SetVar:
			// This event should be sent to dst, so return false,
			return makeSetVarEvent(sql), false
		case *tree.PrepareString:
			return makePrepareEvent(sql), false
		case *tree.Use:
			return makeInitDBEvent(s.Name.Origin()), false
		default:
			return nil, false
		}
	} else if isCmdInitDB(msg) {
		return makeInitDBEvent(getStatement(msg)), false
	} else if isCmdStmtPrepare(msg) && b != nil {
		b.setPrepared(true)
	} else if isCmdStmtClose(msg) && b != nil {
		b.setPrepared(false)
	}
	return nil, false
}

// killQueryEvent is the event that "kill query" statement is captured.
// We need to send this statement to a specified CN server which has
// the connection ID on it.
type killQueryEvent struct {
	baseEvent
	// stmt is the statement that will be sent to server.
	stmt string
	// The ID of connection that needs to be killed.
	connID uint32
}

// makeKillQueryEvent creates a event with TypeKillQuery type.
func makeKillQueryEvent(stmt string, connID uint64) IEvent {
	e := &killQueryEvent{
		stmt:   stmt,
		connID: uint32(connID),
	}
	e.typ = TypeKillQuery
	return e
}

// eventType implements the IEvent interface.
func (e *killQueryEvent) eventType() eventType {
	return TypeKillQuery
}

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
		stmt: stmt,
	}
	e.typ = TypeSetVar
	return e
}

// eventType implements the IEvent interface.
func (e *setVarEvent) eventType() eventType {
	return TypeSetVar
}

// prepareEvent is the event that execute a prepare statement.
type prepareEvent struct {
	baseEvent
	stmt string
}

// makePrepareEvent creates an event with TypePrepare type.
func makePrepareEvent(stmt string) IEvent {
	e := &prepareEvent{
		stmt: stmt,
	}
	e.typ = TypePrepare
	return e
}

// eventType implements the IEvent interface.
func (e *prepareEvent) eventType() eventType {
	return TypePrepare
}

// initDBEvent is the event that execute a use statement.
type initDBEvent struct {
	baseEvent
	db string
}

// makeUseEvent creates an event with TypeUse type.
func makeInitDBEvent(db string) IEvent {
	e := &initDBEvent{
		db: db,
	}
	e.typ = TypeInitDB
	return e
}

// eventType implements the IEvent interface.
func (e *initDBEvent) eventType() eventType {
	return TypeInitDB
}
