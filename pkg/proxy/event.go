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
	"fmt"
	"regexp"
	"strconv"
	"sync"
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
)

var (
	set              = "[sS][eE][tT]"
	session          = "[sS][eE][sS][sS][iI][oO][nN]"
	local            = "[lL][oO][cC][aA][lL]"
	spaceAtLeastZero = "\\s*"
	spaceAtLeastOne  = "\\s+"
	varName          = "[a-zA-Z][a-zA-Z0-9_]*"
	varValue         = "[0-9]+|'[\\s\\S]*'"
	assign           = ":?="
	at               = "@"
	atAt             = "@@"
	dot              = "\\."
)

// patterMap is a map eventType => pattern string
var patternMap = map[eventType]string{
	TypeKillQuery: `^([kK][iI][lL][lL])\s+([qQ][uU][eE][rR][yY])\s+(\d+)$`,
	// TypeSetVar matches set session variable:
	//   - set session key=value;
	//   - set local key=value;
	//   - set @@session.key=value;
	//   - set @@local.key=value;
	//   - set @@key=value;
	//   - set key=value;
	// and set user variable:
	//   - set @key=value;
	TypeSetVar: fmt.Sprintf(`^(%s)%s(%s%s%s|%s%s%s|%s%s%s%s|%s%s%s%s|%s%s|%s|%s%s)%s(%s)%s(%s)$`,
		set, spaceAtLeastOne, // set
		session, spaceAtLeastOne, varName, // session key
		local, spaceAtLeastOne, varName, // local key
		atAt, session, dot, varName, // @@session.key
		atAt, local, dot, varName, // @@local.key
		atAt, varName, // @@key
		varName,     // key
		at, varName, // @key
		spaceAtLeastZero, assign, spaceAtLeastZero, // = or :=
		varValue), // value
}

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

// eventReq is used to make an event.
type eventReq struct {
	// msg is a MySQL packet bytes.
	msg []byte
}

// eventReqPool is used to fetch event request from pool.
var eventReqPool = sync.Pool{
	New: func() interface{} {
		return new(eventReq)
	},
}

// makeEvent parses an event from message bytes. If we got no
// supported event, just return nil. If the second return value
// is true, means that the message has been consumed completely,
// and do not need to send to dst anymore.
func makeEvent(req *eventReq) (IEvent, bool) {
	if req == nil || len(req.msg) < preRecvLen {
		return nil, true
	}
	if req.msg[4] == byte(cmdQuery) {
		stmt := getStatement(req.msg)
		// Get the event type.
		var typ eventType
		var matched bool
		for typ = range patternMap {
			matched, _ = regexp.MatchString(patternMap[typ], stmt)
			if matched {
				break
			}
		}
		if !matched {
			return nil, true
		}
		switch typ {
		case TypeKillQuery:
			return makeKillQueryEvent(stmt, regexp.MustCompile(patternMap[typ])), true
		case TypeSetVar:
			// This event should be sent to dst, so return false,
			return makeSetVarEvent(stmt), false
		default:
			return nil, true
		}
	}
	return nil, true
}

// killQueryEvent is the event that "kill query" statement is captured.
// We need to send this statement to a specified CN server which has
// the connection ID on it.
type killQueryEvent struct {
	baseEvent
	// The ID of connection that needs to be killed.
	connID uint32
	// stmt is the statement that will be sent to server.
	stmt string
}

// makeKillQueryEvent creates a event with TypeKillQuery type.
func makeKillQueryEvent(stmt string, reg *regexp.Regexp) IEvent {
	items := reg.FindStringSubmatch(stmt)
	if len(items) != 4 {
		return nil
	}
	connID, err := strconv.ParseUint(items[3], 10, 32)
	if err != nil {
		return nil
	}

	e := &killQueryEvent{
		connID: uint32(connID),
		stmt:   stmt,
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
