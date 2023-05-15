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
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
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
	case TypeSuspendAccount:
		return "SuspendAccount"
	case TypeDropAccount:
		return "DropAccount"
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
	// TypeSuspendAccount indicates the suspend account statement.
	TypeSuspendAccount eventType = 3
	// TypeDropAccount indicates the drop account statement.
	TypeDropAccount eventType = 4
)

var (
	kill     = "[kK][iI][lL][lL]"
	query    = "[qQ][uU][eE][rR][yY]"
	set      = "[sS][eE][tT]"
	session  = "[sS][eE][sS][sS][iI][oO][nN]"
	local    = "[lL][oO][cC][aA][lL]"
	alter    = "[aA][lL][tT][eE][rR]"
	account  = "[aA][cC][cC][oO][uU][nN][tT]"
	ifExists = "?:[iI][fF]\\s+[eE][xX][iI][sS][tT][sS]\\s+"
	suspend  = "[sS][uU][sS][pP][eE][nN][dD]"
	drop     = "[dD][rR][oO][pP]"

	num              = "\\d+"
	spaceAtLeastZero = "\\s*"
	spaceAtLeastOne  = "\\s+"
	varName          = "[a-zA-Z][a-zA-Z0-9_]*"
	varValue         = "[0-9]+|'[\\s\\S]*'"
	assign           = ":?="
	at               = "@"
	atAt             = "@@"
	dot              = "\\."
	end              = ";?"
)

// patterMap is a map eventType => pattern string
var patternMap = map[eventType]string{
	// Sample: kill query 10
	TypeKillQuery: fmt.Sprintf(`^%s(%s)%s(%s)%s(%s)%s%s%s$`,
		spaceAtLeastZero, kill, spaceAtLeastOne,
		query, spaceAtLeastOne,
		num, spaceAtLeastZero, end, spaceAtLeastZero),

	// TypeSetVar matches set session variable:
	//   - set session key=value;
	//   - set local key=value;
	//   - set @@session.key=value;
	//   - set @@local.key=value;
	//   - set @@key=value;
	//   - set key=value;
	// and set user variable:
	//   - set @key=value;
	TypeSetVar: fmt.Sprintf(`^%s(%s)%s(%s%s%s|%s%s%s|%s%s%s%s|%s%s%s%s|%s%s|%s|%s%s)%s(%s)%s(%s)%s%s%s$`,
		spaceAtLeastZero, set, spaceAtLeastOne, // set
		session, spaceAtLeastOne, varName, // session key
		local, spaceAtLeastOne, varName, // local key
		atAt, session, dot, varName, // @@session.key
		atAt, local, dot, varName, // @@local.key
		atAt, varName, // @@key
		varName,     // key
		at, varName, // @key
		spaceAtLeastZero, assign, spaceAtLeastZero, // = or :=
		varValue, spaceAtLeastZero, end, spaceAtLeastZero), // value

	// Sample: alter account [if exists] acc1 suspend
	TypeSuspendAccount: fmt.Sprintf(`^%s(%s)%s(%s)%s(%s)?(%s)%s(%s)%s%s%s$`,
		spaceAtLeastZero, alter, spaceAtLeastOne,
		account, spaceAtLeastOne, ifExists,
		varName, spaceAtLeastOne,
		suspend, spaceAtLeastZero, end, spaceAtLeastZero),

	// Sample: drop account [if exists] acc1
	TypeDropAccount: fmt.Sprintf(`^%s(%s)%s(%s)%s(%s)?(%s)%s%s%s$`,
		spaceAtLeastZero, drop, spaceAtLeastOne,
		account, spaceAtLeastOne, ifExists,
		varName, spaceAtLeastZero, end, spaceAtLeastZero),
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
	dst io.Writer
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
		return nil, false
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
			return nil, false
		}
		switch typ {
		case TypeKillQuery:
			return makeKillQueryEvent(stmt, regexp.MustCompile(patternMap[typ])), true
		case TypeSetVar:
			// This event should be sent to dst, so return false,
			return makeSetVarEvent(stmt), false
		case TypeSuspendAccount:
			c, ok := req.dst.(net.Conn)
			if !ok {
				return nil, false
			}
			addr := c.RemoteAddr().String()
			if strings.Index(addr, "sock") > 0 && !strings.Contains(addr, "unix") {
				addr = "unix://" + addr
			}
			// Although the suspend statement could be handled directly, but
			// we need to know if the result of this statement is ok. So it
			// is handled in the event, and thus the second return value is true.
			return makeSuspendAccountEvent(stmt,
				addr, regexp.MustCompile(patternMap[typ])), true
		case TypeDropAccount:
			c, ok := req.dst.(net.Conn)
			if !ok {
				return nil, false
			}
			addr := c.RemoteAddr().String()
			if strings.Index(addr, "sock") > 0 && !strings.Contains(addr, "unix") {
				addr = "unix://" + addr
			}
			return makeDropAccountEvent(stmt,
				addr, regexp.MustCompile(patternMap[typ])), true
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
	// stmt is the statement that will be sent to server.
	stmt string
	// The ID of connection that needs to be killed.
	connID uint32
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

// suspendAccountEvent is the event that "alter account xxx suspend" statement
// is captured. We need to send this alter statement to event and execute it,
// and if the result is ok, then construct "kill connection" statement and send it
// to CN servers which have connections with the account.
type suspendAccountEvent struct {
	baseEvent
	// stmt is the statement that will be sent to server.
	stmt string
	// account is the tenant text value.
	account Tenant
	// addr is the remote server's address, which is used
	// to build connection to execute suspend statement.
	addr string
}

// makeSuspendAccountEvent creates a event with TypeSuspendAccount type.
func makeSuspendAccountEvent(stmt string, addr string, reg *regexp.Regexp) IEvent {
	items := reg.FindStringSubmatch(stmt)
	if len(items) != 5 {
		return nil
	}
	e := &suspendAccountEvent{
		stmt:    stmt,
		account: Tenant(items[3]),
		addr:    addr,
	}
	e.typ = TypeSuspendAccount
	return e
}

func (e *suspendAccountEvent) eventType() eventType {
	return TypeSuspendAccount
}

// dropAccountEvent is the event that "drop account xxx" statement
// is captured. The actions are the same as suspendAccountEvent.
type dropAccountEvent struct {
	baseEvent
	// stmt is the statement that will be sent to server.
	stmt string
	// account is the tenant text value.
	account Tenant
	// addr is the remote server's address, which is used
	// to build connection to execute drop statement.
	addr string
}

// makeDropAccountEvent creates a event with TypeDropAccount type.
func makeDropAccountEvent(stmt string, addr string, reg *regexp.Regexp) IEvent {
	items := reg.FindStringSubmatch(stmt)
	if len(items) != 4 {
		return nil
	}
	e := &dropAccountEvent{
		stmt:    stmt,
		account: Tenant(items[3]),
		addr:    addr,
	}
	e.typ = TypeDropAccount
	return e
}

func (e *dropAccountEvent) eventType() eventType {
	return TypeDropAccount
}
