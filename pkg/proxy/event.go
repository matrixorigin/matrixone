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
	"regexp"
	"strconv"
)

// eventType alias uint8, which indicates the type of event.
type eventType uint8

// String returns the string of event type.
func (t eventType) String() string {
	switch t {
	case TypeKillQuery:
		return "KillQuery"
	}
	return "Unknown"
}

const (
	// TypeMin is the minimal event type.
	TypeMin eventType = 0
	// TypeKillQuery indicates the kill query statement.
	TypeKillQuery eventType = 1
)

// patterMap is a map eventType => pattern string
var patternMap = map[eventType]string{
	TypeKillQuery: `^([kK][iI][lL][lL])\s+([qQ][uU][eE][rR][yY])\s+(\d+)$`,
}

// IEventReq is the event request interface.
type IEventReq interface {
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
func sendReq(e IEventReq, c chan<- IEventReq) {
	c <- e
}

// sendResp receives an event response from the channel.
func sendResp(r []byte, c chan<- []byte) {
	c <- r
}

// makeEvent parses a event from message bytes. If we got no
// supported event, just return nil.
func makeEvent(msg []byte) IEventReq {
	if len(msg) < preRecvLen {
		return nil
	}
	if msg[4] == byte(cmdQuery) {
		stmt := getStatement(msg)
		// Get the event type.
		var typ eventType
		for typ = range patternMap {
			matched, err := regexp.MatchString(patternMap[typ], stmt)
			if err == nil && matched {
				break
			}
		}
		switch typ {
		case TypeKillQuery:
			return makeKillQueryEvent(stmt, regexp.MustCompile(patternMap[TypeKillQuery]))
		default:
			return nil
		}
	}
	return nil
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
func makeKillQueryEvent(stmt string, reg *regexp.Regexp) IEventReq {
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
func (ke *killQueryEvent) eventType() eventType {
	return TypeKillQuery
}
