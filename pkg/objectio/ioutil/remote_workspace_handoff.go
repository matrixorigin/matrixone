// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ioutil

import (
	"bytes"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// RemoteWorkspaceHandoffID identifies one object in one physical execution.
// The binary transaction ID is a string so the identity remains comparable.
type RemoteWorkspaceHandoffID struct {
	Version     uint32
	TxnID       string
	ExecutionID [16]byte
	FragmentID  [16]byte
	Attempt     uint32
	ObjectName  string
}

const RemoteWorkspaceHandoffVersion1 uint32 = 1

// RemoteWorkspaceHandoffScope is fixed for a physical remote fragment before
// its first object persistence. The transport supplies ProducerCN for routing.
type RemoteWorkspaceHandoffScope struct {
	Version     uint32
	TxnID       string
	ExecutionID [16]byte
	FragmentID  [16]byte
	Attempt     uint32
	ProducerCN  string
}

func (s RemoteWorkspaceHandoffScope) Object(name string) RemoteWorkspaceHandoffID {
	return RemoteWorkspaceHandoffID{
		Version: s.Version, TxnID: s.TxnID, ExecutionID: s.ExecutionID,
		FragmentID: s.FragmentID, Attempt: s.Attempt, ObjectName: name,
	}
}

// The envelope is attached only to transient DML result batches, never to an
// object or persisted workspace. Unknown or malformed envelopes cannot serve
// as proof of ownership transfer.
var remoteWorkspaceHandoffMagic = [...]byte{'M', 'O', 'W', 'S', 'H', 'O', '0', '1'}

func (s RemoteWorkspaceHandoffScope) Validate() error {
	if s.Version != RemoteWorkspaceHandoffVersion1 || len(s.TxnID) == 0 || len(s.TxnID) > 64 ||
		s.ExecutionID == [16]byte{} || s.FragmentID == [16]byte{} ||
		len(s.ProducerCN) == 0 || len(s.ProducerCN) > 256 {
		return moerr.NewInvalidInputNoCtx("invalid remote workspace handoff scope")
	}
	return nil
}

// MarshalRemoteWorkspaceHandoffScope encodes one producing fragment. The
// object name is taken from each result record, not from the batch boundary.
func MarshalRemoteWorkspaceHandoffScope(s RemoteWorkspaceHandoffScope) ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 8+4+1+len(s.TxnID)+16+16+4+2+len(s.ProducerCN))
	buf = append(buf, remoteWorkspaceHandoffMagic[:]...)
	buf = binary.BigEndian.AppendUint32(buf, s.Version)
	buf = append(buf, byte(len(s.TxnID)))
	buf = append(buf, s.TxnID...)
	buf = append(buf, s.ExecutionID[:]...)
	buf = append(buf, s.FragmentID[:]...)
	buf = binary.BigEndian.AppendUint32(buf, s.Attempt)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(s.ProducerCN)))
	buf = append(buf, s.ProducerCN...)
	return buf, nil
}

// ParseRemoteWorkspaceHandoffScope distinguishes absent metadata from a
// malformed envelope; callers requiring a negotiated handoff must reject both.
func ParseRemoteWorkspaceHandoffScope(buf []byte) (RemoteWorkspaceHandoffScope, bool, error) {
	var s RemoteWorkspaceHandoffScope
	if len(buf) < len(remoteWorkspaceHandoffMagic) || !bytes.Equal(buf[:len(remoteWorkspaceHandoffMagic)], remoteWorkspaceHandoffMagic[:]) {
		return s, false, nil
	}
	const fixed = 8 + 4 + 1 + 16 + 16 + 4 + 2
	if len(buf) < fixed || len(buf) > fixed+64+256 {
		return s, true, moerr.NewInvalidInputNoCtx("malformed remote workspace handoff envelope")
	}
	pos := 8
	s.Version = binary.BigEndian.Uint32(buf[pos:])
	pos += 4
	txnLen := int(buf[pos])
	pos++
	if txnLen == 0 || txnLen > 64 || len(buf)-pos < txnLen+16+16+4+2 {
		return s, true, moerr.NewInvalidInputNoCtx("malformed remote workspace handoff transaction")
	}
	s.TxnID = string(buf[pos : pos+txnLen])
	pos += txnLen
	copy(s.ExecutionID[:], buf[pos:pos+16])
	pos += 16
	copy(s.FragmentID[:], buf[pos:pos+16])
	pos += 16
	s.Attempt = binary.BigEndian.Uint32(buf[pos:])
	pos += 4
	addrLen := int(binary.BigEndian.Uint16(buf[pos:]))
	pos += 2
	if addrLen == 0 || addrLen > 256 || len(buf)-pos != addrLen {
		return s, true, moerr.NewInvalidInputNoCtx("malformed remote workspace handoff address")
	}
	s.ProducerCN = string(buf[pos:])
	if err := s.Validate(); err != nil {
		return RemoteWorkspaceHandoffScope{}, true, err
	}
	return s, true, nil
}

// RemoteWorkspaceHandoffDecision is authoritative only after the coordinator
// has either adopted the object or explicitly confirmed that it did not.
type RemoteWorkspaceHandoffDecision uint8

const (
	RemoteWorkspaceHandoffUnknown RemoteWorkspaceHandoffDecision = iota
	RemoteWorkspaceHandoffAdopted
	RemoteWorkspaceHandoffRejected
)

type RemoteWorkspaceHandoffResolver interface {
	ResolveRemoteWorkspaceHandoff(RemoteWorkspaceHandoffID, RemoteWorkspaceHandoffDecision) (bool, error)
	QueryRemoteWorkspaceHandoff(RemoteWorkspaceHandoffID) (RemoteWorkspaceHandoffDecision, error)
	QueryCoordinatorWorkspaceHandoff(RemoteWorkspaceHandoffID) (RemoteWorkspaceHandoffDecision, error)
}

// ExecuteRemoteWorkspaceHandoff is carried inside an existing query-service
// request understood by older CNs. Only an upgraded peer executes this body.
func ExecuteRemoteWorkspaceHandoff(data []byte, serverAddress string, resolver RemoteWorkspaceHandoffResolver) ([]byte, error) {
	if resolver == nil {
		return nil, moerr.NewNotSupportedNoCtx("remote workspace handoff is unavailable")
	}
	const maxRequestSize = 3 + 384 + 2 + RemoteWorkspaceHandoffDecisionBatchLimit*(2+512) + 1
	if len(data) == 0 || len(data) > maxRequestSize {
		return nil, moerr.NewInvalidInputNoCtx("invalid remote workspace handoff request size")
	}
	queryProducer := len(data) > 0 && data[0] == 'Q'
	queryCoordinator := len(data) > 0 && data[0] == 'C'
	if queryProducer || queryCoordinator {
		data = append([]byte(nil), data...)
		data[0] = 'B'
	}
	var scope RemoteWorkspaceHandoffScope
	var names []string
	var decision RemoteWorkspaceHandoffDecision
	var err error
	if len(data) > 0 && data[0] == 'B' {
		scope, names, decision, err = ParseRemoteWorkspaceHandoffDecisionBatch(data)
	} else {
		var name string
		scope, name, decision, err = ParseRemoteWorkspaceHandoffDecision(data)
		if err == nil {
			names = []string{name}
		}
	}
	if err != nil {
		return nil, err
	}
	if !queryCoordinator && scope.ProducerCN != serverAddress {
		return nil, moerr.NewInvalidInputNoCtx("workspace handoff targeted a different producer")
	}
	result := make([]byte, len(names))
	for i, name := range names {
		id := scope.Object(name)
		switch {
		case queryProducer:
			var state RemoteWorkspaceHandoffDecision
			state, err = resolver.QueryRemoteWorkspaceHandoff(id)
			result[i] = byte(state)
		case queryCoordinator:
			var state RemoteWorkspaceHandoffDecision
			state, err = resolver.QueryCoordinatorWorkspaceHandoff(id)
			result[i] = byte(state)
		default:
			var matched bool
			matched, err = resolver.ResolveRemoteWorkspaceHandoff(id, decision)
			if matched {
				result[i] = 1
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Limit each request independently of the 500,000-object ownership limit.
const RemoteWorkspaceHandoffDecisionBatchLimit = 1024

// MarshalRemoteWorkspaceHandoffDecisionBatch shares the fragment identity for
// a bounded set of independently settled object names.
func MarshalRemoteWorkspaceHandoffDecisionBatch(s RemoteWorkspaceHandoffScope, names []string, decision RemoteWorkspaceHandoffDecision) ([]byte, error) {
	if len(names) == 0 || len(names) > RemoteWorkspaceHandoffDecisionBatchLimit ||
		(decision != RemoteWorkspaceHandoffAdopted && decision != RemoteWorkspaceHandoffRejected) {
		return nil, moerr.NewInvalidInputNoCtx("invalid remote workspace handoff decision batch")
	}
	scope, err := MarshalRemoteWorkspaceHandoffScope(s)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 1+2+len(scope)+2+len(names)*64+1)
	buf = append(buf, 'B')
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(scope)))
	buf = append(buf, scope...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(names)))
	for _, name := range names {
		if len(name) == 0 || len(name) > 512 {
			return nil, moerr.NewInvalidInputNoCtx("invalid remote workspace handoff object name")
		}
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(name)))
		buf = append(buf, name...)
	}
	return append(buf, byte(decision)), nil
}

func ParseRemoteWorkspaceHandoffDecisionBatch(buf []byte) (RemoteWorkspaceHandoffScope, []string, RemoteWorkspaceHandoffDecision, error) {
	var zero RemoteWorkspaceHandoffScope
	invalid := func() (RemoteWorkspaceHandoffScope, []string, RemoteWorkspaceHandoffDecision, error) {
		return zero, nil, RemoteWorkspaceHandoffUnknown, moerr.NewInvalidInputNoCtx("malformed remote workspace handoff decision batch")
	}
	if len(buf) < 6 || buf[0] != 'B' || len(buf) > 3+384+2+RemoteWorkspaceHandoffDecisionBatchLimit*(2+512)+1 {
		return invalid()
	}
	scopeLen := int(binary.BigEndian.Uint16(buf[1:]))
	if scopeLen == 0 || len(buf)-3 < scopeLen+3 {
		return invalid()
	}
	scope, present, err := ParseRemoteWorkspaceHandoffScope(buf[3 : 3+scopeLen])
	if err != nil || !present {
		return invalid()
	}
	pos := 3 + scopeLen
	count := int(binary.BigEndian.Uint16(buf[pos:]))
	pos += 2
	if count == 0 || count > RemoteWorkspaceHandoffDecisionBatchLimit {
		return invalid()
	}
	names := make([]string, 0, count)
	for range count {
		if len(buf)-pos < 3 {
			return invalid()
		}
		n := int(binary.BigEndian.Uint16(buf[pos:]))
		pos += 2
		if n == 0 || n > 512 || len(buf)-pos < n+1 {
			return invalid()
		}
		names = append(names, string(buf[pos:pos+n]))
		pos += n
	}
	if len(buf)-pos != 1 {
		return invalid()
	}
	decision := RemoteWorkspaceHandoffDecision(buf[pos])
	if decision != RemoteWorkspaceHandoffAdopted && decision != RemoteWorkspaceHandoffRejected {
		return invalid()
	}
	return scope, names, decision, nil
}

// MarshalRemoteWorkspaceHandoffDecision carries one object decision. The
// coordinator sends it only after it knows whether that object was adopted.
func MarshalRemoteWorkspaceHandoffDecision(s RemoteWorkspaceHandoffScope, name string, decision RemoteWorkspaceHandoffDecision) ([]byte, error) {
	if len(name) == 0 || len(name) > 512 || (decision != RemoteWorkspaceHandoffAdopted && decision != RemoteWorkspaceHandoffRejected) {
		return nil, moerr.NewInvalidInputNoCtx("invalid remote workspace handoff decision")
	}
	scope, err := MarshalRemoteWorkspaceHandoffScope(s)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 0, 2+len(scope)+2+len(name)+1)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(scope)))
	buf = append(buf, scope...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(name)))
	buf = append(buf, name...)
	return append(buf, byte(decision)), nil
}

func ParseRemoteWorkspaceHandoffDecision(buf []byte) (RemoteWorkspaceHandoffScope, string, RemoteWorkspaceHandoffDecision, error) {
	var zero RemoteWorkspaceHandoffScope
	invalid := func() (RemoteWorkspaceHandoffScope, string, RemoteWorkspaceHandoffDecision, error) {
		return zero, "", RemoteWorkspaceHandoffUnknown, moerr.NewInvalidInputNoCtx("malformed remote workspace handoff decision")
	}
	if len(buf) < 2 {
		return invalid()
	}
	scopeLen := int(binary.BigEndian.Uint16(buf))
	if scopeLen == 0 || len(buf)-2 < scopeLen+3 {
		return invalid()
	}
	scope, present, err := ParseRemoteWorkspaceHandoffScope(buf[2 : 2+scopeLen])
	if err != nil || !present {
		return invalid()
	}
	pos := 2 + scopeLen
	nameLen := int(binary.BigEndian.Uint16(buf[pos:]))
	pos += 2
	if nameLen == 0 || nameLen > 512 || len(buf)-pos != nameLen+1 {
		return invalid()
	}
	decision := RemoteWorkspaceHandoffDecision(buf[len(buf)-1])
	if decision != RemoteWorkspaceHandoffAdopted && decision != RemoteWorkspaceHandoffRejected {
		return invalid()
	}
	return scope, string(buf[pos : pos+nameLen]), decision, nil
}
