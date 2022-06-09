// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"encoding/binary"
	"encoding/gob"
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"
)

var (
	binaryEnc = binary.BigEndian
)

const (
	headerSize = 2
)

const (
	leaseHolderIDTag uint16 = iota + 0xBF01
	truncatedIndexTag
	userEntryTag
	indexTag
)

type leaseHistoryQuery struct {
	index uint64
}

func parseCmdTag(cmd []byte) uint16 {
	return binaryEnc.Uint16(cmd)
}

func parseTruncatedIndex(cmd []byte) uint64 {
	return binaryEnc.Uint64(cmd[headerSize:])
}

func parseLeaseHolderID(cmd []byte) uint64 {
	return binaryEnc.Uint64(cmd[headerSize:])
}

func getSetLeaseHolderCmd(leaseHolderID uint64) []byte {
	cmd := make([]byte, headerSize+8)
	binaryEnc.PutUint16(cmd, leaseHolderIDTag)
	binaryEnc.PutUint64(cmd[headerSize:], leaseHolderID)
	return cmd
}

func getSetTruncatedIndexCmd(index uint64) []byte {
	cmd := make([]byte, headerSize+8)
	binaryEnc.PutUint16(cmd, truncatedIndexTag)
	binaryEnc.PutUint64(cmd[headerSize:], index)
	return cmd
}

func isSetLeaseHolderUpdate(cmd []byte) bool {
	return tagMatch(cmd, leaseHolderIDTag)
}

func isSetTruncatedIndexUpdate(cmd []byte) bool {
	return tagMatch(cmd, truncatedIndexTag)
}

func isUserUpdate(cmd []byte) bool {
	if len(cmd) < headerSize+8 {
		return false
	}
	return parseCmdTag(cmd) == userEntryTag
}

func tagMatch(cmd []byte, expectedTag uint16) bool {
	if len(cmd) != headerSize+8 {
		return false
	}
	return parseCmdTag(cmd) == expectedTag
}

type stateMachine struct {
	shardID        uint64
	replicaID      uint64
	Index          uint64
	LeaseHolderID  uint64
	TruncatedIndex uint64
	LeaseHistory   map[uint64]uint64 // log index -> truncate index
}

var _ (sm.IStateMachine) = (*stateMachine)(nil)

// making this a IConcurrentStateMachine for now, IStateMachine need to be updated
// to provide raft entry index to its Update() method
func newStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	return &stateMachine{
		shardID:      shardID,
		replicaID:    replicaID,
		LeaseHistory: make(map[uint64]uint64),
	}
}

func (s *stateMachine) setLeaseHolderID(index uint64, cmd []byte) {
	if !isSetLeaseHolderUpdate(cmd) {
		panic("not a setLeaseHolder update")
	}
	s.LeaseHolderID = parseLeaseHolderID(cmd)
	s.LeaseHistory[index] = s.LeaseHolderID
}

func (s *stateMachine) truncateLeaseHistory(index uint64) {
	_, index = s.getLeaseHistory(index)
	for key := range s.LeaseHistory {
		if key < index {
			delete(s.LeaseHistory, key)
		}
	}
}

func (s *stateMachine) getLeaseHistory(index uint64) (uint64, uint64) {
	max := uint64(0)
	lease := uint64(0)
	for key, val := range s.LeaseHistory {
		if key >= index {
			continue
		}
		if key > max {
			max = key
			lease = val
		}
	}
	return lease, max
}

func (s *stateMachine) setTruncatedIndex(cmd []byte) bool {
	if !isSetTruncatedIndexUpdate(cmd) {
		panic("not a setTruncatedIndex update")
	}
	index := parseTruncatedIndex(cmd)
	if index > s.TruncatedIndex {
		s.TruncatedIndex = index
		s.truncateLeaseHistory(index)
		return true
	}
	return false
}

// handleUserUpdate returns an empty sm.Result on success or it returns a
// sm.Result value with the Value field set to the current lease holder ID
// to indicate rejection by mismatched lease holder ID.
func (s *stateMachine) handleUserUpdate(index uint64, cmd []byte) sm.Result {
	if !isUserUpdate(cmd) {
		panic("not user update")
	}
	if s.LeaseHolderID != parseLeaseHolderID(cmd) {
		data := make([]byte, 8)
		binaryEnc.PutUint64(data, s.LeaseHolderID)
		return sm.Result{Data: data}
	}
	return sm.Result{Value: index}
}

func (s *stateMachine) Close() error {
	return nil
}

func (s *stateMachine) Update(e sm.Entry) (sm.Result, error) {
	cmd := e.Cmd
	s.Index = e.Index
	if isSetLeaseHolderUpdate(cmd) {
		s.setLeaseHolderID(e.Index, cmd)
		return sm.Result{}, nil
	} else if isSetTruncatedIndexUpdate(cmd) {
		if s.setTruncatedIndex(cmd) {
			return sm.Result{}, nil
		} else {
			return sm.Result{Value: s.TruncatedIndex}, nil
		}
	} else if isUserUpdate(cmd) {
		return s.handleUserUpdate(e.Index, cmd), nil
	}
	panic("corrupted entry")
}

func (s *stateMachine) Lookup(query interface{}) (interface{}, error) {
	if v, ok := query.(uint16); ok {
		if v == indexTag {
			return s.Index, nil
		} else if v == leaseHolderIDTag {
			return s.LeaseHolderID, nil
		} else if v == truncatedIndexTag {
			return s.TruncatedIndex, nil
		} else {
			panic("unknown lookup command type")
		}
	}
	if v, ok := query.(leaseHistoryQuery); ok {
		lease, _ := s.getLeaseHistory(v.index)
		return lease, nil
	}
	panic("unknown lookup command type")
}

func (s *stateMachine) SaveSnapshot(w io.Writer,
	_ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	enc := gob.NewEncoder(w)
	return enc.Encode(s)
}

func (s *stateMachine) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, _ <-chan struct{}) error {
	dec := gob.NewDecoder(r)
	return dec.Decode(s)
}
