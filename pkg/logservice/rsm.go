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
	"io"
	"sync"

	sm "github.com/lni/dragonboat/v3/statemachine"
)

var (
	binaryEnc = binary.BigEndian
)

const (
	headerSize = 2
)

const (
	leaseHolderIDTag  uint16 = 0xBF01
	truncatedIndexTag uint16 = 0xBF02
	userEntryTag      uint16 = 0xBF03
)

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
	return binaryEnc.Uint16(cmd) == userEntryTag
}

func tagMatch(cmd []byte, expectedTag uint16) bool {
	if len(cmd) != headerSize+8 {
		return false
	}
	return binaryEnc.Uint16(cmd) == expectedTag
}

type stateMachine struct {
	shardID        uint64
	replicaID      uint64
	mu             sync.RWMutex
	LeaseHolderID  uint64
	TruncatedIndex uint64
	LeaseHistory   map[uint64]uint64 // log index -> truncate index
}

var _ (sm.IConcurrentStateMachine) = (*stateMachine)(nil)

// making this a IConcurrentStateMachine for now, IStateMachine need to be updated
// to provide raft entry index to its Update() method
func newStateMachine(shardID uint64, replicaID uint64) sm.IConcurrentStateMachine {
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
	s.LeaseHolderID = binaryEnc.Uint64(cmd[headerSize:])
	s.LeaseHistory[index] = s.LeaseHolderID
}

func (s *stateMachine) truncateLeaseHistory(index uint64) {
	for key := range s.LeaseHistory {
		if key < index {
			delete(s.LeaseHistory, key)
		}
	}
}

func (s *stateMachine) setTruncatedIndex(cmd []byte) bool {
	if !isSetTruncatedIndexUpdate(cmd) {
		panic("not a setTruncatedIndex update")
	}
	index := binaryEnc.Uint64(cmd[headerSize:])
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
func (s *stateMachine) handleUserUpdate(cmd []byte) sm.Result {
	if !isUserUpdate(cmd) {
		panic("not user update")
	}
	if s.LeaseHolderID != binaryEnc.Uint64(cmd[headerSize:]) {
		return sm.Result{Value: s.LeaseHolderID}
	}
	return sm.Result{}
}

func (s *stateMachine) Close() error {
	return nil
}

func (s *stateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for idx, e := range entries {
		cmd := e.Cmd
		if isSetLeaseHolderUpdate(cmd) {
			s.setLeaseHolderID(e.Index, cmd)
			entries[idx].Result = sm.Result{}
		} else if isSetTruncatedIndexUpdate(cmd) {
			if s.setTruncatedIndex(cmd) {
				entries[idx].Result = sm.Result{}
			} else {
				entries[idx].Result = sm.Result{Value: s.TruncatedIndex}
			}
		} else if isUserUpdate(cmd) {
			entries[idx].Result = s.handleUserUpdate(cmd)
		} else {
			panic("corrupted entry")
		}
	}
	return entries, nil
}

func (s *stateMachine) Lookup(query interface{}) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := query.(uint16)
	if !ok {
		panic("unknown query type")
	}
	if v == leaseHolderIDTag {
		return s.LeaseHolderID, nil
	} else if v == truncatedIndexTag {
		return s.TruncatedIndex, nil
	}
	panic("unknown lookup command type")
}

func (s *stateMachine) PrepareSnapshot() (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v := &stateMachine{
		LeaseHolderID:  s.LeaseHolderID,
		TruncatedIndex: s.TruncatedIndex,
		LeaseHistory:   make(map[uint64]uint64),
	}
	for key, val := range s.LeaseHistory {
		v.LeaseHistory[key] = val
	}
	return v, nil
}

func (s *stateMachine) SaveSnapshot(sess interface{},
	w io.Writer, _ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	return gobMarshalTo(w, sess.(*stateMachine))
}

func (s *stateMachine) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, _ <-chan struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return gobUnmarshalFrom(r, s)
}
