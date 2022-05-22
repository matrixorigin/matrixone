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

type header struct {
	tag uint16
}

func isSetLeaseHolderUpdate(cmd []byte) bool {
	return tagMatch(cmd, leaseHolderIDTag)
}

func isSetTruncatedIndexUpdate(cmd []byte) bool {
	return tagMatch(cmd, truncatedIndexTag)
}

func isUserUpdate(cmd []byte) bool {
	if len(cmd) < headerSize {
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
	leaseHolderID  uint64
	truncatedIndex uint64
}

var _ (sm.IStateMachine) = (*stateMachine)(nil)

func (s *stateMachine) setLeaseHolderID(cmd []byte) {
	if !isSetLeaseHolderUpdate(cmd) {
		panic("not a setLeaseHolder update")
	}
	s.leaseHolderID = binaryEnc.Uint64(cmd[headerSize:])
}

func (s *stateMachine) setTruncatedIndex(cmd []byte) {
	if !isSetTruncatedIndexUpdate(cmd) {
		panic("not a setTruncatedIndex update")
	}
	s.truncatedIndex = binaryEnc.Uint64(cmd[headerSize:])
}

// handleUserUpdate returns an empty sm.Result on success or it returns a
// sm.Result value with the Value field set to the current lease holder ID
// to indicate rejection by mismatched lease holder ID.
func (s *stateMachine) handleUserUpdate(cmd []byte) (sm.Result, error) {
	if !isUserUpdate(cmd) {
		panic("not user update")
	}
	if s.leaseHolderID != binaryEnc.Uint64(cmd[headerSize:]) {
		return sm.Result{Value: s.leaseHolderID}, nil
	}
	return sm.Result{}, nil
}

func (s *stateMachine) Close() error {
	return nil
}

func (s *stateMachine) Update(cmd []byte) (sm.Result, error) {
	if isSetLeaseHolderUpdate(cmd) {
		s.setLeaseHolderID(cmd)
	} else if isSetTruncatedIndexUpdate(cmd) {
		s.setTruncatedIndex(cmd)
	} else if isUserUpdate(cmd) {
		return s.handleUserUpdate(cmd)
	} else {
		panic("corrupted entry")
	}
	return sm.Result{}, nil
}

func (s *stateMachine) Lookup(query interface{}) (interface{}, error) {
	v, ok := query.(uint16)
	if !ok {
		panic("unknown query type")
	}
	if v == leaseHolderIDTag {
		return s.leaseHolderID, nil
	} else if v == truncatedIndexTag {
		return s.truncatedIndex, nil
	}
	panic("unknown lookup command type")
}

func (s *stateMachine) SaveSnapshot(w io.Writer,
	_ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	ss := make([]byte, 16)
	binaryEnc.PutUint64(ss, s.leaseHolderID)
	binaryEnc.PutUint64(ss, s.truncatedIndex)
	if _, err := w.Write(ss); err != nil {
		return err
	}
	return nil
}

func (s *stateMachine) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, _ <-chan struct{}) error {
	ss := make([]byte, 16)
	if _, err := io.ReadFull(r, ss); err != nil {
		return err
	}
	s.leaseHolderID = binaryEnc.Uint64(ss)
	s.truncatedIndex = binaryEnc.Uint64(ss[8:])
	return nil
}
