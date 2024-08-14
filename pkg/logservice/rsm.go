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
	"fmt"
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// XXX WHY BIG ENDIAN?
var (
	binaryEnc = binary.BigEndian
)

const (
	firstLogShardID uint64 = 1
	headerSize             = pb.HeaderSize
)

// used to indicate query types
type leaseHolderIDQuery struct{}
type indexQuery struct{}
type truncatedLsnQuery struct{}
type leaseHistoryQuery struct{ lsn uint64 }
type requiredLsnQuery struct{}

func getAppendCmd(cmd []byte, replicaID uint64) []byte {
	if len(cmd) < headerSize+8 {
		panic("cmd too small")
	}
	binaryEnc.PutUint32(cmd, uint32(pb.UserEntryUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], replicaID)
	return cmd
}

func parseCmdTag(cmd []byte) pb.UpdateType {
	return pb.UpdateType(binaryEnc.Uint32(cmd))
}

func parseTruncatedLsn(cmd []byte) uint64 {
	return binaryEnc.Uint64(cmd[headerSize:])
}

func parseLeaseHolderID(cmd []byte) uint64 {
	return binaryEnc.Uint64(cmd[headerSize:])
}

func parseTsoUpdateCmd(cmd []byte) uint64 {
	return binaryEnc.Uint64(cmd[headerSize:])
}

func parseRequiredLsn(cmd []byte) uint64 {
	return binaryEnc.Uint64(cmd[headerSize:])
}

func getSetLeaseHolderCmd(leaseHolderID uint64) []byte {
	cmd := make([]byte, headerSize+8)
	binaryEnc.PutUint32(cmd, uint32(pb.LeaseHolderIDUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], leaseHolderID)
	return cmd
}

func getSetTruncatedLsnCmd(lsn uint64) []byte {
	cmd := make([]byte, headerSize+8)
	binaryEnc.PutUint32(cmd, uint32(pb.TruncateLSNUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], lsn)
	return cmd
}

func getTsoUpdateCmd(count uint64) []byte {
	cmd := make([]byte, headerSize+8)
	binaryEnc.PutUint32(cmd, uint32(pb.TSOUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], count)
	return cmd
}

func getSetRequiredLsnCmd(lsn uint64) []byte {
	cmd := make([]byte, headerSize+8)
	binaryEnc.PutUint32(cmd, uint32(pb.RequiredLSNUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], lsn)
	return cmd
}

type stateMachine struct {
	shardID   uint64
	replicaID uint64
	state     pb.RSMState
}

var _ sm.IStateMachine = (*stateMachine)(nil)

func newStateMachine(shardID uint64, replicaID uint64) sm.IStateMachine {
	state := pb.RSMState{
		Tso:          1,
		LeaseHistory: make(map[uint64]uint64),
	}
	return &stateMachine{
		shardID:   shardID,
		replicaID: replicaID,
		state:     state,
	}
}

func (s *stateMachine) truncateLeaseHistory(lsn uint64) {
	_, lsn = s.getLeaseHistory(lsn)
	for key := range s.state.LeaseHistory {
		if key < lsn {
			delete(s.state.LeaseHistory, key)
		}
	}
}

func (s *stateMachine) getLeaseHistory(lsn uint64) (uint64, uint64) {
	max := uint64(0)
	lease := uint64(0)
	for key, val := range s.state.LeaseHistory {
		if key >= lsn {
			continue
		}
		if key > max {
			max = key
			lease = val
		}
	}
	return lease, max
}

func (s *stateMachine) handleSetLeaseHolderID(cmd []byte) sm.Result {
	s.state.LeaseHolderID = parseLeaseHolderID(cmd)
	s.state.LeaseHistory[s.state.Index] = s.state.LeaseHolderID
	return sm.Result{}
}

func (s *stateMachine) handleTruncateLsn(cmd []byte) sm.Result {
	lsn := parseTruncatedLsn(cmd)
	if lsn > s.state.TruncatedLsn {
		s.state.TruncatedLsn = lsn
		s.truncateLeaseHistory(lsn)
		return sm.Result{}
	}
	return sm.Result{Value: s.state.TruncatedLsn}
}

// handleUserUpdate returns an empty sm.Result on success or it returns a
// sm.Result value with the Value field set to the current leaseholder ID
// to indicate rejection by mismatched leaseholder ID.
func (s *stateMachine) handleUserUpdate(cmd []byte) sm.Result {
	// if the leaseholder is reserved, skip the check, as it is the RSM of standby shard.
	if s.state.LeaseHolderID != 0 && s.state.LeaseHolderID != parseLeaseHolderID(cmd) {
		data := make([]byte, 8)
		binaryEnc.PutUint64(data, s.state.LeaseHolderID)
		return sm.Result{Data: data}
	}
	return sm.Result{Value: s.state.Index}
}

func (s *stateMachine) handleTsoUpdate(cmd []byte) sm.Result {
	count := parseTsoUpdateCmd(cmd)
	result := sm.Result{Value: s.state.Tso}
	s.state.Tso += count
	return result
}

func (s *stateMachine) handleSetRequiredLsn(cmd []byte) sm.Result {
	lsn := parseRequiredLsn(cmd)
	if lsn > s.state.RequiredLsn {
		if lsn <= s.state.Index {
			s.state.RequiredLsn = lsn
		} else {
			s.state.RequiredLsn = s.state.Index
		}
		return sm.Result{}
	}
	return sm.Result{Value: s.state.RequiredLsn}
}

func (s *stateMachine) Close() error {
	return nil
}

func (s *stateMachine) Update(e sm.Entry) (sm.Result, error) {
	cmd := e.Cmd
	s.state.Index = e.Index

	switch parseCmdTag(cmd) {
	case pb.LeaseHolderIDUpdate:
		return s.handleSetLeaseHolderID(cmd), nil
	case pb.TruncateLSNUpdate:
		return s.handleTruncateLsn(cmd), nil
	case pb.UserEntryUpdate:
		return s.handleUserUpdate(cmd), nil
	case pb.TSOUpdate:
		return s.handleTsoUpdate(cmd), nil
	case pb.RequiredLSNUpdate:
		return s.handleSetRequiredLsn(cmd), nil
	default:
		panic(fmt.Sprintf("unknown entry type %d", parseCmdTag(cmd)))
	}
}

func (s *stateMachine) Lookup(query interface{}) (interface{}, error) {
	if _, ok := query.(indexQuery); ok {
		return s.state.Index, nil
	} else if _, ok := query.(leaseHolderIDQuery); ok {
		return s.state.LeaseHolderID, nil
	} else if _, ok := query.(truncatedLsnQuery); ok {
		return s.state.TruncatedLsn, nil
	} else if v, ok := query.(leaseHistoryQuery); ok {
		lease, _ := s.getLeaseHistory(v.lsn)
		return lease, nil
	} else if _, ok := query.(requiredLsnQuery); ok {
		return s.state.RequiredLsn, nil
	}
	panic("unknown lookup command type")
}

func (s *stateMachine) SaveSnapshot(w io.Writer,
	_ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	// FIXME: memory recycling when necessary
	data := make([]byte, s.state.ProtoSize())
	n, err := s.state.MarshalToSizedBuffer(data)
	if err != nil {
		return err
	}
	_, err = w.Write(data[:n])
	return err
}

func (s *stateMachine) RecoverFromSnapshot(r io.Reader,
	_ []sm.SnapshotFile, _ <-chan struct{}) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return s.state.Unmarshal(data)
}
