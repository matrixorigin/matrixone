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
	"math"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// XXX WHY BIG ENDIAN?
var (
	binaryEnc = binary.BigEndian
)

const (
	firstLogShardID uint64 = 1
	headerSize             = pb.HeaderSize

	walRecoveryLeaseHolderID uint64 = math.MaxUint64
	walRecoveryBeginMarker   uint64 = 0x4d4f57414c424547 // "MOWALBEG"
	walRecoveryEndMarker     uint64 = 0x4d4f57414c454e44 // "MOWALEND"
	walRecoveryDigestSize           = 64
	walRecoveryControlSize          = headerSize + 16 + walRecoveryDigestSize + 8
)

// used to indicate query types
type leaseHolderIDQuery struct{}
type indexQuery struct{}
type truncatedLsnQuery struct{}
type leaseHistoryQuery struct{ lsn uint64 }
type requiredLsnQuery struct{}
type requiredLsnStateQuery struct{}
type walTruncateStateQuery struct{}

type requiredLsnState struct {
	lsn      uint64
	revision uint64
	owner    uint64
	term     uint64
	leaderID uint64
}

type walTruncateState struct {
	psn uint64
	dsn uint64
}
type walRecoveryStateQuery struct{}

type walRecoveryMarkerState struct {
	Digest                string
	EntryCount            uint64
	CompletedEntries      uint64
	BaseLSN               uint64
	LastLSN               uint64
	OriginalLeaseHolderID uint64
	Complete              bool
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

func getWALRecoveryLeaseCmd(leaseHolderID, marker uint64, digest string, entryCount uint64) []byte {
	if len(digest) != walRecoveryDigestSize {
		panic("invalid WAL recovery digest")
	}
	cmd := make([]byte, walRecoveryControlSize)
	binaryEnc.PutUint32(cmd, uint32(pb.LeaseHolderIDUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], leaseHolderID)
	binaryEnc.PutUint64(cmd[headerSize+8:], marker)
	copy(cmd[headerSize+16:], digest)
	binaryEnc.PutUint64(cmd[headerSize+16+walRecoveryDigestSize:], entryCount)
	return cmd
}

func getBeginWALRecoveryCmd(digest string, entryCount uint64) []byte {
	return getWALRecoveryLeaseCmd(
		walRecoveryLeaseHolderID, walRecoveryBeginMarker, digest, entryCount)
}

func getEndWALRecoveryCmd(digest string, entryCount uint64) []byte {
	return getWALRecoveryLeaseCmd(0, walRecoveryEndMarker, digest, entryCount)
}

func isWALRecoveryLeaseCmd(cmd []byte, marker uint64) bool {
	return len(cmd) == walRecoveryControlSize &&
		parseCmdTag(cmd) == pb.LeaseHolderIDUpdate &&
		binaryEnc.Uint64(cmd[headerSize+8:]) == marker
}

func parseWALRecoveryLeaseCmd(cmd []byte) (string, uint64) {
	digest := string(cmd[headerSize+16 : headerSize+16+walRecoveryDigestSize])
	entryCount := binaryEnc.Uint64(cmd[headerSize+16+walRecoveryDigestSize:])
	return digest, entryCount
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

func getSetRequiredLsnIfCurrentCmd(lsn, expectedLsn, revision, owner uint64) []byte {
	cmd := make([]byte, headerSize+32)
	binaryEnc.PutUint32(cmd, uint32(pb.RequiredLSNConditionalUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], lsn)
	binaryEnc.PutUint64(cmd[headerSize+8:], expectedLsn)
	binaryEnc.PutUint64(cmd[headerSize+16:], revision)
	binaryEnc.PutUint64(cmd[headerSize+24:], owner)
	return cmd
}

func getClaimRequiredLsnCmd(expectedLsn, revision, owner, leaderTerm, leaderID uint64) []byte {
	cmd := make([]byte, headerSize+40)
	binaryEnc.PutUint32(cmd, uint32(pb.RequiredLSNClaimUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], expectedLsn)
	binaryEnc.PutUint64(cmd[headerSize+8:], revision)
	binaryEnc.PutUint64(cmd[headerSize+16:], owner)
	binaryEnc.PutUint64(cmd[headerSize+24:], leaderTerm)
	binaryEnc.PutUint64(cmd[headerSize+32:], leaderID)
	return cmd
}

func getWALTruncateCmd(psn, checkpointedDSN uint64) []byte {
	cmd := make([]byte, headerSize+16)
	binaryEnc.PutUint32(cmd, uint32(pb.WALTruncateUpdate))
	binaryEnc.PutUint64(cmd[headerSize:], psn)
	binaryEnc.PutUint64(cmd[headerSize+8:], checkpointedDSN)
	return cmd
}

type stateMachine struct {
	shardID   uint64
	replicaID uint64
	state     pb.RSMState
	entryTerm func(uint64) (uint64, error)
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
	leaseHolderID := parseLeaseHolderID(cmd)
	if isWALRecoveryLeaseCmd(cmd, walRecoveryBeginMarker) {
		digest, entryCount := parseWALRecoveryLeaseCmd(cmd)
		if s.state.WALRecoveryDigest != "" &&
			(s.state.WALRecoveryDigest != digest ||
				s.state.WALRecoveryEntryCount != entryCount) {
			return rejectedLeaseHolderResult(s.state.LeaseHolderID)
		}
		if s.state.WALRecoveryComplete {
			return sm.Result{Value: s.state.Index}
		}
		if s.state.WALRecoveryDigest == "" {
			s.state.WALRecoveryDigest = digest
			s.state.WALRecoveryEntryCount = entryCount
			s.state.WALRecoveryCompletedEntries = 0
			s.state.WALRecoveryBaseLsn = s.state.Index
			s.state.WALRecoveryLastLsn = s.state.Index
			s.state.WALRecoveryOriginalLeaseHolderID = s.state.LeaseHolderID
		}
		if s.state.LeaseHolderID != walRecoveryLeaseHolderID {
			s.state.LeaseHolderID = walRecoveryLeaseHolderID
			s.state.LeaseHistory[s.state.Index] = s.state.LeaseHolderID
		}
		return sm.Result{Value: s.state.Index}
	}
	if isWALRecoveryLeaseCmd(cmd, walRecoveryEndMarker) {
		digest, entryCount := parseWALRecoveryLeaseCmd(cmd)
		if s.state.WALRecoveryDigest != digest ||
			s.state.WALRecoveryEntryCount != entryCount ||
			s.state.WALRecoveryCompletedEntries != entryCount {
			return rejectedLeaseHolderResult(s.state.LeaseHolderID)
		}
		if s.state.WALRecoveryComplete {
			return sm.Result{Value: s.state.Index}
		}
		if s.state.LeaseHolderID != walRecoveryLeaseHolderID {
			return rejectedLeaseHolderResult(s.state.LeaseHolderID)
		}
		s.state.WALRecoveryComplete = true
		if s.state.LeaseHolderID != s.state.WALRecoveryOriginalLeaseHolderID {
			s.state.LeaseHolderID = s.state.WALRecoveryOriginalLeaseHolderID
			s.state.LeaseHistory[s.state.Index] = s.state.LeaseHolderID
		}
		return sm.Result{Value: s.state.Index}
	}
	// MaxUint64 is reserved for the durable WAL recovery fence. A normal
	// lease update must neither enter nor leave recovery mode.
	if leaseHolderID == walRecoveryLeaseHolderID ||
		s.state.LeaseHolderID == walRecoveryLeaseHolderID {
		return rejectedLeaseHolderResult(s.state.LeaseHolderID)
	}
	s.state.LeaseHolderID = leaseHolderID
	s.state.LeaseHistory[s.state.Index] = s.state.LeaseHolderID
	return sm.Result{}
}

func rejectedLeaseHolderResult(leaseHolderID uint64) sm.Result {
	data := make([]byte, 8)
	binaryEnc.PutUint64(data, leaseHolderID)
	return sm.Result{Data: data}
}

func (s *stateMachine) handleTruncateLsn(cmd []byte) sm.Result {
	lsn := parseTruncatedLsn(cmd)
	// Once a WAL shard uses the checkpoint boundary, a legacy truncation
	// cannot advance its physical prefix without the matching DSN proof.
	if s.state.CheckpointedDSN != 0 {
		return sm.Result{Value: s.state.TruncatedLsn}
	}
	// The truncate command itself occupies s.state.Index. Only entries that
	// preceded it can be truncated; otherwise later acknowledged appends can
	// fall below the persisted truncation watermark.
	if lsn >= s.state.Index {
		return sm.Result{Value: s.state.Index}
	}
	if lsn > s.state.TruncatedLsn {
		s.state.TruncatedLsn = lsn
		s.truncateLeaseHistory(lsn)
		return sm.Result{}
	}
	return sm.Result{Value: s.state.TruncatedLsn}
}

func (s *stateMachine) handleWALTruncate(cmd []byte) sm.Result {
	psn := binaryEnc.Uint64(cmd[headerSize:])
	dsn := binaryEnc.Uint64(cmd[headerSize+8:])
	if psn == 0 || dsn == 0 || psn <= s.state.TruncatedLsn || dsn < s.state.CheckpointedDSN {
		return sm.Result{Value: max(uint64(1), s.state.TruncatedLsn)}
	}
	// The new truncate command itself must remain above the truncated prefix.
	if psn >= s.state.Index {
		return sm.Result{Value: s.state.Index}
	}
	s.state.TruncatedLsn = psn
	s.state.CheckpointedDSN = dsn
	s.truncateLeaseHistory(psn)
	return sm.Result{}
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
	if s.state.LeaseHolderID == walRecoveryLeaseHolderID {
		if s.state.WALRecoveryDigest == "" ||
			s.state.WALRecoveryComplete ||
			s.state.WALRecoveryCompletedEntries >= s.state.WALRecoveryEntryCount {
			return rejectedLeaseHolderResult(s.state.LeaseHolderID)
		}
		s.state.WALRecoveryCompletedEntries++
		s.state.WALRecoveryLastLsn = s.state.Index
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
	if s.state.RequiredLsnConditional {
		return sm.Result{Value: s.state.RequiredLsn, Data: []byte{1}}
	}
	lsn := parseRequiredLsn(cmd)
	if lsn >= s.state.RequiredLsn {
		s.state.RequiredLsn = lsn
		// Legacy snapshots do not contain a revision. Derive it from the
		// retained LSN so replicas restoring at different snapshot indices
		// agree after replaying the same legacy entries.
		s.state.RequiredLsnRevision = lsn
		return sm.Result{}
	}
	return sm.Result{Value: s.state.RequiredLsn}
}

func (s *stateMachine) handleSetRequiredLsnIfCurrent(cmd []byte) sm.Result {
	lsn := binaryEnc.Uint64(cmd[headerSize:])
	expectedLsn := binaryEnc.Uint64(cmd[headerSize+8:])
	revision := binaryEnc.Uint64(cmd[headerSize+16:])
	owner := binaryEnc.Uint64(cmd[headerSize+24:])
	if s.state.RequiredLsn != expectedLsn || s.state.RequiredLsnRevision != revision ||
		owner == 0 || s.state.RequiredLsnOwner != owner ||
		(lsn != 0 && lsn < expectedLsn) {
		return sm.Result{Value: 1}
	}
	s.state.RequiredLsn = lsn
	s.state.RequiredLsnRevision++
	s.state.RequiredLsnConditional = true
	return sm.Result{}
}

func (s *stateMachine) handleClaimRequiredLsn(cmd []byte) sm.Result {
	expectedLsn := binaryEnc.Uint64(cmd[headerSize:])
	revision := binaryEnc.Uint64(cmd[headerSize+8:])
	owner := binaryEnc.Uint64(cmd[headerSize+16:])
	leaderTerm := binaryEnc.Uint64(cmd[headerSize+24:])
	leaderID := binaryEnc.Uint64(cmd[headerSize+32:])
	if owner == 0 || s.state.RequiredLsn != expectedLsn ||
		s.state.RequiredLsnRevision != revision ||
		leaderTerm == 0 || leaderID == 0 ||
		leaderTerm < s.state.RequiredLsnLeaderTerm ||
		(leaderTerm == s.state.RequiredLsnLeaderTerm &&
			s.state.RequiredLsnLeaderID != 0 && leaderID != s.state.RequiredLsnLeaderID) {
		return sm.Result{Value: 1}
	}
	s.state.RequiredLsnOwner = owner
	s.state.RequiredLsnRevision++
	s.state.RequiredLsnLeaderTerm = leaderTerm
	s.state.RequiredLsnLeaderID = leaderID
	s.state.RequiredLsnConditional = true
	return sm.Result{}
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
	case pb.RequiredLSNConditionalUpdate:
		matches, err := s.hasEntryTerm(e.Index, s.state.RequiredLsnLeaderTerm)
		if err != nil {
			return sm.Result{}, err
		}
		if !matches {
			return sm.Result{Value: 1}, nil
		}
		return s.handleSetRequiredLsnIfCurrent(cmd), nil
	case pb.WALTruncateUpdate:
		return s.handleWALTruncate(cmd), nil
	case pb.RequiredLSNClaimUpdate:
		matches, err := s.hasEntryTerm(e.Index, binaryEnc.Uint64(cmd[headerSize+24:]))
		if err != nil {
			return sm.Result{}, err
		}
		if !matches {
			return sm.Result{Value: 1}, nil
		}
		return s.handleClaimRequiredLsn(cmd), nil
	default:
		panic(fmt.Sprintf("unknown entry type %d", parseCmdTag(cmd)))
	}
}

func (s *stateMachine) hasEntryTerm(index, requiredTerm uint64) (bool, error) {
	if s.entryTerm == nil {
		return false, moerr.NewInternalErrorNoCtxf("missing Raft entry term reader for shard %d", s.shardID)
	}
	actualTerm, err := s.entryTerm(index)
	if err != nil {
		return false, moerr.NewInternalErrorNoCtxf("cannot read Raft entry term for shard %d index %d: %v",
			s.shardID, index, err)
	}
	if actualTerm == 0 {
		return false, moerr.NewInternalErrorNoCtxf("missing Raft entry term for shard %d index %d", s.shardID, index)
	}
	return requiredTerm != 0 && actualTerm == requiredTerm, nil
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
	} else if _, ok := query.(requiredLsnStateQuery); ok {
		return requiredLsnState{
			lsn: s.state.RequiredLsn, revision: s.state.RequiredLsnRevision,
			owner: s.state.RequiredLsnOwner, term: s.state.RequiredLsnLeaderTerm,
			leaderID: s.state.RequiredLsnLeaderID,
		}, nil
	} else if _, ok := query.(walTruncateStateQuery); ok {
		return walTruncateState{s.state.TruncatedLsn, s.state.CheckpointedDSN}, nil
	} else if _, ok := query.(walRecoveryStateQuery); ok {
		return walRecoveryMarkerState{
			Digest:                s.state.WALRecoveryDigest,
			EntryCount:            s.state.WALRecoveryEntryCount,
			CompletedEntries:      s.state.WALRecoveryCompletedEntries,
			BaseLSN:               s.state.WALRecoveryBaseLsn,
			LastLSN:               s.state.WALRecoveryLastLsn,
			OriginalLeaseHolderID: s.state.WALRecoveryOriginalLeaseHolderID,
			Complete:              s.state.WALRecoveryComplete,
		}, nil
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
	recovered := pb.RSMState{Tso: 1, LeaseHistory: make(map[uint64]uint64)}
	if err := recovered.Unmarshal(data); err != nil {
		return err
	}
	if !recovered.RequiredLsnConditional {
		recovered.RequiredLsnRevision = recovered.RequiredLsn
	}
	s.state = recovered
	return nil
}
