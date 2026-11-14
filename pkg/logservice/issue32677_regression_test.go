// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/raftpb"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/require"
)

func TestRejectedAppendIsNotUserRecord(t *testing.T) {
	runServiceTest(t, false, true, func(t *testing.T, s *Service) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		connect := func(id uint64) {
			resp := s.handleConnect(ctx, pb.Request{Method: pb.CONNECT,
				LogRequest: pb.LogRequest{ShardID: 1, TNID: id}})
			require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		}
		connect(100)
		connect(101)
		const walPayloadHex = "e8030200010000000000000000000000000000000000000000000000000000000000000000000100000001000000000000000000000000000000830000000100000000000000d5070100150000001c0000000000000083010000000000000000000000000000000000000000000072656a65637465642d6275742d7265706c617965643e00000045000000"
		walPayload, err := hex.DecodeString(walPayloadHex)
		require.NoError(t, err)
		cmd := getTestAppendCmd(100, walPayload)
		resp := s.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL,
			LogRequest: pb.LogRequest{ShardID: 1}}, cmd, true)
		require.NotEqual(t, uint32(moerr.Ok), resp.ErrorCode)
		require.Zero(t, resp.LogResponse.Lsn)
		records, _, err := s.store.queryLog(ctx, 1, 1, 1<<20)
		require.NoError(t, err)
		var found []pb.LogRecord
		for _, rec := range records {
			if bytes.Equal(rec.Data, cmd) {
				found = append(found, rec)
			}
		}
		require.Len(t, found, 1)
		require.Equal(t, pb.LeaseRejected, found[0].Type)
		require.NotZero(t, found[0].Lsn)

		accepted := getTestAppendCmd(101, []byte("accepted-after-rejection"))
		resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL,
			LogRequest: pb.LogRequest{ShardID: 1}}, accepted, true)
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		records, _, err = s.store.queryLog(ctx, 1, found[0].Lsn, 1<<20)
		require.NoError(t, err)
		var acceptedType pb.RecordType
		for _, rec := range records {
			if bytes.Equal(rec.Data, accepted) {
				acceptedType = rec.Type
			}
		}
		require.Equal(t, pb.UserRecord, acceptedType)

		digest := strings.Repeat("a", walRecoveryDigestSize)
		_, err = s.store.propose(ctx, s.store.nh.GetNoOPSession(1), getBeginWALRecoveryCmd(digest, 1))
		require.NoError(t, err)
		firstRecovery := getTestAppendCmd(walRecoveryLeaseHolderID, []byte("recovery-accepted"))
		resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL,
			LogRequest: pb.LogRequest{ShardID: 1}}, firstRecovery, true)
		require.Equal(t, uint32(moerr.Ok), resp.ErrorCode)
		secondRecovery := getTestAppendCmd(walRecoveryLeaseHolderID, []byte("recovery-rejected"))
		resp = s.handleAppend(ctx, pb.Request{Method: pb.APPEND_WAL,
			LogRequest: pb.LogRequest{ShardID: 1}}, secondRecovery, true)
		require.Equal(t, uint32(moerr.ErrNotLeaseHolder), resp.ErrorCode)
		records, _, err = s.store.queryLog(ctx, 1, 1, 1<<20)
		require.NoError(t, err)
		recoveryTypes := make(map[string]pb.RecordType)
		for _, rec := range records {
			if bytes.Equal(rec.Data, firstRecovery) {
				recoveryTypes["first"] = rec.Type
			}
			if bytes.Equal(rec.Data, secondRecovery) {
				recoveryTypes["second"] = rec.Type
			}
		}
		require.Equal(t, pb.UserRecord, recoveryTypes["first"])
		require.Equal(t, pb.LeaseRejected, recoveryTypes["second"])
	})
}

func TestRejectedReadKeepsLeaseSnapshotAcrossTruncate(t *testing.T) {
	machine := newStateMachine(1, 1).(*stateMachine)
	machine.state.Index = 9
	machine.state.LeaseHistory[3] = 100
	machine.state.LeaseHistory[4] = 101
	machine.state.LeaseHistory[7] = 100
	machine.rebuildLeaseIndexes()
	v, err := machine.Lookup(userEntryStateQuery{first: 5, last: 5})
	require.NoError(t, err)
	state := v.(userEntryState)
	machine.truncateLeaseHistory(8) // A truncate after the read snapshot drops the old transition.
	require.NotContains(t, machine.state.LeaseHistory, uint64(4))
	machine.state.TruncatedLsn = 8
	late, err := machine.Lookup(userEntryStateQuery{first: 5, last: 5})
	require.NoError(t, err)
	require.GreaterOrEqual(t, late.(userEntryState).truncatedLSN, uint64(5))
	cmd := getTestAppendCmd(100, []byte("rejected-before-truncate"))
	entry := raftpb.Entry{Index: 5, Type: raftpb.EncodedEntry, Cmd: append([]byte{0}, cmd...)}
	records, err := (&store{}).markEntries(context.Background(), []raftpb.Entry{entry}, state)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, pb.LeaseRejected, records[0].Type)
	var snapshot bytes.Buffer
	require.NoError(t, machine.SaveSnapshot(&snapshot, nil, nil))
	restored := newStateMachine(1, 2).(*stateMachine)
	require.NoError(t, restored.RecoverFromSnapshot(&snapshot, nil, nil))
	restoredState, err := restored.Lookup(userEntryStateQuery{first: 8, last: 8})
	require.NoError(t, err)
	require.Equal(t, uint64(100), restoredState.(userEntryState).lease)
}

func BenchmarkUserEntryStateLookupSmallPage(b *testing.B) {
	machine := newStateMachine(1, 1).(*stateMachine)
	for i := uint64(1); i <= 100_000; i++ {
		machine.state.Index = i
		machine.state.LeaseHolderID = i
		machine.recordLease()
	}
	b.ResetTimer()
	for range b.N {
		_, _ = machine.Lookup(userEntryStateQuery{first: 50_000, last: 50_010})
	}
}
