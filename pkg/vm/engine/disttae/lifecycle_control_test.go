// Copyright 2026 Matrix Origin
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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestLifecycleCommitControlIsImmutableAndOutsideWrites(t *testing.T) {
	transaction := &Transaction{}
	control := &api.LifecycleCommitEntry{
		ProtocolVersion: 1,
		RootId:          "root-1",
		AttemptId:       "attempt-1",
		DatabaseId:      7,
		PhysicalTableId: 42,
	}
	require.NoError(t, transaction.SetLifecycleCommitControl(DNStore{}, control))
	control.RootId = "mutated"
	require.Empty(t, transaction.writes)
	require.Equal(t, "root-1", transaction.lifecycleCommitControl.Entry.RootId)
	require.False(t, transaction.readOnly.Load())
	require.Error(t, transaction.SetLifecycleCommitControl(DNStore{}, control))
}

func TestAppendLifecycleCommitControlAfterOrdinaryEntries(t *testing.T) {
	ordinary := &api.Entry{EntryType: api.Entry_Insert}
	control := &LifecycleCommitControl{Entry: &api.LifecycleCommitEntry{
		ProtocolVersion: 1,
		DatabaseId:      7,
		PhysicalTableId: 42,
	}}
	entries, err := appendLifecycleCommitControl([]*api.Entry{ordinary}, control)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	require.Same(t, ordinary, entries[0])
	require.Equal(t, api.Entry_LifecycleCommit, entries[1].EntryType)
	require.Nil(t, entries[1].Bat)
	require.Same(t, control.Entry, entries[1].LifecycleCommit)
	require.Equal(t, uint64(7), entries[1].DatabaseId)
	require.Equal(t, uint64(42), entries[1].TableId)

	command := &api.PrecommitWriteCmd{
		EntryList:           entries,
		SyncProtectionJobId: "protection-job",
	}
	encoded, err := command.MarshalBinary()
	require.NoError(t, err)
	decoded := new(api.PrecommitWriteCmd)
	require.NoError(t, decoded.UnmarshalBinary(encoded))
	require.Len(t, decoded.EntryList, 2)
	require.Equal(t, api.Entry_Insert, decoded.EntryList[0].EntryType)
	require.Equal(t, api.Entry_LifecycleCommit, decoded.EntryList[1].EntryType)
	require.Nil(t, decoded.EntryList[1].Bat)
	require.Equal(t, control.Entry, decoded.EntryList[1].LifecycleCommit)
	require.Equal(t, "protection-job", decoded.SyncProtectionJobId)
}

func TestAppendLifecycleCommitControlRejectsUnknownVersion(t *testing.T) {
	_, err := appendLifecycleCommitControl(nil, &LifecycleCommitControl{
		Entry: &api.LifecycleCommitEntry{ProtocolVersion: 2},
	})
	require.Error(t, err)
}

func TestGenWriteReqsKeepsCatalogWriteAndLifecycleControlTogether(t *testing.T) {
	proc := testutil.NewProc(t)
	value := batch.NewWithSize(2)
	value.Attrs = []string{objectio.PhysicalAddr_Attr, "marker"}
	value.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	value.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { value.Clean(proc.Mp()) })
	require.NoError(t, vector.AppendFixed(
		value.Vecs[0],
		types.RandomRowid(),
		false,
		proc.Mp(),
	))
	require.NoError(t, vector.AppendFixed(
		value.Vecs[1],
		int64(1),
		false,
		proc.Mp(),
	))
	value.SetRowCount(1)

	store := DNStore{
		ServiceID:         "tn-1",
		TxnServiceAddress: "tn-address",
		Shards: []metadata.TNShard{{
			TNShardRecord: metadata.TNShardRecord{ShardID: 1},
			ReplicaID:     1,
		}},
	}
	transaction := &Transaction{
		proc: proc,
		op:   newTxnOperatorForTest(t),
		writes: []Entry{{
			typ:          INSERT,
			tableId:      2,
			databaseId:   1,
			tableName:    "mo_lifecycle_datasets",
			databaseName: "mo_catalog",
			bat:          value,
			tnStore:      store,
		}},
	}
	require.NoError(t, transaction.SetLifecycleCommitControl(
		store,
		&api.LifecycleCommitEntry{
			ProtocolVersion: 1,
			DatabaseId:      7,
			PhysicalTableId: 42,
		},
	))
	transaction.SetSyncProtectionJobID("protection-job")

	requests, err := genWriteReqs(context.Background(), transaction)
	require.NoError(t, err)
	require.Len(t, requests, 1)
	command := new(api.PrecommitWriteCmd)
	require.NoError(t, command.UnmarshalBinary(
		requests[0].CNRequest.Payload,
	))
	require.Len(t, command.EntryList, 2)
	require.Equal(t, api.Entry_Insert, command.EntryList[0].EntryType)
	require.NotNil(t, command.EntryList[0].Bat)
	require.Equal(t, api.Entry_LifecycleCommit, command.EntryList[1].EntryType)
	require.Nil(t, command.EntryList[1].Bat)
	require.Equal(t, "protection-job", command.SyncProtectionJobId)
}
