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

package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/stretchr/testify/require"
)

func TestValidateLifecycleCommitControl(t *testing.T) {
	now := time.Now()
	stats := lifecycleRPCObjectStats(t)
	control := lifecycleRPCControl(stats, now.Add(time.Minute))
	require.NoError(t, validateLifecycleCommitControl(control, now))

	control.DataSourceObjectStats[0] = control.DataSourceObjectStats[0][:10]
	require.Error(t, validateLifecycleCommitControl(control, now))

	control = lifecycleRPCControl(stats, now.Add(-time.Second))
	require.Error(t, validateLifecycleCommitControl(control, now))

	control = lifecycleRPCControl(stats, now.Add(time.Minute))
	control.RetireMode = api.LifecycleCommitEntry_Rewrite
	require.Error(t, validateLifecycleCommitControl(control, now))
}

func TestValidateLifecycleTransferTableBoundsAndCount(t *testing.T) {
	createdID := objectio.NewObjectid()
	created := objectio.NewObjectStatsWithObjectID(
		&createdID,
		false,
		true,
		false,
	)
	require.NoError(t, objectio.SetObjectStatsRowCnt(created, 2))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(created, 1))
	table := mergesort.NewTransferTableFromMaps(api.TransferMaps{
		{
			{ObjIdx: 0, BlkIdx: 0, RowIdx: 0},
			{ObjIdx: api.NoTransfer},
			{ObjIdx: 0, BlkIdx: 0, RowIdx: 1},
		},
	})
	require.NoError(t, validateLifecycleTransferTable(
		[][]byte{append([]byte(nil), created[:]...)},
		table,
		objectio.BlockMaxRows,
	))
	table.Maps[0][2] = api.TransferDestPos{ObjIdx: api.NoTransfer}
	require.Error(t, validateLifecycleTransferTable(
		[][]byte{append([]byte(nil), created[:]...)},
		table,
		objectio.BlockMaxRows,
	))
	table.Maps[0][2] = api.TransferDestPos{
		ObjIdx: 0,
		BlkIdx: 0,
		RowIdx: objectio.BlockMaxRows,
	}
	require.Error(t, validateLifecycleTransferTable(
		[][]byte{append([]byte(nil), created[:]...)},
		table,
		objectio.BlockMaxRows,
	))
}

func TestValidateLifecycleBookingHeader(t *testing.T) {
	blockCount := int32(1)
	rowCount := int32(100)
	valid := []string{
		string(types.EncodeInt32(&blockCount)),
		string(types.EncodeInt32(&rowCount)),
		"root/attempt/booking-0",
	}
	require.NoError(t, validateLifecycleBookingHeader(
		valid,
		1,
		objectio.BlockMaxRows,
	))
	require.Error(t, validateLifecycleBookingHeader(valid[:2], 1, objectio.BlockMaxRows))
}

func TestValidateTransferMapSourceBounds(t *testing.T) {
	pool := mpool.MustNewZero()
	for _, test := range []struct {
		name      string
		sourceBlk int32
		sourceRow uint32
		wantError bool
	}{
		{name: "valid", sourceBlk: 0, sourceRow: 0},
		{name: "negative block", sourceBlk: -1, sourceRow: 0, wantError: true},
		{name: "block overflow", sourceBlk: 1, sourceRow: 0, wantError: true},
		{name: "row overflow", sourceBlk: 0, sourceRow: 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := batch.NewWithSize(2)
			value.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			value.Vecs[1] = vector.NewVec(types.T_uint32.ToType())
			t.Cleanup(func() { value.Clean(pool) })
			require.NoError(t, vector.AppendFixed(
				value.Vecs[0],
				test.sourceBlk,
				false,
				pool,
			))
			require.NoError(t, vector.AppendFixed(
				value.Vecs[1],
				test.sourceRow,
				false,
				pool,
			))
			value.SetRowCount(1)

			err := validateTransferMapSourceBounds(
				context.Background(),
				value,
				api.TransferMaps{make(api.TransferMap, 1)},
			)
			if test.wantError {
				require.ErrorContains(t, err, "out-of-range source")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateLifecycleCommitControlRejectsDuplicateSourcesAndWrongDigest(t *testing.T) {
	now := time.Now()
	stats := lifecycleRPCObjectStats(t)
	control := lifecycleRPCControl(stats, now.Add(time.Minute))
	control.DataSourceObjectStats = append(
		control.DataSourceObjectStats,
		append([]byte(nil), stats[:]...),
	)
	control.SourceSetDigest = lifecycleSourceSetDigest(control.DataSourceObjectStats)
	require.Error(t, validateLifecycleCommitControl(control, now))

	control = lifecycleRPCControl(stats, now.Add(time.Minute))
	control.SourceSetDigest[0] ^= 1
	require.Error(t, validateLifecycleCommitControl(control, now))
}

func TestValidateLifecycleCommitControlRejectsSourceBytesAboveCertifiedLimit(t *testing.T) {
	now := time.Now()
	first := lifecycleRPCObjectStats(t)
	require.NoError(t, objectio.SetObjectStatsOriginSize(&first, 3<<30))
	second := lifecycleRPCObjectStats(t)
	require.NoError(t, objectio.SetObjectStatsOriginSize(&second, 3<<30))
	control := lifecycleRPCControl(first, now.Add(time.Minute))
	control.DataSourceObjectStats = append(
		control.DataSourceObjectStats,
		append([]byte(nil), second[:]...),
	)
	control.SourceSetDigest = lifecycleSourceSetDigest(control.DataSourceObjectStats)

	err := validateLifecycleCommitControl(control, now)
	require.ErrorContains(t, err, "source bytes")
}

func TestValidateLifecycleCommitControlRejectsAmbiguousProductAndOwner(t *testing.T) {
	now := time.Now()
	stats := lifecycleRPCObjectStats(t)

	control := lifecycleRPCControl(stats, now.Add(time.Minute))
	control.ReceiptId = "receipt"
	require.Error(t, validateLifecycleCommitControl(control, now))

	control = lifecycleRPCControl(stats, now.Add(time.Minute))
	control.DatasetId = ""
	control.ReceiptId = "receipt"
	control.RootId = ""
	control.RetireMode = api.LifecycleCommitEntry_Rewrite
	control.CreatedObjectStats = [][]byte{
		append([]byte(nil), stats[:]...),
	}
	control.TransferBookingLocations = []string{"root/attempt/booking-0"}
	control.TransferMappingDigest = make([]byte, 32)
	control.MaxDeltaRows = 1
	control.MaxDeltaBytes = 1
	control.MaxDeltaBlocks = 1
	require.Error(t, validateLifecycleCommitControl(control, now))
}

func TestValidateLifecycleCommitControlEnforcesRewriteAllocationBounds(t *testing.T) {
	now := time.Now()
	stats := lifecycleRPCObjectStats(t)
	control := lifecycleRPCControl(stats, now.Add(time.Minute))
	control.RetireMode = api.LifecycleCommitEntry_Rewrite
	control.CreatedObjectStats = make([][]byte, lifecycleRewriteMaxCreatedObjects+1)
	for index := range control.CreatedObjectStats {
		control.CreatedObjectStats[index] = append([]byte(nil), stats[:]...)
	}
	control.TransferBookingLocations = []string{"root/attempt/booking-0"}
	control.TransferMappingDigest = make([]byte, 32)
	control.MaxDeltaRows = 1
	control.MaxDeltaBytes = 1
	control.MaxDeltaBlocks = 1
	require.Error(t, validateLifecycleCommitControl(control, now))

	control.CreatedObjectStats = control.CreatedObjectStats[:1]
	control.MaxDeltaBlocks = uint32(stats.BlkCnt()) + 1
	require.Error(t, validateLifecycleCommitControl(control, now))

	control.MaxDeltaBlocks = uint32(stats.BlkCnt())
	control.MaxDeltaRows = lifecycleRewriteMaxDeltaRows + 1
	require.ErrorContains(
		t,
		validateLifecycleCommitControl(control, now),
		"incomplete",
	)

	control.MaxDeltaRows = lifecycleRewriteMaxDeltaRows
	control.MaxDeltaBytes = lifecycleRewriteMaxDeltaBytes + 1
	require.ErrorContains(
		t,
		validateLifecycleCommitControl(control, now),
		"incomplete",
	)
}

func TestValidateLifecycleProtectionJobID(t *testing.T) {
	require.NoError(t, validateLifecycleProtectionJobID("attempt", "attempt-a1b2"))
	require.Error(t, validateLifecycleProtectionJobID("attempt", "attempt"))
	require.Error(t, validateLifecycleProtectionJobID("attempt", "other-a1b2"))
	require.Error(t, validateLifecycleProtectionJobID("attempt", ""))
}

func lifecycleRPCControl(
	stats objectio.ObjectStats,
	deadline time.Time,
) *api.LifecycleCommitEntry {
	sources := [][]byte{append([]byte(nil), stats[:]...)}
	return &api.LifecycleCommitEntry{
		ProtocolVersion:              1,
		RetireMode:                   api.LifecycleCommitEntry_Whole,
		RootId:                       "root",
		AttemptId:                    "attempt",
		DatasetId:                    "dataset",
		DatabaseId:                   7,
		LogicalTableId:               42,
		PhysicalTableId:              42,
		BindingGeneration:            1,
		SchemaDigest:                 make([]byte, 32),
		SourceSnapshotTs:             &timestamp.Timestamp{PhysicalTime: 100},
		SourceSetDigest:              lifecycleSourceSetDigest(sources),
		DataSourceObjectStats:        sources,
		FinalPrepareDeadlineUnixNano: deadline.UnixNano(),
	}
}

func lifecycleRPCObjectStats(t *testing.T) objectio.ObjectStats {
	t.Helper()
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objectID, false, true, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 100))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	return *stats
}
