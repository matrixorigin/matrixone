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
	"path"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	lifecyclepkg "github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/lifecycle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/require"
)

func TestValidateLifecycleRewriteOptions(t *testing.T) {
	objectID := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&objectID, false, true, false)
	segmentID := objectio.NewSegmentid()
	options := LifecycleRewriteOptions{
		SourceSnapshot:             types.BuildTS(100, 0),
		Source:                     *stats,
		LiveSegmentID:              *segmentID,
		MaxCertifiedBlockReadBytes: 256 << 20,
		Classify: func(
			context.Context,
			*batch.Batch,
			*nulls.Nulls,
		) (*nulls.Nulls, error) {
			return nil, nil
		},
		BeforeLiveWrite: func(context.Context, objectio.Segmentid) error {
			return nil
		},
		BookingPath: func(uint32) (string, error) {
			return "root/attempt/booking", nil
		},
	}
	require.NoError(t, validateLifecycleRewriteOptions(options))

	options.BookingPath = nil
	require.Error(t, validateLifecycleRewriteOptions(options))
}

func TestValidateLifecycleBlockReadPeak(t *testing.T) {
	const capBytes = uint64(256 << 20)

	peak, err := lifecycleBlockReadPeakBytes(80 << 20)
	require.NoError(t, err)
	require.Equal(t, capBytes, peak)
	require.NoError(t, validateLifecycleBlockReadPeak(80<<20, capBytes))

	require.ErrorContains(
		t,
		validateLifecycleBlockReadPeak((80<<20)+1, capBytes),
		"RESOURCE_BLOCKED",
	)
	require.ErrorContains(
		t,
		validateLifecycleBlockReadPeak(^uint64(0), capBytes),
		"cannot be estimated safely",
	)
}

func TestLifecycleRewritePressureChargesOnlyVisibleExpiredRows(t *testing.T) {
	report := lifecyclepkg.ObjectScanReport{ExpiredRows: 2, LiveRows: 1}
	expiredBytes, err := lifecycleEstimatedExpiredPressureBytes(900, report)
	require.NoError(t, err)
	require.Equal(t, uint64(600), expiredBytes)

	for _, test := range []struct {
		name   string
		source uint64
		report lifecyclepkg.ObjectScanReport
	}{
		{name: "zero source", report: report},
		{name: "zero expired", source: 900, report: lifecyclepkg.ObjectScanReport{LiveRows: 3}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := lifecycleEstimatedExpiredPressureBytes(test.source, test.report)
			require.ErrorContains(t, err, "MIXED_LAYOUT_BLOCKED")
		})
	}
	// Metrics are observational only; exercising the accounting path must not
	// alter admission or Object retirement behavior.
	lifecycleObserveRewritePressure(900, expiredBytes)
}

func TestValidateLifecycleRewriteLayout(t *testing.T) {
	require.NoError(t, validateLifecycleRewriteLayout(&api.SchemaExtra{
		BlockMaxRows:    options.DefaultBlockMaxRows,
		ObjectMaxBlocks: uint32(options.DefaultBlocksPerObject),
	}))
	require.ErrorContains(t, validateLifecycleRewriteLayout(nil), "RESOURCE_BLOCKED")
	require.ErrorContains(t, validateLifecycleRewriteLayout(&api.SchemaExtra{
		BlockMaxRows:    1024,
		ObjectMaxBlocks: uint32(options.DefaultBlocksPerObject),
	}), "RESOURCE_BLOCKED")
	require.ErrorContains(t, validateLifecycleRewriteLayout(&api.SchemaExtra{
		BlockMaxRows:    options.DefaultBlockMaxRows,
		ObjectMaxBlocks: 8,
	}), "RESOURCE_BLOCKED")
}

func TestTxnTableExposesLifecycleRewriteCapability(t *testing.T) {
	var _ LifecycleObjectRewriter = (*txnTable)(nil)
}

func TestValidateLifecycleRewriteOwnership(t *testing.T) {
	segmentID := objectio.NewSegmentid()
	root := lifecyclepkg.CleanupRoot{
		RootID:            "root",
		AttemptID:         "attempt",
		SegmentID:         segmentID.String(),
		BookingPrefix:     "lifecycle-staging/root/attempt/booking",
		OrdinalUpperBound: 2,
	}
	objectID := objectio.NewObjectidWithSegmentIDAndNum(segmentID, 0)
	stats := objectio.NewObjectStatsWithObjectID(objectID, false, true, true)
	result := LifecycleRewriteResult{
		CreatedObjectStats: [][]byte{stats.Marshal()},
		TransferBookingLocation: []string{
			string(types.EncodeInt32(ptrInt32(1))),
			string(types.EncodeInt32(ptrInt32(1))),
			path.Join(root.BookingPrefix, "booking-000000"),
		},
	}
	require.NoError(t, validateLifecycleRewriteOwnership(root, result))

	wrongSegment := objectio.NewSegmentid()
	wrongID := objectio.NewObjectidWithSegmentIDAndNum(wrongSegment, 0)
	wrongStats := objectio.NewObjectStatsWithObjectID(wrongID, false, true, true)
	result.CreatedObjectStats[0] = wrongStats.Marshal()
	require.ErrorContains(
		t,
		validateLifecycleRewriteOwnership(root, result),
		"segment",
	)

	result.CreatedObjectStats[0] = stats.Marshal()
	result.TransferBookingLocation[2] = "tmp/shared-booking"
	require.ErrorContains(
		t,
		validateLifecycleRewriteOwnership(root, result),
		"Booking",
	)

	result.TransferBookingLocation[2] = root.BookingPrefix + "-sibling/page"
	require.ErrorContains(
		t,
		validateLifecycleRewriteOwnership(root, result),
		"Booking",
	)

	result.TransferBookingLocation[2] = path.Join(root.BookingPrefix, "booking-000000")
	result.CreatedObjectStats[0] = []byte("malformed")
	require.ErrorContains(t, validateLifecycleRewriteOwnership(root, result), "malformed")

	result.CreatedObjectStats = [][]byte{stats.Marshal(), stats.Marshal()}
	require.ErrorContains(t, validateLifecycleRewriteOwnership(root, result), "duplicated")

	result.CreatedObjectStats = [][]byte{stats.Marshal()}
	result.TransferBookingLocation = []string{
		string(types.EncodeInt32(ptrInt32(1))),
	}
	require.ErrorContains(t, validateLifecycleRewriteOwnership(root, result), "no Root-owned Booking")
}

func ptrInt32(value int32) *int32 { return &value }

func TestCNMergeTaskLifecycleHooksPreserveOrdinaryDefaults(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	first := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsBlkCnt(first, 2))
	require.NoError(t, objectio.SetObjectStatsRowCnt(first, 20))
	require.NoError(t, objectio.SetObjectStatsOriginSize(first, 200))
	second := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsBlkCnt(second, 3))
	require.NoError(t, objectio.SetObjectStatsRowCnt(second, 30))
	require.NoError(t, objectio.SetObjectStatsOriginSize(second, 300))

	task := &cnMergeTask{
		taskId: 9,
		host: &txnTable{
			tableId:   42,
			tableName: "events",
			db:        &txnDatabase{databaseId: 7},
			typs:      []types.Type{types.T_int64.ToType()},
		},
		snapshot:      types.BuildTS(100, 2),
		targets:       []objectio.ObjectStats{*first, *second},
		blkCnts:       []int{2, 3},
		sortkeyPos:    0,
		targetObjSize: 128 << 20,
		mp:            mp,
	}
	require.False(t, task.HasBigDelEvent())
	require.Empty(t, task.TaskSourceNote())
	require.Contains(t, task.Name(), "42-events")
	require.True(t, task.DoTransfer())
	task.host.comment = catalog.MO_COMMENT_NO_DEL_HINT
	require.False(t, task.DoTransfer())
	require.Equal(t, 2, task.GetObjectCnt())
	require.Equal(t, []int{2, 3}, task.GetBlkCnts())
	require.Equal(t, []int{0, 2}, task.GetAccBlkCnts())
	require.Equal(t, uint32(objectio.BlockMaxRows), task.GetBlockMaxRows())
	require.Equal(t, uint16(options.DefaultBlocksPerObject), task.GetObjectMaxBlocks())
	require.Equal(t, uint32(128<<20), task.GetTargetObjSize())
	require.Equal(t, types.T_int64.ToType(), task.GetSortKeyType())
	require.Equal(t, uint64(500), task.GetTotalSize())
	require.Equal(t, uint32(50), task.GetTotalRowCnt())
	require.Equal(t, uint64(7), task.GetCommitEntry().DbId)
	require.Equal(t, uint64(42), task.GetCommitEntry().TblId)
	require.Len(t, task.GetCommitEntry().MergedObjs, 2)

	vec, release := task.GetVector(ptrType(types.T_varchar.ToType()))
	require.NotNil(t, vec)
	release()
	require.Equal(t, mp, task.GetMPool())
	require.Equal(t, "CN", task.HostHintName())
	require.NoError(t, task.admitLifecycleBlockRead(0, nil))
	require.Error(t, task.configureLifecycleBlockReadBudget(
		context.Background(),
		0,
	))
	task.lifecycleReadBudget = &lifecycleBlockReadBudget{
		maxBytes: 1,
		metas:    nil,
		next:     nil,
	}
	require.ErrorContains(t, task.admitLifecycleBlockRead(0, nil), "out of range")
	task.Release()
}

func ptrType(value types.Type) *types.Type { return &value }
