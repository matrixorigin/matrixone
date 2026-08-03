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
}

func ptrInt32(value int32) *int32 { return &value }
