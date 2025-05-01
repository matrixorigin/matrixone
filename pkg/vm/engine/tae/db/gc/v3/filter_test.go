// Copyright 2023 Matrix Origin
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

package gc

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

// TestFilterWithEmptySnapshots tests filter behavior with empty snapshots
func TestFilterWithEmptySnapshots(t *testing.T) {
	ctx := context.Background()

	ts := types.BuildTS(100, 0)
	accountSnapshots := make(map[uint32][]types.TS)
	pitrs := &logtail.PitrInfo{}
	snapshotMeta := &logtail.SnapshotMeta{}
	job, transObjects := getJob(ctx, t, nil)

	filter, err := MakeSnapshotAndPitrFineFilter(
		&ts,
		accountSnapshots,
		pitrs,
		snapshotMeta,
		transObjects,
	)
	require.NoError(t, err)
	require.NotNil(t, filter)

	job.filterProvider = &MockFilterProvider{
		fineFilterFn: filter,
		coarseFilterFn: func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
			return nil
		},
	}

	err = job.Execute(ctx)
	require.NoError(t, err)
}

// TestFilterWithOverlappingTimestamps tests filter behavior with overlapping timestamps
func TestFilterWithOverlappingTimestamps(t *testing.T) {
	ctx := context.Background()

	ts := types.BuildTS(100, 0)
	accountSnapshots := map[uint32][]types.TS{
		1: {
			types.BuildTS(45, 0),
			types.BuildTS(55, 0),
			types.BuildTS(65, 0),
		},
	}

	job, transObjects := getJob(ctx, t, nil)

	filter, err := MakeSnapshotAndPitrFineFilter(
		&ts,
		accountSnapshots,
		&logtail.PitrInfo{},
		&logtail.SnapshotMeta{},
		transObjects,
	)
	require.NoError(t, err)

	job.filterProvider = &MockFilterProvider{
		fineFilterFn: filter,
		coarseFilterFn: func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
			return nil
		},
	}

	err = job.Execute(ctx)
	require.NoError(t, err)
}

// TestFilterWithEdgeCases tests filter behavior with edge cases
func TestFilterWithEdgeCases(t *testing.T) {
	ctx := context.Background()

	ts := types.BuildTS(100, 0)
	accountSnapshots := map[uint32][]types.TS{
		1: {types.BuildTS(50, 0)},
	}

	// Test edge cases
	testCases := []struct {
		name     string
		createTS types.TS
		dropTS   types.TS
	}{
		{
			name:     "same_timestamp",
			createTS: types.BuildTS(50, 0),
			dropTS:   types.BuildTS(50, 0),
		},
		{
			name:     "boundary_before",
			createTS: types.BuildTS(49, 0),
			dropTS:   types.BuildTS(50, 0),
		},
		{
			name:     "boundary_after",
			createTS: types.BuildTS(50, 0),
			dropTS:   types.BuildTS(51, 0),
		},
		{
			name:     "far_future",
			createTS: types.BuildTS(200, 0),
			dropTS:   types.BuildTS(300, 0),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			transObjects := map[string]*ObjectEntry{
				tc.name: {
					stats:    &objectio.ObjectStats{},
					createTS: tc.createTS,
					dropTS:   tc.dropTS,
				},
			}

			job, _ := getJob(ctx, t, transObjects)

			filter, err := MakeSnapshotAndPitrFineFilter(
				&ts,
				accountSnapshots,
				&logtail.PitrInfo{},
				&logtail.SnapshotMeta{},
				transObjects,
			)
			require.NoError(t, err)

			job.filterProvider = &MockFilterProvider{
				fineFilterFn: filter,
				coarseFilterFn: func(ctx context.Context, bm *bitmap.Bitmap, bat *batch.Batch, mp *mpool.MPool) error {
					return nil
				},
			}

			err = job.Execute(ctx)
			require.NoError(t, err)
		})
	}
}
