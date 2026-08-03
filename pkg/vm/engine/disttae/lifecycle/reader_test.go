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

package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestSelectProtectionSetSeparatesDataAndTombstoneObjects(t *testing.T) {
	source := lifecycleTestObjectEntry(t, 1)
	tombstone := lifecycleTestObjectEntry(t, 2)
	selector := &testTombstoneSelector{
		selected: []objectio.ObjectEntry{tombstone},
	}
	set, err := SelectProtectionSet(
		context.Background(),
		selector,
		types.BuildTS(100, 0),
		[]objectio.ObjectEntry{source},
		logtailreplay.LifecycleTombstoneSelectionLimits{
			MaxScannedObjects:  10,
			MaxSelectedObjects: 10,
			MaxMetaBytes:       1 << 20,
		},
	)
	require.NoError(t, err)
	require.Len(t, set.DataSources, 1)
	require.Len(t, set.ProtectedTombstones, 1)
	require.Len(t, set.ProtectedObjects, 2)
	require.Equal(t, source.ObjectStats, set.DataSources[0])
	require.Equal(t, tombstone.ObjectStats, set.ProtectedTombstones[0])
	require.NotEqual(t, set.SourceSetDigest, set.ProtectionSetDigest)
	require.Equal(t, set.ProtectedTombstones, selector.selectedStats)
}

func TestAcquireProtectionRegistersBeforeStatAndFailsClosed(t *testing.T) {
	source := lifecycleTestObjectEntry(t, 1)
	set := ProtectionSet{
		DataSources:         []objectio.ObjectStats{source.ObjectStats},
		ProtectedObjects:    []objectio.ObjectStats{source.ObjectStats},
		ProtectionSetDigest: [32]byte{1, 2, 3},
	}
	client := &testProtectionClient{}
	lease, err := AcquireProtection(
		context.Background(),
		client,
		"attempt-1",
		set,
		time.Now().Add(time.Minute),
	)
	require.NoError(t, err)
	require.NotNil(t, lease)
	require.Equal(t, []string{"register", "stat"}, client.calls)
	require.NoError(t, lease.Renew(context.Background(), time.Now().Add(2*time.Minute)))
	require.NoError(t, lease.Release(context.Background()))
	require.Equal(t, []string{"register", "stat", "renew", "release"}, client.calls)

	client = &testProtectionClient{statErr: errors.New("object changed")}
	_, err = AcquireProtection(
		context.Background(),
		client,
		"attempt-2",
		set,
		time.Now().Add(time.Minute),
	)
	require.Error(t, err)
	require.Equal(t, []string{"register", "stat", "release"}, client.calls)
}

func TestObjectScanReportRequiresCompletePhysicalCoverageAndDEL(t *testing.T) {
	report := NewObjectScanReport(2, 4)
	require.NoError(t, report.ObserveClassifiedBlock(
		2,
		nulls.Build(2, 0),
		nulls.Build(2, 1),
	))
	require.ErrorContains(t, report.ValidateComplete(), "scan is incomplete")

	require.NoError(t, report.ObserveClassifiedBlock(2, nil, nil))
	require.NoError(t, report.ValidateComplete())
	require.Equal(t, uint64(1), report.SnapshotDeletedRows)
	require.Equal(t, uint64(1), report.ExpiredRows)
	require.Equal(t, uint64(2), report.LiveRows)
}

func TestObjectScanReportRejectsOutOfRangeAndOverlappingClasses(t *testing.T) {
	report := NewObjectScanReport(1, 2)
	require.Error(t, report.ObserveClassifiedBlock(
		2,
		nulls.Build(2, 2),
		nil,
	))

	report = NewObjectScanReport(1, 2)
	require.Error(t, report.ObserveClassifiedBlock(
		2,
		nulls.Build(2, 1),
		nulls.Build(2, 1),
	))
}

type testTombstoneSelector struct {
	selected      []objectio.ObjectEntry
	selectedStats []objectio.ObjectStats
}

func (selector *testTombstoneSelector) SelectLifecycleTombstoneObjects(
	_ context.Context,
	_ types.TS,
	_ []objectio.ObjectId,
	_ logtailreplay.LifecycleTombstoneSelectionLimits,
) ([]objectio.ObjectEntry, int, error) {
	selector.selectedStats = make([]objectio.ObjectStats, len(selector.selected))
	for index := range selector.selected {
		selector.selectedStats[index] = selector.selected[index].ObjectStats
	}
	return selector.selected, len(selector.selected), nil
}

type testProtectionClient struct {
	calls   []string
	statErr error
}

func (client *testProtectionClient) Register(
	_ context.Context,
	_ string,
	_ []objectio.ObjectStats,
	_ time.Time,
) error {
	client.calls = append(client.calls, "register")
	return nil
}

func (client *testProtectionClient) StatExact(
	_ context.Context,
	_ []objectio.ObjectStats,
) error {
	client.calls = append(client.calls, "stat")
	return client.statErr
}

func (client *testProtectionClient) Renew(
	_ context.Context,
	_ string,
	_ time.Time,
) error {
	client.calls = append(client.calls, "renew")
	return nil
}

func (client *testProtectionClient) Release(
	_ context.Context,
	_ string,
) error {
	client.calls = append(client.calls, "release")
	return nil
}
