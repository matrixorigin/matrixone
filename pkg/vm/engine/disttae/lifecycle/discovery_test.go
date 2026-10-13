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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

type fakeObjectPageSource struct {
	calls []fakeObjectPageCall
	pages []logtailreplay.VisibleDataObjectPage
	err   error
}

type fakeObjectPageCall struct {
	after      *objectio.ObjectNameShort
	maxObjects int
	maxBytes   uint64
}

func (f *fakeObjectPageSource) ScanVisibleDataObjectsPage(
	_ context.Context,
	_ types.TS,
	after *objectio.ObjectNameShort,
	maxObjects int,
	maxBytes uint64,
) (logtailreplay.VisibleDataObjectPage, error) {
	var cloned *objectio.ObjectNameShort
	if after != nil {
		value := *after
		cloned = &value
	}
	f.calls = append(f.calls, fakeObjectPageCall{
		after:      cloned,
		maxObjects: maxObjects,
		maxBytes:   maxBytes,
	})
	if f.err != nil {
		return logtailreplay.VisibleDataObjectPage{}, f.err
	}
	page := f.pages[0]
	f.pages = f.pages[1:]
	return page, nil
}

func TestDiscoverObjectPageCarriesBoundedCursor(t *testing.T) {
	entry := lifecycleTestObjectEntry(t, 1)
	last := *entry.ObjectShortName()
	source := &fakeObjectPageSource{pages: []logtailreplay.VisibleDataObjectPage{{
		Objects:        []objectio.ObjectEntry{entry},
		LastObjectName: &last,
		MetaBytes:      128,
		End:            false,
	}}}
	now := time.Unix(1000, 0)
	snapshot := types.BuildTS(100, 7)

	page, err := DiscoverObjectPage(context.Background(), source, DiscoveryRequest{
		Snapshot: snapshot,
		Now:      now,
		Limits: DiscoveryLimits{
			MaxObjects:   32,
			MaxMetaBytes: 4096,
			MaxDuration:  time.Second,
		},
	})
	require.NoError(t, err)
	require.Len(t, page.Candidates, 1)
	require.Equal(t, snapshot, page.Next.Snapshot)
	require.True(t, page.Next.HasLastObject)
	require.Equal(t, last, page.Next.LastObjectName)
	require.False(t, page.EndOfCycle)
	require.Equal(t, 32, source.calls[0].maxObjects)
	require.Equal(t, uint64(4096), source.calls[0].maxBytes)
}

func TestDiscoverObjectPageCompletesOverdueInProgressCycle(t *testing.T) {
	entry := lifecycleTestObjectEntry(t, 9)
	oldLast := *entry.ObjectShortName()
	now := time.Unix(2000, 0)

	source := &fakeObjectPageSource{pages: []logtailreplay.VisibleDataObjectPage{{
		End: true,
	}}}
	page, err := DiscoverObjectPage(context.Background(), source, DiscoveryRequest{
		Snapshot: types.BuildTS(200, 0),
		Now:      now,
		Cursor: DiscoveryCursor{
			HasLastObject:  true,
			LastObjectName: oldLast,
		},
		LastFullScanAt:   now.Add(-2 * time.Hour),
		FullScanInterval: time.Hour,
		Limits: DiscoveryLimits{
			MaxObjects:   8,
			MaxMetaBytes: 4096,
			MaxDuration:  time.Second,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, source.calls[0].after)
	require.Equal(t, oldLast, *source.calls[0].after)
	require.True(t, page.EndOfCycle)
	require.True(t, page.Next.Wrapped)
	require.Equal(t, now, page.CompletedFullScanAt)
}

func TestDiscoverObjectPageDoesNotResetInProgressCycleAfterInterval(t *testing.T) {
	firstEntry := lifecycleTestObjectEntry(t, 10)
	firstLast := *firstEntry.ObjectShortName()
	secondEntry := lifecycleTestObjectEntry(t, 11)
	secondLast := *secondEntry.ObjectShortName()
	now := time.Unix(3000, 0)
	interval := time.Hour
	source := &fakeObjectPageSource{pages: []logtailreplay.VisibleDataObjectPage{
		{
			Objects:        []objectio.ObjectEntry{firstEntry},
			LastObjectName: &firstLast,
		},
		{
			Objects:        []objectio.ObjectEntry{secondEntry},
			LastObjectName: &secondLast,
		},
		{End: true},
		{End: true},
	}}
	request := DiscoveryRequest{
		Snapshot:         types.BuildTS(300, 0),
		Now:              now,
		LastFullScanAt:   now.Add(-2 * interval),
		FullScanInterval: interval,
		Limits: DiscoveryLimits{
			MaxObjects:   8,
			MaxMetaBytes: 4096,
			MaxDuration:  time.Second,
		},
	}
	first, err := DiscoverObjectPage(context.Background(), source, request)
	require.NoError(t, err)
	require.Nil(t, source.calls[0].after)
	require.Equal(t, now, first.StartedFullScanAt)

	request.Now = now.Add(30 * time.Minute)
	request.LastFullScanAt = first.StartedFullScanAt
	request.Cursor = first.Next
	second, err := DiscoverObjectPage(context.Background(), source, request)
	require.NoError(t, err)
	require.NotNil(t, source.calls[1].after)
	require.Equal(t, firstLast, *source.calls[1].after)
	require.True(t, second.StartedFullScanAt.IsZero())

	request.Now = now.Add(2 * time.Hour)
	request.Cursor = second.Next
	third, err := DiscoverObjectPage(context.Background(), source, request)
	require.NoError(t, err)
	require.NotNil(t, source.calls[2].after)
	require.Equal(t, secondLast, *source.calls[2].after)
	require.True(t, third.StartedFullScanAt.IsZero())
	require.True(t, third.Next.Wrapped)
	require.Equal(t, request.Now, third.CompletedFullScanAt)

	request.Now = now.Add(2*time.Hour + time.Minute)
	request.LastFullScanAt = third.CompletedFullScanAt
	request.Cursor = third.Next
	fourth, err := DiscoverObjectPage(context.Background(), source, request)
	require.NoError(t, err)
	require.Nil(t, source.calls[3].after)
	require.Equal(t, request.Now, fourth.StartedFullScanAt)
}

func TestDiscoverObjectPageKeepsObjectNameHintAcrossSnapshots(t *testing.T) {
	entry := lifecycleTestObjectEntry(t, 3)
	last := *entry.ObjectShortName()
	source := &fakeObjectPageSource{pages: []logtailreplay.VisibleDataObjectPage{{
		End: true,
	}}}
	_, err := DiscoverObjectPage(context.Background(), source, DiscoveryRequest{
		Snapshot: types.BuildTS(200, 0),
		Now:      time.Now(),
		Cursor: DiscoveryCursor{
			Snapshot:       types.BuildTS(100, 0),
			LastObjectName: last,
			HasLastObject:  true,
		},
		Limits: DiscoveryLimits{
			MaxObjects:   8,
			MaxMetaBytes: 4096,
			MaxDuration:  time.Second,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, source.calls[0].after)
	require.Equal(t, last, *source.calls[0].after)
}

func TestDiscoverObjectPageRejectsUnboundedLimits(t *testing.T) {
	_, err := DiscoverObjectPage(context.Background(), &fakeObjectPageSource{}, DiscoveryRequest{
		Snapshot: types.BuildTS(1, 0),
		Now:      time.Now(),
	})
	require.Error(t, err)
}

func lifecycleTestObjectEntry(t *testing.T, number uint16) objectio.ObjectEntry {
	t.Helper()
	stats := objectio.NewObjectStats()
	require.NoError(t, objectio.SetObjectStatsObjectName(
		stats,
		objectio.BuildObjectName(objectio.NewSegmentid(), number),
	))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
	return objectio.ObjectEntry{
		ObjectStats: *stats,
		CreateTime:  types.BuildTS(1, 0),
	}
}
