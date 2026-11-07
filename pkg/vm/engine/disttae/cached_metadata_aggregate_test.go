// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/stretchr/testify/require"
)

func TestCachedMetadataRemainsImmutableDuringAggregation(t *testing.T) {
	f := newTemporalNativeFixture(t)
	e := f.base.origin.eng.(*Engine)
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(f.ctx) })
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.Base.FileService = fs
	f.read.Workspace.(*Transaction).proc = proc
	// Use the existing local partition path; the aggregation and cached
	// metadata access below are the same owners used by ordinary relations.
	f.base.origin.fake = true
	f.base.origin.relKind = "V"
	e.partitions = make(map[[2]uint64]*logtailreplay.Partition)
	part := e.GetOrCreateLatestPart(f.ctx, 7, 10, 98)
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	views := make([]objectio.CachedColumnMeta, 0, 2)
	originals := make([][objectio.ZoneMapSize]byte, 0, 2)
	for _, value := range []uint64{1, 100} {
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = vector.NewVec(types.New(types.T_uint64, 64, -1))
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
		bat.SetRowCount(1)
		writer := ioutil.ConstructWriter(0, []uint16{0}, 0, true, false, fs)
		_, err = writer.WriteBatch(bat)
		require.NoError(t, err)
		_, _, err = writer.Sync(f.ctx)
		require.NoError(t, err)
		bat.Clean(mp)
		stats := writer.GetObjectStats()
		location := stats.ObjectLocation()
		_, err = objectio.FastLoadObjectMeta(f.ctx, &location, false, fs)
		require.NoError(t, err)
		view, ok := objectio.TryCachedColumnMeta(f.ctx, location, 0, 1)
		require.True(t, ok)
		m, ok := view.Object()
		require.True(t, ok)
		views = append(views, view)
		originals = append(originals, m.ZoneMap)
		ps, release := part.MutateState()
		ps.UpdateDuration(types.TS{}, types.MaxTs())
		require.NoError(t, ps.HandleObjectEntry(f.ctx, nil, objectio.ObjectEntry{ObjectStats: stats, CreateTime: types.BuildTS(1, 0)}, false))
		release()
	}
	start := make(chan struct{})
	type aggregateResult struct {
		values [][2]any
		err    error
	}
	result := make(chan aggregateResult, 1)
	observed := make(chan [][objectio.ZoneMapSize]byte, 1)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		values, _, err := f.base.origin.MaxAndMinValues(f.ctx)
		result <- aggregateResult{values: values, err: err}
	}()
	go func() {
		defer wg.Done()
		<-start
		current := make([][objectio.ZoneMapSize]byte, 0, len(views))
		for _, view := range views {
			meta, ok := view.Object()
			if !ok {
				observed <- nil
				return
			}
			current = append(current, meta.ZoneMap)
		}
		observed <- current
	}()
	close(start)
	got, read := <-result, <-observed
	wg.Wait()
	require.NoError(t, got.err)
	require.Len(t, got.values, 1)
	require.Equal(t, uint64(1), got.values[0][0])
	require.Equal(t, uint64(100), got.values[0][1])
	require.Equal(t, originals, read, "the observer must read both unmodified cached zone maps")
	for i, view := range views {
		current, ok := view.Object()
		require.True(t, ok)
		require.Equal(t, originals[i], current.ZoneMap, "aggregation must not mutate the shared cache")
	}
}
