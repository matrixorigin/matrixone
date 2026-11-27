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

package logtail

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestCheckpointMetaZoneMapReads(t *testing.T) {
	ctx := context.Background()
	capacity := toml.ByteSize(8 << 20)
	fs, err := fileservice.NewS3FS(ctx,
		fileservice.ObjectStorageArguments{Name: "checkpoint", Endpoint: "disk", Bucket: t.TempDir()},
		fileservice.CacheConfig{MemoryCapacity: &capacity}, nil, false, false)
	require.NoError(t, err)
	fs.SetAsyncUpdate(false)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	ranges := []ckputil.TableRange{
		checkpointMetaRange(10, ckputil.ObjectType_Data, 0),
		checkpointMetaRange(10, ckputil.ObjectType_Tombstone, 1),
		checkpointMetaRange(20, ckputil.ObjectType_Data, 2),
	}
	loc := writeCheckpointMetaFixture(t, fs, mp, ranges)
	fs.FlushCache(ctx)
	// Count the column reads separately from the header required by both paths.
	_, err = objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		tid  uint64
		want []ckputil.TableRange
		gets int64
	}{
		{name: "below", tid: 0},
		{name: "above", tid: ^uint64(0)},
		{name: "gap", tid: 15, gets: 1},
		{name: "minimum", tid: 10, want: ranges[:2], gets: 2},
		{name: "maximum", tid: 20, want: ranges[2:], gets: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs.FlushCache(ctx)
			var counters perfcounter.CounterSet
			counted := perfcounter.WithCounterSet(ctx, &counters)
			for _, mode := range []string{"cold", "warm"} {
				reader := NewCKPReaderWithTableID_V2(CheckpointCurrentVersion, loc, tc.tid, mp, fs)
				require.NoError(t, reader.ReadMeta(counted), mode)
				got, err := reader.GetTableRanges(counted)
				require.NoError(t, err)
				if tc.want == nil {
					require.Empty(t, got)
				} else {
					require.Equal(t, tc.want, got)
				}
				require.Equal(t, tc.gets, counters.FileService.S3.Get.Load(), mode)
				if tc.gets == 0 {
					require.Zero(t, counters.FileService.S3ReadSize.Load(), mode)
				} else {
					require.Positive(t, counters.FileService.S3ReadSize.Load(), mode)
				}
			}
		})
	}
	t.Run("unprimed header", func(t *testing.T) {
		fresh := writeCheckpointMetaFixture(t, fs, mp, ranges)
		fs.FlushCache(ctx)
		var counters perfcounter.CounterSet
		counted := perfcounter.WithCounterSet(ctx, &counters)
		reader := NewCKPReaderWithTableID_V2(CheckpointCurrentVersion, fresh, 0, mp, fs)
		require.NoError(t, reader.ReadMeta(counted))
		require.EqualValues(t, 1, counters.FileService.S3.Get.Load()) // header only
	})
	t.Run("large metadata block", func(t *testing.T) {
		large := make([]ckputil.TableRange, objectio.BlockMaxRows+1)
		for i := range large {
			large[i] = checkpointMetaRange(10, ckputil.ObjectType_Data, i)
		}
		largeLoc := writeCheckpointMetaFixture(t, fs, mp, large)
		fs.FlushCache(ctx)
		_, err := objectio.FastLoadObjectMeta(ctx, &largeLoc, false, fs)
		require.NoError(t, err)
		var counters perfcounter.CounterSet
		counted := perfcounter.WithCounterSet(ctx, &counters)
		reader := NewCKPReaderWithTableID_V2(CheckpointCurrentVersion, largeLoc, 11, mp, fs)
		require.NoError(t, reader.ReadMeta(counted))
		require.Zero(t, counters.FileService.S3.Get.Load())
	})
	t.Run("concurrent subscribers", func(t *testing.T) {
		type result struct {
			tid    uint64
			ranges []ckputil.TableRange
			err    error
		}
		const subscribers, repetitions = 16, 20
		results := make(chan result, subscribers*repetitions)
		var workers sync.WaitGroup
		workers.Add(subscribers)
		for n := 0; n < subscribers; n++ {
			go func(n int) {
				defer workers.Done()
				for i := 0; i < repetitions; i++ {
					tid := []uint64{0, 10, 15, 20}[n%4]
					reader := NewCKPReaderWithTableID_V2(CheckpointCurrentVersion, loc, tid, mp, fs)
					err := reader.ReadMeta(ctx)
					var got []ckputil.TableRange
					if err == nil {
						got, err = reader.GetTableRanges(ctx)
					}
					results <- result{tid, got, err}
				}
			}(n)
		}
		workers.Wait()
		close(results)
		for got := range results {
			require.NoError(t, got.err)
			switch got.tid {
			case 10:
				require.Equal(t, ranges[:2], got.ranges)
			case 20:
				require.Equal(t, ranges[2:], got.ranges)
			default:
				require.Empty(t, got.ranges)
			}
		}
	})

	// A gap inside the zone map must not read unrelated payload columns.
	readErr := errors.New("injected metadata column read failure")
	wrapped := &checkpointMetaReadFS{FileService: fs, failSelected: true, failure: readErr}
	data, tombstone, err := readMetaWithTableID(ctx, loc, 15, mp, wrapped)
	require.NoError(t, err)
	require.Nil(t, data)
	require.Nil(t, tombstone)
	require.Zero(t, wrapped.selectedReads)
	wrapped.failIDs = true
	_, _, err = readMetaWithTableID(ctx, loc, 15, mp, wrapped)
	require.ErrorIs(t, err, readErr)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = readMetaWithTableID(canceled, loc, 0, mp, fs)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCheckpointMetaWithoutCacheAdmission(t *testing.T) {
	// Constrain metadata cache capacity and disable the FileService cache so
	// duplicate header reads reach storage.
	oldCapacity := objectio.GlobalCacheCapacityHint.Swap(1)
	t.Cleanup(func() {
		objectio.GlobalCacheCapacityHint.Store(oldCapacity)
	})
	ctx := context.Background()
	fs, err := fileservice.NewS3FS(ctx,
		fileservice.ObjectStorageArguments{Name: "checkpoint", Endpoint: "disk", Bucket: t.TempDir()},
		fileservice.DisabledCacheConfig, nil, false, false)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	ranges := []ckputil.TableRange{
		checkpointMetaRange(10, ckputil.ObjectType_Data, 0),
		checkpointMetaRange(20, ckputil.ObjectType_Tombstone, 1),
	}
	for _, tc := range []struct {
		name string
		tid  uint64
		gets int64
	}{
		{name: "outside", tid: 0, gets: 1}, // header only
		{name: "gap", tid: 15, gets: 2},    // header and table IDs
		{name: "hit", tid: 10, gets: 4},    // plus header and selected columns
	} {
		t.Run(tc.name, func(t *testing.T) {
			loc := writeCheckpointMetaFixture(t, fs, mp, ranges)
			var counters perfcounter.CounterSet
			counted := perfcounter.WithCounterSet(ctx, &counters)
			reader := NewCKPReaderWithTableID_V2(CheckpointCurrentVersion, loc, tc.tid, mp, fs)
			require.NoError(t, reader.ReadMeta(counted))
			got, err := reader.GetTableRanges(counted)
			require.NoError(t, err)
			if tc.tid == 10 {
				require.Equal(t, ranges[:1], got)
			} else {
				require.Empty(t, got)
			}
			require.Equal(t, tc.gets, counters.FileService.S3.Get.Load())
		})
	}
}

func TestCheckpointMetaInvalidZoneMapFallsBack(t *testing.T) {
	fs, _, mp := makeCheckpointMetaFixture(t, []ckputil.TableRange{checkpointMetaRange(1, ckputil.ObjectType_Data, 0)})
	want := checkpointMetaRange(10, ckputil.ObjectType_Data, 1)
	value, smaller := uint64(100), uint64(2)
	rightType := index.BuildZM(types.T_uint64, types.EncodeUint64(&value))
	shortBound := append(index.ZM(nil), rightType...)
	shortBound[30] = 1
	reversed := append(index.ZM(nil), rightType...)
	copy(reversed[31:], types.EncodeUint64(&smaller))
	wrongTypeValue := int64(100)
	for _, tc := range []struct {
		name string
		zm   index.ZM
	}{
		{name: "uninitialized"},
		{name: "wrong type", zm: index.BuildZM(types.T_int64, types.EncodeInt64(&wrongTypeValue))},
		{name: "short bound", zm: shortBound},
		{name: "reversed bounds", zm: reversed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bat := ckputil.NewMetaBatch(mp)
			defer bat.Clean(mp)
			require.NoError(t, want.AppendTo(bat, mp))
			name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
			writer, err := objectio.NewObjectWriter(name, fs, 0, ckputil.MetaSeqnums, nil)
			require.NoError(t, err)
			block, err := writer.Write(bat)
			require.NoError(t, err)
			block.ColumnMeta(ckputil.MetaSeqnums[0]).SetZoneMap(tc.zm)
			blocks, err := writer.WriteEnd(context.Background())
			require.NoError(t, err)
			loc := objectio.BuildLocation(name, blocks[0].GetExtent(), 0, blocks[0].GetID())
			data, tombstone, err := readMetaWithTableID(context.Background(), loc, 10, mp, fs)
			require.NoError(t, err)
			require.Equal(t, []ckputil.TableRange{want}, data)
			require.Nil(t, tombstone)
		})
	}
}

func BenchmarkCheckpointMetadataSelection(b *testing.B) {
	for _, matches := range []int{0, 1, 64, 4096} {
		b.Run(fmt.Sprintf("rows8192/matches%d", matches), func(b *testing.B) {
			ranges := make([]ckputil.TableRange, 8192)
			start := (len(ranges) - matches) / 2
			for i := range ranges {
				tid := uint64(1)
				if i >= start {
					tid = 2
				}
				if i >= start+matches {
					tid = 3
				}
				kind := ckputil.ObjectType_Data
				if i%2 != 0 {
					kind = ckputil.ObjectType_Tombstone
				}
				ranges[i] = checkpointMetaRange(tid, kind, i)
			}
			fs, loc, mp := makeCheckpointMetaFixture(b, ranges)
			ctx := context.Background()
			read := func() {
				data, tombstone, err := readMetaWithTableID(ctx, loc, 2, mp, fs)
				if err != nil || len(data)+len(tombstone) != matches {
					b.Fatalf("ranges=%d/%d err=%v", len(data), len(tombstone), err)
				}
			}
			read()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				read()
			}
		})
	}
}
