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

package ioutil

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestSpillBoundedRunsReadBack(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		for _, sortKey := range []int{0, -1} {
			for _, shape := range []string{"disjoint", "constant", "constant_short", "aliased"} {
				t.Run(fmt.Sprintf("pipeline=%t/sortKey=%d/%s", pipeline, sortKey, shape), func(t *testing.T) {
					ctx := context.Background()
					fs := testutil.NewSharedFS()
					t.Cleanup(func() { fs.Close(ctx) })
					mp := mpool.MustNewZero()
					attrs := []string{"id", "payload"}
					typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
					input := batch.NewWithSchema(false, attrs, typs)
					defer input.Clean(mp)
					const rows = 35
					payload := strings.Repeat("x", 200)
					valueForID := func(id int32) (string, bool) {
						if shape != "disjoint" {
							return payload, false
						}
						if id%11 == 0 {
							return "", true
						}
						if id%7 == 0 {
							return fmt.Sprintf("%d", id), false
						}
						return fmt.Sprintf("%d:%s", id, payload), false
					}
					for i := 0; i < rows; i++ {
						id := int32(rows - 1 - i)
						require.NoError(t, vector.AppendFixed(input.Vecs[0], id, false, mp))
						if shape == "disjoint" {
							value, isNull := valueForID(id)
							require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(value), isNull, mp))
						}
					}
					if shape != "disjoint" {
						constRows := rows
						if shape == "constant_short" {
							constRows = 1
						}
						constant, err := vector.NewConstBytes(typs[1], []byte(payload), constRows, mp)
						require.NoError(t, err)
						if shape == "constant" || shape == "constant_short" {
							input.Vecs[1].Free(mp)
							input.Vecs[1] = constant
						} else {
							require.NoError(t, input.Vecs[1].UnionBatch(constant, 0, rows, nil, mp))
							constant.Free(mp)
							require.False(t, input.Vecs[1].VarlenaAreaIsDisjoint())
						}
					}
					input.SetRowCount(rows)
					const limit = 1024
					opts := []SinkerOption{WithMemorySizeThreshold(limit), WithByteBoundedRuns(), WithTailSizeCap(0)}
					if pipeline {
						opts = append(opts, WithPipelineFlush())
					}
					sink := NewSinker(sortKey, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, sortKey, sortKey >= 0, false, 0), mp, fs, opts...)
					t.Cleanup(func() { require.NoError(t, sink.Close()) })
					require.NoError(t, sink.Write(ctx, input))
					require.NoError(t, sink.Sync(ctx))
					stats, tail := sink.GetResult()
					require.Empty(t, tail)
					require.Greater(t, len(stats), 1)
					seen := make(map[int32]bool)
					for _, stat := range stats {
						objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
							loc := objectio.Location(info.MetaLoc[:])
							reader, err := NewObjectReader(fs, loc)
							require.NoError(t, err)
							got, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
							require.NoError(t, err)
							defer release()
							require.Equal(t, int(loc.Rows()), got.RowCount(), "stats-only block row count must match persisted block")
							require.LessOrEqual(t, got.Size(), limit+len(payload)+64)
							ids := vector.MustFixedColNoTypeCheck[int32](got.Vecs[0])
							for i, id := range ids {
								if i > 0 && sortKey >= 0 {
									require.Less(t, ids[i-1], id)
								}
								require.False(t, seen[id], "duplicate row %d", id)
								seen[id] = true
								want, isNull := valueForID(id)
								require.Equal(t, isNull, got.Vecs[1].GetNulls().Contains(uint64(i)))
								if !isNull {
									require.Equal(t, want, string(got.Vecs[1].GetBytesAt(i)))
								}
							}
							return true
						}, stat)
					}
					require.Len(t, seen, rows)
					for i := int32(0); i < rows; i++ {
						require.True(t, seen[i])
					}
				})
			}
		}
	}
}

func TestSortedRunSizeAccountsForAliasesAndDeadArea(t *testing.T) {
	mp := mpool.MustNewZero()
	attrs := []string{"id", "dead", "alias"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType(), types.T_text.ToType()}
	input := batch.NewWithSchema(false, attrs, typs)
	defer input.Clean(mp)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(0), false, mp))
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(1), false, mp))
	require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("d", 1000)), false, mp))
	require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("short"), false, mp))
	require.NoError(t, vector.SetBytesAt(input.Vecs[1], 0, []byte("short"), mp))
	require.True(t, input.Vecs[1].VarlenaAreaIsDisjoint())
	require.Equal(t, 1000, len(input.Vecs[1].GetArea()))
	alias, err := vector.NewConstBytes(typs[2], []byte(strings.Repeat("a", 700)), 2, mp)
	require.NoError(t, err)
	defer alias.Free(mp)
	require.NoError(t, input.Vecs[2].UnionBatch(alias, 0, 2, nil, mp))
	input.SetRowCount(2)
	descriptors := 2 * (typs[0].TypeSize() + typs[1].TypeSize() + typs[2].TypeSize())
	require.Equal(t, descriptors+1000+1400, materializedBatchSize(input))
	require.NoError(t, input.Vecs[1].Shuffle([]int64{1, 0}, mp))
	require.NoError(t, input.Vecs[2].Shuffle([]int64{1, 0}, mp))
	require.Equal(t, descriptors+1000+1400, materializedBatchSize(input), "sorting cannot change accounting")
}

func TestWholeAliasedBatchSpillReadback(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		for _, sortKey := range []int{-1, 0} {
			t.Run(fmt.Sprintf("pipeline=%t/sortKey=%d", pipeline, sortKey), func(t *testing.T) {
				ctx := context.Background()
				fs := testutil.NewSharedFS()
				defer fs.Close(ctx)
				mp := mpool.MustNewZero()
				attrs := []string{"id", "payload"}
				typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
				input := batch.NewWithSchema(false, attrs, typs)
				defer input.Clean(mp)
				const rows = 32
				for i := 0; i < rows; i++ {
					require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
					value := fmt.Sprintf("row-%02d-%s", i, strings.Repeat("x", 128))
					if i == 7 {
						value = "inline"
					}
					require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(value), i == 5, mp))
				}
				sels := make([]int64, rows)
				for i := range sels {
					sels[i] = int64(rows - 1 - i)
				}
				require.NoError(t, input.Vecs[1].Shuffle(sels, mp))
				headers := vector.MustFixedColNoTypeCheck[types.Varlena](input.Vecs[1])
				require.NoError(t, vector.SetFixedAtNoTypeCheck(input.Vecs[1], 1, headers[0]))
				input.SetRowCount(rows)
				sizes := make([]int, len(typs))
				_, hasSize := materializedBatchSizeWithPayloadSizes(input, sizes)
				require.True(t, hasSize, "the full-window payload estimate must be available")
				want := make(map[int32]string, rows)
				nulls := make(map[int32]bool, rows)
				for i := 0; i < rows; i++ {
					id := vector.GetFixedAtNoTypeCheck[int32](input.Vecs[0], i)
					nulls[id] = input.Vecs[1].IsNull(uint64(i))
					if !nulls[id] {
						want[id] = string(input.Vecs[1].GetBytesAt(i))
					}
				}
				opts := []SinkerOption{WithMemorySizeThreshold(2 << 20), WithByteBoundedRuns(), WithTailSizeCap(0)}
				if pipeline {
					opts = append(opts, WithPipelineFlush())
				}
				sink := NewSinker(sortKey, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, sortKey, sortKey >= 0, false, 0), mp, fs, opts...)
				defer sink.Close()
				require.NoError(t, sink.Write(ctx, input))
				require.NoError(t, sink.Sync(ctx))
				stats, _ := sink.GetResult()
				require.Len(t, stats, 1)
				seen := 0
				objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
					loc := objectio.Location(info.MetaLoc[:])
					reader, err := NewObjectReader(fs, loc)
					require.NoError(t, err)
					got, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
					require.NoError(t, err)
					defer release()
					for i, id := range vector.MustFixedColNoTypeCheck[int32](got.Vecs[0]) {
						require.Equal(t, nulls[id], got.Vecs[1].IsNull(uint64(i)))
						if !nulls[id] {
							require.Equal(t, want[id], string(got.Vecs[1].GetBytesAt(i)))
						}
						seen++
					}
					return true
				}, stats[0])
				require.Equal(t, rows, seen)
			})
		}
	}
}

func TestSortedRunAvoidsCopyingDeadSingleRowArea(t *testing.T) {
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	input := batch.NewWithSchema(false, attrs, typs)
	defer input.Clean(mp)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(1), false, mp))
	require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 4096)), false, mp))
	require.NoError(t, vector.SetBytesAt(input.Vecs[1], 0, []byte("ok"), mp))
	input.SetRowCount(1)
	sink := NewSinker(0, attrs, typs, nil, mp, nil, WithMemorySizeThreshold(256), WithByteBoundedRuns())
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	require.NoError(t, sink.Write(context.Background(), input))
	require.Len(t, sink.staged.inMemory, 1)
	got := sink.staged.inMemory[0]
	require.Equal(t, "ok", string(got.Vecs[1].GetBytesAt(0)))
	require.Empty(t, got.Vecs[1].GetArea(), "a single short value must not copy unused input area")
	require.Less(t, sink.StagedSize(), 256)
}

func TestSortedRunOversizedRowAndRepeatedWrites(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	file := &trackingStreamSinker{}
	var objectRows []int
	factory := func(*mpool.MPool, fileservice.FileService) FileSinker { return file }
	sink := NewSinker(0, attrs, typs, factory, mp, nil, WithMemorySizeThreshold(256), WithByteBoundedRuns(), WithTailSizeCap(0))
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	for i, value := range []string{"small", strings.Repeat("x", 2048), "small"} {
		input := batch.NewWithSchema(false, attrs, typs)
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(value), false, mp))
		input.SetRowCount(1)
		before := file.rows
		err := sink.Write(ctx, input)
		input.Clean(mp)
		require.NoError(t, err)
		if file.rows > before {
			objectRows = append(objectRows, file.rows-before)
		}
	}
	before := file.rows
	require.NoError(t, sink.Sync(ctx))
	if file.rows > before {
		objectRows = append(objectRows, file.rows-before)
	}
	require.Equal(t, 3, file.rows)
	require.Equal(t, []int{2, 1}, objectRows, "one large row may exceed the soft limit, but the next row is in a new run")
}

func TestSortedRunWriteOwnedBudget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		limit     int
		constRows int
		wantOwned bool
	}{
		{"fits", 2 << 20, objectio.BlockMaxRows, true},
		{"split", 512 << 10, objectio.BlockMaxRows, false},
		{"short constant fits", 2 << 20, 1, true},
		{"short constant splits", 512 << 10, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mp := mpool.MustNewZero()
			attrs := []string{"id", "payload"}
			typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
			input := batch.NewWithSchema(false, attrs, typs)
			for i := 0; i < objectio.BlockMaxRows; i++ {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
			}
			input.Vecs[1].Free(mp)
			var err error
			input.Vecs[1], err = vector.NewConstBytes(typs[1], []byte(strings.Repeat("x", 128)), tc.constRows, mp)
			require.NoError(t, err)
			input.SetRowCount(objectio.BlockMaxRows)
			file := &trackingStreamSinker{}
			sink := NewSinker(0, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
				WithMemorySizeThreshold(tc.limit), WithByteBoundedRuns(), WithTailSizeCap(0))
			t.Cleanup(func() { require.NoError(t, sink.Close()) })
			owned, err := sink.WriteOwned(ctx, input)
			if !owned {
				defer input.Clean(mp)
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantOwned, owned)
			if !owned {
				require.Equal(t, objectio.BlockMaxRows, input.RowCount())
			}
			require.NoError(t, sink.Sync(ctx))
			require.Equal(t, objectio.BlockMaxRows, file.rows)
		})
	}
}

func TestSortedRunFailureReleasesBoundsAndCanRetry(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	input := batch.NewWithSchema(false, attrs, typs)
	defer input.Clean(mp)
	for i := 0; i < 8; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 128)), false, mp))
	}
	input.SetRowCount(8)
	failure := errors.New("injected sorted-run sink failure")
	file := &trackingStreamSinker{err: failure}
	sink := NewSinker(0, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
		WithMemorySizeThreshold(256), WithByteBoundedRuns(), WithTailSizeCap(0))
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	require.ErrorIs(t, sink.Write(ctx, input), failure)
	require.Empty(t, sink.staged.inMemory)
	require.Empty(t, sink.staged.inMemoryBounds)
	require.Zero(t, sink.StagedSize())
	sink.Reset()
	file.err = nil
	require.NoError(t, sink.Write(ctx, input))
	require.NoError(t, sink.Sync(ctx))
	require.Equal(t, 8, file.rows)
	require.Empty(t, sink.staged.inMemoryBounds)
}

type panicSortedRunSink struct {
	trackingStreamSinker
	panicOnSink bool
}

func (s *panicSortedRunSink) Sink(ctx context.Context, b *batch.Batch) error {
	if s.panicOnSink {
		panic("injected writer panic")
	}
	return s.trackingStreamSinker.Sink(ctx, b)
}

func TestSortedRunPanicReleasesBuffersAndCanRetry(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	input := batch.NewWithSchema(false, attrs, typs)
	defer input.Clean(mp)
	for i := 0; i < 4; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 128)), false, mp))
	}
	input.SetRowCount(4)
	file := &panicSortedRunSink{panicOnSink: true}
	sink := NewSinker(0, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
		WithOffHeap(), WithMemorySizeThreshold(256), WithByteBoundedRuns(), WithTailSizeCap(0))
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	require.PanicsWithValue(t, "injected writer panic", func() { _ = sink.Write(ctx, input) })
	require.Empty(t, sink.staged.inMemory)
	require.Empty(t, sink.staged.inMemoryBounds)
	sink.Reset()
	file.panicOnSink = false
	require.NoError(t, sink.Write(ctx, input))
	require.NoError(t, sink.Sync(ctx))
	require.Equal(t, 4, file.rows)
	require.NoError(t, sink.Close())
	require.Zero(t, mp.CurrNB())
}

func TestRunKeepsFullBlocksBeforeLastPartial(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		for _, sortKey := range []int{0, -1} {
			t.Run(fmt.Sprintf("pipeline=%t/sortKey=%d", pipeline, sortKey), func(t *testing.T) {
				ctx := context.Background()
				fs := testutil.NewSharedFS()
				t.Cleanup(func() { fs.Close(ctx) })
				mp := mpool.MustNewZero()
				attrs := []string{"id", "payload"}
				typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
				input := batch.NewWithSchema(false, attrs, typs)
				defer input.Clean(mp)
				for i := 0; i < objectio.BlockMaxRows; i++ {
					require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
					require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("x"), false, mp))
				}
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(objectio.BlockMaxRows), false, mp))
				large := strings.Repeat("z", 300<<10)
				require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(large), false, mp))
				input.SetRowCount(objectio.BlockMaxRows + 1)
				opts := []SinkerOption{WithMemorySizeThreshold(256 << 10), WithByteBoundedRuns(), WithTailSizeCap(0)}
				if pipeline {
					opts = append(opts, WithPipelineFlush())
				}
				sink := NewSinker(sortKey, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, sortKey, sortKey >= 0, false, 0), mp, fs, opts...)
				t.Cleanup(func() { require.NoError(t, sink.Close()) })
				require.NoError(t, sink.Write(ctx, input))
				require.NoError(t, sink.Sync(ctx))
				stats, _ := sink.GetResult()
				require.Len(t, stats, 1)
				require.Equal(t, uint32(2), stats[0].BlkCnt())
				var blockRows []int
				objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
					loc := objectio.Location(info.MetaLoc[:])
					reader, err := NewObjectReader(fs, loc)
					require.NoError(t, err)
					got, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
					require.NoError(t, err)
					defer release()
					require.Equal(t, int(loc.Rows()), got.RowCount())
					blockRows = append(blockRows, got.RowCount())
					if loc.ID() == 1 {
						require.Equal(t, large, string(got.Vecs[1].GetBytesAt(0)))
					}
					return true
				}, stats...)
				require.Equal(t, []int{objectio.BlockMaxRows, 1}, blockRows)
			})
		}
	}
}

type sortedRunFailureFS struct {
	fileservice.FileService
	writes            atomic.Int32
	deletes           atomic.Int32
	failAt            int32
	writeErr          error
	deleteErr         error
	cleanupContextErr error
	cancel            context.CancelFunc
	panicOnFailure    bool
	firstDone         chan struct{}
	failGate          <-chan struct{}
	firstPath         string
	deletedPaths      []string
}

func (f *sortedRunFailureFS) Write(ctx context.Context, v fileservice.IOVector) error {
	n := f.writes.Add(1)
	if n == f.failAt {
		<-f.firstDone // The first object must really exist before this failure.
		if f.failGate != nil {
			<-f.failGate
		}
		if f.panicOnFailure {
			panic("injected second object panic")
		}
		if f.cancel != nil {
			f.cancel()
			return context.Canceled
		}
		return f.writeErr
	}
	err := f.FileService.Write(ctx, v)
	if n == 1 {
		f.firstPath = v.FilePath
		close(f.firstDone)
	}
	return err
}

func (f *sortedRunFailureFS) Delete(ctx context.Context, paths ...string) error {
	f.deletes.Add(1)
	f.deletedPaths = append(f.deletedPaths, paths...)
	f.cleanupContextErr = ctx.Err()
	if f.deleteErr != nil {
		return f.deleteErr
	}
	return f.FileService.Delete(ctx, paths...)
}

func TestSortedRunFailureCleansUnpublishedObjects(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		pipeline, cancel, deleteFails, panics bool
	}{
		{name: "sync"}, {name: "pipeline", pipeline: true},
		{name: "cancel", cancel: true}, {name: "delete failure", deleteFails: true},
		{name: "panic", panics: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			base := testutil.NewSharedFS()
			t.Cleanup(func() { base.Close(context.Background()) })
			fs := &sortedRunFailureFS{FileService: base, failAt: 2, firstDone: make(chan struct{}),
				writeErr: moerr.NewInternalErrorNoCtx("injected second object failure"), panicOnFailure: tc.panics}
			if tc.cancel {
				fs.cancel = cancel
			}
			if tc.deleteFails {
				fs.deleteErr = errors.New("injected cleanup failure")
			}
			mp := mpool.MustNewZero()
			attrs := []string{"id", "payload"}
			typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
			input := batch.NewWithSchema(false, attrs, typs)
			defer input.Clean(mp)
			for i := 0; i < 8; i++ {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
				require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 128)), false, mp))
			}
			input.SetRowCount(8)
			opts := []SinkerOption{WithMemorySizeThreshold(256), WithByteBoundedRuns(), WithUnpublishedRunCleanup(), WithTailSizeCap(0)}
			if tc.pipeline {
				opts = append(opts, WithPipelineFlush())
			}
			sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs, opts...)
			t.Cleanup(func() { _ = sink.Close() })
			var err error
			write := func() {
				err = sink.Write(ctx, input)
				if err == nil {
					err = sink.Sync(ctx)
				}
			}
			if tc.panics {
				require.PanicsWithValue(t, "injected second object panic", write)
			} else {
				write()
				require.Error(t, err)
				if !tc.cancel {
					require.Same(t, fs.writeErr, err, "cleanup must preserve the SQL error type and code")
				}
			}
			require.Positive(t, fs.deletes.Load())
			require.Contains(t, fs.deletedPaths, fs.firstPath)
			require.NoError(t, fs.cleanupContextErr, "cleanup must survive cancellation of the SQL context")
			files, err := fileservice.SortedList(base.List(context.Background(), ""))
			require.NoError(t, err)
			if tc.deleteFails {
				require.NotEmpty(t, files)
				require.NotEmpty(t, sink.staged.persisted)
				require.Error(t, sink.Sync(context.Background()), "an aborted run must not be published")
				sink.Reset()
				require.Error(t, sink.Write(context.Background(), input), "Reset must retain failed cleanup ownership")
				fs.deleteErr = nil
				count, err := sink.DeletePersisted(context.Background())
				require.NoError(t, err)
				require.Positive(t, count)
				sink.Reset()
				files, err = fileservice.SortedList(base.List(context.Background(), ""))
				require.NoError(t, err)
				require.Empty(t, files)
			} else {
				require.Empty(t, files)
			}
			require.Empty(t, sink.staged.persisted)
			if sink.pipe.result != nil {
				require.Empty(t, sink.pipe.result.persisted)
			}
		})
	}
}

func TestSortedRunFailureKeepsPreviouslyReturnedObjects(t *testing.T) {
	ctx := context.Background()
	base := testutil.NewSharedFS()
	t.Cleanup(func() { base.Close(ctx) })
	fs := &sortedRunFailureFS{FileService: base, failAt: 3, firstDone: make(chan struct{}), writeErr: moerr.NewInternalErrorNoCtx("injected second object failure")}
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	input := batch.NewWithSchema(false, attrs, typs)
	defer input.Clean(mp)
	for i := 0; i < 4; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 128)), false, mp))
	}
	input.SetRowCount(4)
	sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs,
		WithMemorySizeThreshold(1<<20), WithByteBoundedRuns(), WithUnpublishedRunCleanup(), WithTailSizeCap(0))
	t.Cleanup(func() { _ = sink.Close() })
	require.NoError(t, sink.Write(ctx, input))
	require.NoError(t, sink.Sync(ctx))
	before, _ := sink.GetResult()
	require.Len(t, before, 1)
	previousName := before[0].ObjectName().String()
	sink.staged.memorySizeThreshold = 256
	err := sink.Write(ctx, input)
	require.ErrorContains(t, err, "injected second object failure")
	_, err = base.StatFile(ctx, previousName)
	require.NoError(t, err, "an object returned by a prior Sync belongs to the caller")
	files, err := fileservice.SortedList(base.List(ctx, ""))
	require.NoError(t, err)
	require.Len(t, files, 1)
}

func TestSortedRunExcludedModesKeepSingleObject(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sortKey int
		option  SinkerOption
	}{
		{name: "all merged", sortKey: 0, option: WithAllMergeSorted()},
		{name: "dedup", sortKey: 0, option: WithDedupAll()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mp := mpool.MustNewZero()
			attrs := []string{"id", "payload"}
			typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
			input := batch.NewWithSchema(false, attrs, typs)
			defer input.Clean(mp)
			for i := 0; i < 4; i++ {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
				require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 128)), false, mp))
			}
			input.SetRowCount(4)
			file := &trackingStreamSinker{}
			opts := []SinkerOption{WithMemorySizeThreshold(256), WithByteBoundedRuns(), WithTailSizeCap(0)}
			if tc.option != nil {
				opts = append(opts, tc.option)
			}
			sink := NewSinker(tc.sortKey, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil, opts...)
			t.Cleanup(func() { require.NoError(t, sink.Close()) })
			require.NoError(t, sink.Write(ctx, input))
			require.Equal(t, 4, file.rows, "excluded modes retain the existing whole-batch spill")
			require.NoError(t, sink.Sync(ctx))
			stats, _ := sink.GetResult()
			require.Len(t, stats, 1)
			require.Equal(t, 4, file.rows)
		})
	}
}

func TestWriteOwnedConstantPayloadReusedBuffer(t *testing.T) {
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs,
		WithMemorySizeThreshold(128<<20), WithByteBoundedRuns(), WithTailSizeCap(0))
	defer sink.Close()
	first := batch.NewWithSchema(false, attrs, typs)
	for i := 0; i < objectio.BlockMaxRows; i++ {
		require.NoError(t, vector.AppendFixed(first.Vecs[0], int32(i), false, mp))
	}
	first.Vecs[1].Free(mp)
	var err error
	first.Vecs[1], err = vector.NewConstBytes(typs[1], []byte(strings.Repeat("a", 80)), 1, mp)
	require.NoError(t, err)
	first.SetRowCount(objectio.BlockMaxRows)
	owned, err := sink.WriteOwned(ctx, first)
	require.NoError(t, err)
	require.True(t, owned)
	require.NoError(t, sink.Sync(ctx))
	stats, _ := sink.GetResult()
	require.Len(t, stats, 1)
	firstRows := 0
	objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
		loc := objectio.Location(info.MetaLoc[:])
		reader, err := NewObjectReader(fs, loc)
		require.NoError(t, err)
		bat, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
		require.NoError(t, err)
		defer release()
		ids := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0])
		for i, id := range ids {
			require.Equal(t, int32(firstRows), id)
			require.Equal(t, strings.Repeat("a", 80), string(bat.Vecs[1].GetBytesAt(i)))
			firstRows++
		}
		return true
	}, stats[0])
	require.Equal(t, objectio.BlockMaxRows, firstRows)

	second := batch.NewWithSchema(false, attrs, typs)
	defer second.Clean(mp)
	for i := 0; i < objectio.BlockMaxRows+2; i++ {
		require.NoError(t, vector.AppendFixed(second.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendBytes(second.Vecs[1], []byte(fmt.Sprintf("%05d-%s", i, strings.Repeat("b", 40))), false, mp))
	}
	second.SetRowCount(objectio.BlockMaxRows + 2)
	require.NoError(t, sink.Write(ctx, second))
	require.NoError(t, sink.Sync(ctx))
	stats, _ = sink.GetResult()
	require.Len(t, stats, 2)
	gotByID := make(map[int32]string)
	objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
		loc := objectio.Location(info.MetaLoc[:])
		reader, err := NewObjectReader(fs, loc)
		require.NoError(t, err)
		bat, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
		require.NoError(t, err)
		defer release()
		ids := vector.MustFixedColNoTypeCheck[int32](bat.Vecs[0])
		for i, id := range ids {
			gotByID[id] = string(bat.Vecs[1].GetBytesAt(i))
		}
		return true
	}, stats[1])
	require.Len(t, gotByID, objectio.BlockMaxRows+2)
	want := fmt.Sprintf("%05d-%s", objectio.BlockMaxRows+1, strings.Repeat("b", 40))
	got := gotByID[objectio.BlockMaxRows+1]
	require.Equal(t, want, got)
}

func TestWriteOwnedNormalizesColumnLengths(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		for _, sortKey := range []int{-1, 0} {
			for _, shape := range []string{"constant_first", "constant_last", "extra_first", "extra_last"} {
				if sortKey >= 0 && strings.HasPrefix(shape, "constant") {
					continue
				}
				t.Run(fmt.Sprintf("pipeline=%t/sortKey=%d/%s", pipeline, sortKey, shape), func(t *testing.T) {
					ctx := context.Background()
					fs := testutil.NewSharedFS()
					defer fs.Close(ctx)
					mp := mpool.MustNewZero()
					attrs := []string{"id", "payload"}
					typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
					input := batch.NewWithSchema(false, attrs, typs)
					defer func() {
						if input != nil {
							input.Clean(mp)
						}
					}()
					for i := 0; i < objectio.BlockMaxRows; i++ {
						if shape != "constant_first" {
							require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
						}
						if shape != "constant_last" {
							require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(fmt.Sprintf("row-%05d", i)), false, mp))
						}
					}
					if shape == "constant_first" {
						input.Vecs[0].Free(mp)
						var err error
						input.Vecs[0], err = vector.NewConstFixed(typs[0], int32(7), 1, mp)
						require.NoError(t, err)
					}
					if shape == "constant_last" {
						input.Vecs[1].Free(mp)
						var err error
						input.Vecs[1], err = vector.NewConstBytes(typs[1], []byte(strings.Repeat("x", 40)), 1, mp)
						require.NoError(t, err)
					}
					if shape == "extra_first" {
						require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(-1), false, mp))
					}
					if shape == "extra_last" {
						require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("extra"), false, mp))
					}
					input.SetRowCount(objectio.BlockMaxRows)
					opts := []SinkerOption{WithMemorySizeThreshold(4 << 20), WithByteBoundedRuns(), WithTailSizeCap(0)}
					if pipeline {
						opts = append(opts, WithPipelineFlush())
					}
					sink := NewSinker(sortKey, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, sortKey, sortKey >= 0, false, 0), mp, fs, opts...)
					defer sink.Close()
					owned, err := sink.WriteOwned(ctx, input)
					if owned {
						input = nil
					}
					require.NoError(t, err)
					require.False(t, owned, "column lengths require a normalized copy")
					require.NoError(t, sink.Sync(ctx))
					stats, _ := sink.GetResult()
					require.Len(t, stats, 1)
					seen := 0
					objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
						loc := objectio.Location(info.MetaLoc[:])
						require.Equal(t, uint32(objectio.BlockMaxRows), loc.Rows())
						reader, err := NewObjectReader(fs, loc)
						require.NoError(t, err)
						for col := range typs {
							got, release, err := reader.LoadColumns(ctx, []uint16{uint16(col)}, []types.Type{typs[col]}, loc.ID(), mp)
							require.NoError(t, err)
							require.Equal(t, objectio.BlockMaxRows, got.RowCount())
							for i := 0; i < got.RowCount(); i++ {
								if col == 0 {
									want := int32(i)
									if shape == "constant_first" {
										want = 7
									}
									require.Equal(t, want, vector.GetFixedAtNoTypeCheck[int32](got.Vecs[0], i))
								} else {
									want := fmt.Sprintf("row-%05d", i)
									if shape == "constant_last" {
										want = strings.Repeat("x", 40)
									}
									require.Equal(t, want, string(got.Vecs[0].GetBytesAt(i)))
								}
							}
							release()
						}
						seen += int(loc.Rows())
						return true
					}, stats[0])
					require.Equal(t, objectio.BlockMaxRows, seen)
				})
			}
		}
	}
}

func TestWriteOwnedRejectsShortColumn(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	input := batch.NewWithSchema(false, attrs, typs)
	defer input.Clean(mp)
	for i := 0; i < objectio.BlockMaxRows+1; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
		if i < objectio.BlockMaxRows-1 {
			require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("v"), false, mp))
		}
	}
	input.SetRowCount(objectio.BlockMaxRows)
	sink := NewSinker(-1, attrs, typs, nil, mp, nil, WithByteBoundedRuns())
	defer sink.Close()
	owned, err := sink.WriteOwned(ctx, input)
	require.False(t, owned)
	require.ErrorContains(t, err, "spill column 1 has 8191 rows, batch has 8192")
	require.Zero(t, sink.StagedSize())
}

func TestWriteOwnedBorrowedVectorsAreCopied(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	parent := batch.NewWithSchema(false, attrs, typs)
	defer parent.Clean(mp)
	for i := 0; i < objectio.BlockMaxRows; i++ {
		require.NoError(t, vector.AppendFixed(parent.Vecs[0], int32(objectio.BlockMaxRows-i), false, mp))
		require.NoError(t, vector.AppendBytes(parent.Vecs[1], []byte(fmt.Sprintf("value-%05d", i)), false, mp))
	}
	parent.SetRowCount(objectio.BlockMaxRows)
	view := batch.NewWithSize(len(attrs))
	view.Attrs = attrs
	for i := range view.Vecs {
		var err error
		view.Vecs[i], err = parent.Vecs[i].Window(0, objectio.BlockMaxRows)
		require.NoError(t, err)
	}
	view.SetRowCount(objectio.BlockMaxRows)
	defer view.Clean(mp)
	file := &trackingStreamSinker{}
	sink := NewSinker(0, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
		WithMemorySizeThreshold(1<<20), WithByteBoundedRuns(), WithTailSizeCap(0))
	defer sink.Close()
	owned, err := sink.WriteOwned(ctx, view)
	require.NoError(t, err)
	require.False(t, owned)
	require.NoError(t, sink.Sync(ctx))
	require.Equal(t, objectio.BlockMaxRows, file.rows)
	ids := vector.MustFixedColNoTypeCheck[int32](parent.Vecs[0])
	require.Equal(t, int32(objectio.BlockMaxRows), ids[0])
	require.Equal(t, int32(1), ids[len(ids)-1])
	require.Equal(t, "value-00000", string(parent.Vecs[1].GetBytesAt(0)))
}

func TestFetchBufferDetachesAccountedBorrowedVectors(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(16 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(account, 1, 1, 2, 3, 4)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	source := batch.NewWithSchema(true, attrs, typs)
	require.NoError(t, source.SetAllocationAccount(selection))
	for i := 0; i < 4; i++ {
		require.NoError(t, vector.AppendFixed(source.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendBytes(source.Vecs[1], []byte(fmt.Sprintf("source-%d", i)), i == 1, mp))
	}
	source.SetRowCount(4)
	require.NoError(t, source.Vecs[1].PreExtendGrouping(4, mp))
	source.Vecs[1].GetGrouping().Add(2)
	view := batch.NewWithSize(len(attrs))
	view.Attrs = attrs
	for i := range view.Vecs {
		view.Vecs[i], err = source.Vecs[i].WindowWithAllocation(0, 4, mp, selection)
		require.NoError(t, err)
	}
	view.SetRowCount(4)
	buffer := containers.NewBatchFreeList(attrs, typs, true)
	buffer.Putback(view, mp)
	sink := NewSinker(0, attrs, typs, nil, mp, nil, WithBuffer(buffer, false), WithByteBoundedRuns())
	input := batch.NewWithSchema(false, attrs, typs)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(9), false, mp))
	require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("new"), false, mp))
	input.SetRowCount(1)
	require.NoError(t, sink.Write(context.Background(), input))
	require.Same(t, selection, sink.staged.inMemory[0].Vecs[1].AllocationAccountSelection())
	require.Equal(t, "source-0", string(source.Vecs[1].GetBytesAt(0)))
	require.True(t, source.Vecs[1].GetNulls().Contains(1))
	require.True(t, source.Vecs[1].GetGrouping().Contains(2))
	require.NoError(t, sink.Close())
	buffer.Close(mp)
	input.Clean(mp)
	source.Clean(mp)
	require.Zero(t, account.Seal().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestBoundedPartialRefillPreservesValuesAndAccounting(t *testing.T) {
	for _, constant := range []bool{false, true} {
		t.Run(fmt.Sprintf("constant=%t", constant), func(t *testing.T) {
			ctx := context.Background()
			fs := testutil.NewSharedFS()
			defer fs.Close(ctx)
			mp := mpool.MustNewZero()
			attrs := []string{"id", "payload"}
			typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
			sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs,
				WithMemorySizeThreshold(4<<20), WithByteBoundedRuns(), WithTailSizeCap(0))
			defer sink.Close()
			payload := strings.Repeat("x", 128)
			for chunk := 0; chunk < 2; chunk++ {
				input := batch.NewWithSchema(false, attrs, typs)
				for i := 0; i < 5000; i++ {
					id := chunk*5000 + i
					require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(id), false, mp))
					if !constant {
						require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(fmt.Sprintf("%d:%s", id, payload)), false, mp))
					}
				}
				if constant {
					input.Vecs[1].Free(mp)
					var err error
					constRows := 5000
					if chunk == 1 {
						constRows = 1
					}
					input.Vecs[1], err = vector.NewConstBytes(typs[1], []byte(payload), constRows, mp)
					require.NoError(t, err)
				}
				input.SetRowCount(5000)
				require.NoError(t, sink.Write(ctx, input))
				input.Clean(mp)
			}
			require.Len(t, sink.staged.inMemory, 2)
			for i, staged := range sink.staged.inMemory {
				logical := staged.RowCount() * (typs[0].TypeSize() + typs[1].TypeSize())
				for row := 0; row < staged.RowCount(); row++ {
					logical += len(staged.Vecs[1].GetBytesAt(row))
				}
				require.GreaterOrEqual(t, sink.staged.inMemoryBounds[i], logical)
				require.GreaterOrEqual(t, sink.staged.inMemoryBounds[i], staged.Size())
			}
			require.NoError(t, sink.Sync(ctx))
			stats, _ := sink.GetResult()
			seen := 0
			objectio.ForeachBlkInObjStatsList(false, nil, func(info objectio.BlockInfo, _ objectio.BlockObject) bool {
				loc := objectio.Location(info.MetaLoc[:])
				reader, err := NewObjectReader(fs, loc)
				require.NoError(t, err)
				got, release, err := reader.LoadColumns(ctx, []uint16{0, 1}, typs, loc.ID(), mp)
				require.NoError(t, err)
				defer release()
				for row, id := range vector.MustFixedColNoTypeCheck[int32](got.Vecs[0]) {
					want := payload
					if !constant {
						want = fmt.Sprintf("%d:%s", id, payload)
					}
					require.Equal(t, want, string(got.Vecs[1].GetBytesAt(row)))
					seen++
				}
				return true
			}, stats...)
			require.Equal(t, 10000, seen)
		})
	}
}

func TestFailedRunCloseRetainsOwnershipUntilDeleteSucceeds(t *testing.T) {
	ctx := context.Background()
	base := testutil.NewSharedFS()
	defer base.Close(ctx)
	fs := &sortedRunFailureFS{FileService: base, failAt: 3, firstDone: make(chan struct{}),
		writeErr: moerr.NewInternalErrorNoCtx("injected second run failure")}
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	input := batch.NewWithSchema(false, attrs, typs)
	defer input.Clean(mp)
	for i := 0; i < 4; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 128)), false, mp))
	}
	input.SetRowCount(4)
	sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs,
		WithMemorySizeThreshold(1<<20), WithByteBoundedRuns(), WithUnpublishedRunCleanup(), WithTailSizeCap(0))
	require.NoError(t, sink.Write(ctx, input))
	require.NoError(t, sink.Sync(ctx))
	previous, _ := sink.GetResult()
	require.Len(t, previous, 1)
	previousName := previous[0].ObjectName().String()
	sink.staged.memorySizeThreshold = 256
	fs.deleteErr = errors.New("temporary delete failure")
	require.ErrorIs(t, sink.Write(ctx, input), fs.writeErr)
	input.Clean(mp)
	require.Error(t, sink.Close())
	require.Zero(t, mp.CurrNB(), "failed cleanup must still release the buffer pool")
	onHeapBytes, onHeapObjects := mp.OnHeapOutstanding()
	require.Zero(t, onHeapBytes, "failed cleanup must release on-heap buffers")
	require.Zero(t, onHeapObjects, "failed cleanup must release on-heap allocations")
	require.Nil(t, sink.buf.buffers)
	require.Nil(t, sink.fSinker.executor)
	require.NotEmpty(t, sink.staged.persisted)
	require.Error(t, sink.Sync(ctx))
	_, err := base.StatFile(ctx, previousName)
	require.NoError(t, err)
	fs.deleteErr = nil
	require.NoError(t, sink.Close())
	files, err := fileservice.SortedList(base.List(ctx, ""))
	require.NoError(t, err)
	require.Len(t, files, 1)
	_, err = base.StatFile(ctx, previousName)
	require.NoError(t, err, "a prior returned object must survive failed-run cleanup")
	sink.Reset()
	require.Error(t, sink.Write(ctx, input), "a closed sinker must not be reopened by Reset")
}

func TestCloseAndResetCleanUnpublishedRuns(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		for _, reset := range []bool{false, true} {
			t.Run(fmt.Sprintf("pipeline=%t/reset=%t", pipeline, reset), func(t *testing.T) {
				ctx := context.Background()
				fs := testutil.NewSharedFS()
				defer fs.Close(ctx)
				mp := mpool.MustNewZero()
				attrs := []string{"id", "payload"}
				typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
				input := batch.NewWithSchema(false, attrs, typs)
				defer input.Clean(mp)
				for i := 0; i < 8; i++ {
					require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
					require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 128)), false, mp))
				}
				input.SetRowCount(8)
				opts := []SinkerOption{WithMemorySizeThreshold(1 << 20), WithByteBoundedRuns(), WithUnpublishedRunCleanup(), WithTailSizeCap(0)}
				if pipeline {
					opts = append(opts, WithPipelineFlush())
				}
				sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs, opts...)
				defer sink.Close()
				require.NoError(t, sink.Write(ctx, input))
				require.NoError(t, sink.Sync(ctx))
				previous, _ := sink.GetResult()
				require.Len(t, previous, 1)
				previousName := previous[0].ObjectName().String()
				sink.staged.memorySizeThreshold = 256
				require.NoError(t, sink.Write(ctx, input))
				if pipeline {
					require.NoError(t, sink.drainPipeline())
				}
				files, err := fileservice.SortedList(fs.List(ctx, ""))
				require.NoError(t, err)
				require.Greater(t, len(files), 1)
				if reset {
					sink.Reset()
				} else {
					require.NoError(t, sink.Close())
				}
				files, err = fileservice.SortedList(fs.List(ctx, ""))
				require.NoError(t, err)
				require.Len(t, files, 1)
				_, err = fs.StatFile(ctx, previousName)
				require.NoError(t, err, "already returned results must survive")
			})
		}
	}
}

func TestCloseCleansAfterAsynchronousWorkerFailure(t *testing.T) {
	ctx := context.Background()
	base := testutil.NewSharedFS()
	defer base.Close(ctx)
	gate := make(chan struct{})
	fs := &sortedRunFailureFS{FileService: base, failAt: 2, firstDone: make(chan struct{}), failGate: gate,
		writeErr: moerr.NewInternalErrorNoCtx("injected asynchronous failure"), deleteErr: errors.New("injected delete failure")}
	mp := mpool.MustNewZero()
	attrs := []string{"id", "payload"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	input := batch.NewWithSchema(false, attrs, typs)
	defer input.Clean(mp)
	for i := 0; i < 8; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 128)), false, mp))
	}
	input.SetRowCount(8)
	sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs,
		WithMemorySizeThreshold(256), WithByteBoundedRuns(), WithUnpublishedRunCleanup(), WithPipelineFlush(), WithTailSizeCap(0))
	require.NoError(t, sink.Write(ctx, input))
	close(gate)
	require.Same(t, fs.writeErr, sink.drainPipeline())
	require.False(t, sink.staged.aborted)
	require.Same(t, fs.writeErr, sink.Close(), "cleanup must not replace the pipeline error")
	require.NotEmpty(t, sink.staged.persisted)
	fs.deleteErr = nil
	require.NoError(t, sink.Close())
	files, err := fileservice.SortedList(base.List(ctx, ""))
	require.NoError(t, err)
	require.Empty(t, files)
}

type cancelBlockingFS struct {
	fileservice.FileService
	writes    atomic.Int32
	firstDone chan struct{}
	blocked   chan struct{}
}

func (f *cancelBlockingFS) Write(ctx context.Context, v fileservice.IOVector) error {
	switch f.writes.Add(1) {
	case 2:
		err := f.FileService.Write(ctx, v)
		close(f.firstDone)
		return err
	case 3:
		<-f.firstDone
		close(f.blocked)
		<-ctx.Done()
		return ctx.Err()
	default:
		return f.FileService.Write(ctx, v)
	}
}

func TestCloseAndResetCancelActivePipelineBeforeCleanup(t *testing.T) {
	for _, reset := range []bool{false, true} {
		t.Run(fmt.Sprintf("reset=%t", reset), func(t *testing.T) {
			ctx := context.Background()
			base := testutil.NewSharedFS()
			defer base.Close(ctx)
			fs := &cancelBlockingFS{FileService: base, firstDone: make(chan struct{}), blocked: make(chan struct{})}
			mp := mpool.MustNewZero()
			attrs := []string{"id", "payload"}
			typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
			input := batch.NewWithSchema(false, attrs, typs)
			defer input.Clean(mp)
			for i := 0; i < 4; i++ {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(i), false, mp))
				require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 128)), false, mp))
			}
			input.SetRowCount(4)
			sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs,
				WithMemorySizeThreshold(1<<20), WithByteBoundedRuns(), WithUnpublishedRunCleanup(), WithPipelineFlush(), WithTailSizeCap(0))
			defer sink.Close()
			require.NoError(t, sink.Write(ctx, input))
			require.NoError(t, sink.Sync(ctx))
			previous, _ := sink.GetResult()
			require.Len(t, previous, 1)
			previousName := previous[0].ObjectName().String()
			sink.staged.memorySizeThreshold = 256
			require.NoError(t, sink.Write(ctx, input))
			<-fs.blocked
			if reset {
				sink.Reset()
			} else {
				require.ErrorIs(t, sink.Close(), context.Canceled)
			}
			files, err := fileservice.SortedList(base.List(ctx, ""))
			require.NoError(t, err)
			require.Len(t, files, 1)
			_, err = base.StatFile(ctx, previousName)
			require.NoError(t, err)
			require.Nil(t, sink.pipe.result)
		})
	}
}
