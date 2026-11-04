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
	"github.com/stretchr/testify/require"
)

func TestSortedSpillBoundedRunsReadBack(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		for _, shape := range []string{"disjoint", "constant", "aliased"} {
			t.Run(fmt.Sprintf("pipeline=%t/%s", pipeline, shape), func(t *testing.T) {
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
					constant, err := vector.NewConstBytes(typs[1], []byte(payload), rows, mp)
					require.NoError(t, err)
					if shape == "constant" {
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
				opts := []SinkerOption{WithMemorySizeThreshold(1 << 20), WithMaxSortedRunBytes(limit), WithTailSizeCap(0)}
				if pipeline {
					opts = append(opts, WithPipelineFlush())
				}
				sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs, opts...)
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
							if i > 0 {
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
	sink := NewSinker(0, attrs, typs, nil, mp, nil, WithMemorySizeThreshold(1<<20), WithMaxSortedRunBytes(256))
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
	sink := NewSinker(0, attrs, typs, factory, mp, nil, WithMemorySizeThreshold(1<<20), WithMaxSortedRunBytes(256), WithTailSizeCap(0))
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
		wantOwned bool
	}{
		{"fits", 2 << 20, true}, {"split", 512 << 10, false},
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
			input.Vecs[1], err = vector.NewConstBytes(typs[1], []byte(strings.Repeat("x", 128)), objectio.BlockMaxRows, mp)
			require.NoError(t, err)
			input.SetRowCount(objectio.BlockMaxRows)
			file := &trackingStreamSinker{}
			sink := NewSinker(0, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
				WithMemorySizeThreshold(1<<30), WithMaxSortedRunBytes(tc.limit), WithTailSizeCap(0))
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
		WithMemorySizeThreshold(1<<20), WithMaxSortedRunBytes(256), WithTailSizeCap(0))
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
		WithOffHeap(), WithMemorySizeThreshold(1<<20), WithMaxSortedRunBytes(256), WithTailSizeCap(0))
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

func TestSortedRunKeepsFullBlocksBeforeLastPartial(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		t.Run(fmt.Sprintf("pipeline=%t", pipeline), func(t *testing.T) {
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
			opts := []SinkerOption{WithMemorySizeThreshold(1 << 20), WithMaxSortedRunBytes(256 << 10), WithTailSizeCap(0)}
			if pipeline {
				opts = append(opts, WithPipelineFlush())
			}
			sink := NewSinker(0, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, 0, true, false, 0), mp, fs, opts...)
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
	firstPath         string
	deletedPaths      []string
}

func (f *sortedRunFailureFS) Write(ctx context.Context, v fileservice.IOVector) error {
	n := f.writes.Add(1)
	if n == f.failAt {
		<-f.firstDone // The first object must really exist before this failure.
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
			opts := []SinkerOption{WithMemorySizeThreshold(1 << 20), WithMaxSortedRunBytes(256), WithUnpublishedRunCleanup(), WithTailSizeCap(0)}
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
		WithMemorySizeThreshold(1<<20), WithMaxSortedRunBytes(1<<20), WithUnpublishedRunCleanup(), WithTailSizeCap(0))
	t.Cleanup(func() { _ = sink.Close() })
	require.NoError(t, sink.Write(ctx, input))
	require.NoError(t, sink.Sync(ctx))
	before, _ := sink.GetResult()
	require.Len(t, before, 1)
	previousName := before[0].ObjectName().String()
	sink.config.maxSortedRunBytes = 256
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
		{name: "no sort", sortKey: -1},
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
			opts := []SinkerOption{WithMemorySizeThreshold(1 << 20), WithMaxSortedRunBytes(1), WithTailSizeCap(0)}
			if tc.option != nil {
				opts = append(opts, tc.option)
			}
			sink := NewSinker(tc.sortKey, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil, opts...)
			t.Cleanup(func() { require.NoError(t, sink.Close()) })
			require.NoError(t, sink.Write(ctx, input))
			require.Zero(t, file.rows, "excluded modes must not spill early")
			require.NoError(t, sink.Sync(ctx))
			stats, _ := sink.GetResult()
			require.Len(t, stats, 1)
			require.Equal(t, 4, file.rows)
		})
	}
}
