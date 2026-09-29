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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

type orderedScanFailureFS struct {
	fileservice.FileService
	failAt  int
	readErr error
	reads   int
	onRead  func()
	entered chan struct{}
	release chan struct{}
}

func (f *orderedScanFailureFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	f.reads++
	if f.entered != nil {
		close(f.entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.release:
			return errors.New("metadata read did not observe request cancellation")
		}
	}
	if f.failAt == f.reads {
		return f.readErr
	}
	err := f.FileService.Read(ctx, v)
	if f.onRead != nil {
		f.onRead()
	}
	return err
}

func orderedScanSource(fs fileservice.FileService, locations ...objectio.Location) *LocalDisttaeDataSource {
	var blocks objectio.BlockInfoSlice
	for _, loc := range locations {
		blk := &objectio.BlockInfo{}
		blk.SetMetaLocation(loc)
		id := loc.ObjectId()
		blk.BlockID = types.NewBlockidWithObjectID(&id, loc.ID())
		blocks.AppendBlockInfo(blk)
	}
	return &LocalDisttaeDataSource{
		fs: fs,
		table: &txnTable{tableDef: &plan.TableDef{
			Cols:          []*plan.ColDef{{Name: "task_id", Typ: plan.Type{Id: int32(types.T_int64)}, Seqnum: 0}},
			Name2ColIndex: map[string]int32{"task_id": 0},
		}},
		rangeSlice: blocks,
		OrderBy: []*plan.OrderBySpec{{Expr: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int64)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "task_id", ColPos: 0}},
		}}},
	}
}

func writeOrderedScanObject(t *testing.T, fs fileservice.FileService, value int64) objectio.Location {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	defer bat.Clean(mp)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, false, mp))
	bat.SetRowCount(1)
	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	writer, err := objectio.NewObjectWriter(name, fs, 0, []uint16{0}, nil)
	require.NoError(t, err)
	block, err := writer.Write(bat)
	require.NoError(t, err)
	zm := index.NewZM(types.T_int64, 0)
	require.NoError(t, zm.Update(value))
	block.ColumnMeta(0).SetZoneMap(zm)
	blocks, err := writer.WriteEnd(context.Background())
	require.NoError(t, err)
	return objectio.BuildLocation(name, blocks[0].GetExtent(), 1, 0)
}

func TestOrderedScanMetadataFailure(t *testing.T) {
	fs, err := fileservice.NewMemoryFS("ordered-scan-test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	first := writeOrderedScanObject(t, fs, 10)
	second := writeOrderedScanObject(t, fs, 20)
	readErr := errors.New("injected second object metadata read failure")
	broken := &orderedScanFailureFS{FileService: fs, failAt: 2, readErr: readErr}
	source := orderedScanSource(broken, first, second)
	original := append(objectio.BlockInfoSlice(nil), source.rangeSlice...)

	require.ErrorIs(t, source.handleOrderBy(context.Background()), readErr)
	require.Equal(t, 2, broken.reads)
	require.Equal(t, original, source.rangeSlice)
	require.Zero(t, source.rangesCursor)
	require.Nil(t, source.blockZMS)
	require.False(t, source.sorted)

	broken.failAt = 0
	require.NoError(t, source.handleOrderBy(context.Background()))
	require.True(t, source.sorted)
	require.Len(t, source.blockZMS, 2)

	// The datasource contract must return the read error to the reader.
	broken.reads, broken.failAt = 0, 2
	third := writeOrderedScanObject(t, fs, 30)
	fourth := writeOrderedScanObject(t, fs, 40)
	source = orderedScanSource(broken, third, fourth)
	source.table.db = &txnDatabase{}
	source.iteratePhase = engine.Persisted
	source.SetOrderBy(source.OrderBy)
	info, state, err := source.Next(context.Background(), []string{"task_id"}, []types.Type{types.T_int64.ToType()},
		[]uint16{0}, 0, &readutil.MemPKFilter{}, mpool.MustNewZero(), batch.NewWithSize(1))
	require.ErrorIs(t, err, readErr)
	require.Nil(t, info)
	require.Equal(t, engine.Persisted, state)
	require.Zero(t, source.rangesCursor)
}

func TestOrderedScanCanceledDuringMetadataRead(t *testing.T) {
	fs, err := fileservice.NewMemoryFS("ordered-scan-test", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	loc := writeOrderedScanObject(t, fs, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	broken := &orderedScanFailureFS{FileService: fs, onRead: cancel}
	source := orderedScanSource(broken, loc)

	require.ErrorIs(t, source.handleOrderBy(ctx), context.Canceled)
	require.False(t, source.sorted)
	require.Nil(t, source.blockZMS)
	require.Zero(t, source.rangesCursor)

	broken.reads = 0
	require.ErrorIs(t, source.handleOrderBy(ctx), context.Canceled)
	require.Zero(t, broken.reads)

	// Next must pass the active request context to in-flight object I/O,
	// even if the construction context remains healthy.
	fresh := writeOrderedScanObject(t, fs, 20)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()
	reading := &orderedScanFailureFS{FileService: fs, entered: make(chan struct{}), release: make(chan struct{})}
	defer close(reading.release)
	source = orderedScanSource(reading, fresh)
	source.ctx = context.Background()
	source.table.db = &txnDatabase{}
	source.iteratePhase = engine.Persisted
	source.SetOrderBy(source.OrderBy)
	type nextResult struct {
		info  *objectio.BlockInfo
		state engine.DataState
		err   error
	}
	result := make(chan nextResult, 1)
	go func() {
		info, state, err := source.Next(requestCtx, []string{"task_id"}, []types.Type{types.T_int64.ToType()},
			[]uint16{0}, 0, &readutil.MemPKFilter{}, mpool.MustNewZero(), batch.NewWithSize(1))
		result <- nextResult{info, state, err}
	}()
	select {
	case <-reading.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata read was not reached")
	}
	cancelRequest()
	var got nextResult
	select {
	case got = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata read did not stop after request cancellation")
	}
	require.ErrorIs(t, got.err, context.Canceled)
	require.Equal(t, 1, reading.reads)
	require.Nil(t, got.info)
	require.Equal(t, engine.Persisted, got.state)
	require.Zero(t, source.rangesCursor)
}

func TestOrderedScanDisablesUnneededPrefetch(t *testing.T) {
	fs, err := fileservice.NewMemoryFS("ordered-scan-prefetch", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	loc := writeOrderedScanObject(t, fs, 10)
	source := orderedScanSource(fs, loc, loc, loc, loc)
	source.table.db = &txnDatabase{}
	source.iteratePhase = engine.Persisted
	source.SetOrderBy(source.OrderBy)
	info, state, err := source.Next(context.Background(), []string{"task_id"}, []types.Type{types.T_int64.ToType()},
		[]uint16{0}, 0, &readutil.MemPKFilter{}, mpool.MustNewZero(), batch.NewWithSize(1))
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, engine.Persisted, state)
	require.Zero(t, source.rc.batchPrefetchCursor)

	source = orderedScanSource(fs, loc, loc, loc, loc)
	source.SetOrderBy(nil)
	require.False(t, source.rc.prefetchDisabled)

	source.Limit = 1
	source.SetOrderBy([]*plan.OrderBySpec{{}})
	require.False(t, source.rc.prefetchDisabled)
}

func TestOrderedScanSortOrder(t *testing.T) {
	fs, err := fileservice.NewMemoryFS("ordered-scan-order", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	low := writeOrderedScanObject(t, fs, 10)
	high := writeOrderedScanObject(t, fs, 20)
	for _, tc := range []struct {
		name string
		flag plan.OrderBySpec_OrderByFlag
		want objectio.Location
	}{
		{name: "ascending", want: low},
		{name: "descending", flag: plan.OrderBySpec_DESC, want: high},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := orderedScanSource(fs, high, low)
			source.OrderBy[0].Flag = tc.flag
			require.NoError(t, source.handleOrderBy(context.Background()))
			require.Equal(t, tc.want, source.rangeSlice.Get(0).MetaLocation())
		})
	}
}

func BenchmarkOrderedScanSortBlocks(b *testing.B) {
	const count = 1000
	var blocks objectio.BlockInfoSlice
	zms := make([]index.ZM, count)
	for i := range count {
		blk := &objectio.BlockInfo{}
		blk.SetMetaLocation(objectio.NewRandomLocation(uint16(i), 1))
		blocks.AppendBlockInfo(blk)
		zms[i] = index.NewZM(types.T_int64, 0)
		if err := zms[i].Update(int64(count - i)); err != nil {
			b.Fatal(err)
		}
	}
	source := &LocalDisttaeDataSource{blockZMS: make([]index.ZM, count)}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		source.rangeSlice = append(source.rangeSlice[:0], blocks...)
		copy(source.blockZMS, zms)
		source.sortBlockList()
	}
}
