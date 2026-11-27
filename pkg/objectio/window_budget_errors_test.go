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

package objectio

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func TestBudgetedColumnWindowRejectsInvalidRequestsWithoutReading(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	meta := BuildMetaData(1, 1)
	meta.GetBlockMeta(0).BlockHeader().SetRows(3)
	fs := &countingColumnFS{}
	for _, tc := range []struct {
		name       string
		offset     int
		candidates int
		budget     int
		pool       *mpool.MPool
	}{
		{name: "zero rows", candidates: 0, budget: 1, pool: mp},
		{name: "zero budget", candidates: 1, budget: 0, pool: mp},
		{name: "nil pool", candidates: 1, budget: 1},
		{name: "negative offset", offset: -1, candidates: 1, budget: 1, pool: mp},
		{name: "window past block", offset: 2, candidates: 2, budget: 1, pool: mp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, rows, err := ReadOneBlockAllColumnsBudgetedWindow(ctx, &meta, "unused", 0,
				[]uint16{0}, tc.offset, tc.candidates, tc.budget, fileservice.SkipAllCache, fs, tc.pool, nil)
			require.Error(t, err)
			require.Nil(t, got)
			require.Zero(t, rows)
		})
	}
	require.Zero(t, fs.reads.Load())
	require.Zero(t, mp.CurrNB())
}

func TestBudgetedColumnWindowClosesPartialResultsOnCancellationAndBadPayload(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	source := vector.NewVec(types.T_int64.ToType())
	for i := int64(0); i < 3; i++ {
		require.NoError(t, vector.AppendFixed(source, i, false, mp))
	}
	encoded := marshalLegacyTestColumn(t, source)
	source.Free(mp)
	ext := writeLegacyTestExtent(t, fs, "valid-column", encoded, compress.None)
	meta := BuildMetaData(1, 2)
	block := meta.GetBlockMeta(0)
	block.BlockHeader().SetRows(3)
	for _, seq := range []uint16{0, 1} {
		col := block.ColumnMeta(seq)
		col.setDataType(uint8(types.T_int64))
		col.setLocation(ext)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got, rows, err := ReadOneBlockAllColumnsBudgetedWindow(ctx, &meta, "valid-column", 0,
		[]uint16{0, 1}, 0, 3, 1024, fileservice.SkipAllCache, fs, mp, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, got)
	require.Zero(t, rows)

	// The first column is valid. A corrupt second column must release it.
	block.ColumnMeta(1).setLocation(NewExtent(compress.None, uint32(len(encoded)), 4, 4))
	got, rows, err = ReadOneBlockAllColumnsBudgetedWindow(context.Background(), &meta, "valid-column", 0,
		[]uint16{0, 1}, 0, 3, 1024, fileservice.SkipAllCache, fs, mp, nil)
	require.Error(t, err)
	require.Nil(t, got)
	require.Zero(t, rows)
	require.Zero(t, mp.CurrNB())
}

func TestBudgetedColumnWindowSpillFailureAndMissingCacheData(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	ext := writeLegacyTestExtent(t, fs, "spill-source", []byte("bad column"), compress.None)
	_, err = loadBlockWindowPart(ctx, "spill-source", ext, 0, 1, 1,
		fileservice.SkipAllCache, fs, nil, false)
	require.ErrorContains(t, err, "spill is unavailable")
	sentinel := errors.New("temporary file unavailable")
	_, err = loadBlockWindowPart(ctx, "spill-source", ext, 0, 1, 1,
		fileservice.SkipAllCache, fs, func(context.Context) (ColumnWindowSpill, error) { return nil, sentinel }, false)
	require.ErrorIs(t, err, sentinel)
	_, err = loadBlockWindowPart(ctx, "spill-source", ext, 0, 1, 1,
		fileservice.SkipAllCache, fs, func(context.Context) (ColumnWindowSpill, error) { return nil, nil }, false)
	require.ErrorContains(t, err, "spill is nil")

	missing := &partialReadErrorFS{}
	_, err = readCachedColumnRange(ctx, "missing-cache-data", ext, fileservice.SkipAllCache, missing)
	require.ErrorContains(t, err, "cache data is unavailable")
}

func TestBudgetedColumnWindowRejectsMismatchedChunkRowsAndReleasesFailedSpill(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(ctx)
	chunked := twoChunkTestColumn(t, types.T_int64.ToType())
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "wrong-rows", Entries: []fileservice.IOEntry{{Data: chunked, Size: int64(len(chunked))}},
	}))
	ext := NewExtent(compress.Lz4Chunked, 0, uint32(len(chunked)), uint32(len(chunked)))
	err = forEachBlockWindowPart(ctx, "wrong-rows", ext, 3, 0, 2, 1024,
		fileservice.SkipAllCache, fs, nil, func(*blockWindowPart) (bool, error) {
			t.Fatal("a mismatched chunk must not reach the visitor")
			return false, nil
		})
	require.ErrorContains(t, err, "row count mismatch")

	legacy := []byte("legacy")
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: "short-legacy", Entries: []fileservice.IOEntry{{Data: legacy, Size: int64(len(legacy))}},
	}))
	var spill *testColumnWindowSpill
	factory := func(context.Context) (ColumnWindowSpill, error) {
		file, err := os.CreateTemp(t.TempDir(), "column-window-*")
		if err != nil {
			return nil, err
		}
		spill = &testColumnWindowSpill{File: file}
		return spill, nil
	}
	ext = NewExtent(compress.None, 0, uint32(len(legacy)), uint32(len(legacy)+1))
	err = forEachBlockWindowPart(ctx, "short-legacy", ext, 1, 0, 1, 1,
		fileservice.SkipAllCache, fs, factory, func(*blockWindowPart) (bool, error) {
			t.Fatal("a short stream must not reach the visitor")
			return false, nil
		})
	require.ErrorContains(t, err, "decoded size mismatch")
	require.NotNil(t, spill)
	require.True(t, spill.closed)
}

func TestBudgetedColumnWindowRejectsMixedPayloadTypesAndCostOverrun(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	dst := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(dst, int64(7), false, mp))
	part := vector.NewVec(types.T_int32.ToType())
	require.NoError(t, vector.AppendFixed(part, int32(8), false, mp))
	require.ErrorContains(t, appendBlockWindowVector(&dst, part, mp), "type mismatch")
	dst.Free(mp)

	source := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(source, int64(1), false, mp))
	encoded := marshalLegacyTestColumn(t, source)
	source.Free(mp)
	data := fileservice.NewBytes(encoded)
	require.ErrorContains(t, addCachedColumnWindowCosts(data, 1, make([]uint64, 1)), "out of range")
	data.Release()
	require.Zero(t, mp.CurrNB())
}
