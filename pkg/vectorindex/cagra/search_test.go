//go:build gpu

// Copyright 2022 Matrix Origin
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

package cagra

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// loadedModel builds an index, saves it to a tar, then reloads it into GPU
// memory from the local file. Returns the model with Index != nil.
func loadedModel(t *testing.T, id string) *CagraModel[float32] {
	t.Helper()
	built := buildTestModel(t, id)
	tarPath := built.Path
	t.Cleanup(func() { os.Remove(tarPath) })

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	loader := &CagraModel[float32]{
		Id:       id,
		Path:     tarPath,
		Checksum: built.Checksum,
		FileSize: built.FileSize,
		Devices:  []int{0},
	}
	err := loader.LoadIndex(sqlproc, testIdxcfg(), testTblcfg(), 1, false)
	require.NoError(t, err)
	require.NotNil(t, loader.Index)
	return loader
}

// TestCagraSearchEmpty verifies that Search on an empty Indexes slice is a no-op.
func TestCagraSearchEmpty(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	s := NewCagraSearch[float32](testIdxcfg(), testTblcfg(), []int{0})
	require.Empty(t, s.Indexes)

	rt := vectorindex.RuntimeConfig{Limit: 4}
	query := generateTestData(1, testDim)

	keys, dists, err := s.Search(sqlproc, query, rt)
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Empty(t, dists)

	outKeys := make([]int64, 4)
	outDists := make([]float32, 4)
	err = s.SearchFloat32(sqlproc, query, rt, outKeys, outDists)
	require.NoError(t, err)
}

// TestCagraSearchTypeMismatch verifies that passing the wrong query type returns an error.
func TestCagraSearchTypeMismatch(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idx := loadedModel(t, "type-mismatch")
	defer idx.Destroy()

	s := NewCagraSearch[float32](testIdxcfg(), testTblcfg(), []int{0})
	s.Indexes = []*CagraModel[float32]{idx}

	rt := vectorindex.RuntimeConfig{Limit: 4}

	// Pass []float64 instead of []float32.
	_, _, err := s.Search(sqlproc, []float64{1, 2, 3, 4}, rt)
	require.Error(t, err)
}

// TestCagraSearchAndSearchFloat32 tests Search and SearchFloat32 with a single loaded index.
func TestCagraSearchAndSearchFloat32(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idx := loadedModel(t, "search-single")
	defer idx.Destroy()

	s := NewCagraSearch[float32](testIdxcfg(), testTblcfg(), []int{0})
	s.Indexes = []*CagraModel[float32]{idx}

	data := generateTestData(testNVectors, testDim)
	query := data[:testDim] // first vector; internal ID 0 should be closest

	rt := vectorindex.RuntimeConfig{Limit: 4}

	// ---- Search ----
	keysAny, dists, err := s.Search(sqlproc, query, rt)
	require.NoError(t, err)
	keys := keysAny.([]int64)
	require.Equal(t, 4, len(keys))
	require.Equal(t, 4, len(dists))
	fmt.Printf("CagraSearch.Search: keys=%v dists=%v\n", keys, dists)
	require.Equal(t, int64(0), keys[0])
	require.InDelta(t, float64(0), dists[0], 1e-3)

	// ---- SearchFloat32 results must match Search ----
	outKeys := make([]int64, 4)
	outDists := make([]float32, 4)
	err = s.SearchFloat32(sqlproc, query, rt, outKeys, outDists)
	require.NoError(t, err)
	require.Equal(t, keys, outKeys[:len(keys)])
	for i := range dists {
		require.InDelta(t, dists[i], float64(outDists[i]), 1e-5)
	}
}

// TestCagraSearchMultipleIndexes verifies result merging across two sub-indexes.
func TestCagraSearchMultipleIndexes(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idx0 := loadedModel(t, "multi-0")
	defer idx0.Destroy()
	idx1 := loadedModel(t, "multi-1")
	defer idx1.Destroy()

	s := NewCagraSearch[float32](testIdxcfg(), testTblcfg(), []int{0})
	s.Indexes = []*CagraModel[float32]{idx0, idx1}

	data := generateTestData(testNVectors, testDim)
	query := data[:testDim]
	rt := vectorindex.RuntimeConfig{Limit: 4}

	keysAny, dists, err := s.Search(sqlproc, query, rt)
	require.NoError(t, err)
	keys := keysAny.([]int64)
	require.Equal(t, 4, len(keys))
	require.Equal(t, 4, len(dists))
	fmt.Printf("CagraSearch multi: keys=%v dists=%v\n", keys, dists)
	// Both sub-indexes have the same data so key 0 must still top the list.
	require.Equal(t, int64(0), keys[0])
	require.InDelta(t, float64(0), dists[0], 1e-3)
}

// TestCagraSearchLoad tests the full Load path (LoadMetadata + LoadIndex) with mock SQL.
func TestCagraSearchLoad(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	built := buildTestModel(t, "search-load")
	tarPath := built.Path
	defer os.Remove(tarPath)

	// Mock runSql for LoadMetadata.
	origRunSql := runSql
	runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		res := executor.Result{
			Mp: proc.Mp(),
			Batches: []*batch.Batch{
				makeMetaBatch(proc, "search-load", built.Checksum, 0, built.FileSize),
			},
		}
		return res, nil
	}
	defer func() { runSql = origRunSql }()

	// Mock runSql_streaming for LoadIndex (serves the tar as one chunk).
	origStream := runSql_streaming
	runSql_streaming = func(ctx context.Context, sqlproc *sqlexec.SqlProcess, sql string, ch chan executor.Result, errChan chan error) (executor.Result, error) {
		res := executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch(proc, tarPath)}}
		ch <- res
		return executor.Result{}, nil
	}
	defer func() { runSql_streaming = origStream }()

	s := NewCagraSearch[float32](testIdxcfg(), testTblcfg(), []int{0})
	err := s.Load(sqlproc)
	require.NoError(t, err)
	require.Equal(t, 1, len(s.Indexes))
	require.NotNil(t, s.Indexes[0].Index)

	data := generateTestData(testNVectors, testDim)
	query := data[:testDim]
	rt := vectorindex.RuntimeConfig{Limit: 4}

	keysAny, dists, err := s.Search(sqlproc, query, rt)
	require.NoError(t, err)
	keys := keysAny.([]int64)
	require.Equal(t, int64(0), keys[0])
	require.InDelta(t, float64(0), dists[0], 1e-3)

	s.Destroy()
	require.Empty(t, s.Indexes)
}
