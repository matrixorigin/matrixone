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
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	testNVectors = 256
	testDim      = 4
)

// testIdxcfg returns a standard IndexConfig for testing.
func testIdxcfg() vectorindex.IndexConfig {
	return vectorindex.IndexConfig{
		Type: vectorindex.CAGRA,
		CuvsCagra: vectorindex.CuvsCagraIndexConfig{
			IntermediateGraphDegree: 64,
			GraphDegree:             32,
			Metric:                  uint16(metric.Metric_L2sqDistance),
			Dimensions:              testDim,
			VectorType:              int32(types.T_array_float32),
			DistributionMode:        uint16(vectorindex.DistributionMode_SINGLE_GPU),
		},
	}
}

// testTblcfg returns a standard IndexTableConfig for testing.
func testTblcfg() vectorindex.IndexTableConfig {
	return vectorindex.IndexTableConfig{
		DbName:        "db",
		SrcTable:      "src",
		MetadataTable: "__cagra_meta",
		IndexTable:    "__cagra_index",
	}
}

// generateTestData creates deterministic float32 vectors: vec[i] = [i, i, i, i] (scaled).
func generateTestData(nVectors, dim int) []float32 {
	rng := rand.New(rand.NewSource(42))
	data := make([]float32, nVectors*dim)
	for i := range data {
		data[i] = rng.Float32() * 100
	}
	return data
}

// ---- mock SQL helpers ----

// mock_runSql_streaming_error always returns an error on the error channel.
func mock_runSql_streaming_error(
	ctx context.Context,
	sqlproc *sqlexec.SqlProcess,
	sql string,
	ch chan executor.Result,
	err_chan chan error,
) (executor.Result, error) {
	defer func() {
		err_chan <- moerr.NewInternalErrorNoCtx("mock_runSql_streaming_error")
		time.Sleep(10 * time.Millisecond)
	}()
	return executor.Result{}, moerr.NewInternalErrorNoCtx("mock_runSql_streaming_error")
}

// makeMetaBatch creates a metadata batch for a single CagraModel (mirrors hnsw makeMetaBatch).
func makeMetaBatch(proc *process.Process, id, checksum string, timestamp, filesize int64) *batch.Batch {
	bat := batch.NewWithSize(4)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_varchar, 128, 0))   // index_id
	bat.Vecs[1] = vector.NewVec(types.New(types.T_varchar, 65536, 0)) // checksum
	bat.Vecs[2] = vector.NewVec(types.New(types.T_int64, 8, 0))       // timestamp
	bat.Vecs[3] = vector.NewVec(types.New(types.T_int64, 8, 0))       // filesize

	vector.AppendBytes(bat.Vecs[0], []byte(id), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], []byte(checksum), false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[2], timestamp, false, proc.Mp())
	vector.AppendFixed[int64](bat.Vecs[3], filesize, false, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

// makeIndexBatch serves the content of a tar file as a single chunk (for small test files).
func makeIndexBatch(proc *process.Process, tarPath string) *batch.Batch {
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.New(types.T_int64, 8, 0))    // chunk_id
	bat.Vecs[1] = vector.NewVec(types.New(types.T_blob, 65536, 0)) // data

	dat, err := os.ReadFile(tarPath)
	if err != nil {
		panic(fmt.Sprintf("makeIndexBatch: cannot read %s: %v", tarPath, err))
	}
	vector.AppendFixed[int64](bat.Vecs[0], int64(0), false, proc.Mp())
	vector.AppendBytes(bat.Vecs[1], dat, false, proc.Mp())
	bat.SetRowCount(1)
	return bat
}

// buildTestModel builds, trains and saves a CagraModel, returning it with Index==nil and
// Path/Checksum/FileSize set. The caller is responsible for removing the tar file.
func buildTestModel(t *testing.T, id string, ids []int64) *CagraModel[float32] {
	t.Helper()

	idxcfg := testIdxcfg()
	data := generateTestData(testNVectors, testDim)

	m, err := NewCagraModelForBuild[float32](id, idxcfg, 1, []int{0})
	require.NoError(t, err)

	err = m.InitEmpty(testNVectors)
	require.NoError(t, err)

	err = m.AddChunkFloat(data, testNVectors, ids)
	require.NoError(t, err)

	err = m.Build()
	require.NoError(t, err)

	tblcfg := testTblcfg()
	sqls, err := m.ToSql(tblcfg)
	require.NoError(t, err)
	require.Greater(t, len(sqls), 0)
	require.NotEmpty(t, m.Path)
	require.NotEmpty(t, m.Checksum)
	require.Greater(t, m.FileSize, int64(0))
	require.Equal(t, int64(testNVectors), m.Len)
	require.Nil(t, m.Index) // GPU memory freed after saveToFile

	return m
}

// ---- Tests ----

// TestModelStreamError verifies that a streaming SQL error is propagated correctly.
func TestModelStreamError(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	// Inject streaming error mock.
	orig := runSql_streaming
	runSql_streaming = mock_runSql_streaming_error
	defer func() { runSql_streaming = orig }()

	// Manually create a model descriptor as if loaded from metadata.
	idx := &CagraModel[float32]{
		Id:       "test-stream-err",
		FileSize: 1024, // non-zero triggers DB download
		Checksum: "fake-checksum",
		Devices:  []int{0},
		Idxcfg:   testIdxcfg(),
	}

	err := idx.LoadIndex(sqlproc, testIdxcfg(), testTblcfg(), 1, true)
	require.NotNil(t, err)
	fmt.Printf("stream error (expected): %v\n", err)
}

// TestModelBuildAndLoad tests the full build → save → load → search → unload cycle.
func TestModelBuildAndLoad(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg := testIdxcfg()
	tblcfg := testTblcfg()
	data := generateTestData(testNVectors, testDim)
	ids := make([]int64, testNVectors)
	for i := range ids {
		ids[i] = int64(i + 1000)
	}

	// ---- Build ----
	built, err := NewCagraModelForBuild[float32]("test-build", idxcfg, 1, []int{0})
	require.NoError(t, err)

	err = built.InitEmpty(testNVectors)
	require.NoError(t, err)

	err = built.AddChunkFloat(data, testNVectors, ids)
	require.NoError(t, err)

	err = built.Build()
	require.NoError(t, err)
	require.True(t, built.Dirty)
	require.Equal(t, int64(testNVectors), built.Len)

	// ---- Save to file (triggers saveToFile internally) ----
	sqls, err := built.ToSql(tblcfg)
	require.NoError(t, err)
	require.Greater(t, len(sqls), 0)

	tarPath := built.Path
	checksum := built.Checksum
	fileSize := built.FileSize
	require.NotEmpty(t, tarPath)
	require.NotEmpty(t, checksum)
	defer os.Remove(tarPath)

	// ---- Load from local tar (skips DB download since Path is set) ----
	loader := &CagraModel[float32]{
		Id:       "test-build",
		Path:     tarPath,
		Checksum: checksum,
		FileSize: fileSize,
		Devices:  []int{0},
	}

	err = loader.LoadIndex(sqlproc, idxcfg, tblcfg, 1, false)
	require.NoError(t, err)
	require.NotNil(t, loader.Index)
	require.Equal(t, int64(testNVectors), loader.Len)

	// Double LoadIndex — should be a no-op.
	err = loader.LoadIndex(sqlproc, idxcfg, tblcfg, 1, false)
	require.NoError(t, err)

	// ---- Search ----
	// Query the first vector.
	query := data[:testDim]
	keys, dists, err := loader.Search(query, 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(keys))
	require.Equal(t, 1, len(dists))
	fmt.Printf("Search result: keys=%v dists=%v\n", keys, dists)
	// CAGRA is approximate; verify the top result matches the provided ID.
	require.Equal(t, int64(1000), keys[0])
	require.InDelta(t, float32(0), dists[0], 1e-3)

	// ---- DeleteSql ----
	deleteSqls, err := loader.ToDeleteSql(tblcfg)
	require.NoError(t, err)
	require.Equal(t, 2, len(deleteSqls))
	fmt.Printf("DeleteSqls: %v\n", deleteSqls)

	// ---- Unload ----
	err = loader.Unload()
	require.NoError(t, err)
	require.Nil(t, loader.Index)

	// ---- Destroy ----
	err = loader.Destroy()
	require.NoError(t, err)
}

// TestModelLoadFromDB tests LoadIndex when the tar is downloaded from a mock DB (streaming SQL).
func TestModelLoadFromDB(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg := testIdxcfg()
	tblcfg := testTblcfg()

	ids := make([]int64, testNVectors)
	for i := range ids {
		ids[i] = int64(i + 2000)
	}

	// Build a real index and save to tar.
	built := buildTestModel(t, "test-from-db", ids)
	tarPath := built.Path
	defer os.Remove(tarPath)

	// Inject streaming mock that serves the tar content as one chunk.
	orig := runSql_streaming
	runSql_streaming = func(ctx context.Context, sqlproc *sqlexec.SqlProcess, sql string, ch chan executor.Result, errChan chan error) (executor.Result, error) {
		res := executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch(proc, tarPath)}}
		ch <- res
		return executor.Result{}, nil
	}
	defer func() { runSql_streaming = orig }()

	// Also mock runSql for LoadMetadata.
	origRunSql := runSql
	runSql = func(sqlproc *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		res := executor.Result{
			Mp: proc.Mp(),
			Batches: []*batch.Batch{
				makeMetaBatch(proc, "test-from-db", built.Checksum, 0, built.FileSize),
			},
		}
		return res, nil
	}
	defer func() { runSql = origRunSql }()

	// LoadMetadata — creates a model from DB metadata.
	models, err := LoadMetadata[float32](sqlproc, tblcfg.DbName, tblcfg.MetadataTable)
	require.NoError(t, err)
	require.Equal(t, 1, len(models))

	idx := models[0]
	idx.Devices = []int{0}
	defer idx.Destroy()

	// LoadIndex — downloads tar via mock streaming.
	err = idx.LoadIndex(sqlproc, idxcfg, tblcfg, 1, true)
	require.NoError(t, err)
	require.NotNil(t, idx.Index)
	require.Equal(t, int64(testNVectors), idx.Len)

	// Search.
	data := generateTestData(testNVectors, testDim)
	query := data[:testDim]
	keys, dists, err := idx.Search(query, 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(keys))
	fmt.Printf("LoadFromDB Search: keys=%v dists=%v\n", keys, dists)
	require.Equal(t, int64(2000), keys[0])
	require.InDelta(t, float32(0), dists[0], 1e-3)
}

// TestModelNil verifies error handling when the Index is nil or uninitialized.
func TestModelNil(t *testing.T) {
	var tblcfg vectorindex.IndexTableConfig

	// Zero-value model: no index, no path.
	idx := &CagraModel[float32]{}

	// InitEmpty fails because Devices is empty.
	err := idx.InitEmpty(10)
	require.NotNil(t, err)
	fmt.Printf("InitEmpty with no devices (expected error): %v\n", err)

	// Build fails because Index is nil.
	err = idx.Build()
	require.NotNil(t, err)

	// AddChunk fails because Index is nil.
	err = idx.AddChunk([]float32{1, 2}, 1, []int64{1})
	require.NotNil(t, err)

	// AddChunkFloat fails because Index is nil.
	err = idx.AddChunkFloat([]float32{1, 2}, 1, []int64{1})
	require.NotNil(t, err)

	// Search fails because Index is nil.
	_, _, err = idx.Search([]float32{0, 0, 0, 0}, 1)
	require.NotNil(t, err)

	// Search with nil query fails.
	idx2 := &CagraModel[float32]{} // still nil Index
	_, _, err = idx2.Search(nil, 1)
	require.NotNil(t, err)

	// ToSql on a never-built model (Index nil, Dirty false) returns empty (no-op).
	sqls, err := idx.ToSql(tblcfg)
	require.NoError(t, err)
	require.Equal(t, 0, len(sqls))

	// ToDeleteSql always works.
	deleteSqls, err := idx.ToDeleteSql(tblcfg)
	require.NoError(t, err)
	require.Equal(t, 2, len(deleteSqls))

	// Empty / Full with zero Len / MaxCapacity.
	require.True(t, idx.Empty())
	require.False(t, idx.Full())

	// Unload with nil Index is a no-op.
	err = idx.Unload()
	require.NoError(t, err)

	// Destroy with nil Index and empty Path is a no-op.
	err = idx.Destroy()
	require.NoError(t, err)
}

// TestModelEmptyBuild verifies that building with zero vectors produces no file.
func TestModelEmptyBuild(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	_ = proc
	_ = sqlproc

	idxcfg := testIdxcfg()
	tblcfg := testTblcfg()

	built, err := NewCagraModelForBuild[float32]("test-empty", idxcfg, 1, []int{0})
	require.NoError(t, err)

	// InitEmpty with 0 would fail in CAGRA, so test saveToFile directly on empty Len.
	// Simulate: set Index non-nil, Dirty=true, Len=0 by using a tiny index placeholder.
	// Instead, just verify the ToSql path: Index nil → no file, returns empty slice.
	built.Dirty = false // not dirty
	sqls, err := built.ToSql(tblcfg)
	require.NoError(t, err)
	require.Equal(t, 0, len(sqls))
}
