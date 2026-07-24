// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hnsw

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"

	usearch "github.com/unum-cloud/usearch/golang"
)

// TestSaveToFileNoFDLeak covers #25630: SaveToFile creates a temp file via os.CreateTemp but
// must not leak the returned file descriptor. GC is disabled so a leaked *os.File is not closed
// by its finalizer, making the leak observable as a growing open-fd count.
func TestSaveToFileNoFDLeak(t *testing.T) {
	// /dev/fd lists the current process's open descriptors on both Linux (symlink to
	// /proc/self/fd) and macOS; skip anywhere it is unavailable. Use Readdirnames (not
	// os.ReadDir) so we don't fstat each fd entry — stat-ing the directory's own transient
	// fd fails on macOS. The open /dev/fd handle itself is counted, but cancels out of the
	// before/after delta.
	countFDs := func() (int, error) {
		f, err := os.Open("/dev/fd")
		if err != nil {
			return 0, err
		}
		defer f.Close()
		names, err := f.Readdirnames(-1)
		if err != nil {
			return 0, err
		}
		return len(names), nil
	}
	if _, err := countFDs(); err != nil {
		t.Skipf("/dev/fd unavailable on this platform: %v", err)
	}

	// Disable GC so leaked file descriptors are NOT reclaimed by *os.File finalizers.
	oldGC := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(oldGC)

	saveOnce := func() {
		idxcfg := usearch.DefaultConfig(3)
		idxcfg.Metric = usearch.L2sq
		uidx, err := usearch.NewIndex(idxcfg)
		require.NoError(t, err)
		require.NoError(t, uidx.Reserve(1))
		require.NoError(t, uidx.Add(usearch.Key(0), []float32{1, 2, 3}))

		model := &HnswModel[float32]{Index: uidx}
		model.Dirty.Store(true)
		require.NoError(t, model.SaveToFile())
		// SaveToFile destroyed the in-memory index and wrote model.Path; drop the temp file
		// so disk/inode churn doesn't confound the fd measurement.
		if model.Path != "" {
			require.NoError(t, os.Remove(model.Path))
		}
	}

	// Warm up once so first-time allocations don't skew the baseline.
	saveOnce()

	before, err := countFDs()
	require.NoError(t, err)

	const n = 20
	for i := 0; i < n; i++ {
		saveOnce()
	}

	after, err := countFDs()
	require.NoError(t, err)

	// Before the fix each SaveToFile leaked exactly one fd (the CreateTemp handle), so the
	// delta would be ~20. Allow a small slack for unrelated runtime fd churn.
	require.LessOrEqualf(t, after-before, 2,
		"SaveToFile leaked file descriptors: before=%d after=%d (expected ~0, pre-fix ~%d)", before, after, n)
}

// TestSaveToFileCleanupOnError covers #25630's failure path: when a step after the temp file is
// created fails (CheckSum here, forced via the injectable saveToFileCheckSum hook), SaveToFile
// must remove the temp file (no orphan) via the deferred cleanup and leave idx.Path unset.
func TestSaveToFileCleanupOnError(t *testing.T) {
	orig := saveToFileCheckSum
	defer func() { saveToFileCheckSum = orig }()
	saveToFileCheckSum = func(string) (string, error) {
		return "", moerr.NewInternalErrorNoCtx("mock checksum failure")
	}

	before := len(hnswTempFiles())

	idxcfg := usearch.DefaultConfig(3)
	idxcfg.Metric = usearch.L2sq
	uidx, err := usearch.NewIndex(idxcfg)
	require.NoError(t, err)
	require.NoError(t, uidx.Reserve(1))
	require.NoError(t, uidx.Add(usearch.Key(0), []float32{1, 2, 3}))

	model := &HnswModel[float32]{Index: uidx}
	model.Dirty.Store(true)

	err = model.SaveToFile()
	require.Error(t, err, "SaveToFile must propagate the checksum failure")
	require.Empty(t, model.Path, "Path must stay unset on failure")
	require.Equal(t, before, len(hnswTempFiles()), "SaveToFile orphaned a temp file on the error path")

	// the save failed before Destroy, so the index is still live — clean it up.
	if model.Index != nil {
		require.NoError(t, model.Index.Destroy())
	}
}

// give blob
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

func TestModelStreamError(t *testing.T) {
	var err error

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	// stub runSql function
	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming_error

	models, err := LoadMetadata[float32](sqlproc, "db", "meta")
	require.Nil(t, err)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	require.Equal(t, len(models), 1)
	idx := models[0]
	defer idx.Destroy()

	before := hnswTempFiles()

	// load from file
	err = idx.LoadIndex(sqlproc, idxcfg, tblcfg, 0, false)
	fmt.Printf("err %v\n", err)
	require.NotNil(t, err)

	after := hnswTempFiles()
	require.Equal(t, len(before), len(after),
		"temp file leaked after LoadIndex streaming error (view=false)")

	// load from memory
	// view == false
	err = idx.LoadIndexFromBuffer(sqlproc, idxcfg, tblcfg, 0, false)
	fmt.Printf("err %v\n", err)
	require.NotNil(t, err)

	// error from mock_runSql_streaming_error
	before = hnswTempFiles()
	err = idx.LoadIndexFromBuffer(sqlproc, idxcfg, tblcfg, 0, true)
	fmt.Printf("err %v\n", err)
	require.NotNil(t, err)

	after = hnswTempFiles()
	require.Equal(t, len(before), len(after),
		"temp file leaked after LoadIndexFromBuffer streaming error")
}

func doModelSearchTest[T types.RealNumbers](t *testing.T, idx *HnswModel[T], key uint64, v []T) {
	keys, distances, err := idx.Search(v, 4)
	require.Nil(t, err)
	require.Equal(t, len(keys), 4)
	require.Equal(t, keys[0], key)
	require.Equal(t, distances[0], T(0))
	fmt.Printf("%v %v\n", keys, distances)

}

func TestModelFromBuffer(t *testing.T) {
	var err error
	view := true
	fp32a := []float32{0, 1, 2}

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	// stub runSql function
	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming

	models, err := LoadMetadata[float32](sqlproc, "db", "meta")
	require.Nil(t, err)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	require.Equal(t, len(models), 1)
	idx := models[0]
	defer idx.Destroy()

	err = idx.LoadIndexFromBuffer(sqlproc, idxcfg, tblcfg, 0, view)
	require.Nil(t, err)

	// double LoadIndex
	err = idx.LoadIndexFromBuffer(sqlproc, idxcfg, tblcfg, 0, view)
	require.Nil(t, err)

	doModelSearchTest[float32](t, idx, 0, fp32a)

	err = idx.Unload()
	require.NotNil(t, err)

	err = idx.Add(int64(0), fp32a)
	require.NotNil(t, err)

	err = idx.AddWithoutIncr(int64(0), fp32a)
	require.NotNil(t, err)

	err = idx.Remove(int64(0))
	require.NotNil(t, err)
}

func TestModelFromFileViewTrue(t *testing.T) {
	var err error
	view := true
	fp32a := []float32{0, 1, 2}

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	// stub runSql function
	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming

	models, err := LoadMetadata[float32](sqlproc, "db", "meta")
	require.Nil(t, err)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	require.Equal(t, len(models), 1)
	idx := models[0]
	defer idx.Destroy()

	err = idx.LoadIndex(sqlproc, idxcfg, tblcfg, 0, view)
	require.Nil(t, err)

	// double LoadIndex
	err = idx.LoadIndex(sqlproc, idxcfg, tblcfg, 0, view)
	require.Nil(t, err)

	doModelSearchTest[float32](t, idx, 0, fp32a)

	err = idx.Unload()
	require.NotNil(t, err)

	err = idx.Add(int64(0), fp32a)
	require.NotNil(t, err)

	err = idx.AddWithoutIncr(int64(0), fp32a)
	require.NotNil(t, err)

	err = idx.Remove(int64(0))
	require.NotNil(t, err)
}

func TestModel(t *testing.T) {
	var err error
	view := false
	fp32a := []float32{0, 1, 2}
	v1000 := []float32{1000, 2000, 3000}

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	// stub runSql function
	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming

	models, err := LoadMetadata[float32](sqlproc, "db", "meta")
	require.Nil(t, err)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src", MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	require.Equal(t, len(models), 1)
	idx := models[0]
	defer idx.Destroy()

	err = idx.LoadIndex(sqlproc, idxcfg, tblcfg, 0, view)
	require.Nil(t, err)

	// double LoadIndex
	err = idx.LoadIndex(sqlproc, idxcfg, tblcfg, 0, view)
	require.Nil(t, err)

	doModelSearchTest[float32](t, idx, 0, fp32a)

	require.Equal(t, idx.Dirty.Load(), false)

	err = idx.Unload()
	require.Nil(t, err)

	err = idx.LoadIndex(sqlproc, idxcfg, tblcfg, 0, view)
	require.Nil(t, err)

	doModelSearchTest[float32](t, idx, 0, fp32a)

	var found bool
	found, err = idx.Contains(0)
	require.Nil(t, err)
	require.Equal(t, found, true)

	found, err = idx.Contains(1000)
	require.Nil(t, err)
	require.Equal(t, found, false)

	key := int64(1000)
	v := v1000
	full := false
	empty := false

	for i := 0; i < 10; i++ {
		full, err = idx.Full()
		require.Nil(t, err)
		require.Equal(t, full, false)

		empty, err = idx.Empty()
		require.Nil(t, err)
		require.Equal(t, empty, false)

		err = idx.Add(int64(key), v)
		require.Nil(t, err)

		require.Equal(t, idx.Dirty.Load(), true)

		err = idx.Unload()
		require.Nil(t, err)

		err = idx.LoadIndex(sqlproc, idxcfg, tblcfg, 0, view)
		require.Nil(t, err)

		doModelSearchTest[float32](t, idx, uint64(key), v)

		key += 1
		v[0] += 1
	}

	// reset vector to [1000, 2000, 3000]
	key = int64(1000)
	v[0] = 1000

	for i := 0; i < 10; i++ {
		err = idx.Remove(key)
		require.Nil(t, err)
		key += 1
	}

	deletesqls, err := idx.ToDeleteSql(tblcfg)
	require.Nil(t, err)

	fmt.Printf("%v\n", deletesqls)

	// ToSql will release the index so index is nil
	sqls, err := idx.ToSql(tblcfg)
	require.Nil(t, err)
	fmt.Printf("%v\n", sqls)

	// unload with nil index will output error
	err = idx.Unload()
	require.NotNil(t, err)

	// load again
	err = idx.LoadIndex(sqlproc, idxcfg, tblcfg, 0, view)
	require.Nil(t, err)

	key = int64(1000)
	for i := 0; i < 10; i++ {
		found, err = idx.Contains(key)
		require.Nil(t, err)
		require.Equal(t, found, false)
		key += 1
	}

}

func TestModelNil(t *testing.T) {

	var err error
	var tblcfg vectorindex.IndexTableConfig

	idx := HnswModel[float32]{}
	err = idx.SaveToFile()
	require.Nil(t, err)

	sqls, err := idx.ToSql(tblcfg)
	require.Nil(t, err)
	require.Equal(t, len(sqls), 0)

	_, err = idx.Empty()
	require.NotNil(t, err)

	_, err = idx.Full()
	require.NotNil(t, err)

	err = idx.Add(0, nil)
	require.NotNil(t, err)

	err = idx.Remove(0)
	require.NotNil(t, err)

	_, err = idx.Contains(0)
	require.NotNil(t, err)

	err = idx.Unload()
	require.NotNil(t, err)

	_, _, err = idx.Search(nil, 0)
	require.NotNil(t, err)

}

// mock that sends error to error_chan first, then sends data to stream_chan
// after context cancellation, so the drain loop has data to drain.
func mock_runSql_streaming_drain(
	ctx context.Context,
	sqlproc *sqlexec.SqlProcess,
	sql string,
	ch chan executor.Result,
	err_chan chan error,
) (executor.Result, error) {
	proc := sqlproc.Proc
	// Send error immediately — loadChunk will pick it up.
	err_chan <- moerr.NewInternalErrorNoCtx("drain test error")
	// Wait for context cancellation (main loop calls cancel(err)).
	<-ctx.Done()
	// Send data that must be drained by the caller.
	ch <- executor.Result{Mp: proc.Mp(), Batches: []*batch.Batch{makeIndexBatch(proc)}}
	return executor.Result{}, nil
}

// hnswTempFiles returns paths matching the temp file pattern used by
// LoadIndex/LoadIndexFromBuffer (os.CreateTemp("", "hnsw")).
func hnswTempFiles() []string {
	matches, _ := filepath.Glob(filepath.Join(os.TempDir(), "hnsw*"))
	return matches
}

// TestNewHnswModelForBuild covers NewHnswModelForBuild + initIndex (lines 66-112).
func TestNewHnswModelForBuild(t *testing.T) {
	cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	cfg.Usearch.Metric = usearch.L2sq

	idx, err := NewHnswModelForBuild[float32]("build-test", cfg, 1, 64)
	require.NoError(t, err)
	require.NotNil(t, idx.Index)
	defer idx.Destroy()

	require.Equal(t, "build-test", idx.Id)
	require.Equal(t, uint(1), idx.NThread)
	require.Equal(t, uint(64), idx.MaxCapacity)

	empty, err := idx.Empty()
	require.NoError(t, err)
	require.True(t, empty)

	// Add a vector and verify it's there.
	err = idx.Add(42, []float32{1, 2, 3})
	require.NoError(t, err)

	found, err := idx.Contains(42)
	require.NoError(t, err)
	require.True(t, found)
}

// TestLoadIndex_EmptyChecksum covers the "checksum is empty" guard (lines 657-660).
func TestLoadIndex_EmptyChecksum(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src",
		MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	idx := &HnswModel[float32]{FileSize: 1024, Checksum: ""}
	defer idx.Destroy()

	err := idx.LoadIndex(sqlproc, idxcfg, tblcfg, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "checksum is empty")
}

// TestLoadIndex_NewlyCreated covers the FileSize==0 + Path=="" → initIndex path (lines 652-655).
func TestLoadIndex_NewlyCreated(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	idxcfg.IndexCapacity = 64
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src",
		MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	// FileSize=0, Path="" triggers the initIndex path.
	idx := &HnswModel[float32]{MaxCapacity: 64, NThread: 1}
	defer idx.Destroy()

	err := idx.LoadIndex(sqlproc, idxcfg, tblcfg, 0, false)
	require.NoError(t, err)
	require.NotNil(t, idx.Index)

	empty, err := idx.Empty()
	require.NoError(t, err)
	require.True(t, empty)
}

// TestSearch_WrongDimension covers the dimension mismatch guard (lines 878-880).
func TestSearch_WrongDimension(t *testing.T) {
	cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	cfg.Usearch.Metric = usearch.L2sq

	idx, err := NewHnswModelForBuild[float32]("dim-test", cfg, 1, 16)
	require.NoError(t, err)
	defer idx.Destroy()

	// Search with a 2-element vector on a 3-dim index.
	_, _, err = idx.Search([]float32{1, 2}, 4)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dimension not match")
}

// TestStreamingDrain covers the stream_chan drain loop (lines 499-501 in
// LoadIndexFromBuffer, lines 729-731 in LoadIndex) where pending Results
// must be drained and closed after an error cancels the producer.
func TestStreamingDrain(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming_drain

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src",
		MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	// Test LoadIndex drain path.
	models, err := LoadMetadata[float32](sqlproc, "db", "meta")
	require.NoError(t, err)
	idx0 := models[0]
	defer idx0.Destroy()

	before := hnswTempFiles()
	err = idx0.LoadIndex(sqlproc, idxcfg, tblcfg, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drain test error")
	after := hnswTempFiles()
	require.Equal(t, len(before), len(after),
		"temp file leaked after LoadIndex drain")

	// Test LoadIndexFromBuffer drain path.
	models, err = LoadMetadata[float32](sqlproc, "db", "meta")
	require.NoError(t, err)
	idx1 := models[0]
	defer idx1.Destroy()

	before = hnswTempFiles()
	err = idx1.LoadIndexFromBuffer(sqlproc, idxcfg, tblcfg, 0, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "drain test error")
	after = hnswTempFiles()
	require.Equal(t, len(before), len(after),
		"temp file leaked after LoadIndexFromBuffer drain")
}

func TestTempFileCleanup_ChecksumMismatch(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	// Good streaming so the file is written, but we corrupt the checksum.
	runSql = mock_runSql
	runSql_streaming = mock_runSql_streaming

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src",
		MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	// Test LoadIndexFromBuffer with bad checksum
	models, err := LoadMetadata[float32](sqlproc, "db", "meta")
	require.NoError(t, err)
	idx0 := models[0]
	defer idx0.Destroy()
	idx0.Checksum = "bad-checksum"

	before := hnswTempFiles()
	err = idx0.LoadIndexFromBuffer(sqlproc, idxcfg, tblcfg, 0, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Checksum mismatch")
	after := hnswTempFiles()
	require.Equal(t, len(before), len(after),
		"temp file leaked after LoadIndexFromBuffer checksum mismatch")

	// Test LoadIndex with bad checksum (view=false)
	models, err = LoadMetadata[float32](sqlproc, "db", "meta")
	require.NoError(t, err)
	idx1 := models[0]
	defer idx1.Destroy()
	idx1.Checksum = "bad-checksum"

	before = hnswTempFiles()
	err = idx1.LoadIndex(sqlproc, idxcfg, tblcfg, 0, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Checksum mismatch")
	after = hnswTempFiles()
	require.Equal(t, len(before), len(after),
		"temp file leaked after LoadIndex checksum mismatch (view=false)")
}

// TestCorruptedIndexFile covers the usearchidx cleanup defer and
// View/Load error paths when a file has valid checksum but is not a
// valid usearch index (e.g. disk corruption, truncated write).
func TestCorruptedIndexFile(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(3)}
	idxcfg.Usearch.Metric = usearch.L2sq
	idxcfg.IndexCapacity = 64
	tblcfg := vectorindex.IndexTableConfig{DbName: "db", SrcTable: "src",
		MetadataTable: "__secondary_meta", IndexTable: "__secondary_index"}

	garbage := []byte{}

	// Helper: create an empty file with matching checksum.
	makeEmptyFile := func(t *testing.T) (string, string) {
		t.Helper()
		p := filepath.Join(t.TempDir(), "empty.hnsw")
		require.NoError(t, os.WriteFile(p, garbage, 0644))
		cs, err := vectorindex.CheckSum(p)
		require.NoError(t, err)
		return p, cs
	}

	// LoadIndexFromBuffer: checksum passes, View() fails → usearchidx cleanup.
	p0, cs0 := makeEmptyFile(t)
	idx0 := &HnswModel[float32]{Path: p0, FileSize: int64(len(garbage)), Checksum: cs0}
	err := idx0.LoadIndexFromBuffer(sqlproc, idxcfg, tblcfg, 0, true)
	require.Error(t, err, "View() should fail on empty file")
	require.Nil(t, idx0.Index)

	// LoadIndex (view=true): Load() fails → usearchidx cleanup.
	p1, cs1 := makeEmptyFile(t)
	idx1 := &HnswModel[float32]{Path: p1, FileSize: int64(len(garbage)), Checksum: cs1}
	err = idx1.LoadIndex(sqlproc, idxcfg, tblcfg, 0, true)
	require.Error(t, err, "Load() should fail on empty file (view=true)")
	require.Nil(t, idx1.Index)

	// LoadIndex (view=false): Load() fails → usearchidx cleanup.
	p2, cs2 := makeEmptyFile(t)
	idx2 := &HnswModel[float32]{Path: p2, FileSize: int64(len(garbage)), Checksum: cs2}
	err = idx2.LoadIndex(sqlproc, idxcfg, tblcfg, 0, false)
	require.Error(t, err, "Load() should fail on empty file (view=false)")
	require.Nil(t, idx2.Index)
}
