//go:build gpu

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

package ivfpq

import (
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func noCdcEvents(t *testing.T, sqlproc *sqlexec.SqlProcess) {
	orig := runSql
	runSql = func(_ *sqlexec.SqlProcess, _ string) (executor.Result, error) {
		return executor.Result{Mp: sqlproc.Proc.Mp()}, nil
	}
	t.Cleanup(func() { runSql = orig })
}

// A model written with PersistFiles is loaded from its index file, and a view-mode load
// releases the file once the GPU holds the index.
func TestModelIndexFileWriteThenLoad(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	noCdcEvents(t, sqlproc)
	files := swapIndexFiles(t)

	built := buildTestModel(t, "if-roundtrip", nil)
	t.Cleanup(func() { os.Remove(built.Path) })
	require.NoError(t, built.PersistFiles(sqlproc, nil, testTblcfg(), nil))
	tar, err := os.ReadFile(built.Path)
	require.NoError(t, err)
	require.Equal(t, tar, files.get(t, "if-roundtrip"))

	idx := &IvfpqModel[float32, float32]{Id: built.Id, FileSize: built.FileSize, Checksum: built.Checksum, Devices: []int{0}}
	t.Cleanup(func() { idx.Destroy() })
	require.NoError(t, idx.LoadIndex(sqlproc, testIdxcfg(), testTblcfg(), 1, true))
	require.NotNil(t, idx.Index)
	require.Equal(t, 1, files.opens)
	require.Equal(t, 1, files.hits, "the load is served from the entry PersistFiles cached")
	require.Zero(t, files.holds(idx.Id), "a view-mode load releases its index file")
	require.Empty(t, idx.Path)
}

// A fetched model tar stays held until the model releases it, and only once.
func TestModelIndexFileHeldUntilDestroy(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	files := swapIndexFiles(t)

	built := buildTestModel(t, "if-held", nil)
	t.Cleanup(func() { os.Remove(built.Path) })
	files.putFile(t, built.Id, built.Path)

	idx := &IvfpqModel[float32, float32]{Id: built.Id, FileSize: built.FileSize, Checksum: built.Checksum}
	path, err := idx.FetchArtifact(sqlproc, testTblcfg())
	require.NoError(t, err)
	idx.Path = path
	require.Equal(t, 1, files.holds(idx.Id))
	// A second fetch releases the first file's hold.
	path, err = idx.FetchArtifact(sqlproc, testTblcfg())
	require.NoError(t, err)
	idx.Path = path
	require.Equal(t, 1, files.holds(idx.Id))
	require.NoError(t, idx.Destroy())
	require.Zero(t, files.holds(idx.Id))
	require.Empty(t, idx.Path)
}

// The model's recorded MD5 is what the index file is checked against.
func TestModelIndexFileChecksumMismatch(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	noCdcEvents(t, sqlproc)
	files := swapIndexFiles(t)

	built := buildTestModel(t, "if-bad-sum", nil)
	t.Cleanup(func() { os.Remove(built.Path) })
	files.putFile(t, built.Id, built.Path)

	idx := &IvfpqModel[float32, float32]{Id: built.Id, FileSize: built.FileSize, Checksum: "00000000000000000000000000000000", Devices: []int{0}}
	t.Cleanup(func() { idx.Destroy() })
	require.ErrorContains(t, idx.LoadIndex(sqlproc, testIdxcfg(), testTblcfg(), 1, true), "checksum mismatch")
	require.Nil(t, idx.Index)
	require.Zero(t, files.holds(idx.Id))
}

// PersistFiles refuses an artifact shorter than the size its metadata row records.
func TestPersistFilesShortArtifact(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	swapIndexFiles(t)

	built := buildTestModel(t, "if-short", nil)
	t.Cleanup(func() { os.Remove(built.Path) })
	built.FileSize++
	require.ErrorContains(t, built.PersistFiles(sqlproc, nil, testTblcfg(), nil), "want")
}

// PersistFiles refuses an artifact whose bytes do not have the MD5 its metadata row records.
func TestPersistFilesChecksumMismatch(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	swapIndexFiles(t)

	built := buildTestModel(t, "if-bad-write", nil)
	t.Cleanup(func() { os.Remove(built.Path) })
	built.Checksum = "00000000000000000000000000000000"
	require.ErrorContains(t, built.PersistFiles(sqlproc, nil, testTblcfg(), nil), "with checksum")
}
