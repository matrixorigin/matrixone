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

package cagra

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// The cagra twin of ivfpq's build_tmpdir_test.go. The two builders carry the same
// temp-directory contract and were changed together, so leaving one untested means
// a change can pass CI while breaking half the pair.

func newTestBuild(t *testing.T) *CagraBuild[float32, float32] {
	t.Helper()
	return newTestBuildIn(t, "")
}

// newTestBuildIn builds with an explicit spill directory. "" means "no LOCAL
// fileservice", which os.MkdirTemp reads as $TMPDIR.
func newTestBuildIn(t *testing.T, spillDir string) *CagraBuild[float32, float32] {
	t.Helper()
	b, err := NewCagraBuild[float32, float32]("uid:0:0", testIdxcfg(),
		vectorindex.IndexTableConfig{DbName: "db", SrcTable: "t"}, 1, []int{0}, spillDir)
	require.NoError(t, err)
	return b
}

// TestBuildTmpDirIsPrivateAndReclaimed pins the contract that lets Destroy reclaim
// every packed tar in one step: one directory per build, removed whole. Before it,
// each tar was removed individually and a single failed remove stranded a file for
// the lifetime of the process.
func TestBuildTmpDirIsPrivateAndReclaimed(t *testing.T) {
	b := newTestBuild(t)

	fi, err := os.Stat(b.tmpDir)
	require.NoError(t, err, "builder must create its temp directory up front")
	require.True(t, fi.IsDir())
	require.Equal(t, os.TempDir(), filepath.Dir(b.tmpDir),
		"an empty spill dir must fall back to $TMPDIR")
	require.True(t, strings.HasPrefix(filepath.Base(b.tmpDir), fmt.Sprintf("mo-cagra-%d-", os.Getpid())),
		"orphans must be attributable to a pid, got %q", filepath.Base(b.tmpDir))

	stray := filepath.Join(b.tmpDir, "cagra-stray.tar")
	require.NoError(t, os.WriteFile(stray, []byte("x"), 0o600))

	require.NoError(t, b.Destroy())
	_, err = os.Stat(b.tmpDir)
	require.True(t, os.IsNotExist(err), "Destroy must remove the whole build directory")
	_, err = os.Stat(stray)
	require.True(t, os.IsNotExist(err), "a tar Destroy could not remove individually must still go")

	// Destroy runs on the abort path as well as the happy path.
	require.NoError(t, b.Destroy(), "Destroy must be idempotent")
}

// Concurrent CREATE INDEX statements share a pid, so uniqueness has to come from
// MkdirTemp's random suffix. If two builders ever shared a directory, one Destroy
// would delete the other's in-flight tars.
func TestBuildTmpDirsAreDistinctWithinOneProcess(t *testing.T) {
	const n = 4
	seen := make(map[string]bool, n)
	builds := make([]*CagraBuild[float32, float32], 0, n)
	t.Cleanup(func() {
		for _, b := range builds {
			_ = b.Destroy()
		}
	})

	for i := 0; i < n; i++ {
		b := newTestBuild(t)
		builds = append(builds, b)
		require.NotEmpty(t, b.tmpDir)
		require.False(t, seen[b.tmpDir], "two builders in one process shared %q", b.tmpDir)
		seen[b.tmpDir] = true
	}
	require.Len(t, seen, n)

	require.NoError(t, builds[0].Destroy())
	for _, b := range builds[1:] {
		_, err := os.Stat(b.tmpDir)
		require.NoError(t, err, "one builder's Destroy removed another's directory")
	}
}

// TestBuildTmpDirHonoursSpillDir pins where the packed sub-index tars land.
//
// They used to go to $TMPDIR unconditionally. Each tar is a whole sub-index, so a
// large build pushes GB through this directory, and /tmp is often a small or slow
// mount while the LOCAL fileservice is the volume the operator provisioned for it.
func TestBuildTmpDirHonoursSpillDir(t *testing.T) {
	spill := t.TempDir()
	b := newTestBuildIn(t, spill)
	t.Cleanup(func() { _ = b.Destroy() })

	require.Equal(t, spill, filepath.Dir(b.tmpDir),
		"the build directory must be created inside the LOCAL spill dir, not $TMPDIR")
	require.True(t, strings.HasPrefix(filepath.Base(b.tmpDir), fmt.Sprintf("mo-cagra-%d-", os.Getpid())),
		"the pid-tagged naming must survive the move")

	m, err := b.getOrCreateCurrent()
	require.NoError(t, err)
	require.Equal(t, b.tmpDir, m.TmpDir,
		"the model must pack into the builder's directory")

	// Destroy reclaims its own directory and must NOT touch the shared spill dir --
	// other builds are using it concurrently.
	require.NoError(t, b.Destroy())
	_, err = os.Stat(b.tmpDir)
	require.True(t, os.IsNotExist(err), "Destroy must remove its own directory")
	_, err = os.Stat(spill)
	require.NoError(t, err, "Destroy must NOT remove the shared spill directory")
}

// Concurrent builds share one spill dir; MkdirTemp's random suffix is what keeps
// them apart, exactly as it did under $TMPDIR.
func TestBuildTmpDirsDistinctWithinOneSpillDir(t *testing.T) {
	spill := t.TempDir()
	seen := make(map[string]bool, 4)
	for i := 0; i < 4; i++ {
		b := newTestBuildIn(t, spill)
		t.Cleanup(func() { _ = b.Destroy() })
		require.Equal(t, spill, filepath.Dir(b.tmpDir))
		require.False(t, seen[b.tmpDir], "two builders shared %q", b.tmpDir)
		seen[b.tmpDir] = true
	}
	require.Len(t, seen, 4)
}

// TestBuildTmpDirReachesTheModel closes the loop: the directory is only useful if
// the tar actually lands in it. saveToFile calls os.CreateTemp(idx.TmpDir, ...), so
// an unset TmpDir would silently fall back to $TMPDIR and defeat the mechanism.
func TestBuildTmpDirReachesTheModel(t *testing.T) {
	spill := t.TempDir()
	b := newTestBuildIn(t, spill)
	t.Cleanup(func() { _ = b.Destroy() })

	m, err := b.getOrCreateCurrent()
	require.NoError(t, err)
	require.Equal(t, b.tmpDir, m.TmpDir)

	// Go through the MODEL's AddChunk, not the raw cuVS handle: saveToFile
	// short-circuits on an empty index and only the wrapper maintains Len.
	rows := uint64(testNVectors)
	require.NoError(t, m.AddChunk(make([]float32, uint64(testDim)*rows), rows,
		make([]int64, rows)))
	require.NoError(t, m.Build())
	require.NoError(t, m.saveToFile())
	require.NotEmpty(t, m.Path)
	require.Equal(t, b.tmpDir, filepath.Dir(m.Path), "tar written outside the build directory")
	require.Equal(t, spill, filepath.Dir(filepath.Dir(m.Path)),
		"and therefore under the LOCAL spill dir, not /tmp")
}
