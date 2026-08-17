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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// newTestBuild constructs a builder without touching the GPU: NewIvfpqBuild only
// creates the temp directory and fills the struct.
func newTestBuild(t *testing.T) *IvfpqBuild[float32, float32] {
	t.Helper()
	b, err := NewIvfpqBuild[float32, float32]("uid:0:0", testIdxcfg(),
		vectorindex.IndexTableConfig{DbName: "db", SrcTable: "t"}, 1, []int{0})
	require.NoError(t, err)
	return b
}

// TestBuildTmpDirIsPrivateAndReclaimed pins the temp-directory contract that makes
// Destroy able to reclaim every packed tar in one step.
//
// Before this, each sub-index wrote its tar directly into $TMPDIR and Destroy removed
// them one file at a time, so a single failed remove stranded a tar for the lifetime
// of the process. A per-build directory turns that into one RemoveAll.
func TestBuildTmpDirIsPrivateAndReclaimed(t *testing.T) {
	b := newTestBuild(t)

	// The directory exists, sits under $TMPDIR, and names the owning process so an
	// operator can attribute anything a crash leaves behind.
	fi, err := os.Stat(b.tmpDir)
	require.NoError(t, err, "builder must create its temp directory up front")
	require.True(t, fi.IsDir())
	require.Equal(t, os.TempDir(), filepath.Dir(b.tmpDir))
	require.True(t, strings.HasPrefix(filepath.Base(b.tmpDir), fmt.Sprintf("mo-ivfpq-%d-", os.Getpid())),
		"orphans must be attributable to a pid, got %q", filepath.Base(b.tmpDir))

	// A file written into it is reclaimed with the directory.
	stray := filepath.Join(b.tmpDir, "ivfpq-stray.tar")
	require.NoError(t, os.WriteFile(stray, []byte("x"), 0o600))

	require.NoError(t, b.Destroy())
	_, err = os.Stat(b.tmpDir)
	require.True(t, os.IsNotExist(err), "Destroy must remove the whole build directory")
	_, err = os.Stat(stray)
	require.True(t, os.IsNotExist(err), "a tar Destroy could not remove individually must still go")

	// Destroy is called from the abort path as well as the happy path, so it must not
	// fail when it has already run.
	require.NoError(t, b.Destroy(), "Destroy must be idempotent")
}

// TestBuildTmpDirsAreDistinctWithinOneProcess is the property the pid alone does NOT
// give: concurrent CREATE INDEX statements share a pid, so uniqueness has to come from
// MkdirTemp's random suffix. If two builders ever shared a directory, one Destroy would
// delete the other's in-flight tars.
func TestBuildTmpDirsAreDistinctWithinOneProcess(t *testing.T) {
	const n = 4
	seen := make(map[string]bool, n)
	builds := make([]*IvfpqBuild[float32, float32], 0, n)
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

	// Destroying one leaves the others' directories intact.
	require.NoError(t, builds[0].Destroy())
	for _, b := range builds[1:] {
		_, err := os.Stat(b.tmpDir)
		require.NoError(t, err, "one builder's Destroy removed another's directory")
	}
}

// TestBuildTmpDirReachesTheModel closes the loop: the directory is only useful if the
// sub-index actually packs into it. saveToFile calls os.CreateTemp(idx.TmpDir, ...), so
// an unset TmpDir would silently fall back to $TMPDIR and defeat the whole mechanism.
func TestBuildTmpDirReachesTheModel(t *testing.T) {
	b := newTestBuild(t)
	t.Cleanup(func() { _ = b.Destroy() })

	m, err := b.getOrCreateCurrent()
	require.NoError(t, err)
	require.Equal(t, b.tmpDir, m.TmpDir,
		"the model must pack into the builder's directory, not $TMPDIR")

	// And the tar really lands there. Go through the MODEL's AddChunk, not the raw
	// cuVS handle: saveToFile short-circuits on idx.Len == 0, and only the model
	// wrapper maintains Len.
	rows := uint64(testNVectors)
	require.NoError(t, m.AddChunk(make([]float32, uint64(testDim)*rows), rows,
		make([]int64, rows)))
	require.NoError(t, m.Build())
	require.NoError(t, m.saveToFile())
	require.NotEmpty(t, m.Path)
	require.Equal(t, b.tmpDir, filepath.Dir(m.Path), "tar written outside the build directory")
}
