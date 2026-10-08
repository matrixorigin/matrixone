// Copyright 2021 Matrix Origin
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

package memory

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
)

// A nil or LOCAL-less fileservice must yield "" -- os.MkdirTemp reads that as
// $TMPDIR, so the fallback is the pre-change behaviour and no caller needs a branch.
func TestHostSpillDirFallsBackWithoutLocalFS(t *testing.T) {
	require.Equal(t, "", HostSpillDir(context.Background(), nil, "cn0"))
}

// With a LOCAL fileservice the directory is created under its root, so the tars
// land on the provisioned data volume instead of /tmp.
func TestHostSpillDirCreatesUnderLocalRoot(t *testing.T) {
	root := t.TempDir()
	local, err := fileservice.NewLocalFS(context.Background(),
		defines.LocalFileServiceName, root, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	got := HostSpillDir(context.Background(), local, "cn0")
	require.Equal(t, filepath.Join(root, localSpillSubdir, "cn0"), got,
		"each CN gets its own subdirectory so a shared LOCAL volume stays safe to sweep")

	fi, err := os.Stat(got)
	require.NoError(t, err, "the directory must be created, not just named")
	require.True(t, fi.IsDir())

	// Idempotent: a second build must not fail on an existing directory.
	require.Equal(t, got, HostSpillDir(context.Background(), local, "cn0"))
}

// A spill file outliving the process that mapped it is an orphan: nothing else collects
// the LOCAL volume, so the next start of the SAME CN reclaims its own leftovers.
func TestHostSpillDirSweepsOrphansOnFirstUse(t *testing.T) {
	root := t.TempDir()
	local, err := fileservice.NewLocalFS(context.Background(),
		defines.LocalFileServiceName, root, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	// A previous incarnation of cn0 left a model behind, and a neighbour CN has a live one.
	mine := filepath.Join(root, localSpillSubdir, "cn0")
	theirs := filepath.Join(root, localSpillSubdir, "cn1")
	require.NoError(t, os.MkdirAll(mine, 0o755))
	require.NoError(t, os.MkdirAll(theirs, 0o755))
	orphan := filepath.Join(mine, "hnsw123456")
	live := filepath.Join(theirs, "hnsw999999")
	require.NoError(t, os.WriteFile(orphan, []byte("stale model"), 0o644))
	require.NoError(t, os.WriteFile(live, []byte("in use"), 0o644))

	got := HostSpillDir(context.Background(), local, "cn0")
	require.Equal(t, mine, got)

	_, err = os.Stat(orphan)
	require.True(t, os.IsNotExist(err), "this CN's own leftover is reclaimed")
	_, err = os.Stat(live)
	require.NoError(t, err, "another CN's file is never touched")

	// The sweep runs once per process: a file created after it must survive later calls,
	// or an in-flight load would lose its own mapping.
	inflight := filepath.Join(mine, "hnsw777777")
	require.NoError(t, os.WriteFile(inflight, []byte("mapped right now"), 0o644))
	require.Equal(t, mine, HostSpillDir(context.Background(), local, "cn0"))
	_, err = os.Stat(inflight)
	require.NoError(t, err, "the sweep must not run again and delete a live file")
}

// With no service id there is no owner to attribute files to, so nothing is swept.
func TestHostSpillDirDoesNotSweepWithoutService(t *testing.T) {
	root := t.TempDir()
	local, err := fileservice.NewLocalFS(context.Background(),
		defines.LocalFileServiceName, root, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	shared := filepath.Join(root, localSpillSubdir, "shared")
	require.NoError(t, os.MkdirAll(shared, 0o755))
	keep := filepath.Join(shared, "hnsw000")
	require.NoError(t, os.WriteFile(keep, []byte("x"), 0o644))

	require.Equal(t, shared, HostSpillDir(context.Background(), local, ""))
	_, err = os.Stat(keep)
	require.NoError(t, err)
}

// A service id becomes a path segment, so it must not be able to escape the spill
// directory. A real CN uuid passes through untouched; anything that could traverse is
// neutralised rather than preserved.
func TestSpillOwnerSanitizes(t *testing.T) {
	require.Equal(t, "shared", spillOwner(""))
	require.Equal(t, "dd1dccb4-4d3c-41f8-b482-5251dc7a41bf",
		spillOwner("dd1dccb4-4d3c-41f8-b482-5251dc7a41bf"),
		"a configured CN uuid is already a safe segment")

	for _, hostile := range []string{"/../../etc", "..", "a/b", `a\b`, "a\x00b"} {
		got := spillOwner(hostile)
		require.NotContains(t, got, "/", hostile)
		require.NotContains(t, got, `\`, hostile)
		require.NotContains(t, got, "..", hostile)
		require.Equal(t, got, filepath.Base(got), "stays a single segment: %q", hostile)
	}
}
