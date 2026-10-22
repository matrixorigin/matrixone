// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package m0admission

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func writeAdmissionJSON(t *testing.T, path string, value any) {
	t.Helper()
	b, e := json.Marshal(value)
	require.NoError(t, e)
	require.NoError(t, os.WriteFile(path, b, 0600))
}

func admissionFixture(t *testing.T) (*Manifest, string, *os.File) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	t.Chdir(root)
	exe, err := os.Executable()
	require.NoError(t, err)
	digest, err := fileHash(exe)
	require.NoError(t, err)
	config := filepath.Join(root, "launch.toml")
	require.NoError(t, os.WriteFile(config, []byte("# test configuration\n"), 0600))
	configHash, err := fileHash(config)
	require.NoError(t, err)
	manifest := &Manifest{Marker: Marker{FormatVersion: 1, BinarySHA256: digest, SourceSHA: strings.Repeat("1", 40), NativeSHA256: strings.Repeat("2", 64)},
		RunID: "admission-unit", Root: root, Args: []string{"-launch", config}, Files: map[string]string{config: configHash}}
	manifest.DataRoots = []string{root}
	markerBytes, err := json.Marshal(manifest.Marker)
	require.NoError(t, err)
	require.NoError(t, unix.Setxattr(root, DataXattrName, markerBytes, 0))
	path := filepath.Join(root, "manifest.json")
	writeAdmissionJSON(t, path, manifest)
	writeAdmissionJSON(t, filepath.Join(root, MarkerName), manifest.Marker)
	lock, err := os.OpenFile(filepath.Join(root, LockName), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	require.NoError(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	t.Cleanup(func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); _ = lock.Close() })
	t.Setenv(ManifestEnv, path)
	t.Setenv(LockFDEnv, strconv.Itoa(int(lock.Fd())))
	return manifest, path, lock
}

func TestBirthAdmissionAbsentIsClosed(t *testing.T) {
	t.Setenv(ManifestEnv, "")
	close, err := Initialize(nil)
	require.NoError(t, err)
	defer close()
	require.False(t, Enabled())
}

func TestBirthAdmissionIdentityAndLifetime(t *testing.T) {
	manifest, _, _ := admissionFixture(t)
	close, err := Initialize(manifest.Args)
	if !birthBuildEnabled {
		require.Error(t, err)
		require.False(t, Enabled())
		return
	}
	require.NoError(t, err)
	defer close()
	require.True(t, Enabled())
	var paths []string
	for path := range manifest.Files {
		paths = append(paths, path)
	}
	require.NoError(t, CheckConfigFiles(paths))
	require.NoError(t, CheckManagedPath(filepath.Join(manifest.Root, "data", "new")))
	require.Error(t, CheckManagedPath(filepath.Dir(manifest.Root)))
	require.NoError(t, os.WriteFile(paths[0], []byte("# changed"), 0600))
	require.Error(t, CheckConfigFiles(paths))
	close()
	require.False(t, Enabled())
}

func TestBirthAdmissionRejectsUnsafeIdentity(t *testing.T) {
	for _, scenario := range []string{"binary", "marker", "lock-missing", "lock-unheld", "args", "config", "unknown-format", "symlink-escape", "data-rebound"} {
		t.Run(scenario, func(t *testing.T) {
			manifest, path, lock := admissionFixture(t)
			args := manifest.Args
			switch scenario {
			case "binary":
				manifest.BinarySHA256 = strings.Repeat("0", 64)
				writeAdmissionJSON(t, path, manifest)
			case "data-rebound":
				other := manifest.Marker
				other.BinarySHA256 = strings.Repeat("4", 64)
				data, e := json.Marshal(other)
				require.NoError(t, e)
				require.NoError(t, unix.Setxattr(manifest.Root, DataXattrName, data, 0))
			case "marker":
				marker := manifest.Marker
				marker.SourceSHA = strings.Repeat("3", 40)
				writeAdmissionJSON(t, filepath.Join(manifest.Root, MarkerName), marker)
			case "lock-missing":
				t.Setenv(LockFDEnv, "")
			case "lock-unheld":
				require.NoError(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_UN))
			case "args":
				args = []string{"-cfg", "other.toml"}
			case "config":
				for config := range manifest.Files {
					require.NoError(t, os.WriteFile(config, []byte("changed"), 0600))
				}
			case "unknown-format":
				manifest.FormatVersion++
				writeAdmissionJSON(t, path, manifest)
			case "symlink-escape":
				outside := t.TempDir()
				link := filepath.Join(manifest.Root, "escape")
				require.NoError(t, os.Symlink(outside, link))
				require.Error(t, CheckPath(manifest.Root, filepath.Join(link, "new")))
				manifest.Root = link
				writeAdmissionJSON(t, path, manifest)
			}
			_, err := Initialize(args)
			require.Error(t, err)
			require.False(t, Enabled())
		})
	}
}
