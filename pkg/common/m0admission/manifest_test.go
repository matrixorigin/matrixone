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

package m0admission

import (
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	manifest, path, _ := admissionFixture(t)
	extra := filepath.Join(manifest.Root, "extra.toml")
	require.NoError(t, os.WriteFile(extra, []byte("# second bound configuration\n"), 0600))
	digest, err := fileHash(extra)
	require.NoError(t, err)
	manifest.Files[extra] = digest
	writeAdmissionJSON(t, path, manifest)
	close, err := Initialize(manifest.Args)
	if !birthBuildEnabled {
		if close != nil {
			t.Cleanup(close)
		}
		require.ErrorContains(t, err, "this binary lacks birth-fill capability")
		require.Nil(t, close)
		require.False(t, Enabled())
		// Ordinary binaries remain closed through the public entry point. The
		// same manifest contract still needs unit coverage in both build modes.
		close, err = initializeManifest(path, manifest.Args)
	}
	require.NoError(t, err)
	defer close()
	require.True(t, Enabled())
	paths := make([]string, 0, len(manifest.Files))
	for path := range manifest.Files {
		paths = append(paths, path)
	}
	require.NoError(t, CheckConfigFiles(paths))
	require.ErrorContains(t, CheckConfigFiles(nil), "configuration set differs")
	require.ErrorContains(t, CheckConfigFiles([]string{paths[0], filepath.Join(manifest.Root, "undeclared.toml")}), "undeclared configuration")
	require.ErrorContains(t, CheckConfigFiles([]string{paths[0], paths[0]}), "repeated configuration")
	require.NoError(t, CheckManagedPath(filepath.Join(manifest.Root, "data", "new")))
	require.ErrorContains(t, CheckManagedPath("relative"), "path must be absolute and clean")
	require.Error(t, CheckManagedPath(filepath.Dir(manifest.Root)))
	require.ErrorIs(t, CheckManagedPath(filepath.Join(extra, "child")), syscall.ENOTDIR)
	broken := filepath.Join(manifest.Root, "broken")
	require.NoError(t, os.Symlink(filepath.Join(manifest.Root, "absent"), broken))
	require.ErrorIs(t, CheckManagedPath(filepath.Join(broken, "child")), os.ErrNotExist)
	require.NoError(t, CheckDataDirectory(manifest.Root))
	require.ErrorContains(t, CheckDataDirectory(filepath.Dir(manifest.Root)), "path escapes managed root")
	require.ErrorContains(t, CheckDataDirectory(filepath.Join(manifest.Root, "undeclared")), "undeclared data/cache directory")
	t.Run("live data binding", func(t *testing.T) {
		original, err := json.Marshal(manifest.Marker)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, unix.Setxattr(manifest.Root, DataXattrName, original, 0)) })
		changed := manifest.Marker
		changed.BinarySHA256 = strings.Repeat("4", 64)
		data, err := json.Marshal(changed)
		require.NoError(t, err)
		require.NoError(t, unix.Setxattr(manifest.Root, DataXattrName, data, 0))
		require.ErrorContains(t, CheckDataDirectory(manifest.Root), "data-directory binary binding differs")
		require.NoError(t, unix.Setxattr(manifest.Root, DataXattrName, []byte("{"), 0))
		require.ErrorContains(t, CheckDataDirectory(manifest.Root), "unexpected EOF")
	})
	_, err = initializeManifest(path, manifest.Args)
	require.ErrorContains(t, err, "deployment already initialized")
	require.True(t, Enabled(), "failed reinitialization must preserve the active owner")
	require.NoError(t, os.WriteFile(paths[0], []byte("# changed"), 0600))
	require.Error(t, CheckConfigFiles(paths))
	require.NoError(t, os.Remove(paths[0]))
	require.ErrorIs(t, CheckConfigFiles(paths), os.ErrNotExist)
	close()
	require.False(t, Enabled())
	require.NoError(t, CheckManagedPath("unused outside admission"))
	require.NoError(t, CheckConfigFiles(nil))
}

func TestBirthAdmissionRejectsUnsafeIdentity(t *testing.T) {
	for _, scenario := range []string{
		"binary", "marker", "lock-missing", "lock-unheld", "args", "config", "unknown-format", "symlink-escape", "data-rebound", "unknown-field", "trailing-json",
		"wrong-cwd", "manifest-outside-root", "lock-rebound", "marker-missing", "no-data-roots", "duplicate-data-root", "missing-data-root", "file-data-root", "outside-data-root", "config-missing", "config-outside-root", "config-is-directory",
		"lock-closed", "lock-path-missing", "lock-path-escape", "data-marker-invalid", "run-marker-invalid",
	} {
		t.Run(scenario, func(t *testing.T) {
			manifest, path, lock := admissionFixture(t)
			args := manifest.Args
			var expected string
			var expectedCause error
			switch scenario {
			case "binary":
				expected = "executable identity differs"
				manifest.BinarySHA256 = strings.Repeat("0", 64)
				writeAdmissionJSON(t, path, manifest)
			case "data-rebound":
				expected = "data-directory format/binary differs"
				other := manifest.Marker
				other.BinarySHA256 = strings.Repeat("4", 64)
				data, e := json.Marshal(other)
				require.NoError(t, e)
				require.NoError(t, unix.Setxattr(manifest.Root, DataXattrName, data, 0))
			case "marker":
				expected = "data format/binary binding differs"
				marker := manifest.Marker
				marker.SourceSHA = strings.Repeat("3", 40)
				writeAdmissionJSON(t, filepath.Join(manifest.Root, MarkerName), marker)
			case "lock-missing":
				expected = "missing inherited lock"
				t.Setenv(LockFDEnv, "")
			case "lock-unheld":
				expected = "lifetime lock is not held"
				require.NoError(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_UN))
			case "args":
				expected = "invalid manifest identity or arguments"
				args = []string{"-cfg", "other.toml"}
			case "config":
				expected = "config identity differs"
				for config := range manifest.Files {
					require.NoError(t, os.WriteFile(config, []byte("changed"), 0600))
				}
			case "unknown-format":
				expected = "invalid manifest identity or arguments"
				manifest.FormatVersion++
				writeAdmissionJSON(t, path, manifest)
			case "symlink-escape":
				expected = "invalid managed root"
				outside := t.TempDir()
				link := filepath.Join(manifest.Root, "escape")
				require.NoError(t, os.Symlink(outside, link))
				require.Error(t, CheckPath(manifest.Root, filepath.Join(link, "new")))
				manifest.Root = link
				writeAdmissionJSON(t, path, manifest)
			case "unknown-field":
				expected = "unknown field"
				data, err := json.Marshal(manifest)
				require.NoError(t, err)
				data = append(data[:len(data)-1], []byte(",\"unexpected\":true}")...)
				require.NoError(t, os.WriteFile(path, data, 0600))
			case "trailing-json":
				expected = "trailing manifest data"
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, append(data, []byte(" {}")...), 0600))
			case "wrong-cwd":
				expected = "working directory differs"
				t.Chdir(filepath.Dir(manifest.Root))
			case "manifest-outside-root":
				expected = "path escapes managed root"
				path = filepath.Join(t.TempDir(), "manifest.json")
				writeAdmissionJSON(t, path, manifest)
			case "lock-rebound":
				expected = "lock identity differs"
				other, err := os.CreateTemp(manifest.Root, "other-lock")
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, other.Close()) })
				t.Setenv(LockFDEnv, strconv.Itoa(int(other.Fd())))
			case "marker-missing":
				expectedCause = os.ErrNotExist
				require.NoError(t, os.Remove(filepath.Join(manifest.Root, MarkerName)))
			case "lock-closed":
				expectedCause = syscall.EBADF
				require.NoError(t, lock.Close())
			case "lock-path-missing", "lock-path-escape":
				expectedCause = os.ErrNotExist
				lockPath := filepath.Join(manifest.Root, LockName)
				require.NoError(t, os.Remove(lockPath))
				if scenario == "lock-path-escape" {
					expectedCause = nil
					expected = "path escapes managed root"
					outside := filepath.Join(t.TempDir(), "foreign-lock")
					require.NoError(t, os.WriteFile(outside, nil, 0600))
					require.NoError(t, os.Symlink(outside, lockPath))
				}
			case "data-marker-invalid":
				expected = "unexpected EOF"
				require.NoError(t, unix.Setxattr(manifest.Root, DataXattrName, []byte("{"), 0))
			case "run-marker-invalid":
				expected = "unexpected EOF"
				require.NoError(t, os.WriteFile(filepath.Join(manifest.Root, MarkerName), []byte("{"), 0600))
			case "no-data-roots", "duplicate-data-root", "missing-data-root", "file-data-root", "outside-data-root":
				switch scenario {
				case "no-data-roots":
					expected = "no bound data directories"
					manifest.DataRoots = nil
				case "duplicate-data-root":
					expected = "repeated data directory"
					manifest.DataRoots = append(manifest.DataRoots, manifest.Root)
				case "missing-data-root":
					expectedCause = os.ErrNotExist
					manifest.DataRoots = []string{filepath.Join(manifest.Root, "absent")}
				case "file-data-root":
					expected = "data root is not a directory"
					manifest.DataRoots = []string{path}
				case "outside-data-root":
					expected = "path escapes managed root"
					manifest.DataRoots = []string{filepath.Dir(manifest.Root)}
				}
				writeAdmissionJSON(t, path, manifest)
			case "config-missing":
				expectedCause = os.ErrNotExist
				for config := range manifest.Files {
					require.NoError(t, os.Remove(config))
				}
			case "config-outside-root":
				expected = "path escapes managed root"
				manifest.Files = map[string]string{filepath.Join(t.TempDir(), "config.toml"): "unused"}
				writeAdmissionJSON(t, path, manifest)
			case "config-is-directory":
				expected = "not a regular file"
				manifest.Files = map[string]string{manifest.Root: "unused"}
				writeAdmissionJSON(t, path, manifest)
			}
			close, err := initializeManifest(path, args)
			if close != nil {
				t.Cleanup(close)
			}
			if expectedCause != nil {
				require.ErrorIs(t, err, expectedCause)
			} else {
				require.ErrorContains(t, err, expected)
			}
			require.Nil(t, close)
			require.False(t, Enabled())
		})
	}
}

func TestBirthAdmissionUnadmittedDataDirectory(t *testing.T) {
	t.Setenv(ManifestEnv, "")
	root := t.TempDir()
	marker := Marker{FormatVersion: 1, BinarySHA256: strings.Repeat("1", 64), SourceSHA: strings.Repeat("2", 40), NativeSHA256: strings.Repeat("3", 64)}
	data, err := json.Marshal(marker)
	require.NoError(t, err)
	require.NoError(t, unix.Setxattr(root, DataXattrName, data, 0))
	require.NoFileExists(t, filepath.Join(root, MarkerName), "this must prove the actual directory marker, without a run sidecar")
	err = CheckDataDirectory(filepath.Join(root, "new-child"))
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "admission failures use the standard error contract")
	require.NoError(t, unix.Removexattr(root, DataXattrName))
	require.NoError(t, CheckDataDirectory(filepath.Join(root, "new-child")))
	for _, tc := range []struct {
		payload []byte
		err     string
	}{
		{nil, "invalid data-directory marker size"},
		{[]byte("{} {}"), "trailing data-directory marker"},
		{[]byte(`{"unknown":1}`), "unknown field"},
	} {
		require.NoError(t, unix.Setxattr(root, DataXattrName, tc.payload, 0))
		require.ErrorContains(t, CheckDataDirectory(filepath.Join(root, "new-child")), tc.err)
	}
	require.NoError(t, unix.Removexattr(root, DataXattrName))
	writeAdmissionJSON(t, filepath.Join(root, MarkerName), marker)
	require.ErrorContains(t, CheckDataDirectory(filepath.Join(root, "new-child")), "candidate data requires its guarded binary")
}
