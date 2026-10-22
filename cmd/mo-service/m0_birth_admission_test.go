//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/m0admission"
	"github.com/stretchr/testify/require"
)

func TestM0BirthStartupAdmission(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	t.Chdir(root)
	hash := func(path string) string {
		f, e := os.Open(path)
		require.NoError(t, e)
		defer f.Close()
		h := sha256.New()
		_, e = io.Copy(h, f)
		require.NoError(t, e)
		return hex.EncodeToString(h.Sum(nil))
	}
	write := func(path string, value any) {
		b, e := json.Marshal(value)
		require.NoError(t, e)
		require.NoError(t, os.WriteFile(path, b, 0600))
	}
	launch := filepath.Join(root, "launch.toml")
	require.NoError(t, os.WriteFile(launch, []byte("# launch hash fixture"), 0600))
	cfg := &LaunchConfig{}
	files := map[string]string{launch: hash(launch)}
	for _, role := range []string{"LOG", "TN", "CN"} {
		path := filepath.Join(root, role+".toml")
		require.NoError(t, os.WriteFile(path, []byte(fmt.Sprintf("service-type=%q\ndata-dir=%q\n", role, filepath.Join(root, "data"))), 0600))
		files[path] = hash(path)
		switch role {
		case "LOG":
			cfg.LogServiceConfigFiles = []string{path}
		case "TN":
			cfg.TNServiceConfigsFiles = []string{path}
		case "CN":
			cfg.CNServiceConfigsFiles = []string{path}
		}
	}
	exe, err := os.Executable()
	require.NoError(t, err)
	marker := m0admission.Marker{FormatVersion: 1, BinarySHA256: hash(exe), SourceSHA: strings.Repeat("1", 40), NativeSHA256: strings.Repeat("2", 64)}
	manifest := m0admission.Manifest{Marker: marker, RunID: "startup-unit", Root: root, Args: []string{"-launch", launch}, Files: files}
	manifestPath := filepath.Join(root, "manifest.json")
	write(manifestPath, manifest)
	write(filepath.Join(root, m0admission.MarkerName), marker)
	lock, err := os.OpenFile(filepath.Join(root, m0admission.LockName), os.O_CREATE|os.O_RDWR, 0600)
	require.NoError(t, err)
	defer lock.Close()
	require.NoError(t, syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	t.Setenv(m0admission.ManifestEnv, manifestPath)
	t.Setenv(m0admission.LockFDEnv, strconv.Itoa(int(lock.Fd())))
	close, err := m0admission.Initialize(manifest.Args)
	require.NoError(t, err)
	defer close()
	oldLaunch := *launchFile
	*launchFile = launch
	t.Cleanup(func() { *launchFile = oldLaunch })
	require.NoError(t, validateM0BirthLaunch(cfg))
	cfg.Dynamic.Enable = true
	require.Error(t, validateM0BirthLaunch(cfg))
	cfg.Dynamic.Enable = false
	cfg.CNServiceConfigsFiles = append(cfg.CNServiceConfigsFiles, cfg.CNServiceConfigsFiles[0])
	require.Error(t, validateM0BirthLaunch(cfg))
	cfg.CNServiceConfigsFiles = cfg.CNServiceConfigsFiles[:1]
	outside := NewConfig()
	outside.DataDir = filepath.Dir(root)
	require.Error(t, validateM0BirthConfig(outside))
	close()
	require.Error(t, validateM0BirthConfig(&Config{DataDir: filepath.Join(root, "data")}))
}
