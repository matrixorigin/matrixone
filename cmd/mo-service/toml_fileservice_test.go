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

package main

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// keep in sync with the switch in pkg/fileservice/config.go:NewFileService
var knownFileServiceBackends = map[string]bool{
	"MEM": true, "DISK": true, "DISK-V2": true,
	"DISK-ETL": true, "DISK-TMP": true, "S3": true, "MINIO": true,
}

func launchTomlFiles(t *testing.T, patterns ...string) []string {
	root := filepath.Join("..", "..")
	var files []string
	for _, p := range patterns {
		m, err := filepath.Glob(filepath.Join(root, p))
		require.NoError(t, err)
		files = append(files, m...)
	}
	require.NotEmpty(t, files, "no launch tomls found")
	return files
}

// TestLaunchTomlsParse confirms every shipped *.toml service config still parses
// into a Config after the DISK-V2 edits. (*.toml.base files are templates with
// %placeholder% values that are substituted before use, so they are not valid
// TOML on their own and are covered by the backend-string scan below instead.)
func TestLaunchTomlsParse(t *testing.T) {
	files := launchTomlFiles(t,
		"etc/launch/*.toml",
		"etc/launch-*/*.toml",
		"etc/launch-*/config/*.toml",
		// etc/v1 is the legacy-DISK compatibility tree shipped for
		// "run old data after upgrading past the DISK-V2 default"; it must
		// parse and use only known backends too.
		"etc/v1/launch/*.toml",
		"etc/v1/launch-*/*.toml",
		"etc/v1/launch-*/config/*.toml",
	)
	for _, f := range files {
		// launch.toml is a cluster manifest (lists of service files), not a
		// service Config — skip it.
		if filepath.Base(f) == "launch.toml" {
			continue
		}
		data, err := os.ReadFile(f)
		require.NoErrorf(t, err, "read %s", f)

		var cfg Config
		require.NoErrorf(t, parseFromString(string(data), &cfg), "parse %s", f)

		for _, fs := range cfg.FileServices {
			require.Truef(t, knownFileServiceBackends[strings.ToUpper(fs.Backend)],
				"%s: fileservice %q has unknown backend %q", f, fs.Name, fs.Backend)
		}
	}
}

// TestLaunchTomlBackendsValid scans every launch toml AND template for
// `backend = "X"` and asserts X is a backend the dispatcher recognizes. A pure
// string scan so it also covers the %placeholder% .toml.base templates.
func TestLaunchTomlBackendsValid(t *testing.T) {
	files := launchTomlFiles(t,
		"etc/launch/*.toml",
		"etc/launch-*/*.toml",
		"etc/launch-*/*.toml.base",
		"etc/launch-*/config/*.toml",
		// etc/v1 legacy-DISK compatibility tree (see TestLaunchTomlsParse).
		"etc/v1/launch/*.toml",
		"etc/v1/launch-*/*.toml",
		"etc/v1/launch-*/*.toml.base",
		"etc/v1/launch-*/config/*.toml",
	)
	// matches a fileservice backend line; excludes Clock/Txn `backend` keys,
	// which live under [tn.Txn.Storage] / [clock] and use values like TAE/HLC.
	re := regexp.MustCompile(`(?m)^\s*backend\s*=\s*"([^"]+)"`)
	for _, f := range files {
		data, err := os.ReadFile(f)
		require.NoErrorf(t, err, "read %s", f)
		for _, m := range re.FindAllStringSubmatch(string(data), -1) {
			val := strings.ToUpper(m[1])
			// skip non-fileservice backends (txn storage / clock / logservice)
			switch val {
			case "TAE", "MEM-KV", "MEMORYKV", "MEMORY", "HLC", "LOGSERVICE", "NONE":
				continue
			}
			require.Truef(t, knownFileServiceBackends[val],
				"%s: unknown fileservice backend %q", f, m[1])
		}
	}
}

type launchCNTopology struct {
	name       string
	staticDirs []string
	dynamicDir string
}

func loadLaunchCNConfigs(t *testing.T, topology launchCNTopology) []Config {
	t.Helper()
	root := filepath.Join("..", "..", "etc")

	var files []string
	if topology.dynamicDir == "" {
		for _, dir := range topology.staticDirs {
			files = append(files, filepath.Join(root, dir, "cn*.toml"))
		}
	} else {
		generatedDir := t.TempDir()
		require.NoError(t, genDynamicCNConfigs(generatedDir, Dynamic{
			ServiceCount: 2,
			CNTemplate:   filepath.Join(root, topology.dynamicDir, "cn.toml.base"),
		}))
		files = append(files, filepath.Join(generatedDir, "cn-*.toml"))
	}

	var matched []string
	for _, pattern := range files {
		matches, err := filepath.Glob(pattern)
		require.NoError(t, err)
		matched = append(matched, matches...)
	}
	require.Len(t, matched, 2)

	configs := make([]Config, 0, len(matched))
	for _, file := range matched {
		cfg := NewConfig()
		require.NoError(t, parseConfigFromFile(file, cfg))
		cfg.setFileserviceDefaultValues()
		configs = append(configs, *cfg)
	}
	return configs
}

func fileServiceByPath(t *testing.T, cfg Config, filePath string) fileservice.Config {
	t.Helper()
	for _, service := range cfg.FileServices {
		if strings.EqualFold(service.Name, filePath) {
			require.NotEmpty(t, service.DataDir)
			return service
		}
	}
	t.Fatalf("fileservice %s not found", filePath)
	return fileservice.Config{}
}

func normalizedDataDir(t *testing.T, dir string) string {
	t.Helper()
	absolute, err := filepath.Abs(dir)
	require.NoError(t, err)
	return filepath.Clean(absolute)
}

func loadLaunchPeerLocalRoots(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "..", "etc", dir, "*.toml"))
	require.NoError(t, err)
	require.NotEmpty(t, files)

	var roots []string
	for _, file := range files {
		if filepath.Base(file) == "launch.toml" {
			continue
		}
		cfg := NewConfig()
		require.NoError(t, parseConfigFromFile(file, cfg))
		cfg.setFileserviceDefaultValues()
		if strings.EqualFold(cfg.ServiceType, "CN") {
			continue
		}
		roots = append(roots, normalizedDataDir(
			t,
			fileServiceByPath(t, *cfg, defines.LocalFileServiceName).DataDir,
		))
	}
	return roots
}

func TestLaunchTopologiesIsolateCNLocalFileServices(t *testing.T) {
	topologies := []launchCNTopology{
		{name: "multi-cn", staticDirs: []string{"launch-multi-cn"}},
		{name: "multi-cn-with-proxy", staticDirs: []string{"launch-with-proxy"}},
		{name: "dynamic-cn", dynamicDir: "launch-dynamic-cn"},
		{name: "dynamic-cn-with-proxy", dynamicDir: "launch-dynamic-with-proxy"},
		{name: "v1-multi-cn", staticDirs: []string{"v1/launch-multi-cn"}},
		{name: "v1-multi-cn-with-proxy", staticDirs: []string{"v1/launch-with-proxy"}},
		{name: "v1-dynamic-cn", dynamicDir: "v1/launch-dynamic-cn"},
		{name: "v1-dynamic-cn-with-proxy", dynamicDir: "v1/launch-dynamic-with-proxy"},
	}

	for _, topology := range topologies {
		t.Run(topology.name, func(t *testing.T) {
			configs := loadLaunchCNConfigs(t, topology)

			var localRoots []string
			var sharedRoots []string
			for _, cfg := range configs {
				localRoots = append(localRoots, normalizedDataDir(
					t,
					fileServiceByPath(t, cfg, defines.LocalFileServiceName).DataDir,
				))
				sharedRoots = append(sharedRoots, normalizedDataDir(
					t,
					fileServiceByPath(t, cfg, defines.SharedFileServiceName).DataDir,
				))
			}

			require.NotEqual(t, localRoots[0], localRoots[1],
				"concurrent CNs must not share a LOCAL root")
			require.Equal(t, sharedRoots[0], sharedRoots[1],
				"CNs in one topology must retain their common SHARED root")

			peerDir := topology.dynamicDir
			if peerDir == "" {
				peerDir = topology.staticDirs[0]
			}
			for _, peerRoot := range loadLaunchPeerLocalRoots(t, peerDir) {
				require.NotContains(t, localRoots, peerRoot,
					"a CN LOCAL root must not overlap a concurrently startable peer")
			}
		})
	}
}

func newLaunchTestLocalFS(t *testing.T, name, dir string) (*fileservice.LocalFS, *fileservice.FileServices, func()) {
	t.Helper()
	local, err := fileservice.NewLocalFS2(
		context.Background(),
		defines.LocalFileServiceName,
		dir,
		fileservice.DisabledCacheConfig,
		nil,
	)
	require.NoError(t, err)
	services, err := fileservice.NewFileServices(name, local)
	require.NoError(t, err)
	var closeOnce sync.Once
	closeServices := func() {
		closeOnce.Do(func() {
			services.Close(context.Background())
		})
	}
	t.Cleanup(closeServices)
	return local, services, closeServices
}

func writeSpillFile(t *testing.T, local *fileservice.LocalFS, name string, data []byte) {
	t.Helper()
	spill := fileservice.SubPath(local, defines.SpillFileServiceName)
	require.NoError(t, spill.Write(context.Background(), fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Size: int64(len(data)), Data: data}},
	}))
}

func requireSpillFile(t *testing.T, local *fileservice.LocalFS, name string, expected []byte) {
	t.Helper()
	spill := fileservice.SubPath(local, defines.SpillFileServiceName)
	vector := fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Size: int64(len(expected))}},
	}
	require.NoError(t, spill.Read(context.Background(), &vector))
	require.Equal(t, expected, vector.Entries[0].Data)
}

func requireSpillFileMissing(t *testing.T, local *fileservice.LocalFS, name string) {
	t.Helper()
	spill := fileservice.SubPath(local, defines.SpillFileServiceName)
	vector := fileservice.IOVector{
		FilePath: name,
		Entries:  []fileservice.IOEntry{{Size: 1}},
	}
	require.Error(t, spill.Read(context.Background(), &vector))
}

func TestClearSpillFilesPreservesPeerCNLocalRoot(t *testing.T) {
	configs := loadLaunchCNConfigs(t, launchCNTopology{
		name:       "multi-cn",
		staticDirs: []string{"launch-multi-cn"},
	})
	root := t.TempDir()
	cn1Dir := filepath.Join(root, filepath.Base(normalizedDataDir(
		t,
		fileServiceByPath(t, configs[0], defines.LocalFileServiceName).DataDir,
	)))
	cn2Dir := filepath.Join(root, filepath.Base(normalizedDataDir(
		t,
		fileServiceByPath(t, configs[1], defines.LocalFileServiceName).DataDir,
	)))
	require.NotEqual(t, cn1Dir, cn2Dir,
		"startup cleanup fixture requires the shipped CN roots to be distinct")

	cn1Local, cn1Services, _ := newLaunchTestLocalFS(t, "cn1", cn1Dir)
	cn2Local, cn2Services, closeCN2 := newLaunchTestLocalFS(t, "cn2", cn2Dir)

	peer := []byte("cn1-live-spill")
	ownStale := []byte("cn2-stale-spill")
	writeSpillFile(t, cn1Local, "peer", peer)
	writeSpillFile(t, cn2Local, "stale", ownStale)

	require.NoError(t, clearSpillFiles(context.Background(), cn2Services))
	requireSpillFile(t, cn1Local, "peer", peer)
	requireSpillFileMissing(t, cn2Local, "stale")

	// A fresh FS instance on the same stable root cleans only its own restart leftovers.
	closeCN2()
	cn2Local, cn2Services, _ = newLaunchTestLocalFS(t, "cn2", cn2Dir)
	writeSpillFile(t, cn2Local, "stale-again", ownStale)
	require.NoError(t, clearSpillFiles(context.Background(), cn2Services))
	requireSpillFile(t, cn1Local, "peer", peer)
	requireSpillFileMissing(t, cn2Local, "stale-again")

	// Reverse ownership: cleaning CN1 must leave CN2's live spill alone.
	writeSpillFile(t, cn2Local, "peer", peer)
	writeSpillFile(t, cn1Local, "stale", ownStale)
	require.NoError(t, clearSpillFiles(context.Background(), cn1Services))
	requireSpillFile(t, cn2Local, "peer", peer)
	requireSpillFileMissing(t, cn1Local, "stale")
}
