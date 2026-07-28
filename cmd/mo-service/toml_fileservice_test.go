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
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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
