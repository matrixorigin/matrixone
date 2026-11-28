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

package optools

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestComposeLogCleanupPreservesSymlinkTarget(t *testing.T) {
	source, err := os.ReadFile("compose_bvt/compose_bvt.sh")
	if err != nil {
		t.Fatal(err)
	}
	code := strings.SplitN(strings.SplitN(string(source), "<<'PY'\n", 2)[1], "\nPY", 2)[0]
	for _, mode := range []string{"directory", "symlink", "dangling"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			logs := filepath.Join(root, "workspace with spaces", "docker-compose-log")
			if err := os.MkdirAll(filepath.Dir(logs), 0700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(root, "external")
			if err := os.Mkdir(target, 0700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(target, "retain")
			if err := os.WriteFile(sentinel, []byte("original"), 0600); err != nil {
				t.Fatal(err)
			}
			if mode == "directory" {
				if err := os.Mkdir(logs, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(logs, "old"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				link := target
				if mode == "dangling" {
					link = filepath.Join(target, "missing")
				}
				if err := os.Symlink(link, logs); err != nil {
					t.Fatal(err)
				}
			}
			// Feed the actual inline cleanup, without starting Docker services.
			cmd := exec.Command("python3", "-", logs)
			cmd.Stdin = strings.NewReader(code)
			output, err := cmd.CombinedOutput()
			if mode == "directory" {
				if err != nil {
					t.Fatalf("%v: %s", err, output)
				}
				entries, err := os.ReadDir(logs)
				if err != nil || len(entries) != 0 {
					t.Fatalf("log directory not empty: %v", err)
				}
			} else if err == nil || !strings.Contains(string(output), "refusing symlink") {
				t.Fatalf("symlink accepted: %v %s", err, output)
			}
			data, err := os.ReadFile(sentinel)
			if err != nil || string(data) != "original" {
				t.Fatalf("target altered: %v", err)
			}
		})
	}
}
