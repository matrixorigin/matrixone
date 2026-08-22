//go:build gpu

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

package cuvs

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTestTar writes a tar (optionally gzipped) containing the given files.
// `manifest` is written verbatim as manifest.json when non-nil; extras is a
// map of extra file names → bytes. Returns the tar path.
func writeTestTar(t *testing.T, gzipped bool, manifest []byte, extras map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	name := "test.tar"
	if gzipped {
		name = "test.tar.gz"
	}
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create tar: %v", err)
	}
	defer f.Close()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	write := func(name string, body []byte) {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			t.Fatalf("tar hdr %s: %v", name, err)
		}
		if _, err := tw.Write(body); err != nil {
			t.Fatalf("tar body %s: %v", name, err)
		}
	}
	if manifest != nil {
		write("manifest.json", manifest)
	}
	for k, v := range extras {
		write(k, v)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}

	if gzipped {
		gz := gzip.NewWriter(f)
		if _, err := gz.Write(buf.Bytes()); err != nil {
			t.Fatalf("gzip write: %v", err)
		}
		if err := gz.Close(); err != nil {
			t.Fatalf("gzip close: %v", err)
		}
	} else {
		if _, err := f.Write(buf.Bytes()); err != nil {
			t.Fatalf("file write: %v", err)
		}
	}
	return path
}

func TestPeekTarManifestNShards(t *testing.T) {
	cases := []struct {
		name     string
		gzipped  bool
		manifest []byte
		want     uint32
		wantErr  string // substring; empty = no error
	}{
		{
			name:     "sharded 2 via shard_sizes",
			manifest: []byte(`{"build_params":{"shard_sizes":[500000,500000]}}`),
			want:     2,
		},
		{
			name:     "sharded 4 via components.shards",
			manifest: []byte(`{"components":{"shards":["s_0.bin","s_1.bin","s_2.bin","s_3.bin"]}}`),
			want:     4,
		},
		{
			name:     "sharded 2 gzipped",
			gzipped:  true,
			manifest: []byte(`{"build_params":{"shard_sizes":[1,1]}}`),
			want:     2,
		},
		{
			name:     "single / replicated has no shards keys",
			manifest: []byte(`{"schema_version":1,"index_type":"cagra"}`),
			want:     0,
		},
		{
			name:     "shard_sizes wins over components.shards when both present",
			manifest: []byte(`{"build_params":{"shard_sizes":[1,1,1]},"components":{"shards":["a","b"]}}`),
			want:     3,
		},
		{
			name:     "missing manifest",
			manifest: nil, // no manifest at all
			wantErr:  "manifest.json not found",
		},
		{
			name:     "malformed manifest json",
			manifest: []byte(`{not json`),
			wantErr:  "parse manifest",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTestTar(t, tc.gzipped, tc.manifest, nil)
			got, err := PeekTarManifestNShards(path)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("want shard count %d, got %d", tc.want, got)
			}
		})
	}
}

func TestPeekTarManifestNShards_BadPath(t *testing.T) {
	_, err := PeekTarManifestNShards(filepath.Join(t.TempDir(), "nonexistent.tar"))
	if err == nil {
		t.Fatal("expected error on missing tar path")
	}
}

func TestResolveDevicesForTarLoad(t *testing.T) {
	writeShardTar := func(n int) string {
		if n == 0 {
			// SINGLE / REPLICATED — no shards entry
			return writeTestTar(t, false, []byte(`{"index_type":"cagra"}`), nil)
		}
		sizes := "["
		for i := 0; i < n; i++ {
			if i > 0 {
				sizes += ","
			}
			sizes += "1"
		}
		sizes += "]"
		return writeTestTar(t, false, []byte(`{"build_params":{"shard_sizes":`+sizes+`}}`), nil)
	}

	cases := []struct {
		name       string
		devices    []int
		shardCount int
		wantErr    string
		wantResult []int  // devices returned on success (nil = same slice header, len check only when wantLen set)
		wantLen    int    // if wantResult is nil, verify len(result) == wantLen
	}{
		{
			name:       "SINGLE/REPLICATED tar returns devices unchanged",
			devices:    []int{0, 1, 2},
			shardCount: 0,
			wantResult: []int{0, 1, 2},
		},
		{
			name:       "shards == devices: pass through",
			devices:    []int{0, 1},
			shardCount: 2,
			wantResult: []int{0, 1},
		},
		{
			name:       "shards < devices: trim to first N",
			devices:    []int{0, 1, 2, 3},
			shardCount: 2,
			wantResult: []int{0, 1},
		},
		{
			name:       "single-GPU host, 2 shards: pad [0,0] (sim path)",
			devices:    []int{0},
			shardCount: 2,
			wantResult: []int{0, 0},
		},
		{
			name:       "single-GPU host, 4 shards: pad to 4",
			devices:    []int{5},
			shardCount: 4,
			wantResult: []int{5, 5, 5, 5},
		},
		{
			name:       "multi-GPU host, shards > devices: ERROR (no silent overload)",
			devices:    []int{0, 1},
			shardCount: 3,
			wantErr:    "needs 3 devices",
		},
		{
			name:       "4-shard index on 2-GPU host: ERROR",
			devices:    []int{0, 1},
			shardCount: 4,
			wantErr:    "needs 4 devices",
		},
		{
			name:       "empty devices with shards: ERROR (no fallback)",
			devices:    []int{},
			shardCount: 2,
			wantErr:    "none are available",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tarPath := writeShardTar(tc.shardCount)
			resolved, shardCount, err := ResolveDevicesForTarLoad(tc.devices, tarPath)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got success (resolved=%v shardCount=%d)",
						tc.wantErr, resolved, shardCount)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if int(shardCount) != tc.shardCount {
				t.Fatalf("shard count: want %d, got %d", tc.shardCount, shardCount)
			}
			if tc.wantResult != nil {
				if len(resolved) != len(tc.wantResult) {
					t.Fatalf("resolved len: want %d, got %d (%v)", len(tc.wantResult), len(resolved), resolved)
				}
				for i, v := range tc.wantResult {
					if resolved[i] != v {
						t.Fatalf("resolved[%d]: want %d, got %d (full: %v)", i, v, resolved[i], resolved)
					}
				}
			}
		})
	}
}
