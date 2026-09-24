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

package fileservice

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalWriterCloseCleanup(t *testing.T) {
	for _, kind := range []string{"local", "etl"} {
		for _, scenario := range []string{"success", "closed file", "ensure dir", "rename", "sync dir"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				var fs ReaderWriterFileService
				var dirs map[string]*os.File
				if kind == "local" {
					local, err := NewLocalFS(ctx, "local", root, DisabledCacheConfig, nil)
					require.NoError(t, err)
					t.Cleanup(func() { local.Close(ctx) })
					fs, dirs = local, local.dirFiles
				} else {
					local, err := NewLocalETLFS("etl", root)
					require.NoError(t, err)
					t.Cleanup(func() { local.Close(ctx) })
					fs, dirs = local, local.dirFiles
				}
				target := "target"
				if scenario == "ensure dir" {
					target = "blocked/child/target"
				}
				w, err := fs.NewWriter(ctx, target)
				require.NoError(t, err)
				closed := false
				t.Cleanup(func() {
					if !closed {
						_ = w.Close()
					}
				})
				// Keep the underlying descriptor available for cleanup and ownership assertions.
				var f *os.File
				switch raw := w.(*writeCloser).w.(type) {
				case *os.File:
					f = raw
				case *FileWithChecksum[*os.File]:
					f = raw.underlying
				default:
					t.Fatalf("unexpected writer %T", raw)
				}
				_, err = w.Write([]byte("x"))
				require.NoError(t, err)
				switch scenario {
				case "closed file":
					require.NoError(t, f.Close())
				case "ensure dir":
					require.NoError(t, os.WriteFile(filepath.Join(root, "blocked"), nil, 0600))
				case "rename":
					require.NoError(t, os.Mkdir(filepath.Join(root, target), 0700))
				case "sync dir":
					dir, err := os.Open(root)
					require.NoError(t, err)
					require.NoError(t, dir.Close())
					parent, _ := filepath.Split(filepath.Join(root, target))
					if old := dirs[parent]; old != nil {
						require.NoError(t, old.Close())
					}
					dirs[parent] = dir
				}
				closed = true
				err = w.Close()
				if scenario == "success" {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				_, err = f.Stat()
				require.ErrorIs(t, err, os.ErrClosed)
				leftovers, err := filepath.Glob(filepath.Join(root, ".tmp.*"))
				require.NoError(t, err)
				require.Empty(t, leftovers)
				if scenario == "success" || scenario == "sync dir" {
					r, err := fs.NewReader(ctx, target)
					require.NoError(t, err)
					t.Cleanup(func() { _ = r.Close() })
					data, err := io.ReadAll(r)
					require.NoError(t, err)
					require.Equal(t, "x", string(data))
				}
			})
		}
	}
}
