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

package testutil

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fixtureDirectoryTB struct {
	testing.TB
	roots []string
}

func (tb *fixtureDirectoryTB) TempDir() string {
	path := tb.TB.TempDir()
	tb.roots = append(tb.roots, path)
	return path
}
func fixtureDescriptors(t *testing.T, roots []string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	result := make(map[string]string)
	for _, entry := range entries {
		fd := filepath.Join("/proc/self/fd", entry.Name())
		target, err := os.Readlink(fd)
		if err != nil {
			continue
		}
		for _, root := range roots {
			if target == root || strings.HasPrefix(target, root+"/") {
				result[fd] = target
				break
			}
		}
	}
	return result
}
func TestProcessFixtureFileServiceLifetime(t *testing.T) {
	ensureAutoIncrService("")
	const defaultTag = "must_new_zero_no_fixed"
	before := fixturePoolCount(t, defaultTag)
	borrowed := mpool.MustNew("fixture-replacement-pool")
	t.Cleanup(func() { mpool.DeleteMPool(borrowed) })
	replacement := &fixtureBorrowedFS{}
	for _, replace := range []bool{false, true} {
		name := "defaults"
		if replace {
			name = "replaced-fields"
		}
		require.True(t, t.Run(name, func(t *testing.T) {
			// TempDir registers one shared removal callback. Place our oracle after
			// it, and before fixture destruction, to observe close before removal.
			t.TempDir()
			tb := &fixtureDirectoryTB{TB: t}
			var ownedFS fileservice.FileService
			var descriptors map[string]string
			t.Cleanup(func() {
				if ownedFS == nil {
					return
				}
				for fd, target := range descriptors {
					current, err := os.Readlink(fd)
					require.True(t, os.IsNotExist(err) || (err == nil && current != target), "fixture descriptor still open: %s", target)
				}
				for _, root := range tb.roots {
					_, err := os.Stat(root)
					require.NoError(t, err, "service must close before TempDir removal")
				}
			})
			proc := NewProcess(tb)
			ownedFS = proc.GetFileService()
			for _, service := range []string{defines.LocalFileServiceName, defines.SharedFileServiceName, defines.ETLFileServiceName} {
				require.NoError(t, ownedFS.Write(context.Background(), fileservice.IOVector{FilePath: service + ":owned-probe", Entries: []fileservice.IOEntry{{Size: 1, Data: []byte{1}}}}))
			}
			descriptors = fixtureDescriptors(t, tb.roots)
			require.Len(t, descriptors, 3)
			if replace {
				proc.SetMPool(borrowed)
				proc.SetFileService(replacement)
			}
		}))
		require.Equal(t, before, fixturePoolCount(t, defaultTag))
		require.Zero(t, replacement.closes)
	}
	block, err := borrowed.Alloc(1, true)
	require.NoError(t, err)
	borrowed.Free(block)
	require.Equal(t, 1, fixturePoolCount(t, "fixture-replacement-pool"))
}
