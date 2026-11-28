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

package fileservice

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type mutableAnonymousSpillFault struct {
	MutableFileService
	createAndRemove func(context.Context, string) (*os.File, error)
}

func (f *mutableAnonymousSpillFault) CreateAndRemoveFile(ctx context.Context, name string) (*os.File, error) {
	return f.createAndRemove(ctx, name)
}

func TestMutableAnonymousSpillUnlinkFailureFencesRoot(t *testing.T) {
	for _, closeFirst := range []bool{false, true} {
		name := "unlink failure"
		if closeFirst {
			name = "unlink and close failure"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			local, view := testSpillRoot(t)
			var created *os.File
			view.MutableFileService = &mutableAnonymousSpillFault{
				MutableFileService: view.MutableFileService,
				createAndRemove: func(ctx context.Context, name string) (*os.File, error) {
					return createAndRemoveMutableLocalFile(ctx, local.Name(), name,
						func(path string) string { return filepath.Join(local.rootPath, "spill", path) },
						func(file *os.File) error {
							created = file
							// A nonempty directory at the original pathname makes the
							// actual unlink fail without global hooks or permissions.
							require.NoError(t, os.Remove(file.Name()))
							require.NoError(t, os.Mkdir(file.Name(), 0700))
							require.NoError(t, os.WriteFile(filepath.Join(file.Name(), "keep"), []byte("owned"), 0600))
							if closeFirst {
								require.NoError(t, file.Close())
							}
							return unlinkAnonymousLocalFile(file)
						})
				},
			}
			file, err := view.CreateAndRemoveFile(ctx, "blocked")
			require.Nil(t, file)
			require.Error(t, err)
			var unlink *anonymousUnlinkError
			require.ErrorAs(t, err, &unlink)
			var pathErr *os.PathError
			require.ErrorAs(t, err, &pathErr)
			require.Equal(t, filepath.Join(local.rootPath, "spill", "blocked"), pathErr.Path)
			if closeFirst {
				require.ErrorIs(t, err, os.ErrClosed, "preserve the secondary close failure")
			}
			require.NotNil(t, created)
			_, writeErr := created.Write([]byte("must be closed"))
			require.ErrorIs(t, writeErr, os.ErrClosed)
			require.ErrorIs(t, view.root.Err(), ErrSpillRootFenced)
			file, err = view.CreateFile(ctx, "next")
			require.Nil(t, file)
			require.ErrorIs(t, err, ErrSpillRootFenced)
			require.NoFileExists(t, filepath.Join(local.rootPath, "spill", "next"))
			_, other := testSpillRoot(t)
			file, err = other.CreateFile(ctx, "healthy")
			require.NoError(t, err, "an unrelated root must remain usable")
			require.NoError(t, file.Close())
			require.NoError(t, other.RemoveFile(ctx, "healthy"))
		})
	}
}
