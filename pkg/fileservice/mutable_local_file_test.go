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

package fileservice

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestCreateAndRemoveMutableLocalFile(t *testing.T) {
	t.Run("unlink failure closes the created file", func(t *testing.T) {
		root := t.TempDir()
		native := func(path string) string { return filepath.Join(root, path) }
		path := native("spill")
		unlinkErr := &os.PathError{Op: "remove", Path: path, Err: os.ErrPermission}
		var created *os.File
		t.Cleanup(func() {
			if created != nil {
				_ = created.Close()
			}
		})

		file, err := createAndRemoveMutableLocalFile(context.Background(), "local", "spill", native, func(f *os.File) error {
			created = f
			return unlinkErr
		})
		require.Nil(t, file)
		require.Same(t, unlinkErr, err)
		require.ErrorIs(t, err, os.ErrPermission)
		var pathErr *os.PathError
		require.ErrorAs(t, err, &pathErr)
		require.Equal(t, path, pathErr.Path)
		require.NotNil(t, created)
		_, err = created.Write([]byte("must not be written"))
		require.ErrorIs(t, err, os.ErrClosed)
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Zero(t, info.Size(), "failed unlink may leave the named file")
	})

	t.Run("entry and create errors do not unlink", func(t *testing.T) {
		root := t.TempDir()
		native := func(path string) string { return filepath.Join(root, path) }
		calls := 0
		unlink := func(*os.File) error { calls++; return nil }
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		file, err := createAndRemoveMutableLocalFile(canceled, "local", "spill", native, unlink)
		require.Nil(t, file)
		require.ErrorIs(t, err, context.Canceled)
		_, err = os.Stat(native("spill"))
		require.ErrorIs(t, err, os.ErrNotExist)

		expired, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancelDeadline()
		file, err = createAndRemoveMutableLocalFile(expired, "local", "spill", native, unlink)
		require.Nil(t, file)
		require.ErrorIs(t, err, context.DeadlineExceeded)

		file, err = createAndRemoveMutableLocalFile(context.Background(), "local", "", native, unlink)
		require.Nil(t, file)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), "%v", err)

		file, err = createAndRemoveMutableLocalFile(context.Background(), "local", JoinPath("other", "spill"), native, unlink)
		require.Nil(t, file)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongService), "%v", err)

		file, err = createAndRemoveMutableLocalFile(context.Background(), "local", "missing/spill", native, unlink)
		require.Nil(t, file)
		require.ErrorIs(t, err, os.ErrNotExist)
		var pathErr *os.PathError
		require.ErrorAs(t, err, &pathErr)
		require.Zero(t, calls)
	})

	for _, failUnlink := range []bool{false, true} {
		name := "late cancellation after unlink"
		if failUnlink {
			name = "late cancellation with unlink error"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			native := func(path string) string { return filepath.Join(root, path) }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var created *os.File
			t.Cleanup(func() {
				if created != nil {
					_ = created.Close()
				}
			})
			file, err := createAndRemoveMutableLocalFile(ctx, "local", "spill", native, func(f *os.File) error {
				created = f
				cancel()
				if failUnlink {
					return os.ErrPermission
				}
				return os.Remove(f.Name())
			})
			require.ErrorIs(t, ctx.Err(), context.Canceled)
			if failUnlink {
				require.Nil(t, file)
				require.ErrorIs(t, err, os.ErrPermission)
				require.NotNil(t, created)
				_, err = created.Write([]byte("closed"))
				require.ErrorIs(t, err, os.ErrClosed)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, file)
			_, err = os.Stat(native("spill"))
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = file.Write([]byte("content"))
			require.NoError(t, err)
			buf := make([]byte, len("content"))
			_, err = file.ReadAt(buf, 0)
			require.NoError(t, err)
			require.Equal(t, "content", string(buf))
			require.NoError(t, file.Close())
		})
	}
}
