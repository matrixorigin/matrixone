// Copyright 2021 - 2024 Matrix Origin
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

package logservice

import (
	"context"
	"testing"

	vfs2 "github.com/lni/vfs"
	"github.com/stretchr/testify/assert"
)

func TestArchiverIO(t *testing.T) {
	vfs := vfs2.NewMem()

	t.Run("fs nil", func(t *testing.T) {
		a := newArchiveIO(vfs, nil)
		assert.NoError(t, a.Write(context.Background(), "", ""))
	})

	t.Run("file empty", func(t *testing.T) {
		a := newArchiveIO(vfs, newFS())
		assert.NoError(t, a.Write(context.Background(), "", ""))
	})

	t.Run("source file not exist", func(t *testing.T) {
		a := newArchiveIO(vfs, newFS())
		assert.Error(t, a.Write(context.Background(), "", "a.log"))
	})

	t.Run("ok", func(t *testing.T) {
		a := newArchiveIO(vfs, newFS())
		// prepare the log file
		f, err := vfs.Create("a.log")
		assert.NoError(t, err)
		defer f.Close()
		n, err := f.Write([]byte("test"))
		assert.NoError(t, err)
		assert.Equal(t, 4, n)

		ctx := context.Background()
		assert.NoError(t, a.Write(ctx, "", "a.log"))

		files, err := a.List(ctx, "")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(files))

		rc, err := a.Open(ctx, "", "a.log")
		assert.NoError(t, err)
		defer func() {
			assert.NoError(t, rc.Close())
		}()
		buf := make([]byte, 10)
		n, err = rc.Read(buf)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)

		assert.NoError(t, vfs.MkdirAll("d/", 0755))
		assert.NoError(t, a.Download(ctx, "", "a.log", "d/b.log"))
		downloaded, err := vfs.List("d/")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(downloaded))

		f, err = vfs.Open("d/b.log")
		assert.NoError(t, err)
		defer f.Close()
		n, err = f.Read(buf)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, []byte("test"), buf[:n])

		assert.NoError(t, a.Delete(ctx, "", "a.log"))
		files, err = a.List(ctx, "")
		assert.NoError(t, err)
		assert.Equal(t, 0, len(files))
	})
}
