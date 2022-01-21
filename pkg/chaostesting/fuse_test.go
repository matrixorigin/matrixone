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

package fz

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestFuse(t *testing.T) {
	NewScope().Fork(func() TempDirModel {
		return "fuse"
	}, func() IsTesting {
		return true
	}).Call(func(
		dir TempDir,
		cleanup Cleanup,
	) {
		defer cleanup()

		test := func(path string) {
			f, err := os.Create(path)
			ce(err)

			_, err = f.Write([]byte("foo"))
			ce(err)

			_, err = f.Seek(0, 0)
			ce(err)
			data, err := io.ReadAll(f)
			ce(err)
			if !bytes.Equal(data, []byte("foo")) {
				t.Fatal()
			}

			ce(f.Close())

			data, err = os.ReadFile(path)
			ce(err)
			if !bytes.Equal(data, []byte("foo")) {
				t.Fatal()
			}
		}

		path := filepath.Join(string(dir), "foo")
		test(path)

		dirName := "bar"
		dirPath := filepath.Join(string(dir), dirName)
		ce(os.Mkdir(dirPath, 0755))

		test(filepath.Join(dirPath, "foo"))

		dirPath2 := filepath.Join(string(dir), "baz")
		ce(os.Rename(dirPath, dirPath2))

		test(filepath.Join(dirPath2, "qux"))

	})
}
