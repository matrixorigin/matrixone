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
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"testing/fstest"
)

func testFS(
	t *testing.T,
	dir string,
) {

	fstest.TestFS(os.DirFS(dir))

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

		stat, err := os.Stat(path)
		ce(err)
		if stat.Name() != filepath.Base(path) {
			t.Fatal()
		}
		if stat.Size() != 3 {
			t.Fatal()
		}

		data, err = os.ReadFile(path)
		ce(err)
		if !bytes.Equal(data, []byte("foo")) {
			t.Fatal()
		}

		ce(os.Remove(path))
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

	ce(os.WriteFile(
		filepath.Join(dir, "1"),
		[]byte("foo"),
		0644,
	))

	//TODO
	//ce(os.Symlink(
	//	filepath.Join(dir, "1"),
	//	filepath.Join(dir, "sym"),
	//))

	//TODO
	//ce(os.Link(
	//	filepath.Join(dir, "1"),
	//	filepath.Join(dir, "link"),
	//))

	var paths []string
	ce(filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		paths = append(paths, path)
		return nil
	}))
	if len(paths) == 0 {
		t.Fatal()
	}

	f, err := os.Create(filepath.Join(dir, "lock"))
	ce(err)
	defer f.Close()
	fd := f.Fd()
	ce(syscall.Flock(int(fd), syscall.LOCK_NB|syscall.LOCK_EX))
	ce(syscall.Flock(int(fd), syscall.LOCK_UN))

}
