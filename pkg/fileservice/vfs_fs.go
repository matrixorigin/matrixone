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
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/vfs"
)

// VfsFS is a FileService implementation backed by a vfs instance
type VfsFS struct {
	fs vfs.FS

	sync.RWMutex
	dirFiles map[string]vfs.File
}

var _ FileService = new(VfsFS)

func NewVfsFS(fs vfs.FS) (*VfsFS, error) {

	// create tmp dir
	if err := fs.MkdirAll(".tmp", 0755); err != nil {
		return nil, err
	}

	return &VfsFS{
		fs:       fs,
		dirFiles: make(map[string]vfs.File),
	}, nil
}

func (l *VfsFS) Write(ctx context.Context, vector IOVector) error {
	nativePath := l.toNativeFilePath(vector.FilePath)

	// check existence
	_, err := l.fs.Stat(nativePath)
	if err == nil {
		// existed
		return ErrFileExisted
	}

	return l.write(ctx, vector)
}

func (l *VfsFS) write(ctx context.Context, vector IOVector) error {
	nativePath := l.toNativeFilePath(vector.FilePath)

	// sort
	sort.Slice(vector.Entries, func(i, j int) bool {
		return vector.Entries[i].Offset < vector.Entries[j].Offset
	})

	// size
	var size int64
	if len(vector.Entries) > 0 {
		last := vector.Entries[len(vector.Entries)-1]
		size = int64(last.Offset + last.Size)
	}

	// write
	tempName := make([]byte, 32)
	_, err := rand.Read(tempName)
	if err != nil {
		return err
	}
	tempPath := filepath.Join(".tmp", fmt.Sprintf("%x", tempName))
	f, err := l.fs.Create(tempPath)
	if err != nil {
		return err
	}
	n, err := io.Copy(f, newIOEntriesReader(vector.Entries))
	if err != nil {
		return err
	}
	if n != size {
		return ErrSizeNotMatch
	}
	if err := f.Close(); err != nil {
		return err
	}

	// ensure parent dir
	parentDir, _ := filepath.Split(nativePath)
	err = l.ensureDir(parentDir)
	if err != nil {
		return err
	}

	// move
	if err := l.fs.Rename(tempPath, nativePath); err != nil {
		return err
	}

	if err := l.syncDir(parentDir); err != nil {
		return err
	}

	return nil
}

func (l *VfsFS) Read(ctx context.Context, vector *IOVector) error {

	nativePath := l.toNativeFilePath(vector.FilePath)
	f, err := l.fs.Open(nativePath)
	if os.IsNotExist(err) {
		return ErrFileNotFound
	}
	if err != nil {
		return nil
	}
	defer f.Close()

	_, readLen, readToEnd := vector.offsetRange()
	if readToEnd {
		stat, err := f.Stat()
		if err != nil {
			return err
		}
		readLen = int(stat.Size())
	}

	content, err := io.ReadAll(io.LimitReader(f, int64(readLen)))
	if err != nil {
		return err
	}

	for i, entry := range vector.Entries {
		if entry.ignore {
			continue
		}

		if entry.Offset >= len(content) {
			return ErrEmptyRange
		}
		if entry.Size < 0 {
			entry.Size = readLen
		}
		end := entry.Offset + entry.Size
		if end > len(content) {
			return ErrUnexpectedEOF
		}
		data := content[entry.Offset:end]
		if len(data) == 0 {
			return ErrEmptyRange
		}

		setData := true

		if w := vector.Entries[i].WriterForRead; w != nil {
			setData = false
			_, err := w.Write(data)
			if err != nil {
				return err
			}
		}

		if ptr := vector.Entries[i].ReadCloserForRead; ptr != nil {
			setData = false
			*ptr = io.NopCloser(bytes.NewReader(data))
		}

		if setData {
			if len(entry.Data) < entry.Size {
				entry.Data = data
			} else {
				copy(entry.Data, data)
			}
		}

		if err := entry.setObjectFromData(); err != nil {
			return err
		}

		vector.Entries[i] = entry
	}

	return nil

}

func (l *VfsFS) List(ctx context.Context, dirPath string) (ret []DirEntry, err error) {

	nativePath := l.toNativeFilePath(dirPath)
	f, err := l.fs.Open(nativePath)
	if os.IsNotExist(err) {
		err = nil
		return
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	names, err := l.fs.List(nativePath)
	for _, name := range names {
		if strings.HasPrefix(name, ".") {
			continue
		}
		stat, err := l.fs.Stat(filepath.Join(nativePath, name))
		if err != nil {
			return nil, err
		}
		ret = append(ret, DirEntry{
			Name:  name,
			IsDir: stat.IsDir(),
			Size:  int(stat.Size()),
		})
	}

	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Name < ret[j].Name
	})

	if err != nil {
		return ret, err
	}

	return
}

func (l *VfsFS) Delete(ctx context.Context, filePath string) error {
	nativePath := l.toNativeFilePath(filePath)
	_, err := l.fs.Stat(nativePath)
	if os.IsNotExist(err) {
		return ErrFileNotFound
	}
	if err != nil {
		return err
	}
	err = l.fs.Remove(nativePath)
	if err != nil {
		return err
	}
	parentDir, _ := filepath.Split(nativePath)
	err = l.syncDir(parentDir)
	if err != nil {
		return err
	}
	return nil
}

func (l *VfsFS) ensureDir(nativePath string) error {
	nativePath = filepath.Clean(nativePath)
	if nativePath == "" || nativePath == "." {
		return nil
	}

	// check existence by l.dirFiles
	l.RLock()
	_, ok := l.dirFiles[nativePath]
	if ok {
		// dir existed
		l.RUnlock()
		return nil
	}
	l.RUnlock()

	// check existence by fstat
	_, err := l.fs.Stat(nativePath)
	if err == nil {
		// existed
		return nil
	}

	// ensure parent
	parent, _ := filepath.Split(nativePath)
	if parent != nativePath {
		if err := l.ensureDir(parent); err != nil {
			return err
		}
	}

	// create
	if err := l.fs.MkdirAll(nativePath, 0755); err != nil {
		return err
	}

	// sync parent dir
	if err := l.syncDir(parent); err != nil {
		return err
	}

	return nil
}

func (l *VfsFS) toOSPath(filePath string) string {
	if os.PathSeparator == '/' {
		return filePath
	}
	return strings.ReplaceAll(filePath, "/", osPathSeparatorStr)
}

func (l *VfsFS) syncDir(nativePath string) error {
	l.Lock()
	f, ok := l.dirFiles[nativePath]
	if !ok {
		var err error
		f, err = l.fs.Open(nativePath)
		if err != nil {
			l.Unlock()
			return err
		}
		l.dirFiles[nativePath] = f
	}
	l.Unlock()
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func (l *VfsFS) toNativeFilePath(filePath string) string {
	return l.toOSPath(filePath)
}

var _ ReplaceableFileService = new(VfsFS)

func (l *VfsFS) Replace(ctx context.Context, vector IOVector) error {
	return l.write(ctx, vector)
}
