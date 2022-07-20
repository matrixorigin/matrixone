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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// LocalFS is a FileService implementation backed by local file system
type LocalFS struct {
	rootPath string

	sync.RWMutex
	dirFiles map[string]*os.File
}

var _ FileService = new(LocalFS)

func NewLocalFS(rootPath string) (*LocalFS, error) {

	const sentinelFileName = "thisisalocalfileservicedir"

	// ensure dir
	f, err := os.Open(rootPath)
	if os.IsNotExist(err) {
		// not exists, create
		err := os.MkdirAll(rootPath, 0755)
		if err != nil {
			return nil, err
		}
		err = os.WriteFile(filepath.Join(rootPath, sentinelFileName), nil, 0644)
		if err != nil {
			return nil, err
		}

	} else if err != nil {
		// stat error
		return nil, err

	} else {
		// existed, check if a real file service dir
		defer f.Close()
		entries, err := f.ReadDir(1)
		if len(entries) == 0 {
			if errors.Is(err, io.EOF) {
				// empty dir, ok
			} else if err != nil {
				// ReadDir error
				return nil, err
			}
		} else {
			// not empty, check sentinel file
			_, err := os.Stat(filepath.Join(rootPath, sentinelFileName))
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("%s is not a file service dir", rootPath)
			} else if err != nil {
				return nil, err
			}
		}
	}

	// create tmp dir
	if err := os.MkdirAll(filepath.Join(rootPath, ".tmp"), 0755); err != nil {
		return nil, err
	}

	return &LocalFS{
		rootPath: rootPath,
		dirFiles: make(map[string]*os.File),
	}, nil
}

func (l *LocalFS) Write(ctx context.Context, vector IOVector) error {
	nativePath := l.toNativeFilePath(vector.FilePath)

	// check existence
	_, err := os.Stat(nativePath)
	if err == nil {
		// existed
		return ErrFileExisted
	}

	return l.write(ctx, vector)
}

func (l *LocalFS) write(ctx context.Context, vector IOVector) error {
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
	f, err := os.CreateTemp(
		filepath.Join(l.rootPath, ".tmp"),
		"*.tmp",
	)
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
	if err := os.Rename(f.Name(), nativePath); err != nil {
		return err
	}

	if err := l.syncDir(parentDir); err != nil {
		return err
	}

	return nil
}

func (l *LocalFS) Read(ctx context.Context, vector *IOVector) error {

	nativePath := l.toNativeFilePath(vector.FilePath)
	f, err := os.Open(nativePath)
	if os.IsNotExist(err) {
		return ErrFileNotFound
	}
	if err != nil {
		return nil
	}
	defer f.Close()

	min, max, readToEnd := vector.offsetRange()
	if readToEnd {
		stat, err := f.Stat()
		if err != nil {
			return err
		}
		max = int(stat.Size())
	}
	readLen := max - min

	_, err = f.Seek(int64(min), io.SeekStart)
	if err != nil {
		return err
	}
	//TODO use multiple ReadAt
	content, err := io.ReadAll(io.LimitReader(f, int64(readLen)))
	if err != nil {
		return err
	}

	for i, entry := range vector.Entries {
		start := entry.Offset - min
		if start >= len(content) {
			return ErrEmptyRange
		}
		if entry.Size < 0 {
			entry.Size = max
		}
		end := start + entry.Size
		if end > len(content) {
			return ErrUnexpectedEOF
		}
		data := content[start:end]
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
				vector.Entries[i].Data = data
			} else {
				copy(entry.Data, data)
			}
		}
	}

	return nil

}

func (l *LocalFS) List(ctx context.Context, dirPath string) (ret []DirEntry, err error) {

	nativePath := l.toNativeFilePath(dirPath)
	f, err := os.Open(nativePath)
	if os.IsNotExist(err) {
		err = nil
		return
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	entries, err := f.ReadDir(-1)
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		ret = append(ret, DirEntry{
			Name:  name,
			IsDir: entry.IsDir(),
			Size:  int(info.Size()),
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

func (l *LocalFS) Delete(ctx context.Context, filePath string) error {
	nativePath := l.toNativeFilePath(filePath)
	_, err := os.Stat(nativePath)
	if os.IsNotExist(err) {
		return ErrFileNotFound
	}
	if err != nil {
		return err
	}
	err = os.Remove(nativePath)
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

func (l *LocalFS) ensureDir(nativePath string) error {
	nativePath = filepath.Clean(nativePath)
	if nativePath == "" {
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
	_, err := os.Stat(nativePath)
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
	if err := os.Mkdir(nativePath, 0755); err != nil {
		return err
	}

	// sync parent dir
	if err := l.syncDir(parent); err != nil {
		return err
	}

	return nil
}

var osPathSeparatorStr = string([]rune{os.PathSeparator})

func (l *LocalFS) toOSPath(filePath string) string {
	if os.PathSeparator == '/' {
		return filePath
	}
	return strings.ReplaceAll(filePath, "/", osPathSeparatorStr)
}

func (l *LocalFS) syncDir(nativePath string) error {
	l.Lock()
	f, ok := l.dirFiles[nativePath]
	if !ok {
		var err error
		f, err = os.Open(nativePath)
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

func (l *LocalFS) toNativeFilePath(filePath string) string {
	return filepath.Join(l.rootPath, l.toOSPath(filePath))
}

var _ MutableFileService = new(LocalFS)

func (l *LocalFS) Mutate(ctx context.Context, vector IOVector) error {

	// sort
	sort.Slice(vector.Entries, func(i, j int) bool {
		return vector.Entries[i].Offset < vector.Entries[j].Offset
	})

	// open
	nativePath := l.toNativeFilePath(vector.FilePath)
	f, err := os.OpenFile(nativePath, os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		return ErrFileNotFound
	}
	defer f.Close()

	// write
	for _, entry := range vector.Entries {
		_, err := f.Seek(int64(entry.Offset), 0)
		if err != nil {
			return err
		}
		var src io.Reader
		if entry.ReaderForWrite != nil {
			src = entry.ReaderForWrite
		} else {
			src = bytes.NewReader(entry.Data)
		}
		n, err := io.Copy(f, src)
		if err != nil {
			return err
		}
		if int(n) != entry.Size {
			return ErrSizeNotMatch
		}
	}

	return nil
}

var _ ReplaceableFileService = new(LocalFS)

func (l *LocalFS) Replace(ctx context.Context, vector IOVector) error {
	return l.write(ctx, vector)
}
