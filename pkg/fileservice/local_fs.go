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
	"io"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"go.uber.org/zap"
)

// LocalFS is a FileService implementation backed by local file system
type LocalFS struct {
	name     string
	rootPath string

	sync.RWMutex
	dirFiles map[string]*os.File

	memCache    *MemCache
	asyncUpdate bool

	perfCounterSets []*perfcounter.CounterSet
}

var _ FileService = new(LocalFS)

const (
	localFSSentinelFileName = ".thisisalocalfileservicedir"
)

func NewLocalFS(
	name string,
	rootPath string,
	memCacheCapacity int64,
	perfCounterSets []*perfcounter.CounterSet,
) (*LocalFS, error) {

	// ensure dir
	f, err := os.Open(rootPath)
	if os.IsNotExist(err) {
		// not exists, create
		err := os.MkdirAll(rootPath, 0755)
		if err != nil {
			return nil, err
		}
		err = os.WriteFile(filepath.Join(rootPath, localFSSentinelFileName), nil, 0644)
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
			_, err := os.Stat(filepath.Join(rootPath, localFSSentinelFileName))
			if os.IsNotExist(err) {
				return nil, moerr.NewInternalErrorNoCtx("%s is not a file service dir", rootPath)
			} else if err != nil {
				return nil, err
			}
		}
	}

	// create tmp dir
	if err := os.MkdirAll(filepath.Join(rootPath, ".tmp"), 0755); err != nil {
		return nil, err
	}

	fs := &LocalFS{
		name:            name,
		rootPath:        rootPath,
		dirFiles:        make(map[string]*os.File),
		asyncUpdate:     true,
		perfCounterSets: perfCounterSets,
	}
	if memCacheCapacity > 0 {
		fs.memCache = NewMemCache(
			WithLRU(memCacheCapacity),
			WithPerfCounterSets(perfCounterSets),
		)
		logutil.Info("fileservice: cache initialized", zap.Any("fs-name", name), zap.Any("capacity", memCacheCapacity))
	}

	return fs, nil
}

func (l *LocalFS) Name() string {
	return l.name
}

func (l *LocalFS) Write(ctx context.Context, vector IOVector) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	path, err := ParsePathAtService(vector.FilePath, l.name)
	if err != nil {
		return err
	}
	nativePath := l.toNativeFilePath(path.File)

	// check existence
	_, err = os.Stat(nativePath)
	if err == nil {
		// existed
		return moerr.NewFileAlreadyExistsNoCtx(path.File)
	}

	return l.write(ctx, vector)
}

func (l *LocalFS) write(ctx context.Context, vector IOVector) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	FSProfileHandler.AddSample()

	path, err := ParsePathAtService(vector.FilePath, l.name)
	if err != nil {
		return err
	}
	nativePath := l.toNativeFilePath(path.File)

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
	fileWithChecksum := NewFileWithChecksum(ctx, f, _BlockContentSize, l.perfCounterSets)
	n, err := io.Copy(fileWithChecksum, newIOEntriesReader(ctx, vector.Entries))
	if err != nil {
		return err
	}
	if n != size {
		sizeUnknown := false
		for _, entry := range vector.Entries {
			if entry.Size < 0 {
				sizeUnknown = true
				break
			}
		}
		if !sizeUnknown {
			return moerr.NewSizeNotMatchNoCtx(path.File)
		}
	}
	if err := f.Sync(); err != nil {
		return err
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

func (l *LocalFS) Read(ctx context.Context, vector *IOVector) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	if l.memCache != nil {
		if err := l.memCache.Read(ctx, vector); err != nil {
			return err
		}
		defer func() {
			if err != nil {
				return
			}
			err = l.memCache.Update(ctx, vector, l.asyncUpdate)
		}()
	}

	if err := l.read(ctx, vector); err != nil {
		return err
	}

	return nil
}

func (l *LocalFS) read(ctx context.Context, vector *IOVector) error {
	if vector.allDone() {
		return nil
	}

	FSProfileHandler.AddSample()

	path, err := ParsePathAtService(vector.FilePath, l.name)
	if err != nil {
		return err
	}
	nativePath := l.toNativeFilePath(path.File)

	file, err := os.Open(nativePath)
	if os.IsNotExist(err) {
		return moerr.NewFileNotFoundNoCtx(path.File)
	}
	if err != nil {
		return err
	}
	defer file.Close()

	for i, entry := range vector.Entries {
		if entry.Size == 0 {
			return moerr.NewEmptyRangeNoCtx(path.File)
		}

		if entry.done {
			continue
		}

		if entry.WriterForRead != nil {
			fileWithChecksum := NewFileWithChecksum(ctx, file, _BlockContentSize, l.perfCounterSets)

			if entry.Offset > 0 {
				_, err := fileWithChecksum.Seek(int64(entry.Offset), io.SeekStart)
				if err != nil {
					return err
				}
			}
			r := (io.Reader)(fileWithChecksum)
			if entry.Size > 0 {
				r = io.LimitReader(r, int64(entry.Size))
			}

			if entry.ToObject != nil {
				r = io.TeeReader(r, entry.WriterForRead)
				cr := &countingReader{
					R: r,
				}
				obj, size, err := entry.ToObject(cr, nil)
				if err != nil {
					return err
				}
				vector.Entries[i].Object = obj
				vector.Entries[i].ObjectSize = size
				if entry.Size > 0 && cr.N != entry.Size {
					return moerr.NewUnexpectedEOFNoCtx(path.File)
				}

			} else {
				n, err := io.Copy(entry.WriterForRead, r)
				if err != nil {
					return err
				}
				if entry.Size > 0 && n != int64(entry.Size) {
					return moerr.NewUnexpectedEOFNoCtx(path.File)
				}
			}

		} else if entry.ReadCloserForRead != nil {
			file, err := os.Open(nativePath)
			if os.IsNotExist(err) {
				return moerr.NewFileNotFoundNoCtx(path.File)
			}
			if err != nil {
				return err
			}
			fileWithChecksum := NewFileWithChecksum(ctx, file, _BlockContentSize, l.perfCounterSets)

			if entry.Offset > 0 {
				_, err := fileWithChecksum.Seek(int64(entry.Offset), io.SeekStart)
				if err != nil {
					return err
				}
			}
			r := (io.Reader)(fileWithChecksum)
			if entry.Size > 0 {
				r = io.LimitReader(r, int64(entry.Size))
			}

			if entry.ToObject == nil {
				*entry.ReadCloserForRead = &readCloser{
					r:         r,
					closeFunc: file.Close,
				}

			} else {
				buf := new(bytes.Buffer)
				*entry.ReadCloserForRead = &readCloser{
					r: io.TeeReader(r, buf),
					closeFunc: func() error {
						defer file.Close()
						obj, size, err := entry.ToObject(buf, buf.Bytes())
						if err != nil {
							return err
						}
						vector.Entries[i].Object = obj
						vector.Entries[i].ObjectSize = size
						return nil
					},
				}
			}

		} else {
			fileWithChecksum := NewFileWithChecksum(ctx, file, _BlockContentSize, l.perfCounterSets)

			if entry.Offset > 0 {
				_, err := fileWithChecksum.Seek(int64(entry.Offset), io.SeekStart)
				if err != nil {
					return err
				}
			}
			r := (io.Reader)(fileWithChecksum)
			if entry.Size > 0 {
				r = io.LimitReader(r, int64(entry.Size))
			}

			if entry.Size < 0 {
				data, err := io.ReadAll(r)
				if err != nil {
					return err
				}
				entry.Data = data
				entry.Size = int64(len(data))

			} else {
				if int64(len(entry.Data)) < entry.Size {
					entry.Data = make([]byte, entry.Size)
				}
				n, err := io.ReadFull(r, entry.Data)
				if err != nil {
					return err
				}
				if int64(n) != entry.Size {
					return moerr.NewUnexpectedEOFNoCtx(path.File)
				}
			}

			if err := entry.setObjectFromData(); err != nil {
				return err
			}

			vector.Entries[i] = entry

		}

	}

	return nil

}

func (l *LocalFS) List(ctx context.Context, dirPath string) (ret []DirEntry, err error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	FSProfileHandler.AddSample()

	path, err := ParsePathAtService(dirPath, l.name)
	if err != nil {
		return nil, err
	}
	nativePath := l.toNativeFilePath(path.File)

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
		fileSize := info.Size()
		nBlock := ceilingDiv(fileSize, _BlockSize)
		contentSize := fileSize - _ChecksumSize*nBlock

		isDir, err := entryIsDir(nativePath, name, info)
		if err != nil {
			return nil, err
		}
		ret = append(ret, DirEntry{
			Name:  name,
			IsDir: isDir,
			Size:  contentSize,
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

func (l *LocalFS) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	FSProfileHandler.AddSample()

	path, err := ParsePathAtService(filePath, l.name)
	if err != nil {
		return nil, err
	}
	nativePath := l.toNativeFilePath(path.File)

	stat, err := os.Stat(nativePath)
	if os.IsNotExist(err) {
		return nil, moerr.NewFileNotFound(ctx, filePath)
	}

	if stat.IsDir() {
		return nil, moerr.NewFileNotFound(ctx, filePath)
	}

	fileSize := stat.Size()
	nBlock := ceilingDiv(fileSize, _BlockSize)
	contentSize := fileSize - _ChecksumSize*nBlock

	return &DirEntry{
		Name:  pathpkg.Base(filePath),
		IsDir: false,
		Size:  contentSize,
	}, nil
}

func (l *LocalFS) Delete(ctx context.Context, filePaths ...string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	FSProfileHandler.AddSample()

	for _, filePath := range filePaths {
		if err := l.deleteSingle(ctx, filePath); err != nil {
			return err
		}
	}
	return nil
}

func (l *LocalFS) deleteSingle(ctx context.Context, filePath string) error {
	path, err := ParsePathAtService(filePath, l.name)
	if err != nil {
		return err
	}
	nativePath := l.toNativeFilePath(path.File)

	_, err = os.Stat(nativePath)
	if os.IsNotExist(err) {
		return moerr.NewFileNotFoundNoCtx(path.File)
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
	return filepath.Join(l.rootPath, toOSPath(filePath))
}

var _ MutableFileService = new(LocalFS)

func (l *LocalFS) NewMutator(ctx context.Context, filePath string) (Mutator, error) {
	path, err := ParsePathAtService(filePath, l.name)
	if err != nil {
		return nil, err
	}
	nativePath := l.toNativeFilePath(path.File)
	f, err := os.OpenFile(nativePath, os.O_RDWR, 0644)
	if os.IsNotExist(err) {
		return nil, moerr.NewFileNotFoundNoCtx(path.File)
	}
	return &LocalFSMutator{
		osFile:           f,
		fileWithChecksum: NewFileWithChecksum(ctx, f, _BlockContentSize, l.perfCounterSets),
	}, nil
}

type LocalFSMutator struct {
	osFile           *os.File
	fileWithChecksum *FileWithChecksum[*os.File]
}

func (l *LocalFSMutator) Mutate(ctx context.Context, entries ...IOEntry) error {
	return l.mutate(ctx, 0, entries...)
}

func (l *LocalFSMutator) Append(ctx context.Context, entries ...IOEntry) error {
	offset, err := l.fileWithChecksum.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	return l.mutate(ctx, offset, entries...)
}

func (l *LocalFSMutator) mutate(ctx context.Context, baseOffset int64, entries ...IOEntry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	FSProfileHandler.AddSample()

	// write
	for _, entry := range entries {

		if entry.ReaderForWrite != nil {
			// seek and copy
			_, err := l.fileWithChecksum.Seek(entry.Offset+baseOffset, 0)
			if err != nil {
				return err
			}
			n, err := io.Copy(l.fileWithChecksum, entry.ReaderForWrite)
			if err != nil {
				return err
			}
			if int64(n) != entry.Size {
				return moerr.NewSizeNotMatchNoCtx("")
			}

		} else {
			// WriteAt
			n, err := l.fileWithChecksum.WriteAt(entry.Data, int64(entry.Offset+baseOffset))
			if err != nil {
				return err
			}
			if int64(n) != entry.Size {
				return moerr.NewSizeNotMatchNoCtx("")
			}
		}
	}

	return nil
}

func (l *LocalFSMutator) Close() error {
	// sync
	if err := l.osFile.Sync(); err != nil {
		return err
	}

	// close
	if err := l.osFile.Close(); err != nil {
		return err
	}

	return nil
}

var _ ReplaceableFileService = new(LocalFS)

func (l *LocalFS) Replace(ctx context.Context, vector IOVector) error {
	return l.write(ctx, vector)
}

var _ CachingFileService = new(LocalFS)

func (l *LocalFS) FlushCache() {
	if l.memCache != nil {
		l.memCache.Flush()
	}
}

func (l *LocalFS) SetAsyncUpdate(b bool) {
	l.asyncUpdate = b
}

func entryIsDir(path string, name string, entry fs.FileInfo) (bool, error) {
	if entry.IsDir() {
		return true, nil
	}
	if entry.Mode().Type()&fs.ModeSymlink > 0 {
		stat, err := os.Stat(filepath.Join(path, name))
		if err != nil {
			return false, err
		}
		return entryIsDir(path, name, stat)
	}
	return false, nil
}
