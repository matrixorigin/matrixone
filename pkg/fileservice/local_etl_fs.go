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
	"cmp"
	"context"
	"io"
	"iter"
	"os"
	pathpkg "path"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// LocalETLFS is a FileService implementation backed by local file system and suitable for ETL operations
type LocalETLFS struct {
	name     string
	rootPath string

	sync.RWMutex
	dirFiles map[string]*os.File
}

var _ FileService = new(LocalETLFS)

func NewLocalETLFS(name string, rootPath string) (*LocalETLFS, error) {
	rootPath, rootFile, err := prepareLocalRoot(rootPath)
	if err != nil {
		return nil, err
	}
	if rootFile != nil {
		defer rootFile.Close()
	}

	// resolve symlinks in the path to get the canonical path
	// This is best-effort: if EvalSymlinks fails (e.g., due to race conditions
	// in concurrent tests), we fall back to using the original path
	if resolved, err := filepath.EvalSymlinks(rootPath); err == nil {
		rootPath = resolved
	}
	// If EvalSymlinks fails, continue with the original rootPath
	// This makes the code more resilient to race conditions in test environments

	return &LocalETLFS{
		name:     name,
		rootPath: rootPath,
		dirFiles: make(map[string]*os.File),
	}, nil
}

func (l *LocalETLFS) Name() string {
	return l.name
}

func (l *LocalETLFS) Close(ctx context.Context) {
	closeDirFiles(&l.RWMutex, l.dirFiles)
}

func (l *LocalETLFS) Write(ctx context.Context, vector IOVector) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	path, err := parseFilePathAtService(vector.FilePath, l.name)
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

func (l *LocalETLFS) write(ctx context.Context, vector IOVector) error {
	path, err := parseFilePathAtService(vector.FilePath, l.name)
	if err != nil {
		return err
	}
	nativePath := l.toNativeFilePath(path.File)

	// sort
	slices.SortFunc(vector.Entries, func(a, b IOEntry) int {
		return cmp.Compare(a.Offset, b.Offset)
	})

	// size
	var size int64
	if len(vector.Entries) > 0 {
		last := vector.Entries[len(vector.Entries)-1]
		size = int64(last.Offset + last.Size)
	}

	r := newIOEntriesReader(ctx, vector.Entries)

	// write
	f, err := os.CreateTemp(
		l.rootPath,
		".tmp.*",
	)
	if err != nil {
		return err
	}
	tempPath := f.Name()
	open := true
	owned := true
	defer func() {
		if open {
			_ = f.Close()
		}
		if owned {
			_ = os.Remove(tempPath)
		}
	}()

	var buf []byte
	put := ioBufferPool.Get(&buf)
	defer put.Put()
	n, err := io.CopyBuffer(f, r, buf)
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
	if err := f.Close(); err != nil {
		open = false
		return err
	}
	open = false

	// ensure parent dir
	parentDir, _ := filepath.Split(nativePath)
	err = l.ensureDir(parentDir)
	if err != nil {
		return err
	}

	// move
	if exclusiveWrite(ctx) {
		// Link installs the complete temporary file atomically and refuses an
		// existing name, using the filesystem's own alias equivalence.
		if err := os.Link(tempPath, nativePath); err != nil {
			if os.IsExist(err) {
				return moerr.NewFileAlreadyExistsNoCtx(path.File)
			}
			return err
		}
	} else {
		if err := os.Rename(tempPath, nativePath); err != nil {
			return err
		}
		owned = false
	}

	if err := l.syncDir(parentDir); err != nil {
		return err
	}

	return nil
}

func (l *LocalETLFS) Read(ctx context.Context, vector *IOVector) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVectorNoCtx()
	}

	path, err := parseFilePathAtService(vector.FilePath, l.name)
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

	for i, entry := range vector.Entries {
		if entry.Size == 0 {
			return moerr.NewEmptyRangeNoCtx(path.File)
		}

		if entry.done {
			continue
		}

		if entry.WriterForRead != nil {
			f, err := os.Open(nativePath)
			if os.IsNotExist(err) {
				return moerr.NewFileNotFoundNoCtx(path.File)
			}
			if err != nil {
				return err
			}
			defer f.Close()
			if entry.Offset > 0 {
				if _, err := f.Seek(int64(entry.Offset), io.SeekStart); err != nil {
					return err
				}
			}
			r := (io.Reader)(f)
			if entry.Size > 0 {
				r = io.LimitReader(r, int64(entry.Size))
			}

			if entry.ToCacheData != nil {
				r = io.TeeReader(r, entry.WriterForRead)
				counter := new(atomic.Int64)
				cr := &countingReader{
					R: r,
					C: counter,
				}
				cacheData, err := entry.ToCacheData(ctx, cr, nil, DefaultCacheDataAllocator())
				if err != nil {
					return err
				}
				if cacheData == nil {
					panic("ToCacheData returns nil cache data")
				}
				vector.Entries[i].CachedData = cacheData
				if entry.Size > 0 && counter.Load() != entry.Size {
					return moerr.NewUnexpectedEOFNoCtx(path.File)
				}

			} else {
				var buf []byte
				put := ioBufferPool.Get(&buf)
				defer put.Put()
				n, err := io.CopyBuffer(entry.WriterForRead, r, buf)
				if err != nil {
					return err
				}
				if entry.Size > 0 && n != int64(entry.Size) {
					return moerr.NewUnexpectedEOFNoCtx(path.File)
				}
			}

		} else if entry.ReadCloserForRead != nil {
			f, err := os.Open(nativePath)
			if os.IsNotExist(err) {
				return moerr.NewFileNotFoundNoCtx(path.File)
			}
			if err != nil {
				return err
			}
			if entry.Offset > 0 {
				if _, err := f.Seek(int64(entry.Offset), io.SeekStart); err != nil {
					return err
				}
			}
			r := (io.Reader)(f)
			if entry.Size > 0 {
				r = io.LimitReader(r, int64(entry.Size))
			}
			if entry.ToCacheData == nil {
				*entry.ReadCloserForRead = &readCloser{
					r:         r,
					closeFunc: f.Close,
				}
			} else {
				buf := new(bytes.Buffer)
				*entry.ReadCloserForRead = &readCloser{
					r: io.TeeReader(r, buf),
					closeFunc: func() error {
						defer f.Close()
						cacheData, err := entry.ToCacheData(ctx, buf, buf.Bytes(), DefaultCacheDataAllocator())
						if err != nil {
							return err
						}
						if cacheData != nil {
							panic("ToCacheData returns nil cache data")
						}
						vector.Entries[i].CachedData = cacheData
						return nil
					},
				}
			}

		} else {
			f, err := os.Open(nativePath)
			if os.IsNotExist(err) {
				return moerr.NewFileNotFoundNoCtx(path.File)
			}
			if err != nil {
				return err
			}
			defer f.Close()

			if entry.Offset > 0 {
				_, err = f.Seek(int64(entry.Offset), io.SeekStart)
				if err != nil {
					return err
				}
			}
			r := (io.Reader)(f)
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

			if err := entry.setCachedData(ctx, DefaultCacheDataAllocator()); err != nil {
				return err
			}

			vector.Entries[i] = entry
		}

	}

	return nil

}

func (l *LocalETLFS) ReadCache(ctx context.Context, vector *IOVector) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := parseFilePathAtService(vector.FilePath, l.name)
	return err
}

func (l *LocalETLFS) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	path, err := parseFilePathAtService(filePath, l.name)
	if err != nil {
		return nil, err
	}
	nativePath := l.toNativeFilePath(path.File)

	stat, err := os.Stat(nativePath)
	if os.IsNotExist(err) {
		return nil, moerr.NewFileNotFoundNoCtx(path.File)
	}
	if err != nil {
		return nil, err
	}

	if stat.IsDir() {
		return nil, moerr.NewFileNotFoundNoCtx(path.File)
	}

	return &DirEntry{
		Name:  pathpkg.Base(filePath),
		IsDir: false,
		Size:  stat.Size(),
	}, nil
}

func (l *LocalETLFS) PrefetchFile(ctx context.Context, filePath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := parseFilePathAtService(filePath, l.name)
	return err
}

func (l *LocalETLFS) List(ctx context.Context, dirPath string) iter.Seq2[*DirEntry, error] {
	return l.list(ctx, dirPath, false)
}

// ListWithHidden includes dot-prefixed entries for callers that explicitly
// select them. Ordinary List keeps hiding these entries, including temporary
// files used by atomic writes.
func (l *LocalETLFS) ListWithHidden(ctx context.Context, dirPath string) iter.Seq2[*DirEntry, error] {
	return l.list(ctx, dirPath, true)
}

func (l *LocalETLFS) list(ctx context.Context, dirPath string, includeHidden bool) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		select {
		case <-ctx.Done():
			yield(nil, ctx.Err())
			return
		default:
		}

		path, err := ParsePathAtService(dirPath, l.name)
		if err != nil {
			yield(nil, err)
			return
		}
		nativePath := l.toNativeFilePath(path.File)

		f, err := os.Open(nativePath)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			yield(nil, err)
			return
		}
		defer f.Close()

		entries, err := f.ReadDir(-1)
		for _, entry := range entries {
			name := entry.Name()
			if !includeHidden && strings.HasPrefix(name, ".") {
				continue
			}
			info, ok, err := localDirEntryInfo(entry)
			if err != nil {
				yield(nil, err)
				return
			}
			if !ok {
				continue
			}
			isDir, err := entryIsDir(nativePath, name, info)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(&DirEntry{
				Name:  name,
				IsDir: isDir,
				Size:  info.Size(),
			}, nil) {
				return
			}
		}

		if err != nil {
			yield(nil, err)
			return
		}

	}
}

func (l *LocalETLFS) Delete(ctx context.Context, filePaths ...string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	for _, filePath := range filePaths {
		if err := l.deleteSingle(ctx, filePath); err != nil {
			return err
		}
	}
	return nil
}

func (l *LocalETLFS) deleteSingle(ctx context.Context, filePath string) error {
	path, err := parseFilePathAtService(filePath, l.name)
	if err != nil {
		return err
	}
	return deleteLocalFile(&l.RWMutex, l.dirFiles, l.toNativeFilePath(path.File))
}

var _ ReaderWriterFileService = new(LocalETLFS)

func (l *LocalETLFS) NewReader(ctx context.Context, filePath string) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	path, err := parseFilePathAtService(filePath, l.name)
	if err != nil {
		return nil, err
	}
	nativePath := l.toNativeFilePath(path.File)

	_, err = os.Stat(nativePath)
	if os.IsNotExist(err) {
		return nil, moerr.NewFileNotFoundNoCtx(path.File)
	}
	if err != nil {
		return nil, err
	}

	f, err := os.Open(nativePath)
	if os.IsNotExist(err) {
		return nil, moerr.NewFileNotFoundNoCtx(path.File)
	}
	if err != nil {
		return nil, err
	}
	return &readCloser{
		r:         f,
		closeFunc: f.Close,
	}, nil
}

func (l *LocalETLFS) NewWriter(ctx context.Context, filePath string) (io.WriteCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	path, err := parseFilePathAtService(filePath, l.name)
	if err != nil {
		return nil, err
	}
	nativePath := l.toNativeFilePath(path.File)

	// check existence
	_, err = os.Stat(nativePath)
	if err == nil {
		// existed
		return nil, moerr.NewFileAlreadyExistsNoCtx(path.File)
	}

	f, err := os.CreateTemp(
		l.rootPath,
		".tmp.*",
	)
	if err != nil {
		return nil, err
	}

	return &writeCloser{
		w: f,
		closeFunc: func() error {
			open, owned := true, true
			defer func() {
				if open {
					_ = f.Close()
				}
				if owned {
					_ = os.Remove(f.Name())
				}
			}()
			// close
			err := f.Close()
			open = false
			if err != nil {
				return err
			}
			// ensure parent dir
			parentDir, _ := filepath.Split(nativePath)
			if err := l.ensureDir(parentDir); err != nil {
				return err
			}
			// move
			if err := os.Rename(f.Name(), nativePath); err != nil {
				return err
			}
			// Rename transfers ownership to the destination, even if sync fails.
			owned = false
			// sync parent dir
			if err := l.syncDir(parentDir); err != nil {
				return err
			}
			return nil
		},
	}, nil
}

func (l *LocalETLFS) ensureDir(nativePath string) error {
	return ensureLocalDir(&l.RWMutex, l.dirFiles, nativePath)
}

func (l *LocalETLFS) syncDir(nativePath string) error {
	return syncLocalDir(&l.RWMutex, l.dirFiles, nativePath)
}

func (l *LocalETLFS) Cost() *CostAttr {
	return &CostAttr{
		List: CostLow,
	}
}

func (l *LocalETLFS) toNativeFilePath(filePath string) string {
	return filepath.Join(l.rootPath, toOSPath(filePath))
}

var _ ETLFileService = new(LocalETLFS)

func (l *LocalETLFS) ETLCompatible() {}

var _ MutableFileService = new(LocalETLFS)

func (l *LocalETLFS) NewMutator(ctx context.Context, filePath string) (Mutator, error) {
	f, err := openLocalMutatorFile(ctx, l.name, filePath, l.toNativeFilePath)
	if err != nil {
		return nil, err
	}
	return &LocalETLFSMutator{osFile: f}, nil
}

type LocalETLFSMutator struct {
	osFile *os.File
}

func (l *LocalETLFSMutator) Mutate(ctx context.Context, entries ...IOEntry) error {
	return l.mutate(ctx, 0, entries...)
}

func (l *LocalETLFSMutator) Append(ctx context.Context, entries ...IOEntry) error {
	offset, err := l.osFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	return l.mutate(ctx, offset, entries...)
}

func (l *LocalETLFSMutator) mutate(ctx context.Context, baseOffset int64, entries ...IOEntry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return writeLocalMutatorEntries(l.osFile, baseOffset, entries)
}

func (l *LocalETLFSMutator) Close() error {
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

func (l *LocalETLFS) EnsureDir(ctx context.Context, filePath string) error {
	return ensureMutableLocalDir(ctx, l.name, filePath, l.toNativeFilePath, l.ensureDir)
}

func (l *LocalETLFS) OpenFile(ctx context.Context, filePath string) (*os.File, error) {
	return openMutableLocalFile(ctx, l.name, filePath, l.toNativeFilePath)
}

func (l *LocalETLFS) CreateFile(ctx context.Context, filePath string) (*os.File, error) {
	return createMutableLocalFile(ctx, l.name, filePath, l.toNativeFilePath)
}

func (l *LocalETLFS) RemoveFile(ctx context.Context, filePath string) error {
	return removeMutableLocalFile(ctx, l.name, filePath, l.toNativeFilePath)
}

func (l *LocalETLFS) CreateAndRemoveFile(ctx context.Context, filePath string) (*os.File, error) {
	err := ctx.Err()
	if err != nil {
		return nil, err
	}

	path, err := parseFilePathAtService(filePath, l.name)
	if err != nil {
		return nil, err
	}
	nativePath := l.toNativeFilePath(path.File)
	f, err := os.Create(nativePath)
	if err != nil {
		return nil, err
	}

	// do not check error for this one
	os.Remove(nativePath)
	return f, nil
}
