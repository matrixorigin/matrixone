//go:build iceberggo

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

package iceberggo

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"math"
	"path/filepath"
	"strings"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	mopio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
)

const defaultMaxMaterializedReadBytes int64 = 512 << 20
const defaultMaxMaterializedWriteBytes int64 = 512 << 20

type FileServiceIO struct {
	Ctx                       context.Context
	Provider                  mopio.ObjectIOProvider
	ScopeForLocation          mopio.ObjectScopeForLocation
	MaxMaterializedReadBytes  int64
	MaxMaterializedWriteBytes int64
}

type fileServiceResolve struct {
	fs       fileservice.ETLFileService
	readPath string
}

func NewFileServiceIO(ctx context.Context, provider mopio.ObjectIOProvider, scopeForLocation mopio.ObjectScopeForLocation) *FileServiceIO {
	return &FileServiceIO{
		Ctx:              ctx,
		Provider:         provider,
		ScopeForLocation: scopeForLocation,
	}
}

func (iofs *FileServiceIO) Open(name string) (icebergio.File, error) {
	resolved, err := iofs.resolve("open", name)
	if err != nil {
		return nil, err
	}
	data, err := iofs.readAll(resolved)
	if err != nil {
		return nil, iofs.pathError("open", name, err)
	}
	return &fileServiceFile{
		name:   filepath.Base(strings.TrimSpace(name)),
		reader: bytes.NewReader(data),
		size:   int64(len(data)),
	}, nil
}

func (iofs *FileServiceIO) ReadFile(name string) ([]byte, error) {
	resolved, err := iofs.resolve("read", name)
	if err != nil {
		return nil, err
	}
	data, err := iofs.readAll(resolved)
	if err != nil {
		return nil, iofs.pathError("read", name, err)
	}
	return data, nil
}

func (iofs *FileServiceIO) Create(name string) (icebergio.FileWriter, error) {
	resolved, err := iofs.resolve("create", name)
	if err != nil {
		return nil, err
	}
	return &fileServiceWriter{
		ctx:      iofs.ctx(),
		fs:       resolved.fs,
		readPath: resolved.readPath,
		maxBytes: iofs.maxMaterializedWriteBytes(),
	}, nil
}

func (iofs *FileServiceIO) WriteFile(name string, data []byte) error {
	resolved, err := iofs.resolve("write", name)
	if err != nil {
		return err
	}
	return iofs.writeAll("write", name, resolved, data)
}

func (iofs *FileServiceIO) Remove(name string) error {
	resolved, err := iofs.resolve("remove", name)
	if err != nil {
		return err
	}
	if err := resolved.fs.Delete(iofs.ctx(), resolved.readPath); err != nil {
		return iofs.pathError("remove", name, err)
	}
	return nil
}

func (iofs *FileServiceIO) resolve(op, name string) (fileServiceResolve, error) {
	ctx := iofs.ctx()
	if iofs.Provider == nil {
		return fileServiceResolve{}, iofs.pathError(op, name, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg-go FileServiceIO requires ObjectIOProvider", nil)))
	}
	if strings.TrimSpace(name) == "" {
		return fileServiceResolve{}, iofs.pathError(op, name, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg-go FileServiceIO requires non-empty path", nil)))
	}
	scope := mopio.ObjectScope{StorageLocation: strings.TrimSpace(name)}
	if iofs.ScopeForLocation != nil {
		scope = iofs.ScopeForLocation(name)
		if strings.TrimSpace(scope.StorageLocation) == "" {
			scope.StorageLocation = strings.TrimSpace(name)
		}
	}
	resolvedFS, readPath, err := iofs.Provider.Resolve(ctx, scope)
	if err != nil {
		return fileServiceResolve{}, iofs.pathError(op, name, err)
	}
	if resolvedFS == nil || strings.TrimSpace(readPath) == "" {
		return fileServiceResolve{}, iofs.pathError(op, name, api.ToMOErr(ctx, api.NewError(api.ErrObjectIO, "Iceberg-go FileServiceIO resolved empty FileService or path", map[string]string{
			"location": iofs.redact(name),
		})))
	}
	return fileServiceResolve{fs: resolvedFS, readPath: strings.TrimSpace(readPath)}, nil
}

func (iofs *FileServiceIO) readAll(resolved fileServiceResolve) ([]byte, error) {
	maxBytes := iofs.MaxMaterializedReadBytes
	if maxBytes <= 0 || maxBytes > defaultMaxMaterializedReadBytes {
		maxBytes = defaultMaxMaterializedReadBytes
	}
	var stream io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: resolved.readPath,
		Policy:   fileservice.SkipFullFilePreloads,
		Entries: []fileservice.IOEntry{{
			Offset:            0,
			Size:              -1,
			ReadCloserForRead: &stream,
		}},
	}
	if err := resolved.fs.Read(iofs.ctx(), &vec); err != nil {
		if stream != nil {
			err = errors.Join(err, stream.Close())
		}
		return nil, api.ToMOErr(iofs.ctx(), api.WrapError(api.ErrObjectIO, "Iceberg-go FileServiceIO read failed", nil, err))
	}
	if len(vec.Entries) == 0 || stream == nil {
		return nil, api.ToMOErr(iofs.ctx(), api.NewError(api.ErrObjectIO, "Iceberg-go FileServiceIO read returned no entries", nil))
	}
	data, readErr := api.ReadAllBounded(stream, maxBytes)
	if err := errors.Join(readErr, stream.Close()); err != nil {
		if errors.Is(err, api.ErrMaterializationLimitExceeded) {
			return nil, api.ToMOErr(iofs.ctx(), api.NewError(api.ErrPlanningLimitExceeded, "Iceberg-go FileServiceIO object exceeds the materialization limit", nil))
		}
		return nil, api.ToMOErr(iofs.ctx(), api.WrapError(api.ErrObjectIO, "Iceberg-go FileServiceIO stream read failed", nil, err))
	}
	return data, nil
}

func (iofs *FileServiceIO) writeAll(op, name string, resolved fileServiceResolve, data []byte) error {
	if int64(len(data)) > iofs.maxMaterializedWriteBytes() {
		return iofs.pathError(op, name, api.ToMOErr(iofs.ctx(), api.NewError(api.ErrPlanningLimitExceeded, "Iceberg-go FileServiceIO write exceeds the materialization limit", nil)))
	}
	// FileService.Write consumes the vector synchronously. Borrow the caller's
	// payload for that call and skip S3's full-object disk-cache tee so the
	// adapter does not create an unaccounted second copy.
	if err := resolved.fs.Write(iofs.ctx(), fileservice.IOVector{
		FilePath: resolved.readPath,
		Policy:   fileservice.SkipDiskCacheWrites,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   data,
		}},
	}); err != nil {
		return iofs.pathError(op, name, api.ToMOErr(iofs.ctx(), api.WrapError(api.ErrObjectIO, "Iceberg-go FileServiceIO write failed", nil, err)))
	}
	return nil
}

func (iofs *FileServiceIO) maxMaterializedWriteBytes() int64 {
	maxBytes := iofs.MaxMaterializedWriteBytes
	if maxBytes <= 0 || maxBytes > defaultMaxMaterializedWriteBytes {
		maxBytes = defaultMaxMaterializedWriteBytes
	}
	if maxBytes > int64(math.MaxInt) {
		return int64(math.MaxInt)
	}
	return maxBytes
}

func (iofs *FileServiceIO) pathError(op, name string, err error) error {
	return &fs.PathError{
		Op:   op,
		Path: iofs.redact(name),
		Err:  err,
	}
}

func (iofs *FileServiceIO) redact(name string) string {
	if iofs.Provider != nil {
		return iofs.Provider.RedactPath(name)
	}
	return mopio.RedactObjectPath(name)
}

func (iofs *FileServiceIO) ctx() context.Context {
	if iofs != nil && iofs.Ctx != nil {
		return iofs.Ctx
	}
	return context.Background()
}

type fileServiceFile struct {
	name   string
	reader *bytes.Reader
	size   int64
	closed bool
}

func (f *fileServiceFile) Stat() (fs.FileInfo, error) {
	return fileServiceFileInfo{name: f.name, size: f.size}, nil
}

func (f *fileServiceFile) Read(data []byte) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.reader.Read(data)
}

func (f *fileServiceFile) ReadAt(data []byte, offset int64) (int, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.reader.ReadAt(data, offset)
}

func (f *fileServiceFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, fs.ErrClosed
	}
	return f.reader.Seek(offset, whence)
}

func (f *fileServiceFile) Close() error {
	f.closed = true
	return nil
}

type fileServiceFileInfo struct {
	name string
	size int64
}

func (info fileServiceFileInfo) Name() string       { return info.name }
func (info fileServiceFileInfo) Size() int64        { return info.size }
func (info fileServiceFileInfo) Mode() fs.FileMode  { return 0o444 }
func (info fileServiceFileInfo) ModTime() time.Time { return time.Time{} }
func (info fileServiceFileInfo) IsDir() bool        { return false }
func (info fileServiceFileInfo) Sys() any           { return nil }

type fileServiceWriter struct {
	ctx      context.Context
	fs       fileservice.ETLFileService
	readPath string
	data     []byte
	maxBytes int64
	writeErr error
	closed   bool
}

func (w *fileServiceWriter) Write(data []byte) (int, error) {
	if w.closed {
		return 0, fs.ErrClosed
	}
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if int64(len(data)) > w.maxBytes-int64(len(w.data)) {
		w.writeErr = api.ToMOErr(w.ctx, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg-go FileServiceIO writer exceeds the materialization limit", nil))
		return 0, w.writeErr
	}
	required := len(w.data) + len(data)
	if required > cap(w.data) {
		nextCapacity := cap(w.data) * 2
		if nextCapacity < 512 {
			nextCapacity = 512
		}
		if nextCapacity < required {
			nextCapacity = required
		}
		maxCapacity := int(w.maxBytes)
		if nextCapacity > maxCapacity {
			nextCapacity = maxCapacity
		}
		if cap(w.data) > maxCapacity-nextCapacity {
			w.writeErr = api.ToMOErr(w.ctx, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg-go FileServiceIO writer growth exceeds the materialization limit", nil))
			return 0, w.writeErr
		}
		next := make([]byte, len(w.data), nextCapacity)
		copy(next, w.data)
		w.data = next
	}
	start := len(w.data)
	w.data = w.data[:required]
	copy(w.data[start:], data)
	return len(data), nil
}

func (w *fileServiceWriter) ReadFrom(reader io.Reader) (int64, error) {
	if w.closed {
		return 0, fs.ErrClosed
	}
	return io.Copy(w, reader)
}

func (w *fileServiceWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.writeErr != nil {
		return w.writeErr
	}
	if err := w.fs.Write(w.ctx, fileservice.IOVector{
		FilePath: w.readPath,
		Policy:   fileservice.SkipDiskCacheWrites,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(w.data)),
			Data:   w.data,
		}},
	}); err != nil {
		return api.ToMOErr(w.ctx, api.WrapError(api.ErrObjectIO, "Iceberg-go FileServiceIO writer close failed", nil, err))
	}
	return nil
}

var _ icebergio.ReadFileIO = (*FileServiceIO)(nil)
var _ icebergio.WriteFileIO = (*FileServiceIO)(nil)
