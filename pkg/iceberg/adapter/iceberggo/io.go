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
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	mopio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
)

type FileServiceIO struct {
	Ctx              context.Context
	Provider         mopio.ObjectIOProvider
	ScopeForLocation mopio.ObjectScopeForLocation
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
	vec := fileservice.IOVector{
		FilePath: resolved.readPath,
		Policy:   fileservice.SkipFullFilePreloads,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   -1,
		}},
	}
	if err := resolved.fs.Read(iofs.ctx(), &vec); err != nil {
		return nil, api.ToMOErr(iofs.ctx(), api.WrapError(api.ErrObjectIO, "Iceberg-go FileServiceIO read failed", nil, err))
	}
	if len(vec.Entries) == 0 {
		return nil, api.ToMOErr(iofs.ctx(), api.NewError(api.ErrObjectIO, "Iceberg-go FileServiceIO read returned no entries", nil))
	}
	return append([]byte(nil), vec.Entries[0].Data...), nil
}

func (iofs *FileServiceIO) writeAll(op, name string, resolved fileServiceResolve, data []byte) error {
	payload := append([]byte(nil), data...)
	if err := resolved.fs.Write(iofs.ctx(), fileservice.IOVector{
		FilePath: resolved.readPath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(payload)),
			Data:   payload,
		}},
	}); err != nil {
		return iofs.pathError(op, name, api.ToMOErr(iofs.ctx(), api.WrapError(api.ErrObjectIO, "Iceberg-go FileServiceIO write failed", nil, err)))
	}
	return nil
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
	buf      bytes.Buffer
	closed   bool
}

func (w *fileServiceWriter) Write(data []byte) (int, error) {
	if w.closed {
		return 0, fs.ErrClosed
	}
	return w.buf.Write(data)
}

func (w *fileServiceWriter) ReadFrom(reader io.Reader) (int64, error) {
	if w.closed {
		return 0, fs.ErrClosed
	}
	return w.buf.ReadFrom(reader)
}

func (w *fileServiceWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	payload := append([]byte(nil), w.buf.Bytes()...)
	if err := w.fs.Write(w.ctx, fileservice.IOVector{
		FilePath: w.readPath,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(payload)),
			Data:   payload,
		}},
	}); err != nil {
		return api.ToMOErr(w.ctx, api.WrapError(api.ErrObjectIO, "Iceberg-go FileServiceIO writer close failed", nil, err))
	}
	return nil
}

var _ icebergio.ReadFileIO = (*FileServiceIO)(nil)
var _ icebergio.WriteFileIO = (*FileServiceIO)(nil)
