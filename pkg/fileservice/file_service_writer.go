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
	"context"
	"errors"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"
)

type FileServiceWriter struct {
	Reader      *io.PipeReader
	Writer      *io.PipeWriter
	Group       *errgroup.Group
	Filepath    string
	FileService FileService
	ctx         context.Context
	owned       bool
	closeOnce   sync.Once
}

func NewFileServiceWriter(moPath string, ctx context.Context) (*FileServiceWriter, error) {
	return NewFileServiceWriterWithFileService(nil, moPath, ctx)
}

// NewFileServiceWriterWithFileService is the Datalink entry point when a
// configured service registry is available. Argumented and service-less paths
// create a per-writer ETL service; named paths borrow the configured service.
// The writer closes only services it created itself.
func NewFileServiceWriterWithFileService(fileService FileService, moPath string, ctx context.Context) (*FileServiceWriter, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var readPath string
	var err error

	path, err := ParsePath(moPath)
	if err != nil {
		return nil, err
	}
	w := &FileServiceWriter{
		ctx: ctx,
		// GetForETL creates a local service for an empty service component and
		// a dynamic service for argumented paths. A named service is borrowed
		// from the caller's registry and must remain open after this writer.
		owned: path.Service == "" || len(path.ServiceArguments) > 0,
	}

	w.Filepath = moPath

	w.Reader, w.Writer = io.Pipe()

	w.FileService, readPath, err = GetForETLWithParsedPath(ctx, fileService, w.Filepath, path)
	if err != nil {
		w.closeOwnedFileService()
		return nil, err
	}

	asyncWriteFunc := func() error {
		vec := IOVector{
			FilePath: readPath,
			Entries: []IOEntry{
				{
					ReaderForWrite: w.Reader,
					Size:           -1,
				},
			},
		}
		err := w.FileService.Write(ctx, vec)
		if err != nil {
			err2 := w.Reader.CloseWithError(err)
			if err2 != nil {
				return err2
			}
		}
		return err
	}

	w.Group, _ = errgroup.WithContext(ctx)
	w.Group.Go(asyncWriteFunc)

	return w, nil
}

func (w *FileServiceWriter) closeOwnedFileService() {
	w.closeOnce.Do(func() {
		if w.owned && w.FileService != nil {
			w.FileService.Close(w.ctx)
		}
		w.FileService = nil
	})
}

func (w *FileServiceWriter) Write(b []byte) (int, error) {
	n, err := w.Writer.Write(b)
	if err != nil {
		err2 := w.Writer.CloseWithError(err)
		if err2 != nil {
			return 0, err2
		}
	}
	return n, err
}

func (w *FileServiceWriter) Close() error {
	err := w.Writer.Close()
	err2 := w.Group.Wait()
	err3 := w.Reader.Close()
	err = errors.Join(err, err2, err3)

	w.Reader = nil
	w.Writer = nil
	w.Group = nil
	w.closeOwnedFileService()
	return err
}

// Abort terminates the write without finalizing the target file: the pipe is
// closed with cause, so the in-flight FileService.Write fails and discards its
// partial output (local: the temp file is removed; object stores: the put is
// not completed) instead of persisting a truncated file the way Close would.
// Best-effort cleanup: idempotent, and safe after Close.
func (w *FileServiceWriter) Abort(cause error) {
	if w.Writer != nil {
		_ = w.Writer.CloseWithError(cause)
	}
	if w.Group != nil {
		_ = w.Group.Wait()
	}
	if w.Reader != nil {
		_ = w.Reader.Close()
	}

	w.Reader = nil
	w.Writer = nil
	w.Group = nil
	w.closeOwnedFileService()
}
