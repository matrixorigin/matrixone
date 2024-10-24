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

	"golang.org/x/sync/errgroup"
)

type FileServiceWriter struct {
	Reader      *io.PipeReader
	Writer      *io.PipeWriter
	Group       *errgroup.Group
	Filepath    string
	FileService FileService
}

func NewFileServiceWriter(moPath string, ctx context.Context) (*FileServiceWriter, error) {

	var readPath string
	var err error

	w := &FileServiceWriter{}

	w.Filepath = moPath

	w.Reader, w.Writer = io.Pipe()

	w.FileService, readPath, err = GetForETL(ctx, nil, w.Filepath)
	if err != nil {
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
	return err
}
