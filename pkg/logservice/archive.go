// Copyright 2021 - 2024 Matrix Origin
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

package logservice

import (
	"context"
	"io"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/vfs"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const (
	archiveLogDir = "logs"
)

type archiverIO struct {
	vfs vfs.FS
	fs  fileservice.FileService
}

func newArchiveIO(vfs vfs.FS, fs fileservice.FileService) config.ArchiveIO {
	return &archiverIO{
		vfs: vfs,
		fs:  fs,
	}
}

// Write implements the config.ArchiveIO interface.
func (a *archiverIO) Write(ctx context.Context, subDir string, filePath string) error {
	if a.fs == nil {
		return nil
	}
	if filePath == "" {
		return nil
	}
	file, err := a.vfs.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	ioVec := fileservice.IOVector{
		FilePath: a.vfs.PathJoin(archiveLogDir, subDir, a.vfs.PathBase(filePath)),
		Entries: []fileservice.IOEntry{
			{
				Size:           -1,
				ReaderForWrite: file,
			},
		},
	}
	return a.fs.Write(ctx, ioVec)
}

// List implements the config.ArchiveIO interface.
func (a *archiverIO) List(ctx context.Context, subDir string) ([]string, error) {
	entries, err := a.fs.List(ctx, a.vfs.PathJoin(archiveLogDir, subDir))
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir {
			files = append(files, entry.Name)
		}
	}
	return files, nil
}

// Open implements the config.ArchiveIO interface.
func (a *archiverIO) Open(ctx context.Context, subDir, filename string) (io.ReadCloser, error) {
	var r io.ReadCloser
	ioVec := fileservice.IOVector{
		FilePath: a.vfs.PathJoin(archiveLogDir, subDir, a.vfs.PathBase(filename)),
		Entries: []fileservice.IOEntry{
			{
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}
	if err := a.fs.Read(ctx, &ioVec); err != nil {
		return nil, err
	}
	return r, nil
}

func (a *archiverIO) Download(ctx context.Context, subDir, filename, dstFile string) error {
	f, err := a.vfs.Create(dstFile)
	if err != nil {
		return err
	}
	defer f.Close()
	ioVec := fileservice.IOVector{
		FilePath: a.vfs.PathJoin(archiveLogDir, subDir, a.vfs.PathBase(filename)),
		Entries: []fileservice.IOEntry{
			{
				Size:          -1,
				WriterForRead: f,
			},
		},
	}
	if err := a.fs.Read(ctx, &ioVec); err != nil {
		return err
	}
	return nil
}

// Delete implements the config.ArchiveIO interface.
func (a *archiverIO) Delete(ctx context.Context, subDir string, files ...string) error {
	toDelete := make([]string, 0, len(files))
	for _, file := range files {
		toDelete = append(toDelete, a.vfs.PathJoin(archiveLogDir, subDir, file))
	}
	return a.fs.Delete(ctx, toDelete...)
}
