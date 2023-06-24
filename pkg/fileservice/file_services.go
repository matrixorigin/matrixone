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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type FileServices struct {
	defaultName string
	mappings    map[string]FileService
}

func NewFileServices(defaultName string, fss ...FileService) (*FileServices, error) {
	f := &FileServices{
		defaultName: defaultName,
		mappings:    make(map[string]FileService),
	}
	for _, fs := range fss {
		name := strings.ToLower(fs.Name())
		if _, ok := f.mappings[name]; ok {
			return nil, moerr.NewDupServiceNameNoCtx(name)
		}
		f.mappings[name] = fs
	}
	return f, nil
}

var _ FileService = &FileServices{}

func (f *FileServices) Delete(ctx context.Context, filePaths ...string) error {
	for _, filePath := range filePaths {
		if err := f.deleteSingle(ctx, filePath); err != nil {
			return err
		}
	}
	return nil
}

func (f *FileServices) deleteSingle(ctx context.Context, filePath string) error {
	path, err := ParsePathAtService(filePath, "")
	if err != nil {
		return err
	}
	if path.Service == "" {
		path.Service = f.defaultName
	}
	fs, err := Get[FileService](f, path.Service)
	if err != nil {
		return err
	}
	return fs.Delete(ctx, filePath)
}

func (f *FileServices) List(ctx context.Context, dirPath string) ([]DirEntry, error) {
	path, err := ParsePathAtService(dirPath, "")
	if err != nil {
		return nil, err
	}
	if path.Service == "" {
		path.Service = f.defaultName
	}
	fs, err := Get[FileService](f, path.Service)
	if err != nil {
		return nil, err
	}
	return fs.List(ctx, dirPath)
}

func (f *FileServices) Preload(ctx context.Context, dirPath string) error {
	path, err := ParsePathAtService(dirPath, "")
	if err != nil {
		return err
	}
	if path.Service == "" {
		path.Service = f.defaultName
	}
	fs, err := Get[FileService](f, path.Service)
	if err != nil {
		return err
	}
	return fs.Preload(ctx, dirPath)
}

func (f *FileServices) Name() string {
	return f.defaultName
}

func (f *FileServices) Read(ctx context.Context, vector *IOVector) error {
	path, err := ParsePathAtService(vector.FilePath, "")
	if err != nil {
		return err
	}
	if path.Service == "" {
		path.Service = f.defaultName
	}

	fs, err := Get[FileService](f, path.Service)
	if err != nil {
		return err
	}
	return fs.Read(ctx, vector)
}

func (f *FileServices) Write(ctx context.Context, vector IOVector) error {
	path, err := ParsePathAtService(vector.FilePath, "")
	if err != nil {
		return err
	}
	if path.Service == "" {
		path.Service = f.defaultName
	}
	fs, err := Get[FileService](f, path.Service)
	if err != nil {
		return err
	}
	return fs.Write(ctx, vector)
}

func (f *FileServices) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	path, err := ParsePathAtService(filePath, "")
	if err != nil {
		return nil, err
	}
	if path.Service == "" {
		path.Service = f.defaultName
	}
	fs, err := Get[FileService](f, path.Service)
	if err != nil {
		return nil, err
	}
	return fs.StatFile(ctx, filePath)
}
