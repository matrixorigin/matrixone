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
	"fmt"
	"strings"
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
			return nil, fmt.Errorf("%w: %s", ErrDuplicatedName, name)
		}
		f.mappings[name] = fs
	}
	return f, nil
}

var _ FileService = &FileServices{}

func (f *FileServices) Delete(ctx context.Context, filePath string) error {
	name, path, err := splitPath("", filePath)
	if err != nil {
		return err
	}
	if name == "" {
		name = f.defaultName
	}
	fs, err := Get[FileService](f, name)
	if err != nil {
		return err
	}
	return fs.Delete(ctx, path)
}

func (f *FileServices) List(ctx context.Context, dirPath string) ([]DirEntry, error) {
	name, path, err := splitPath("", dirPath)
	if err != nil {
		return nil, err
	}
	if name == "" {
		name = f.defaultName
	}
	fs, err := Get[FileService](f, name)
	if err != nil {
		return nil, err
	}
	return fs.List(ctx, path)
}

func (f *FileServices) Name() string {
	return f.defaultName
}

func (f *FileServices) Read(ctx context.Context, vector *IOVector) error {
	name, _, err := splitPath("", vector.FilePath)
	if err != nil {
		return err
	}
	if name == "" {
		name = f.defaultName
	}
	fs, err := Get[FileService](f, name)
	if err != nil {
		return err
	}
	return fs.Read(ctx, vector)
}

func (f *FileServices) Write(ctx context.Context, vector IOVector) error {
	name, _, err := splitPath("", vector.FilePath)
	if err != nil {
		return err
	}
	if name == "" {
		name = f.defaultName
	}
	fs, err := Get[FileService](f, name)
	if err != nil {
		return err
	}
	return fs.Write(ctx, vector)
}
