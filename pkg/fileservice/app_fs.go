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
	"iter"
	"path"
)

type AppConfig struct {
	Name string
	GCFn func(filePath string, fs FileService) (neesGC bool, err error)
}

var _ FileService = (*AppFS)(nil)

type AppFS struct {
	tmpFS     *TmpFileService
	appConfig *AppConfig
}

func (fs *AppFS) getAppDir() string {
	return fs.appConfig.Name
}

func (fs *AppFS) Name() string {
	return fs.appConfig.Name
}

func (fs *AppFS) toAppFilePath(filePath string) (string, error) {
	parsed, err := parseFilePathAtService(filePath, fs.Name())
	if err != nil {
		return "", err
	}
	return path.Join(fs.getAppDir(), parsed.File), nil
}

func (fs *AppFS) Write(
	ctx context.Context,
	vector IOVector,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filePath, err := fs.toAppFilePath(vector.FilePath)
	if err != nil {
		return err
	}
	vector.FilePath = filePath
	return fs.tmpFS.Write(ctx, vector)
}
func (fs *AppFS) Read(
	ctx context.Context,
	vector *IOVector,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filePath, err := fs.toAppFilePath(vector.FilePath)
	if err != nil {
		return err
	}
	vector.FilePath = filePath
	return fs.tmpFS.Read(ctx, vector)
}
func (fs *AppFS) ReadCache(ctx context.Context, vector *IOVector) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filePath, err := fs.toAppFilePath(vector.FilePath)
	if err != nil {
		return err
	}
	vector.FilePath = filePath
	return fs.tmpFS.ReadCache(ctx, vector)
}
func (fs *AppFS) List(ctx context.Context, dirPath string) iter.Seq2[*DirEntry, error] {
	dir := fs.getAppDir()
	dirPath = path.Join(dir, dirPath)
	return fs.tmpFS.List(ctx, dirPath)
}
func (fs *AppFS) Delete(ctx context.Context, filePaths ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	newFilePaths := make([]string, len(filePaths))
	for i, filePath := range filePaths {
		appFilePath, err := fs.toAppFilePath(filePath)
		if err != nil {
			return err
		}
		newFilePaths[i] = appFilePath
	}
	return fs.tmpFS.Delete(ctx, newFilePaths...)
}
func (fs *AppFS) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	appFilePath, err := fs.toAppFilePath(filePath)
	if err != nil {
		return nil, err
	}
	return fs.tmpFS.StatFile(ctx, appFilePath)
}
func (fs *AppFS) PrefetchFile(ctx context.Context, filePath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	appFilePath, err := fs.toAppFilePath(filePath)
	if err != nil {
		return err
	}
	return fs.tmpFS.PrefetchFile(ctx, appFilePath)
}
func (fs *AppFS) Cost() *CostAttr {
	panic("not implemented")
}
func (fs *AppFS) Close(ctx context.Context) {}
