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
func (fs *AppFS) Write(
	ctx context.Context,
	vector IOVector,
) error {
	dir := fs.getAppDir()
	vector.FilePath = path.Join(dir, vector.FilePath)
	return fs.tmpFS.Write(ctx, vector)
}
func (fs *AppFS) Read(
	ctx context.Context,
	vector *IOVector,
) error {
	dir := fs.getAppDir()
	vector.FilePath = path.Join(dir, vector.FilePath)
	return fs.tmpFS.Read(ctx, vector)
}
func (fs *AppFS) ReadCache(ctx context.Context, vector *IOVector) error {
	panic("not implemented")
}
func (fs *AppFS) List(ctx context.Context, dirPath string) iter.Seq2[*DirEntry, error] {
	dir := fs.getAppDir()
	dirPath = path.Join(dir, dirPath)
	return fs.tmpFS.List(ctx, dirPath)
}
func (fs *AppFS) Delete(ctx context.Context, filePaths ...string) error {
	newFilePaths := make([]string, len(filePaths))
	dir := fs.getAppDir()
	for i, filePath := range filePaths {
		newFilePaths[i] = path.Join(dir, filePath)
	}
	return fs.tmpFS.Delete(ctx, newFilePaths...)
}
func (fs *AppFS) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	dir := fs.getAppDir()
	filePath = path.Join(dir, filePath)
	return fs.tmpFS.StatFile(ctx, filePath)
}
func (fs *AppFS) PrefetchFile(ctx context.Context, filePath string) error {
	panic("not implemented")
}
func (fs *AppFS) Cost() *CostAttr {
	panic("not implemented")
}
func (fs *AppFS) Close(ctx context.Context) {}
