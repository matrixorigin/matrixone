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
	"testing"

	"github.com/stretchr/testify/assert"
	"iter"
)

func TestGetForBackup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs, err := GetForBackup(ctx, dir)
	assert.Nil(t, err)
	localFS, ok := fs.(*LocalFS)
	assert.True(t, ok)
	assert.Equal(t, dir, localFS.rootPath)
}

func TestGetForBackupS3Opts(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	spec := JoinPath("s3-opts,endpoint=disk,bucket="+dir+",prefix=backup-prefix,name=backup", "object")
	fs, err := GetForBackup(ctx, spec)
	assert.Nil(t, err)
	s3fs, ok := fs.(*S3FS)
	assert.True(t, ok)
	assert.Equal(t, "backup", s3fs.name)
	assert.Equal(t, "backup-prefix", s3fs.keyPrefix)
}

type dummyFileService struct{ name string }

func (d dummyFileService) Delete(ctx context.Context, filePaths ...string) error { return nil }
func (d dummyFileService) Name() string                                          { return d.name }
func (d dummyFileService) Read(ctx context.Context, vector *IOVector) error      { return nil }
func (d dummyFileService) ReadCache(ctx context.Context, vector *IOVector) error { return nil }
func (d dummyFileService) Write(ctx context.Context, vector IOVector) error      { return nil }
func (d dummyFileService) List(ctx context.Context, dirPath string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		yield(&DirEntry{Name: "a"}, nil)
	}
}
func (d dummyFileService) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	return &DirEntry{Name: filePath}, nil
}
func (d dummyFileService) PrefetchFile(ctx context.Context, filePath string) error { return nil }
func (d dummyFileService) Cost() *CostAttr                                         { return nil }
func (d dummyFileService) Close(ctx context.Context)                               {}

func TestGetFromMappings(t *testing.T) {
	fs1 := dummyFileService{name: "first"}
	fs2 := dummyFileService{name: "second"}
	mapping, err := NewFileServices("first", fs1, fs2)
	assert.NoError(t, err)

	var res FileService
	res, err = Get[FileService](mapping, "second")
	assert.NoError(t, err)
	assert.Equal(t, "second", res.Name())

	_, err = Get[FileService](mapping, "missing")
	assert.Error(t, err)

	res, err = Get[FileService](fs1, "first")
	assert.NoError(t, err)
	assert.Equal(t, "first", res.Name())

	_, err = Get[FileService](fs1, "other")
	assert.Error(t, err)
}
