// Copyright 2024 Matrix Origin
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

package main

import (
	"context"
	"io"
	"iter"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func Test_saveProfile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	fs, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, dir)
	assert.Nil(t, err)
	defer fs.Close(ctx)
	globalEtlFS = fs
	saveCpuProfile(time.Second)
	saveProfiles()
}

func Test_saveProfile2(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewMemoryFS("memory", fileservice.DisabledCacheConfig, nil)
	assert.NoError(t, err)
	defer fs.Close(ctx)
	globalEtlFS = fs
	saveCpuProfile(time.Second)
}

func Test_saveProfile3(t *testing.T) {
	ctx := context.Background()
	sigs := make(chan os.Signal, 1)
	close(sigs)
	*profileInterval = time.Second * 10
	dir := t.TempDir()
	fs, err := fileservice.NewLocalETLFS(defines.ETLFileServiceName, dir)
	assert.Nil(t, err)
	defer fs.Close(ctx)
	globalEtlFS = fs
	saveProfilesLoop(sigs)
}

func Test_saveProfile4(t *testing.T) {
	saveProfileWithType("cpu", func(writer io.Writer) error {
		return context.DeadlineExceeded
	})
}

var _ fileservice.FileService = &testFS{}

type testFS struct {
}

func (tfs *testFS) Name() string {
	//TODO implement me
	panic("implement me")
}

func (tfs *testFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	return moerr.NewInternalError(ctx, "return err")
}

func (tfs *testFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	//TODO implement me
	panic("implement me")
}

func (tfs *testFS) ReadCache(ctx context.Context, vector *fileservice.IOVector) error {
	//TODO implement me
	panic("implement me")
}

func (tfs *testFS) List(ctx context.Context, dirPath string) iter.Seq2[*fileservice.DirEntry, error] {
	//TODO implement me
	panic("implement me")
}

func (tfs *testFS) Delete(ctx context.Context, filePaths ...string) error {
	//TODO implement me
	panic("implement me")
}

func (tfs *testFS) StatFile(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
	//TODO implement me
	panic("implement me")
}

func (tfs *testFS) PrefetchFile(ctx context.Context, filePath string) error {
	//TODO implement me
	panic("implement me")
}

func (tfs *testFS) Cost() *fileservice.CostAttr {
	//TODO implement me
	panic("implement me")
}

func (tfs *testFS) Close(ctx context.Context) {
	//TODO implement me
	panic("implement me")
}

func Test_saveMallocProfile5(t *testing.T) {
	globalEtlFS = &testFS{}
	saveMallocProfile()
}
