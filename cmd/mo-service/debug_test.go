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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

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

func (tfs *testFS) List(ctx context.Context, dirPath string) ([]fileservice.DirEntry, error) {
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

func (tfs *testFS) Close() {
	//TODO implement me
	panic("implement me")
}

func Test_saveMallocProfile(t *testing.T) {
	globalEtlFS = &testFS{}
	saveMallocProfile()
}
