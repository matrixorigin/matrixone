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
	"testing"

	"iter"

	"github.com/stretchr/testify/require"
)

type errorListFileService struct {
	name    string
	listErr error
}

func (e *errorListFileService) Name() string { return e.name }
func (e *errorListFileService) Write(ctx context.Context, vector IOVector) error {
	return nil
}
func (e *errorListFileService) Read(ctx context.Context, vector *IOVector) error {
	return nil
}
func (e *errorListFileService) ReadCache(ctx context.Context, vector *IOVector) error {
	return nil
}
func (e *errorListFileService) List(ctx context.Context, dirPath string) iter.Seq2[*DirEntry, error] {
	return func(yield func(*DirEntry, error) bool) {
		yield(nil, e.listErr)
	}
}
func (e *errorListFileService) Delete(ctx context.Context, filePaths ...string) error {
	return nil
}
func (e *errorListFileService) StatFile(ctx context.Context, filePath string) (*DirEntry, error) {
	return nil, nil
}
func (e *errorListFileService) PrefetchFile(ctx context.Context, filePath string) error {
	return nil
}
func (e *errorListFileService) Cost() *CostAttr { return nil }
func (e *errorListFileService) Close(ctx context.Context) {
}

func TestTmpFileServiceGCHandlesNilEntry(t *testing.T) {
	fs := &TmpFileService{
		FileService: &errorListFileService{
			name:    "tmp",
			listErr: errors.New("list failed"),
		},
		apps: make(map[string]*AppFS),
	}
	appConfig := &AppConfig{
		Name: "app",
		GCFn: func(filePath string, fs FileService) (bool, error) {
			return false, nil
		},
	}
	app := &AppFS{
		tmpFS:     fs,
		appConfig: appConfig,
	}
	fs.apps[appConfig.Name] = app

	require.NotPanics(t, func() {
		fs.gc(context.Background())
	})
}
