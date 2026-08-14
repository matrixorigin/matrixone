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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"iter"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

type errorListFileService struct {
	name       string
	listErr    error
	entryName  string
	withEntry  bool
	closeCount atomic.Int64
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
		var entry *DirEntry
		if e.withEntry {
			entry = &DirEntry{Name: e.entryName}
		}
		yield(entry, e.listErr)
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
	e.closeCount.Add(1)
}

func TestTmpFileServiceInstancesOwnTheirRoots(t *testing.T) {
	ctx := context.Background()
	firstRoot := t.TempDir()
	secondRoot := t.TempDir()
	first, err := NewTmpFileService("tmp", firstRoot, time.Hour)
	require.NoError(t, err)
	defer first.Close(ctx)
	second, err := NewTmpFileService("tmp", secondRoot, time.Hour)
	require.NoError(t, err)
	defer second.Close(ctx)

	require.NotSame(t, first, second)
	canonicalFirstRoot, err := filepath.EvalSymlinks(firstRoot)
	require.NoError(t, err)
	canonicalSecondRoot, err := filepath.EvalSymlinks(secondRoot)
	require.NoError(t, err)
	require.Equal(t, canonicalFirstRoot, first.FileService.(*LocalETLFS).rootPath)
	require.Equal(t, canonicalSecondRoot, second.FileService.(*LocalETLFS).rootPath)
	for fs, value := range map[*TmpFileService]byte{first: 1, second: 2} {
		require.NoError(t, fs.Write(ctx, IOVector{
			FilePath: "same-name",
			Entries:  []IOEntry{{Size: 1, Data: []byte{value}}},
		}))
	}
	firstData, err := os.ReadFile(filepath.Join(firstRoot, "same-name"))
	require.NoError(t, err)
	secondData, err := os.ReadFile(filepath.Join(secondRoot, "same-name"))
	require.NoError(t, err)
	require.Equal(t, []byte{1}, firstData)
	require.Equal(t, []byte{2}, secondData)
}

func TestTmpFileServiceConcurrentCloseHasOneOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	storage := &errorListFileService{name: "tmp"}
	fs := &TmpFileService{
		FileService: storage,
		cancel:      cancel,
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fs.Close(ctx)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), storage.closeCount.Load())
}

func TestTmpFileServiceGCHandlesNilEntry(t *testing.T) {
	fs := &TmpFileService{
		FileService: &errorListFileService{
			name:    "tmp",
			listErr: moerr.NewInternalErrorNoCtx("list failed"),
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

func TestTmpFileServiceInitHandlesNilEntry(t *testing.T) {
	fs := &TmpFileService{
		FileService: &errorListFileService{
			name:    "tmp",
			listErr: moerr.NewInternalErrorNoCtx("list failed"),
		},
		apps: make(map[string]*AppFS),
	}

	require.NotPanics(t, fs.init)
}

func TestTmpFileServiceGCHandlesErrorWithEntry(t *testing.T) {
	fs := &TmpFileService{
		FileService: &errorListFileService{
			name:      "tmp",
			listErr:   moerr.NewInternalErrorNoCtx("list failed"),
			entryName: "file",
			withEntry: true,
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
