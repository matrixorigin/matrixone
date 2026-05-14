// Copyright 2021 Matrix Origin
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

package model

import (
	"context"
	"iter"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failWriteFS struct {
	failCount        atomic.Int32
	maxFails         int32
	fileExistsOnFail int32 // if >0, return FileAlreadyExists on this attempt number
}

func (fs *failWriteFS) Name() string { return "fail-write-fs" }
func (fs *failWriteFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	n := fs.failCount.Add(1)
	if fs.fileExistsOnFail > 0 && n == fs.fileExistsOnFail {
		return moerr.NewFileAlreadyExistsNoCtx(vector.FilePath)
	}
	if n <= fs.maxFails {
		return moerr.NewInternalErrorNoCtxf("injected IO error (attempt %d)", n)
	}
	return nil
}
func (fs *failWriteFS) Read(ctx context.Context, vector *fileservice.IOVector) error {
	panic("unimplemented")
}
func (fs *failWriteFS) ReadCache(ctx context.Context, vector *fileservice.IOVector) error {
	panic("unimplemented")
}
func (fs *failWriteFS) List(ctx context.Context, dirPath string) iter.Seq2[*fileservice.DirEntry, error] {
	panic("unimplemented")
}
func (fs *failWriteFS) Delete(ctx context.Context, filePaths ...string) error {
	panic("unimplemented")
}
func (fs *failWriteFS) StatFile(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
	panic("unimplemented")
}
func (fs *failWriteFS) PrefetchFile(ctx context.Context, filePath string) error {
	panic("unimplemented")
}
func (fs *failWriteFS) Cost() *fileservice.CostAttr { panic("unimplemented") }
func (fs *failWriteFS) Close(ctx context.Context)   {}

func makeTestPages(n int) ([]*TransferHashPage, fileservice.IOVector) {
	sid := objectio.NewSegmentid()
	createdObjs := []*objectio.ObjectId{objectio.NewObjectidWithSegmentIDAndNum(sid, 2)}

	pages := make([]*TransferHashPage, 0, n)
	ioVector := fileservice.IOVector{
		FilePath: "test-transfer-page",
	}

	for j := 0; j < n; j++ {
		id := &common.ID{BlockID: *objectio.NewBlockid(sid, uint16(j), 0)}
		page := &TransferHashPage{
			id:      id,
			ttl:     ttl,
			diskTTL: diskTTL,
			objects: createdObjs,
		}
		now := time.Now()
		page.bornTS.Store(&now)

		m := make(api.TransferMap)
		for i := 0; i < 100; i++ {
			m[uint32(i)] = api.TransferDestPos{BlkIdx: 0, RowIdx: uint32(i)}
		}
		page.hashmap.Store(&m)

		_ = AddTransferPage(page, &ioVector)
		pages = append(pages, page)
	}
	return pages, ioVector
}

func TestWriteTransferPage_AllRetriesFail(t *testing.T) {
	fs := &failWriteFS{maxFails: 10}
	pages, ioVector := makeTestPages(3)

	err := WriteTransferPage(context.Background(), fs, pages, ioVector)

	require.Error(t, err)
	assert.Equal(t, int32(transferPageWriteMaxRetry), fs.failCount.Load())
	for _, page := range pages {
		assert.Empty(t, page.path.Name, "path should not be set when write fails")
	}
}

func TestWriteTransferPage_SucceedsAfterRetry(t *testing.T) {
	fs := &failWriteFS{maxFails: 2}
	pages, ioVector := makeTestPages(3)

	err := WriteTransferPage(context.Background(), fs, pages, ioVector)

	require.NoError(t, err)
	assert.Equal(t, int32(3), fs.failCount.Load())
	for i, page := range pages {
		assert.Equal(t, ioVector.FilePath, page.path.Name)
		assert.Equal(t, ioVector.Entries[i].Offset, page.path.Offset)
		assert.Equal(t, ioVector.Entries[i].Size, page.path.Size)
	}
}

func TestWriteTransferPage_SucceedsFirstTry(t *testing.T) {
	fs := &failWriteFS{maxFails: 0}
	pages, ioVector := makeTestPages(2)

	err := WriteTransferPage(context.Background(), fs, pages, ioVector)

	require.NoError(t, err)
	assert.Equal(t, int32(1), fs.failCount.Load())
	for i, page := range pages {
		assert.Equal(t, ioVector.FilePath, page.path.Name)
		assert.Equal(t, ioVector.Entries[i].Offset, page.path.Offset)
		assert.Equal(t, ioVector.Entries[i].Size, page.path.Size)
	}
}

func TestWriteTransferPage_FileAlreadyExistsOnRetry(t *testing.T) {
	// First attempt fails with IO error, second attempt returns FileAlreadyExists.
	// WriteTransferPage should treat FileAlreadyExists as success.
	fs := &failWriteFS{maxFails: 10, fileExistsOnFail: 2}
	pages, ioVector := makeTestPages(2)

	err := WriteTransferPage(context.Background(), fs, pages, ioVector)

	require.NoError(t, err)
	assert.Equal(t, int32(2), fs.failCount.Load())
	for i, page := range pages {
		assert.Equal(t, ioVector.FilePath, page.path.Name)
		assert.Equal(t, ioVector.Entries[i].Offset, page.path.Offset)
		assert.Equal(t, ioVector.Entries[i].Size, page.path.Size)
	}
}

func TestWriteTransferPage_FileAlreadyExistsOnFirstAttempt(t *testing.T) {
	// First attempt returns FileAlreadyExists (rare but possible if a
	// previous call's rename succeeded). Should still set paths.
	fs := &failWriteFS{maxFails: 10, fileExistsOnFail: 1}
	pages, ioVector := makeTestPages(2)

	err := WriteTransferPage(context.Background(), fs, pages, ioVector)

	require.NoError(t, err)
	assert.Equal(t, int32(1), fs.failCount.Load())
	for i, page := range pages {
		assert.Equal(t, ioVector.FilePath, page.path.Name)
		assert.Equal(t, ioVector.Entries[i].Offset, page.path.Offset)
		assert.Equal(t, ioVector.Entries[i].Size, page.path.Size)
	}
}

func TestWriteTransferPage_ContextCancellation(t *testing.T) {
	// If context is cancelled mid-retry, WriteTransferPage should return
	// immediately instead of sleeping.
	fs := &failWriteFS{maxFails: 10}
	pages, ioVector := makeTestPages(2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := WriteTransferPage(ctx, fs, pages, ioVector)

	require.Error(t, err)
	// Should exit after first failed attempt + seeing ctx.Done(),
	// NOT exhaust all retries.
	assert.Less(t, fs.failCount.Load(), int32(transferPageWriteMaxRetry))
	for _, page := range pages {
		assert.Empty(t, page.path.Name, "path should not be set on cancellation")
	}
}

func TestTransferPage_TransferWithNoPath(t *testing.T) {
	// When write fails, path.Name is empty. Transfer() should return ok=false
	// without panicking, even after hashmap is cleared.
	sid := objectio.NewSegmentid()
	createdObjs := []*objectio.ObjectId{objectio.NewObjectidWithSegmentIDAndNum(sid, 2)}
	id := &common.ID{BlockID: *objectio.NewBlockid(sid, 0, 0)}
	page := &TransferHashPage{
		id:      id,
		ttl:     ttl,
		diskTTL: diskTTL,
		objects: createdObjs,
	}
	now := time.Now()
	page.bornTS.Store(&now)

	m := make(api.TransferMap)
	m[uint32(0)] = api.TransferDestPos{BlkIdx: 0, RowIdx: 42}
	page.hashmap.Store(&m)

	// Transfer works while hashmap is present
	_, ok := page.Transfer(0)
	assert.True(t, ok)

	// Simulate TTL clearing the hashmap (as happens after ttl elapses)
	page.hashmap.Store(nil)

	// With no path set, loadTable returns nil — Transfer returns ok=false
	_, ok = page.Transfer(0)
	assert.False(t, ok)
}
