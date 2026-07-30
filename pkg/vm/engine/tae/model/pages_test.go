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
	"bytes"
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
	fileExistsOnFail int32
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

func makeTestPages(n int) ([]*TransferHashPage, fileservice.IOVector, []*bytes.Buffer) {
	sid := objectio.NewSegmentid()
	createdObjs := []*objectio.ObjectId{objectio.NewObjectidWithSegmentIDAndNum(sid, 2)}

	pages := make([]*TransferHashPage, 0, n)
	ioVector := fileservice.IOVector{
		FilePath: "test-transfer-page",
	}
	var marshalBufs []*bytes.Buffer

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

		m := make(api.TransferMap, 100)
		for i := range m {
			m[i] = api.TransferDestPos{ObjIdx: 0, BlkIdx: 0, RowIdx: uint32(i)}
		}
		page.hashmap.Store(&m)

		_ = AddTransferPage(page, &ioVector, &marshalBufs)
		pages = append(pages, page)
	}
	return pages, ioVector, marshalBufs
}

func TestWriteTransferPage_AllRetriesFail(t *testing.T) {
	fs := &failWriteFS{maxFails: 10}
	pages, ioVector, bufs := makeTestPages(3)

	err := WriteTransferPage(context.Background(), fs, pages, ioVector, bufs)

	require.Error(t, err)
	assert.Equal(t, int32(transferPageWriteMaxRetry), fs.failCount.Load())
	for _, page := range pages {
		assert.Empty(t, page.path.Name, "path should not be set when write fails")
	}
}

func TestWriteTransferPage_SucceedsAfterRetry(t *testing.T) {
	fs := &failWriteFS{maxFails: 2}
	pages, ioVector, bufs := makeTestPages(3)

	err := WriteTransferPage(context.Background(), fs, pages, ioVector, bufs)

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
	pages, ioVector, bufs := makeTestPages(2)

	err := WriteTransferPage(context.Background(), fs, pages, ioVector, bufs)

	require.NoError(t, err)
	assert.Equal(t, int32(1), fs.failCount.Load())
	for i, page := range pages {
		assert.Equal(t, ioVector.FilePath, page.path.Name)
		assert.Equal(t, ioVector.Entries[i].Offset, page.path.Offset)
		assert.Equal(t, ioVector.Entries[i].Size, page.path.Size)
	}
}

func TestWriteTransferPage_FileAlreadyExistsOnRetry(t *testing.T) {
	fs := &failWriteFS{maxFails: 10, fileExistsOnFail: 2}
	pages, ioVector, bufs := makeTestPages(2)

	err := WriteTransferPage(context.Background(), fs, pages, ioVector, bufs)

	require.NoError(t, err)
	assert.Equal(t, int32(2), fs.failCount.Load())
	for i, page := range pages {
		assert.Equal(t, ioVector.FilePath, page.path.Name)
		assert.Equal(t, ioVector.Entries[i].Offset, page.path.Offset)
		assert.Equal(t, ioVector.Entries[i].Size, page.path.Size)
	}
}

func TestWriteTransferPage_FileAlreadyExistsOnFirstAttempt(t *testing.T) {
	fs := &failWriteFS{maxFails: 10, fileExistsOnFail: 1}
	pages, ioVector, bufs := makeTestPages(2)

	err := WriteTransferPage(context.Background(), fs, pages, ioVector, bufs)

	require.NoError(t, err)
	assert.Equal(t, int32(1), fs.failCount.Load())
	for i, page := range pages {
		assert.Equal(t, ioVector.FilePath, page.path.Name)
		assert.Equal(t, ioVector.Entries[i].Offset, page.path.Offset)
		assert.Equal(t, ioVector.Entries[i].Size, page.path.Size)
	}
}

func TestWriteTransferPage_ContextCancellation(t *testing.T) {
	fs := &failWriteFS{maxFails: 10}
	pages, ioVector, bufs := makeTestPages(2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := WriteTransferPage(ctx, fs, pages, ioVector, bufs)

	require.Error(t, err)
	assert.Less(t, fs.failCount.Load(), int32(transferPageWriteMaxRetry))
	for _, page := range pages {
		assert.Empty(t, page.path.Name, "path should not be set on cancellation")
	}
}

func TestTransferPage_TransferWithNoPath(t *testing.T) {
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

	m := make(api.TransferMap, 1)
	m[0] = api.TransferDestPos{ObjIdx: 0, BlkIdx: 0, RowIdx: 42}
	page.hashmap.Store(&m)

	_, ok := page.Transfer(0)
	assert.True(t, ok)

	page.hashmap.Store(nil)

	_, ok = page.Transfer(0)
	assert.False(t, ok)
}
