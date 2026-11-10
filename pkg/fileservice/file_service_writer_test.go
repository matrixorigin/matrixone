// Copyright 2026 Matrix Origin
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
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

type trackingETLFileService struct {
	*MemoryFS
	closeCount atomic.Int64
}

type failingETLFileService struct {
	*trackingETLFileService
	writeErr error
}

func (f *failingETLFileService) Write(ctx context.Context, vector IOVector) error {
	return f.writeErr
}

type completionFailingETLFileService struct {
	*trackingETLFileService
	writeErr error
}

func (f *completionFailingETLFileService) Write(ctx context.Context, vector IOVector) error {
	for _, entry := range vector.Entries {
		if entry.ReaderForWrite == nil {
			continue
		}
		if _, err := io.Copy(io.Discard, entry.ReaderForWrite); err != nil {
			return err
		}
	}
	return f.writeErr
}

func (f *trackingETLFileService) Close(ctx context.Context) {
	f.closeCount.Add(1)
}

// TestFileServiceWriterCloseAndAbort: Close finalizes the file; Abort discards
// the partial output instead of persisting it, and is idempotent / safe after
// Close as documented.
func TestFileServiceWriterCloseAndAbort(t *testing.T) {
	ctx := WithSQLFileAccess(context.Background(), "sys", "moadmin")
	dir := t.TempDir()

	// Close persists what was written.
	closed := filepath.Join(dir, "closed.csv")
	w, err := NewFileServiceWriter(closed, ctx)
	require.NoError(t, err)
	_, err = w.Write([]byte("a,b\n"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	data, err := os.ReadFile(closed)
	require.NoError(t, err)
	require.Equal(t, "a,b\n", string(data))

	// Abort discards the partial output: the target file never appears.
	aborted := filepath.Join(dir, "aborted.csv")
	w2, err := NewFileServiceWriter(aborted, ctx)
	require.NoError(t, err)
	_, err = w2.Write([]byte("partial"))
	require.NoError(t, err)
	w2.Abort(moerr.NewInternalErrorNoCtx("test abort"))
	_, statErr := os.Stat(aborted)
	require.True(t, os.IsNotExist(statErr), "aborted file must not be persisted")

	// Idempotent, and safe after the fields were nilled.
	w2.Abort(moerr.NewInternalErrorNoCtx("again"))

	// Safe after Close too.
	w3, err := NewFileServiceWriter(filepath.Join(dir, "both.csv"), ctx)
	require.NoError(t, err)
	_, err = w3.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, w3.Close())
	w3.Abort(moerr.NewInternalErrorNoCtx("after close"))
}

func TestFileServiceWriterDoesNotCloseBorrowedService(t *testing.T) {
	ctx := WithSQLFileAccess(context.Background(), "sys", "moadmin")
	memory, err := NewMemoryFS("shared", DisabledCacheConfig, nil)
	require.NoError(t, err)
	service := &trackingETLFileService{MemoryFS: memory}

	w, err := NewFileServiceWriterWithFileService(service, "shared:owned.txt", ctx)
	require.NoError(t, err)
	_, err = w.Write([]byte("owned by the registry"))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.Zero(t, service.closeCount.Load(), "borrowed named services must outlive one writer")

	var reader io.ReadCloser
	vec := IOVector{
		FilePath: "owned.txt",
		Entries:  []IOEntry{{Size: -1, ReadCloserForRead: &reader}},
	}
	require.NoError(t, service.Read(ctx, &vec))
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, []byte("owned by the registry"), data)
}

func TestFileServiceWriterPropagatesWriteFailure(t *testing.T) {
	ctx := WithSQLFileAccess(context.Background(), "sys", "moadmin")
	memory, err := NewMemoryFS("shared", DisabledCacheConfig, nil)
	require.NoError(t, err)
	writeErr := errors.New("write failed")
	service := &failingETLFileService{
		trackingETLFileService: &trackingETLFileService{MemoryFS: memory},
		writeErr:               writeErr,
	}

	w, err := NewFileServiceWriterWithFileService(service, "shared:failed.txt", ctx)
	require.NoError(t, err)
	_, err = w.Write([]byte("not persisted"))
	require.ErrorIs(t, err, writeErr)
	w.Abort(err)
	require.Zero(t, service.closeCount.Load(), "borrowed service must not be closed after a write failure")
	_, err = memory.StatFile(ctx, "failed.txt")
	require.Error(t, err, "failed writes must not leave a persisted file")
}

func TestFileServiceWriterPropagatesCompletionFailure(t *testing.T) {
	ctx := WithSQLFileAccess(context.Background(), "sys", "moadmin")
	memory, err := NewMemoryFS("shared", DisabledCacheConfig, nil)
	require.NoError(t, err)
	completionErr := errors.New("completion failed")
	service := &completionFailingETLFileService{
		trackingETLFileService: &trackingETLFileService{MemoryFS: memory},
		writeErr:               completionErr,
	}

	w, err := NewFileServiceWriterWithFileService(service, "shared:completion.txt", ctx)
	require.NoError(t, err)
	_, err = w.Write([]byte("drained before completion failure"))
	require.NoError(t, err)
	require.ErrorIs(t, w.Close(), completionErr)
	require.Zero(t, service.closeCount.Load(), "borrowed service must remain usable after completion failure")
	_, err = memory.StatFile(ctx, "completion.txt")
	require.Error(t, err, "completion failure must not leave a persisted file")
}
