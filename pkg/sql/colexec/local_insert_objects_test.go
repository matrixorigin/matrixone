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

package colexec

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

type failOnceObjectDelete struct {
	fileservice.FileService
	fail bool
}

type blockedObjectDelete struct {
	fileservice.FileService
	blocked string
	cancel  context.CancelFunc
	deleted []string
}

func (f *blockedObjectDelete) Delete(ctx context.Context, paths ...string) error {
	if paths[0] == f.blocked {
		f.cancel()
		return ctx.Err()
	}
	f.deleted = append(f.deleted, paths...)
	return nil
}

func (f *failOnceObjectDelete) Delete(ctx context.Context, paths ...string) error {
	if f.fail {
		f.fail = false
		return errors.New("injected delete failure")
	}
	return f.FileService.Delete(ctx, paths...)
}

func TestLocalInsertObjectsHandoffAndRetry(t *testing.T) {
	ctx := t.Context()
	disk, err := fileservice.NewLocalFS(ctx, "shared", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer disk.Close(context.Background())
	fs := &failOnceObjectDelete{FileService: disk, fail: true}
	workerCtx, stopWorker := context.WithCancel(ctx)
	stopWorker()
	registry := NewLocalInsertObjectRegistry(workerCtx, fs, 2)
	stream := registry.NewStream()
	require.NoError(t, stream.Begin(ctx))

	save := func() objectio.ObjectName {
		name := objectio.MockObjectName()
		require.NoError(t, stream.Reserve(ctx, *name.ObjectId()))
		require.NoError(t, fs.Write(ctx, fileservice.IOVector{
			FilePath: name.String(),
			Entries:  []fileservice.IOEntry{{Size: 1, Data: []byte{1}}},
		}))
		return name
	}
	adopted, pending := save(), save()
	require.True(t, moerr.IsMoErrCode(stream.Reserve(ctx, *objectio.MockObjectName().ObjectId()), moerr.ErrOOM))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, stream.Reserve(canceled, *objectio.MockObjectName().ObjectId()), context.Canceled)

	stream.Adopt(*adopted.ObjectId())
	require.Equal(t, 1, stream.Finish())
	<-registry.done
	require.Equal(t, 1, registry.collect(ctx))
	_, err = fs.StatFile(ctx, pending.String())
	require.NoError(t, err)
	_, err = fs.StatFile(ctx, adopted.String())
	require.NoError(t, err)
	require.NoError(t, stream.Begin(ctx))
	next := save()
	require.True(t, moerr.IsMoErrCode(stream.Reserve(ctx, *objectio.MockObjectName().ObjectId()), moerr.ErrOOM))
	require.Zero(t, registry.collect(ctx))
	_, err = fs.StatFile(ctx, pending.String())
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), err)
	_, err = fs.StatFile(ctx, next.String())
	require.NoError(t, err)
	stream.Adopt(*next.ObjectId())
	require.Zero(t, stream.Finish())
	require.Zero(t, registry.used)
}

func TestLocalInsertObjectsTimeoutDoesNotStarveOtherDeletes(t *testing.T) {
	ctx := t.Context()
	workerCtx, stopWorker := context.WithCancel(ctx)
	stopWorker()
	blocked, healthy := objectio.MockObjectName(), objectio.MockObjectName()
	attempt, cancel := context.WithCancel(ctx)
	defer cancel()
	fs := &blockedObjectDelete{blocked: blocked.String(), cancel: cancel}
	registry := NewLocalInsertObjectRegistry(workerCtx, fs, 2)
	stream := registry.NewStream()
	for _, name := range []objectio.ObjectName{blocked, healthy} {
		require.NoError(t, stream.Begin(ctx))
		require.NoError(t, stream.Reserve(ctx, *name.ObjectId()))
		require.Equal(t, 1, stream.Finish())
	}
	<-registry.done
	require.Equal(t, 2, registry.collect(attempt))
	nextAttempt, nextCancel := context.WithCancel(ctx)
	defer nextCancel()
	fs.cancel = nextCancel
	require.Equal(t, 1, registry.collect(nextAttempt))
	require.Equal(t, []string{healthy.String()}, fs.deleted)
}

func TestLocalInsertObjectsCloseDrainsFinishedWriters(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	fs, err := fileservice.NewLocalFS(ctx, "shared", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	registry := NewLocalInsertObjectRegistry(ctx, fs, 1)
	stream := registry.NewStream()
	require.NoError(t, stream.Begin(ctx))
	name := objectio.MockObjectName()
	require.NoError(t, stream.Reserve(ctx, *name.ObjectId()))
	require.NoError(t, fs.Write(ctx, fileservice.IOVector{
		FilePath: name.String(),
		Entries:  []fileservice.IOEntry{{Size: 1, Data: []byte{1}}},
	}))

	done := make(chan error, 1)
	go func() { done <- registry.Close(ctx) }()
	require.Eventually(t, func() bool {
		registry.mu.Lock()
		defer registry.mu.Unlock()
		return registry.closed
	}, time.Second, time.Millisecond)
	require.Error(t, registry.NewStream().Begin(ctx))
	require.Equal(t, 1, stream.Finish())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("shutdown did not drain the finished writer")
	}
	_, err = fs.StatFile(ctx, name.String())
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound), err)
}
