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

package ioutil

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type workspaceAmbiguousFS struct {
	fileservice.FileService
	writes  atomic.Int32
	deletes atomic.Int32
	failure error
}

func TestWorkspaceBorrowedWriteRespectsObjectAdmission(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		t.Run(map[bool]string{false: "sync", true: "pipeline"}[pipeline], func(t *testing.T) {
			ctx := context.Background()
			base := testutil.NewSharedFS()
			defer base.Close(ctx)
			fs := &workspaceAmbiguousFS{FileService: base}
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			src := batch.NewWithSchema(false, []string{"v"}, []types.Type{types.T_int64.ToType()})
			defer src.Clean(mp)
			require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(7), false, mp))
			src.SetRowCount(1)
			const unit = int64(1 << 20)
			peak, _, err := BatchIOEnvelope(src, unit)
			require.NoError(t, err)
			throttle := rscthrottler.NewMemThrottler("workspace-borrowed", 1, rscthrottler.WithConstLimit(8<<30))
			ws, err := throttle.(rscthrottler.WorkingSetThrottler).ReserveWorkingSetCandidatesUntilFit(ctx, []rscthrottler.WorkingSetSpec{{UnitBytes: unit, IOBytes: 3 * peak}})
			require.NoError(t, err)
			defer ws.Close()
			lease, err := ws.AcquireIO(ctx, peak)
			require.NoError(t, err)
			defer lease.Release()
			refused := errors.New("object admission refused")
			opts := []SinkerOption{WithMemoryWorkspace(ws), WithBufferBytes(unit), WithWorkspaceOwnedCleanup(), WithUncommittedCapacityHook(func(string) error { return refused })}
			if pipeline {
				opts = append(opts, WithPipelineFlush())
			}
			sink := NewSinker(-1, []string{"v"}, []types.Type{types.T_int64.ToType()}, NewFSinkerImplFactory([]uint16{0}, -1, false, false, 0), mp, fs, opts...)
			defer sink.Close()
			err = sink.WriteBorrowedToFile(ctx, src, lease)
			if err == nil {
				err = sink.Sync(ctx)
			}
			require.ErrorIs(t, err, refused)
			require.Zero(t, fs.writes.Load(), "capacity refusal must precede persistence")
			require.Empty(t, sink.AbandonAndRetainableNames())
		})
	}
}

func (fs *workspaceAmbiguousFS) Write(ctx context.Context, v fileservice.IOVector) error {
	fs.writes.Add(1)
	if err := fs.FileService.Write(ctx, v); err != nil {
		return err
	}
	return fs.failure
}
func (fs *workspaceAmbiguousFS) Delete(ctx context.Context, names ...string) error {
	fs.deletes.Add(1)
	return fs.FileService.Delete(ctx, names...)
}
func TestBackportWorkspaceSinkerAdmissionAndAmbiguousSync(t *testing.T) {
	for _, pipeline := range []bool{false, true} {
		for _, deny := range []bool{false, true} {
			name := "sync"
			if pipeline {
				name = "pipeline"
			}
			if deny {
				name += "/refused"
			} else {
				name += "/ambiguous"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				base := testutil.NewSharedFS()
				t.Cleanup(func() { base.Close(ctx) })
				failure := errors.New("ambiguous object persistence")
				refused := errors.New("unpublished admission exhausted")
				fs := &workspaceAmbiguousFS{FileService: base, failure: failure}
				mp := mpool.MustNewZero()
				t.Cleanup(func() { mpool.DeleteMPool(mp) })
				var mu sync.Mutex
				admitted := map[string]bool{}
				hook := func(name string) error {
					if deny {
						return refused
					}
					mu.Lock()
					admitted[name] = true
					mu.Unlock()
					return nil
				}
				opts := []SinkerOption{WithMemorySizeThreshold(1), WithTailSizeCap(0), WithByteBoundedRuns(), WithUnpublishedRunCleanup(), WithWorkspaceOwnedCleanup(), WithUncommittedCapacityHook(hook)}
				if pipeline {
					opts = append(opts, WithPipelineFlush())
				}
				sink := NewSinker(-1, []string{"v"}, []types.Type{types.T_int64.ToType()}, NewFSinkerImplFactory([]uint16{0}, -1, false, false, 0), mp, fs, opts...)
				t.Cleanup(func() { _ = sink.Close() })
				src := batch.NewWithSchema(false, []string{"v"}, []types.Type{types.T_int64.ToType()})
				t.Cleanup(func() { src.Clean(mp) })
				require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(7), false, mp))
				src.SetRowCount(1)
				err := sink.Write(ctx, src)
				if err == nil {
					err = sink.Sync(ctx)
				}
				if deny {
					require.ErrorIs(t, err, refused)
					require.Zero(t, fs.writes.Load())
					require.Empty(t, sink.AbandonAndRetainableNames())
				} else {
					require.ErrorIs(t, err, failure)
					names := sink.AbandonAndRetainableNames()
					require.Len(t, names, 1)
					mu.Lock()
					reserved := admitted[names[0]]
					mu.Unlock()
					require.True(t, reserved)
					_, err = base.StatFile(ctx, names[0])
					require.NoError(t, err)
					_, err = sink.DeletePersisted(ctx)
					require.ErrorContains(t, err, "workspace owns")
					require.Zero(t, fs.deletes.Load())
					sink.Reset()
					require.Empty(t, sink.PersistedNames())
					// The caller now owns the snapshot; closing cannot delete that object.
					require.NoError(t, sink.Close())
					_, err = base.StatFile(ctx, names[0])
					require.NoError(t, err)
				}
				require.Zero(t, fs.deletes.Load())
			})
		}
	}
}

type workspacePausedFile struct {
	name      string
	entered   chan struct{}
	syncCalls atomic.Int32
}

func (s *workspacePausedFile) Sink(context.Context, *batch.Batch) error { return nil }
func (s *workspacePausedFile) Sync(ctx context.Context) (*objectio.ObjectStats, error) {
	s.syncCalls.Add(1)
	close(s.entered)
	<-ctx.Done()
	s.name = ""
	return nil, context.Cause(ctx)
}
func (s *workspacePausedFile) ActiveObjectName() string { return s.name }
func (s *workspacePausedFile) Reset()                   {}
func (s *workspacePausedFile) Close() error             { return nil }
func TestBackportWorkspaceCancelJoinsAndKeepsPreSyncName(t *testing.T) {
	pool := NewSinkPool(1, 1)
	t.Cleanup(pool.Close)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	result := &pipelineResult{ctx: ctx, cancel: cancel}
	file := &workspacePausedFile{name: "possibly-persisted-object", entered: make(chan struct{})}
	sink := &Sinker{}
	WithWorkspaceOwnedCleanup()(sink)
	sink.pipe.enabled = true
	sink.pipe.result = result
	require.NoError(t, pool.Submit(&poolSinkJob{factory: func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, result: result}))
	select {
	case <-file.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Sync did not start")
	}
	sink.CancelPending()
	names := sink.AbandonAndRetainableNames()
	require.Equal(t, []string{"possibly-persisted-object"}, names)
	require.Equal(t, int32(1), file.syncCalls.Load())
	require.ErrorIs(t, result.getError(), context.Canceled)
}
