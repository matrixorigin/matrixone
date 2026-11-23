// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type boundedFailureFS struct {
	fileservice.FileService
	writes    int
	failAt    int
	writeErr  error
	deleteErr error
	deletes   atomic.Int32
}

func (fs *boundedFailureFS) Write(ctx context.Context, v fileservice.IOVector) error {
	fs.writes++
	if err := fs.FileService.Write(ctx, v); err != nil {
		return err
	}
	if fs.writes == fs.failAt {
		return fs.writeErr
	}
	return nil
}

func (fs *boundedFailureFS) Delete(ctx context.Context, paths ...string) error {
	fs.deletes.Add(1)
	if fs.deleteErr != nil {
		return fs.deleteErr
	}
	return fs.FileService.Delete(ctx, paths...)
}

type cancelAfterFirstDeleteFS struct {
	fileservice.FileService
	cancel context.CancelFunc
	calls  []int
}

func (fs *cancelAfterFirstDeleteFS) Delete(_ context.Context, paths ...string) error {
	fs.calls = append(fs.calls, len(paths))
	if fs.cancel != nil {
		fs.cancel()
		fs.cancel = nil
	}
	return nil
}

func TestBoundedRunCleanupStopsBetweenDeleteBatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fs := &cancelAfterFirstDeleteFS{cancel: cancel}
	sink := NewSinker(-1, nil, nil, nil, mpool.MustNewZero(), fs,
		WithMemorySizeThreshold(1024), WithBoundedRuns())
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	for i := 0; i <= unpublishedObjectDeleteBatchSize; i++ {
		sink.staged.unpublished = append(sink.staged.unpublished, fmt.Sprintf("object-%d", i))
	}
	want := sink.PendingUnpublishedRuns()
	names, err := sink.RetryUnpublishedRuns(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []int{unpublishedObjectDeleteBatchSize}, fs.calls)
	require.Equal(t, want, names)
	require.Equal(t, want, sink.PendingUnpublishedRuns(), "unconfirmed names stay retryable")

	names, err = sink.RetryUnpublishedRuns(context.Background())
	require.NoError(t, err)
	require.Equal(t, want, names)
	require.Equal(t, []int{unpublishedObjectDeleteBatchSize, unpublishedObjectDeleteBatchSize, 1}, fs.calls)
	require.Empty(t, sink.PendingUnpublishedRuns())
}

type stopAtMissingFS struct {
	fileservice.FileService
	calls         [][]string
	cancel        context.CancelCauseFunc
	cancelCause   error
	cancelOnCall  int
	failPath      string
	injectedError error
	ignoreCancel  bool
	canceledCalls int
}

func (fs *stopAtMissingFS) Delete(ctx context.Context, paths ...string) error {
	fs.calls = append(fs.calls, append([]string(nil), paths...))
	if ctx.Err() != nil {
		fs.canceledCalls++
	}
	if fs.ignoreCancel {
		ctx = context.WithoutCancel(ctx)
	}
	for _, path := range paths {
		if path == fs.failPath {
			return fs.injectedError
		}
		if _, err := fs.FileService.StatFile(ctx, path); err != nil {
			if fs.cancel != nil && len(fs.calls) == fs.cancelOnCall {
				fs.cancel(fs.cancelCause)
				fs.cancel = nil
			}
			return err
		}
		if err := fs.FileService.Delete(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

func TestDeleteUnpublishedObjectsConfirmsMissingBatch(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled=%t", canceled), func(t *testing.T) {
			base := testutil.NewSharedFS()
			t.Cleanup(func() { base.Close(context.Background()) })
			require.NoError(t, base.Write(context.Background(), fileservice.IOVector{FilePath: "present", Entries: []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}}}))
			fs := &stopAtMissingFS{FileService: base, ignoreCancel: true}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if canceled {
				cancel()
			}
			count, err := DeleteUnpublishedObjects(ctx, fs, "missing", "present")
			require.NoError(t, err)
			require.Equal(t, 2, count)
			require.Equal(t, [][]string{{"missing", "present"}, {"missing"}, {"present"}}, fs.calls)
			if canceled {
				require.Equal(t, len(fs.calls), fs.canceledCalls)
			}
			_, err = base.StatFile(context.Background(), "present")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
		})
	}
}

func TestBoundedRunCleanupConfirmsEveryNameAfterMissing(t *testing.T) {
	for _, mode := range []string{"success", "failure", "cancel_batch", "cancel_fallback"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			base := testutil.NewSharedFS()
			t.Cleanup(func() { base.Close(ctx) })
			require.NoError(t, base.Write(ctx, fileservice.IOVector{FilePath: "z-present", Entries: []fileservice.IOEntry{{Offset: 0, Size: 1, Data: []byte("x")}}}))
			fs := &stopAtMissingFS{FileService: base}
			sink := NewSinker(-1, nil, nil, nil, mpool.MustNewZero(), fs,
				WithMemorySizeThreshold(1024), WithBoundedRuns())
			t.Cleanup(func() { require.NoError(t, sink.Close()) })
			sink.staged.unpublished = []string{"a-missing", "z-present"}
			want := sink.PendingUnpublishedRuns()
			var operationCtx context.Context = ctx
			if mode == "failure" {
				fs.failPath = "z-present"
				fs.injectedError = errors.New("delete unavailable")
			} else if mode == "cancel_batch" || mode == "cancel_fallback" {
				var cancel context.CancelCauseFunc
				operationCtx, cancel = context.WithCancelCause(ctx)
				defer cancel(nil)
				fs.cancel = cancel
				fs.cancelCause = moerr.NewFileNotFoundNoCtx("cancel cause, not a deletion result")
				fs.cancelOnCall = 1
				if mode == "cancel_fallback" {
					fs.cancelOnCall = 2
				}
			}
			names, err := sink.RetryUnpublishedRuns(operationCtx)
			require.Equal(t, want, names)
			if mode == "success" {
				require.NoError(t, err)
				require.Len(t, fs.calls, 3, "the missing batch must be checked one name at a time")
			} else {
				if mode == "failure" {
					require.ErrorIs(t, err, fs.injectedError)
				} else {
					require.Same(t, fs.cancelCause, err)
					require.Len(t, fs.calls, fs.cancelOnCall, "cancellation must stop the fallback")
				}
				require.Equal(t, want, sink.PendingUnpublishedRuns())
				_, statErr := base.StatFile(ctx, "z-present")
				require.NoError(t, statErr)
				fs.failPath = ""
				names, err = sink.RetryUnpublishedRuns(ctx)
				require.NoError(t, err)
				require.Equal(t, want, names)
			}
			require.Empty(t, sink.PendingUnpublishedRuns())
			_, err = base.StatFile(ctx, "z-present")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
		})
	}
}

func TestBoundedRunCleanupDistinguishesMissingCancelCause(t *testing.T) {
	base := testutil.NewSharedFS()
	t.Cleanup(func() { base.Close(context.Background()) })
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := moerr.NewFileNotFoundNoCtx("cancel cause")
	fs := &stopAtMissingFS{FileService: base, cancel: cancel, cancelCause: cause, cancelOnCall: 1}
	sink := NewSinker(-1, nil, nil, nil, mpool.MustNewZero(), fs,
		WithMemorySizeThreshold(1024), WithBoundedRuns())
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	sink.staged.unpublished = []string{"missing"}
	names, err := sink.RetryUnpublishedRuns(ctx)
	require.Same(t, cause, err)
	require.Equal(t, []string{"missing"}, names)
	require.Equal(t, names, sink.PendingUnpublishedRuns())
	_, err = sink.RetryUnpublishedRuns(context.Background())
	require.NoError(t, err)
	require.Empty(t, sink.PendingUnpublishedRuns())
}

func TestBoundedWorkspaceFailureRetainsNamesWithoutDeleting(t *testing.T) {
	ctx := context.Background()
	base := testutil.NewSharedFS()
	t.Cleanup(func() { base.Close(ctx) })
	wantErr := moerr.NewInternalErrorNoCtx("commit-ambiguous write")
	fs := &boundedFailureFS{FileService: base, failAt: 2, writeErr: wantErr}
	mp := mpool.MustNewZero()
	sink := NewSinker(-1, []string{"id", "value"}, []types.Type{types.T_int32.ToType(), types.T_text.ToType()},
		NewFSinkerImplFactory([]uint16{0, 1}, -1, false, false, 0), mp, fs,
		WithMemorySizeThreshold(1024), WithTailSizeCap(0), WithBoundedRuns(), WithWorkspaceOwnedCleanup())
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	input := boundedInput(t, mp, strings.Repeat("a", 800), strings.Repeat("b", 800), strings.Repeat("c", 800))
	defer input.Clean(mp)
	require.Same(t, wantErr, sink.Write(ctx, input))
	names := sink.PersistedNames()
	require.Len(t, names, 2)
	require.Zero(t, fs.deletes.Load(), "the workspace owns cleanup")
	for _, name := range names {
		_, err := base.StatFile(ctx, name)
		require.NoError(t, err)
	}
	_, err := sink.RetryUnpublishedRuns(ctx)
	require.ErrorContains(t, err, "workspace owns unpublished spill cleanup")
	_, err = sink.DeletePersisted(ctx)
	require.ErrorContains(t, err, "workspace owns unpublished spill cleanup")
	require.Zero(t, fs.deletes.Load())
	require.ErrorContains(t, sink.Write(ctx, input), "aborted")
	require.ElementsMatch(t, names, sink.AbandonAndRetainableNames())
	// The workspace has taken the snapshot, so Reset may discard this generation.
	sink.Reset()
	require.Empty(t, sink.PersistedNames())
	require.NoError(t, sink.Write(ctx, input))
}

func boundedInput(t *testing.T, mp *mpool.MPool, values ...string) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSchema(false, []string{"id", "value"}, []types.Type{types.T_int32.ToType(), types.T_text.ToType()})
	for i, value := range values {
		require.NoError(t, vector.AppendFixed(bat.Vecs[0], int32(i), false, mp))
		require.NoError(t, vector.AppendBytes(bat.Vecs[1], []byte(value), false, mp))
	}
	bat.SetRowCount(len(values))
	return bat
}

func TestBoundedRunCleanupRetryKeepsHandedOffObject(t *testing.T) {
	ctx := context.Background()
	base := testutil.NewSharedFS()
	t.Cleanup(func() { base.Close(ctx) })
	operationErr := moerr.NewInternalErrorNoCtx("injected commit-ambiguous write")
	fs := &boundedFailureFS{FileService: base, failAt: 3, writeErr: operationErr, deleteErr: errors.New("delete unavailable")}
	mp := mpool.MustNewZero()
	attrs := []string{"id", "value"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	sink := NewSinker(-1, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, -1, false, false, 0), mp, fs, WithMemorySizeThreshold(1024), WithTailSizeCap(0), WithBoundedRuns())
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	first := boundedInput(t, mp, strings.Repeat("p", 800))
	defer first.Clean(mp)
	require.NoError(t, sink.Write(ctx, first))
	require.NoError(t, sink.Sync(ctx))
	published, _ := sink.GetResult()
	require.Len(t, published, 1)
	publishedName := published[0].ObjectName().String()

	next := boundedInput(t, mp, strings.Repeat("a", 800), strings.Repeat("b", 800), strings.Repeat("c", 800))
	defer next.Clean(mp)
	err := sink.Write(ctx, next)
	require.Same(t, operationErr, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	names := sink.PendingUnpublishedRuns()
	require.Len(t, names, 2)
	require.NotContains(t, names, publishedName)
	require.Equal(t, []string{publishedName}, sink.AbandonAndRetainableNames(),
		"pending names must remain solely with local cleanup")
	for _, name := range names {
		_, err := base.StatFile(ctx, name)
		require.NoError(t, err)
	}
	sink.Reset()
	require.ElementsMatch(t, names, sink.PendingUnpublishedRuns())
	require.ErrorContains(t, sink.Write(ctx, first), "aborted")
	_, err = sink.RetryUnpublishedRuns(ctx)
	require.ErrorContains(t, err, "delete unavailable")
	require.ElementsMatch(t, names, sink.PendingUnpublishedRuns())
	fs.deleteErr = nil
	deleted, err := sink.RetryUnpublishedRuns(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, names, deleted)
	require.Empty(t, sink.PendingUnpublishedRuns())
	for _, name := range names {
		_, err := base.StatFile(ctx, name)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	}
	_, err = base.StatFile(ctx, publishedName)
	require.NoError(t, err, "a previously handed-off object must survive")
	sink.Reset()
	require.NoError(t, sink.Write(ctx, first))
	require.NoError(t, sink.Sync(ctx))
}

type boundedCancelFS struct {
	fileservice.FileService
	entered chan struct{}
	once    sync.Once
	deletes atomic.Int32
}

func (fs *boundedCancelFS) Write(ctx context.Context, _ fileservice.IOVector) error {
	fs.once.Do(func() { close(fs.entered) })
	<-ctx.Done()
	return context.Cause(ctx)
}
func (fs *boundedCancelFS) Delete(ctx context.Context, paths ...string) error {
	fs.deletes.Add(1)
	if err := ctx.Err(); err != nil {
		return err
	}
	return fs.FileService.Delete(ctx, paths...)
}

func TestBoundedWorkspaceAbandonCancelsBeforeSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := testutil.NewSharedFS()
	t.Cleanup(func() { base.Close(context.Background()) })
	fs := &boundedCancelFS{FileService: base, entered: make(chan struct{})}
	mp := mpool.MustNewZero()
	sink := NewSinker(-1, []string{"id", "value"}, []types.Type{types.T_int32.ToType(), types.T_text.ToType()},
		NewFSinkerImplFactory([]uint16{0, 1}, -1, false, false, 0), mp, fs,
		WithMemorySizeThreshold(1), WithTailSizeCap(0), WithPipelineFlush(), WithBoundedRuns(), WithWorkspaceOwnedCleanup())
	t.Cleanup(func() { _ = sink.Close() })
	input := boundedInput(t, mp, "value")
	defer input.Clean(mp)
	require.NoError(t, sink.Write(ctx, input))
	select {
	case <-fs.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not enter Write")
	}
	done := make(chan []string, 1)
	go func() { done <- sink.AbandonAndRetainableNames() }()
	select {
	case names := <-done:
		require.Len(t, names, 1, "a commit-ambiguous name must reach the owner")
	case <-time.After(10 * time.Second):
		cancel()
		<-done
		t.Fatal("abandon did not cancel the worker before waiting")
	}
	require.Zero(t, fs.deletes.Load())
	require.ErrorContains(t, sink.Write(context.Background(), input), "aborted")
}

func TestBoundedPipelineSyncCancellationDrainsAndRetries(t *testing.T) {
	ctx := context.Background()
	base := testutil.NewSharedFS()
	t.Cleanup(func() { base.Close(ctx) })
	fs := &boundedCancelFS{FileService: base, entered: make(chan struct{})}
	mp := mpool.MustNewZero()
	attrs := []string{"id", "value"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	sink := NewSinker(-1, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, -1, false, false, 0), mp, fs, WithMemorySizeThreshold(1), WithTailSizeCap(0), WithPipelineFlush(), WithBoundedRuns())
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	input := boundedInput(t, mp, "value")
	defer input.Clean(mp)
	require.NoError(t, sink.Write(ctx, input))
	select {
	case <-fs.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not enter Write")
	}
	syncCtx, cancel := context.WithCancelCause(ctx)
	cause := moerr.NewInternalErrorNoCtx("cancelled later Sync")
	done := make(chan error, 1)
	go func() { done <- sink.Sync(syncCtx) }()
	cancel(cause)
	select {
	case err := <-done:
		require.Same(t, cause, err)
	case <-time.After(10 * time.Second):
		t.Fatal("Sync failed to cancel and drain its worker")
	}
	names := sink.PendingUnpublishedRuns()
	require.Len(t, names, 1)
	sink.Reset()
	require.ElementsMatch(t, names, sink.PendingUnpublishedRuns())
	_, err := sink.RetryUnpublishedRuns(ctx)
	require.NoError(t, err)
	sink.Reset()
	require.Empty(t, sink.PendingUnpublishedRuns())
}

// One storage write deliberately completes after a sibling has failed and
// cancelled the generation, modeling a commit that was already in flight.
type boundedSiblingFS struct {
	fileservice.FileService
	count           atomic.Int32
	firstStarted    chan struct{}
	secondPersisted chan struct{}
	releaseFirst    chan struct{}
	failure         error
	mu              sync.Mutex
	paths           []string
	deleted         []string
}

func (fs *boundedSiblingFS) Write(ctx context.Context, v fileservice.IOVector) error {
	n := fs.count.Add(1)
	if n == 1 {
		close(fs.firstStarted)
		<-fs.releaseFirst
		ctx = context.WithoutCancel(ctx)
	}
	if err := fs.FileService.Write(ctx, v); err != nil {
		return err
	}
	fs.mu.Lock()
	fs.paths = append(fs.paths, v.FilePath)
	fs.mu.Unlock()
	if n == 2 {
		close(fs.secondPersisted)
		return fs.failure
	}
	return nil
}

func (fs *boundedSiblingFS) Delete(ctx context.Context, paths ...string) error {
	fs.mu.Lock()
	fs.deleted = append(fs.deleted, paths...)
	fs.mu.Unlock()
	return fs.FileService.Delete(ctx, paths...)
}

func TestBoundedPipelineDrainsSuccessfulSiblingAfterFailure(t *testing.T) {
	for _, workspaceOwned := range []bool{false, true} {
		t.Run(fmt.Sprintf("workspaceOwned=%t", workspaceOwned), func(t *testing.T) {
			ctx := context.Background()
			base := testutil.NewSharedFS()
			t.Cleanup(func() { base.Close(ctx) })
			fs := &boundedSiblingFS{
				FileService:     base,
				firstStarted:    make(chan struct{}),
				secondPersisted: make(chan struct{}),
				releaseFirst:    make(chan struct{}),
				failure:         moerr.NewInternalErrorNoCtx("second write failed after commit"),
			}
			var release sync.Once
			defer release.Do(func() { close(fs.releaseFirst) })
			mp := mpool.MustNewZero()
			attrs := []string{"id", "value"}
			typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
			opts := []SinkerOption{WithMemorySizeThreshold(1), WithTailSizeCap(0), WithPipelineFlush(), WithBoundedRuns()}
			if workspaceOwned {
				opts = append(opts, WithWorkspaceOwnedCleanup())
			}
			sink := NewSinker(-1, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, -1, false, false, 0), mp, fs, opts...)
			t.Cleanup(func() { _ = sink.Close() })
			input := boundedInput(t, mp, "value")
			defer input.Clean(mp)
			require.NoError(t, sink.Write(ctx, input))
			select {
			case <-fs.firstStarted:
			case <-time.After(10 * time.Second):
				t.Fatal("first write did not start")
			}
			writeDone := make(chan error, 1)
			go func() { writeDone <- sink.Write(ctx, input) }()
			select {
			case <-fs.secondPersisted:
			case <-time.After(10 * time.Second):
				t.Fatal("second write did not persist")
			}
			release.Do(func() { close(fs.releaseFirst) })
			var err error
			select {
			case err = <-writeDone:
			case <-time.After(10 * time.Second):
				t.Fatal("sibling cleanup did not finish")
			}
			if err == nil {
				err = sink.Sync(ctx)
			}
			require.Same(t, fs.failure, err)
			fs.mu.Lock()
			paths := append([]string(nil), fs.paths...)
			deleted := append([]string(nil), fs.deleted...)
			fs.mu.Unlock()
			require.Len(t, paths, 2)
			if workspaceOwned {
				require.Empty(t, deleted)
				require.ElementsMatch(t, paths, sink.PersistedNames())
				for _, path := range paths {
					_, err := base.StatFile(ctx, path)
					require.NoError(t, err)
				}
				require.ElementsMatch(t, paths, sink.AbandonAndRetainableNames())
				sink.Reset()
				require.Empty(t, sink.PersistedNames())
			} else {
				require.ElementsMatch(t, paths, deleted)
				for _, path := range paths {
					_, err := base.StatFile(ctx, path)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
				}
				require.Empty(t, sink.PendingUnpublishedRuns())
				sink.Reset()
			}
		})
	}
}

type metadataCaptureSink struct {
	visit func(*batch.Batch)
}

func (s *metadataCaptureSink) Sink(_ context.Context, bat *batch.Batch) error {
	s.visit(bat)
	return nil
}
func (*metadataCaptureSink) Sync(context.Context) (*objectio.ObjectStats, error) {
	return &objectio.ObjectStats{}, nil
}
func (*metadataCaptureSink) Reset()       {}
func (*metadataCaptureSink) Close() error { return nil }

func TestBoundedRunsPreserveTransientMetadataAtSink(t *testing.T) {
	for _, sortKey := range []int{-1, 0} {
		t.Run(fmt.Sprintf("sort=%d", sortKey), func(t *testing.T) {
			mp := mpool.MustNewZero()
			attrs := []string{"id", "value"}
			typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
			input := batch.NewWithSchema(false, attrs, typs)
			defer input.Clean(mp)
			for i := 0; i < 4; i++ {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(3-i), false, mp))
				require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte(strings.Repeat("x", 200)), false, mp))
			}
			input.SetRowCount(4)
			input.Vecs[1].GetGrouping().Add(1, 3)
			require.NoError(t, input.Vecs[1].SetPrepareParamKindsWithMP([]vector.PrepareParamKind{
				vector.PrepareParamFloat, vector.PrepareParamNone, vector.PrepareParamInteger, vector.PrepareParamFloat,
			}, mp))
			require.NoError(t, input.Vecs[1].SetStringSourcesWithMP([]types.StringSource{
				types.StringSourceCOMStmt, types.StringSourceSQLPrepare, types.StringSourceExpression, types.StringSourceCOMStmt,
			}, mp))
			require.NoError(t, input.Vecs[1].SetRuntimeStringDomainsWithMP([]types.RuntimeStringDomain{
				types.RuntimeStringBinary, types.RuntimeStringText, types.RuntimeStringInherit, types.RuntimeStringBinary,
			}, mp))
			seen := make(map[int32]bool)
			file := &metadataCaptureSink{visit: func(got *batch.Batch) {
				ids := vector.MustFixedColNoTypeCheck[int32](got.Vecs[0])
				for row, id := range ids {
					original := 3 - int(id)
					require.False(t, seen[id])
					seen[id] = true
					require.Equal(t, input.Vecs[1].GetGrouping().Contains(uint64(original)), got.Vecs[1].GetGrouping().Contains(uint64(row)))
					require.Equal(t, input.Vecs[1].GetPrepareParamKindAt(original), got.Vecs[1].GetPrepareParamKindAt(row))
					require.Equal(t, input.Vecs[1].GetStringSourceAt(original), got.Vecs[1].GetStringSourceAt(row))
					require.Equal(t, input.Vecs[1].GetRuntimeStringDomainAt(original), got.Vecs[1].GetRuntimeStringDomainAt(row))
				}
			}}
			sink := NewSinker(sortKey, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
				WithMemorySizeThreshold(500), WithTailSizeCap(0), WithBoundedRuns())
			defer sink.Close()
			require.NoError(t, sink.Write(context.Background(), input))
			require.NoError(t, sink.Sync(context.Background()))
			require.Len(t, seen, 4)
		})
	}
}

type countFileSinker struct {
	objects int
	rows    int
}

func (s *countFileSinker) Sink(_ context.Context, bat *batch.Batch) error {
	s.rows += bat.RowCount()
	return nil
}
func (s *countFileSinker) Sync(context.Context) (*objectio.ObjectStats, error) {
	s.objects++
	return &objectio.ObjectStats{}, nil
}
func (*countFileSinker) Reset()       {}
func (*countFileSinker) Close() error { return nil }

func TestBoundedRunDoesNotRepeatedlyChargeDeadAliasedArea(t *testing.T) {
	for _, nullsOnly := range []bool{true, false} {
		t.Run(fmt.Sprintf("null=%v", nullsOnly), func(t *testing.T) {
			mp := mpool.MustNewZero()
			typ := types.T_text.ToType()
			input := batch.NewWithSchema(false, []string{"value"}, []types.Type{typ})
			defer input.Clean(mp)
			const rows = objectio.BlockMaxRows
			alias, err := vector.NewConstBytes(typ, []byte(strings.Repeat("x", 16360)), rows, mp)
			require.NoError(t, err)
			require.NoError(t, input.Vecs[0].UnionBatch(alias, 0, rows, nil, mp))
			alias.Free(mp)
			input.SetRowCount(rows)
			require.False(t, input.Vecs[0].VarlenaAreaIsDisjoint())
			if nullsOnly {
				input.Vecs[0].GetNulls().AddRange(0, rows)
			} else {
				for i := 0; i < rows; i++ {
					require.NoError(t, vector.SetBytesAt(input.Vecs[0], i, []byte("inline"), mp))
				}
			}
			file := &countFileSinker{}
			sink := NewSinker(-1, input.Attrs, []types.Type{typ}, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
				WithMemorySizeThreshold(128<<20), WithTailSizeCap(0), WithBoundedRuns())
			defer sink.Close()
			for range 8 {
				require.NoError(t, sink.Write(context.Background(), input))
			}
			require.Zero(t, file.objects, "dead source area must not be multiplied into each staged run")
			require.NoError(t, sink.Sync(context.Background()))
			require.Equal(t, 1, file.objects)
			require.Equal(t, rows*8, file.rows)
		})
	}
}

func TestBoundedPipelineTerminalSelection(t *testing.T) {
	t.Run("worker error is retained", func(t *testing.T) {
		workerCtx, workerCancel := context.WithCancel(context.Background())
		defer workerCancel()
		r := &pipelineResult{ctx: workerCtx, cancel: workerCancel}
		sink := &Sinker{}
		sink.pipe.result = r
		workerErr := errors.New("worker error")
		r.setError(workerErr)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Same(t, workerErr, sink.collectPipeline(ctx))
	})
	t.Run("cancellation wins before handoff", func(t *testing.T) {
		workerCtx, workerCancel := context.WithCancel(context.Background())
		defer workerCancel()
		r := &pipelineResult{ctx: workerCtx, cancel: workerCancel}
		r.pending.Add(1)
		sink := &Sinker{}
		sink.pipe.result = r
		ctx, cancel := context.WithCancelCause(context.Background())
		cause := errors.New("cancel first")
		done := make(chan error, 1)
		go func() { done <- sink.collectPipeline(ctx) }()
		cancel(cause)
		select {
		case <-workerCtx.Done():
		case <-time.After(10 * time.Second):
			t.Fatal("cancellation did not reach workers")
		}
		r.setError(errors.New("later worker error"))
		r.pending.Done()
		select {
		case err := <-done:
			require.Same(t, cause, err)
		case <-time.After(10 * time.Second):
			t.Fatal("pipeline did not drain")
		}
	})
	t.Run("later cancellation cannot retract handoff", func(t *testing.T) {
		workerCtx, workerCancel := context.WithCancel(context.Background())
		defer workerCancel()
		r := &pipelineResult{ctx: workerCtx, cancel: workerCancel, persisted: []objectio.ObjectStats{{}}}
		sink := &Sinker{}
		sink.pipe.result = r
		ctx, cancel := context.WithCancel(context.Background())
		require.NoError(t, sink.collectPipeline(ctx))
		cancel()
		require.Len(t, sink.staged.persisted, 1)
		require.NoError(t, r.getError())
	})
}

func TestBoundedRunsExactLimitAndOversizedRows(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limit   int
		values  []string
		objects int
	}{
		{"exact", 256, []string{strings.Repeat("a", 100), strings.Repeat("b", 100)}, 1},
		{"exceeds_by_one", 256, []string{strings.Repeat("a", 100), strings.Repeat("b", 101)}, 2},
		{"oversized_between_small", 256, []string{"small", strings.Repeat("a", 2048), "small"}, 3},
		{"each_row_oversized", 1, []string{"small", "small"}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			attrs := []string{"id", "value"}
			typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
			file := &countFileSinker{}
			sink := NewSinker(-1, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
				WithMemorySizeThreshold(tc.limit), WithTailSizeCap(0), WithBoundedRuns())
			defer sink.Close()
			for _, value := range tc.values {
				input := boundedInput(t, mp, value)
				err := sink.Write(context.Background(), input)
				input.Clean(mp)
				require.NoError(t, err)
			}
			require.NoError(t, sink.Sync(context.Background()))
			require.Equal(t, tc.objects, file.objects)
			require.Equal(t, len(tc.values), file.rows)
		})
	}
}

func TestBoundedWriteOwnedEmptyConstant(t *testing.T) {
	mp := mpool.MustNewZero()
	typ := types.T_text.ToType()
	input := batch.NewWithSize(1)
	input.Attrs = []string{"v"}
	vec, err := vector.NewConstBytes(typ, []byte(strings.Repeat("x", 100)), 1, mp)
	require.NoError(t, err)
	vec.SetLength(0) // retained scalar storage with no logical rows
	input.Vecs[0] = vec
	defer input.Clean(mp)
	sink := NewSinker(-1, input.Attrs, []types.Type{typ}, nil, mp, nil,
		WithMemorySizeThreshold(1024), WithBoundedRuns())
	defer sink.Close()
	owned, err := sink.WriteOwned(context.Background(), input)
	require.NoError(t, err)
	require.False(t, owned)
	require.Empty(t, sink.GetInMemoryData())
}

func TestBoundedRunReleasesOversizedBuffers(t *testing.T) {
	mp := mpool.MustNewZero()
	input := boundedInput(t, mp, strings.Repeat("x", 2048))
	defer input.Clean(mp)
	before := mp.CurrNB()
	file := &countFileSinker{}
	sink := NewSinker(-1, []string{"id", "value"}, []types.Type{types.T_int32.ToType(), types.T_text.ToType()},
		func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
		WithMemorySizeThreshold(256), WithTailSizeCap(0), WithOffHeap(), WithBoundedRuns())
	defer sink.Close()
	require.NoError(t, sink.Write(context.Background(), input))
	require.Equal(t, 1, file.objects)
	require.Equal(t, before, mp.CurrNB(), "the oversized staging batch must not remain in the freelist")
	require.NoError(t, sink.Sync(context.Background()))
}

type gatedWorkerErrorFS struct {
	fileservice.FileService
	entered chan struct{}
	release chan struct{}
	err     error
}

func (f *gatedWorkerErrorFS) Write(ctx context.Context, _ fileservice.IOVector) error {
	close(f.entered)
	select {
	case <-f.release:
		return f.err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func TestBoundedPipelinePreservesWorkerErrorAtPublicEntry(t *testing.T) {
	for _, method := range []string{"Write", "WriteOwned", "Sync"} {
		t.Run(method, func(t *testing.T) {
			ctx := context.Background()
			base := testutil.NewSharedFS()
			t.Cleanup(func() { base.Close(ctx) })
			workerErr := moerr.NewInternalErrorNoCtx("earlier worker error")
			fs := &gatedWorkerErrorFS{FileService: base, entered: make(chan struct{}), release: make(chan struct{}), err: workerErr}
			var release sync.Once
			defer release.Do(func() { close(fs.release) })
			mp := mpool.MustNewZero()
			attrs := []string{"id", "value"}
			typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
			sink := NewSinker(-1, attrs, typs, NewFSinkerImplFactory([]uint16{0, 1}, -1, false, false, 0), mp, fs,
				WithMemorySizeThreshold(1), WithTailSizeCap(0), WithPipelineFlush(), WithBoundedRuns())
			t.Cleanup(func() { _ = sink.Close() })
			input := boundedInput(t, mp, "value")
			defer input.Clean(mp)
			require.NoError(t, sink.Write(ctx, input))
			select {
			case <-fs.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("worker did not enter Write")
			}
			release.Do(func() { close(fs.release) })
			select {
			case <-sink.pipe.result.ctx.Done(): // setError records the worker error before cancelling
			case <-time.After(10 * time.Second):
				t.Fatal("worker did not select its error")
			}
			cancelled, cancel := context.WithCancelCause(ctx)
			cancel(errors.New("later cancellation"))
			var err error
			switch method {
			case "Write":
				err = sink.Write(cancelled, input)
			case "WriteOwned":
				_, err = sink.WriteOwned(cancelled, input)
			case "Sync":
				err = sink.Sync(cancelled)
			}
			require.Same(t, workerErr, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
			require.Len(t, sink.PendingUnpublishedRuns(), 1)
			_, err = sink.RetryUnpublishedRuns(ctx)
			require.NoError(t, err)
			sink.Reset()
		})
	}
}

func TestBoundedRunSortFailureReleasesAndResets(t *testing.T) {
	const capacity = 1 << 20
	mp, err := mpool.NewMPool("bounded-sort-failure", capacity, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	attrs := []string{"id", "value"}
	typs := []types.Type{types.T_int32.ToType(), types.T_text.ToType()}
	input := batch.NewWithSchema(true, attrs, typs)
	t.Cleanup(func() { input.Clean(mp) })
	const rows = 128
	sources := make([]types.StringSource, rows)
	for i := 0; i < rows; i++ {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(rows-1-i), false, mp))
		require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("value"), false, mp))
		if i%2 == 0 {
			sources[i] = types.StringSourceCOMStmt
		} else {
			sources[i] = types.StringSourceSQLPrepare
		}
	}
	input.SetRowCount(rows)
	require.NoError(t, input.Vecs[1].SetStringSourcesWithMP(sources, mp))
	file := &countFileSinker{}
	sink := NewSinker(0, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
		WithMemorySizeThreshold(capacity), WithTailSizeCap(0), WithOffHeap(), WithBoundedRuns())
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	require.NoError(t, sink.Write(context.Background(), input))
	require.Len(t, sink.GetInMemoryData(), 1)
	// Leave less than the 128-byte sidecar needed by sorting.
	remaining := capacity - int(mp.CurrNB()) - 64
	require.Positive(t, remaining)
	filler, err := mp.Alloc(remaining, true)
	require.NoError(t, err)
	defer func() {
		if filler != nil {
			mp.Free(filler)
		}
	}()
	err = sink.Sync(context.Background())
	require.Error(t, err)
	require.Empty(t, sink.GetInMemoryData())
	require.ErrorContains(t, sink.Write(context.Background(), input), "aborted")
	mp.Free(filler)
	filler = nil
	sink.Reset()
	require.NoError(t, sink.Write(context.Background(), input))
	require.NoError(t, sink.Sync(context.Background()))
	require.Equal(t, rows, file.rows)
}

func TestUnboundedRunSortFailureKeepsStagedData(t *testing.T) {
	const capacity = 1 << 20
	mp, err := mpool.NewMPool("unbounded-sort-failure", capacity, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	attrs := []string{"key"}
	typs := []types.Type{types.T_text.ToType()}
	input := batch.NewWithSchema(true, attrs, typs)
	t.Cleanup(func() { input.Clean(mp) })
	const rows = 128
	sources := make([]types.StringSource, rows)
	for i := range sources {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(fmt.Sprintf("%03d", rows-1-i)), false, mp))
		if i%2 == 0 {
			sources[i] = types.StringSourceCOMStmt
		} else {
			sources[i] = types.StringSourceSQLPrepare
		}
	}
	input.SetRowCount(rows)
	require.NoError(t, input.Vecs[0].SetStringSourcesWithMP(sources, mp))
	file := &countFileSinker{}
	sink := NewSinker(0, attrs, typs, func(*mpool.MPool, fileservice.FileService) FileSinker { return file }, mp, nil,
		WithMemorySizeThreshold(capacity), WithTailSizeCap(0), WithOffHeap())
	t.Cleanup(func() { require.NoError(t, sink.Close()) })
	require.NoError(t, sink.Write(context.Background(), input))
	require.Len(t, sink.GetInMemoryData(), 1)
	remaining := capacity - int(mp.CurrNB()) - 64
	require.Positive(t, remaining)
	filler, err := mp.Alloc(remaining, true)
	require.NoError(t, err)
	defer func() {
		if filler != nil {
			mp.Free(filler)
		}
	}()
	require.Error(t, sink.Sync(context.Background()))
	require.Len(t, sink.GetInMemoryData(), 1)
	require.Equal(t, "127", string(sink.GetInMemoryData()[0].Vecs[0].GetBytesAt(0)))
	mp.Free(filler)
	filler = nil
	require.NoError(t, sink.Sync(context.Background()))
	require.Equal(t, rows, file.rows)
}
