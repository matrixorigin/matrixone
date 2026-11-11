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

package publication

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
)

func publicationTestMemory(t testing.TB, total int64) (context.Context, rscthrottler.WorkingSetThrottler) {
	t.Helper()
	owner := rscthrottler.NewMemThrottler(t.Name(), 1, rscthrottler.WithConstLimit(total), rscthrottler.WithMemorySampleForTesting(total, 0)).(rscthrottler.WorkingSetThrottler)
	return context.WithValue(context.Background(), publicationMemoryKey{}, owner), owner
}

func fetchTestContext(t testing.TB, size int64) context.Context {
	t.Helper()
	ctx, _ := publicationTestMemory(t, 1<<30)
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(stats, uint32(size)))
	lease, release, err := reservePublicationObject(ctx, stats, 0, false)
	require.NoError(t, err)
	t.Cleanup(release)
	return context.WithValue(ctx, publicationFetchMemoryKey{}, lease)
}

type observedObjectAdmission struct {
	rscthrottler.WorkingSetThrottler
	calls  atomic.Int32
	second chan struct{}
}

func (o *observedObjectAdmission) ReserveWorkingSetCandidatesUntilFit(ctx context.Context, specs []rscthrottler.WorkingSetSpec) (*rscthrottler.WorkingSet, error) {
	if o.calls.Add(1) == 2 {
		close(o.second)
	}
	return o.WorkingSetThrottler.ReserveWorkingSetCandidatesUntilFit(ctx, specs)
}

type heldObjectWrite struct {
	fileservice.FileService
	calls                     atomic.Int32
	entered, canceled, retire chan struct{}
}

func (fs *heldObjectWrite) Write(ctx context.Context, v fileservice.IOVector) error {
	if fs.calls.Add(1) == 1 {
		close(fs.entered)
		select {
		case <-fs.retire:
		case <-ctx.Done():
			close(fs.canceled)
			<-fs.retire
			return ctx.Err()
		}
	}
	return fs.FileService.Write(ctx, v)
}

func awaitObjectPhase(t *testing.T, phase <-chan struct{}) {
	t.Helper()
	select {
	case <-phase:
	case <-time.After(10 * time.Second):
		t.Fatal("object lifecycle did not progress")
	}
}

func TestOrdinaryObjectOwnsBufferThroughWrite(t *testing.T) {
	for _, mode := range []string{"release", "cancel_waiter", "cancel_writer"} {
		t.Run(mode, func(t *testing.T) {
			const size = int64(4096)
			claim, err := publicationBufferBytes(size)
			require.NoError(t, err)
			total := 2*claim - 1 // The actual coordinator admits exactly one object.
			ctx, owner := publicationTestMemory(t, total)
			observed := &observedObjectAdmission{WorkingSetThrottler: owner, second: make(chan struct{})}
			ctx = context.WithValue(ctx, publicationMemoryKey{}, observed)
			local, err := fileservice.NewLocalFS(ctx, "SHARED", t.TempDir(), fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { local.Close(context.Background()) })
			fs := &heldObjectWrite{FileService: local, entered: make(chan struct{}), canceled: make(chan struct{}), retire: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(fs.retire) }) }
			first, cancelFirst := context.WithCancel(ctx)
			second, cancelSecond := context.WithCancel(ctx)
			t.Cleanup(func() { cancelFirst(); cancelSecond(); release() })
			var reads atomic.Int32
			payload := bytes.Repeat([]byte{'x'}, int(size))
			executor := fetchTestExecutor(size, 1, func(context.Context, int64) ([]byte, error) { reads.Add(1); return payload, nil })
			stats := make([]*objectio.ObjectStats, 2)
			for i := range stats {
				id := objectio.NewObjectid()
				stats[i] = objectio.NewObjectStatsWithObjectID(&id, false, false, false)
				require.NoError(t, objectio.SetObjectStatsSize(stats[i], uint32(size)))
			}
			done1, done2 := make(chan struct{}), make(chan struct{})
			var err1, err2 error
			run := func(ctx context.Context, stat *objectio.ObjectStats) error {
				_, err := FilterObject(ctx, stat.Marshal(), types.TS{}, executor, false, fs, nil, nil, nil, "acc", "pub", nil, nil, nil, nil)
				return err
			}
			go func() { defer close(done1); err1 = run(first, stats[0]) }()
			t.Cleanup(func() { cancelFirst(); release(); <-done1 })
			awaitObjectPhase(t, fs.entered)
			go func() { defer close(done2); err2 = run(second, stats[1]) }()
			t.Cleanup(func() { cancelSecond(); <-done2 })
			awaitObjectPhase(t, observed.second)
			require.Equal(t, int32(1), reads.Load(), "the next object cannot fetch while the first buffer is in use")
			require.Equal(t, total-claim, owner.WorkingSetHeadroom().Available)
			switch mode {
			case "cancel_waiter":
				cancelSecond()
				awaitObjectPhase(t, done2)
				require.ErrorIs(t, err2, context.Canceled)
			case "cancel_writer":
				cancelFirst()
				awaitObjectPhase(t, fs.canceled)
				require.Equal(t, total-claim, owner.WorkingSetHeadroom().Available, "cancellation is not write retirement")
				select {
				case <-done1:
					t.Fatal("returned budget while Write still owns the buffer")
				default:
				}
			}
			release()
			awaitObjectPhase(t, done1)
			awaitObjectPhase(t, done2)
			if mode == "cancel_writer" {
				require.ErrorIs(t, err1, context.Canceled)
			} else {
				require.NoError(t, err1)
			}
			if mode != "cancel_waiter" {
				require.NoError(t, err2)
				v := fileservice.IOVector{FilePath: stats[1].ObjectName().String(), Entries: []fileservice.IOEntry{{Offset: 0, Size: size}}}
				require.NoError(t, local.Read(ctx, &v))
				require.Equal(t, payload, v.Entries[0].Data)
			}
			require.Equal(t, total, owner.WorkingSetHeadroom().Available)
		})
	}
}

func TestObjectGrowthReleasesBeforeReadmission(t *testing.T) {
	const total, reported, actual = int64(1 << 20), int64(1024), int64(64 << 10)
	ctx, owner := publicationTestMemory(t, total)
	observed := &observedObjectAdmission{WorkingSetThrottler: owner, second: make(chan struct{})}
	ctx = context.WithValue(ctx, publicationMemoryKey{}, observed)
	held, ok, err := owner.TryReserveWorkingSet(ctx, rscthrottler.WorkingSetSpec{UnitBytes: 1, Inputs: []rscthrottler.InputSlotSpec{{Count: 1, Bytes: total - 7*1024}}})
	require.NoError(t, err)
	require.True(t, ok)
	t.Cleanup(held.Close)
	var reads atomic.Int32
	executor := fetchTestExecutor(actual, 1, func(context.Context, int64) ([]byte, error) {
		reads.Add(1)
		return bytes.Repeat([]byte{'x'}, int(actual)), nil
	})
	local, err := fileservice.NewLocalFS(ctx, "SHARED", t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close(context.Background()) })
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(stats, uint32(reported)))
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	done := make(chan struct{})
	var result error
	go func() {
		defer close(done)
		_, result = FilterObject(ctx, stats.Marshal(), types.TS{}, executor, false, local, nil, nil, nil, "acc", "pub", nil, nil, nil, nil)
	}()
	t.Cleanup(func() { cancel(); held.Close(); <-done })
	awaitObjectPhase(t, observed.second)
	require.Zero(t, reads.Load(), "neither attempt allocates/fetches data before adequate admission")
	require.Equal(t, int64(7*1024), owner.WorkingSetHeadroom().Available, "the first attempt has returned its entire promise")
	held.Close()
	awaitObjectPhase(t, done)
	require.NoError(t, result)
	require.Equal(t, int32(1), reads.Load())
	require.Equal(t, total, owner.WorkingSetHeadroom().Available)
}

func TestFetchRejectsMissingOwnerBeforeData(t *testing.T) {
	executor := fetchTestExecutor(64<<20, 1, func(context.Context, int64) ([]byte, error) { t.Fatal("ownerless data request"); return nil, nil })
	data, err := fetchObjectChunks(context.Background(), executor, "obj", nil, "acc", "pub", 64<<20)
	require.ErrorContains(t, err, "object reservation")
	require.Nil(t, data)
}

func fetchTestExecutor(size, count int64, data func(context.Context, int64) ([]byte, error)) *mockSQLExecutorForJob {
	return &mockSQLExecutorForJob{execFn: func(ctx context.Context, _ *ActiveRoutine, _ uint32, query string, _, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
		index, err := strconv.ParseInt(query[strings.LastIndexByte(query, ' ')+1:], 10, 64)
		if err != nil {
			return nil, nil, err
		}
		var payload []byte
		if index != 0 {
			payload, err = data(ctx, index)
			if err != nil {
				return nil, nil, err
			}
		}
		return newMockResult(&mockScanner{nextFn: func() bool { return true }, scanFn: func(dest ...interface{}) error {
			*dest[0].(*[]byte) = payload
			*dest[1].(*int64) = size
			*dest[2].(*int64) = index
			*dest[3].(*int64) = count
			*dest[4].(*bool) = index == count
			return nil
		}, closeFn: func() error { return nil }, errFn: func() error { return nil }}), noopCancel, nil
	}}
}

func TestOrdinaryObjectRetiresChunksBeforeRefund(t *testing.T) {
	original := GetObjectFromUpstreamWithWorker
	t.Cleanup(func() { GetObjectFromUpstreamWithWorker = original })
	GetObjectFromUpstreamWithWorker = func(ctx context.Context, e SQLExecutor, name string, w GetChunkWorker, acc, pub string) ([]byte, error) {
		return fetchObjectChunks(ctx, e, name, w, acc, pub, 1<<20)
	}
	const total, size = int64(16 << 20), int64(2 << 20)
	ctx, owner := publicationTestMemory(t, total)
	ctx, cancel := context.WithCancel(ctx)
	entered, canceled, retire := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(retire) }) }
	worker := NewGetChunkWorker()
	t.Cleanup(func() { cancel(); release(); worker.Stop() })
	executor := fetchTestExecutor(size, 2, func(ctx context.Context, index int64) ([]byte, error) {
		if index == 1 {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-retire
			return nil, ctx.Err()
		}
		return bytes.Repeat([]byte{'x'}, 1<<20), nil
	})
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(stats, uint32(size)))
	done := make(chan struct{})
	var result error
	go func() {
		defer close(done)
		_, result = FilterObject(ctx, stats.Marshal(), types.TS{}, executor, false, nil, nil, worker, nil, "acc", "pub", nil, nil, nil, nil)
	}()
	t.Cleanup(func() { cancel(); release(); <-done })
	awaitObjectPhase(t, entered)
	cancel()
	awaitObjectPhase(t, canceled)
	claim, err := publicationBufferBytes(size)
	require.NoError(t, err)
	require.Equal(t, total-claim, owner.WorkingSetHeadroom().Available)
	select {
	case <-done:
		t.Fatal("returned credit while an accepted chunk was still retiring")
	default:
	}
	release()
	awaitObjectPhase(t, done)
	require.Error(t, result)
	require.Equal(t, total, owner.WorkingSetHeadroom().Available)
}

func TestAppendableObjectFetchUsesSameOwner(t *testing.T) {
	const total, size = int64(1 << 20), int64(4096)
	ctx, owner := publicationTestMemory(t, total)
	claim, err := publicationBufferBytes(size)
	require.NoError(t, err)
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(stats, uint32(size)))
	calls := 0
	executor := fetchTestExecutor(size, 1, func(context.Context, int64) ([]byte, error) {
		calls++
		require.Equal(t, total-claim, owner.WorkingSetHeadroom().Available)
		return nil, moerr.NewInternalErrorNoCtx("permission denied")
	})
	_, err = FilterObject(ctx, stats.Marshal(), types.TS{}, executor, false, nil, nil, nil, nil, "acc", "pub", nil, nil, nil, nil)
	require.Error(t, err)
	require.Positive(t, calls)
	require.Equal(t, total, owner.WorkingSetHeadroom().Available)
}

func TestProtectionCancelsQueuedObjectAdmission(t *testing.T) {
	// The process-wide semaphore must belong to the parent, not a test bubble.
	_ = getOrCreateChunkSemaphore()
	for _, mode := range []string{"expire", "renew", "shorten", "unregister", "parent_cancel"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const total, size = int64(1 << 20), int64(4096)
				ctx, owner := publicationTestMemory(t, total)
				parent, cancel := context.WithCancel(ctx)
				defer cancel()
				w := &worker{syncProtectionJobs: make(map[string]*syncProtectionEntry)}
				w.RegisterSyncProtection("test", time.Now().Add(10*time.Millisecond).UnixNano())
				ctx, stop := withSyncProtection(parent, w, "test")
				defer stop()
				held, ok, err := owner.TryReserveWorkingSet(ctx, rscthrottler.WorkingSetSpec{UnitBytes: 1, Inputs: []rscthrottler.InputSlotSpec{{Count: 1, Bytes: total - 1}}})
				require.NoError(t, err)
				require.True(t, ok)
				defer held.Close()
				observed := &observedObjectAdmission{WorkingSetThrottler: owner, second: make(chan struct{})}
				ctx = context.WithValue(ctx, publicationMemoryKey{}, observed)
				id := objectio.NewObjectid()
				stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
				require.NoError(t, objectio.SetObjectStatsSize(stats, uint32(size)))
				var fetches atomic.Int32
				executor := fetchTestExecutor(size, 1, func(context.Context, int64) ([]byte, error) {
					fetches.Add(1)
					return bytes.Repeat([]byte{'x'}, int(size)), nil
				})
				done := make(chan struct{})
				var result error
				go func() {
					defer close(done)
					_, result = FilterObject(ctx, stats.Marshal(), types.TS{}, executor, false, &protectionTestFS{}, nil, nil, nil, "acc", "pub", nil, nil, nil, func() bool { return time.Now().UnixNano() < w.GetSyncProtectionTTL("test") })
				}()
				defer func() { cancel(); held.Close(); <-done }()
				synctest.Wait()
				require.Equal(t, int32(1), observed.calls.Load(), "real object reservation is queued")
				require.Zero(t, fetches.Load())
				switch mode {
				case "expire":
					time.Sleep(11 * time.Millisecond) // Advance synctest's virtual clock, without wall-clock waiting.
				case "renew":
					time.Sleep(5 * time.Millisecond)
					w.RegisterSyncProtection("test", time.Now().Add(15*time.Millisecond).UnixNano())
					time.Sleep(6 * time.Millisecond) // Past the old deadline, before the renewed deadline.
					synctest.Wait()
					require.NoError(t, context.Cause(ctx))
					select {
					case <-done:
						t.Fatal("renewal ended a valid queued object")
					default:
					}
					held.Close()
				case "shorten":
					w.RegisterSyncProtection("test", time.Now().Add(3*time.Millisecond).UnixNano())
					time.Sleep(4 * time.Millisecond) // Earlier deadline passes while the old timer is still pending.
				case "unregister":
					w.UnregisterSyncProtection("test")
				case "parent_cancel":
					cancel()
				}
				synctest.Wait()
				select {
				case <-done:
				default:
					t.Fatal("protection/capacity change did not settle admission")
				}
				if mode == "expire" || mode == "shorten" || mode == "unregister" {
					require.ErrorIs(t, result, ErrSyncProtectionTTLExpired)
				} else if mode == "parent_cancel" {
					require.ErrorIs(t, result, context.Canceled)
				} else {
					require.NoError(t, result)
					require.Equal(t, int32(1), fetches.Load())
				}
				held.Close()
				require.Equal(t, total, owner.WorkingSetHeadroom().Available)
				stop()
				require.Error(t, context.Cause(ctx))
			})
		})
	}
}

func TestProtectionContextWithoutRegistration(t *testing.T) {
	parent := context.Background()
	for _, id := range []string{"", "absent"} {
		ctx, stop := withSyncProtection(parent, nil, id)
		require.Equal(t, parent, ctx)
		stop()
	}
}

type protectionTestFS struct{ fileservice.FileService }

func (*protectionTestFS) Write(ctx context.Context, _ fileservice.IOVector) error {
	return context.Cause(ctx)
}

func TestApplyObjectsUsesEngineAdmission(t *testing.T) {
	for _, mode := range []string{"runtime_owner", "explicit_owner", "missing_owner"} {
		t.Run(mode, func(t *testing.T) {
			const total = int64(1 << 20)
			parent, owner := publicationTestMemory(t, total)
			// Start without a private admission value, as direct engine callers do.
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if mode == "explicit_owner" {
				ctx = context.WithValue(ctx, publicationMemoryKey{}, owner)
			}
			rt := runtime.DefaultRuntime()
			runtime.SetupServiceBasedRuntime(t.Name(), rt)
			_, other := publicationTestMemory(t, total)
			if mode == "runtime_owner" {
				rt.SetGlobalVariables(runtime.CNMemoryThrottler, owner)
			} else if mode == "explicit_owner" {
				rt.SetGlobalVariables(runtime.CNMemoryThrottler, other)
			}
			eng := mock_frontend.NewMockEngine(gomock.NewController(t))
			eng.EXPECT().GetService().Return(t.Name()).AnyTimes()
			id := objectio.NewObjectid()
			stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
			require.NoError(t, objectio.SetObjectStatsSize(stats, 4096))
			calls := 0
			executor := &mockSQLExecutorForJob{execFn: func(fetchCtx context.Context, _ *ActiveRoutine, _ uint32, _ string, _, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
				calls++
				require.Same(t, parent.Value(publicationMemoryKey{}), fetchCtx.Value(publicationMemoryKey{}))
				require.Less(t, owner.WorkingSetHeadroom().Available, total, "reserve before entering upstream")
				cancel()
				return nil, noopCancel, fetchCtx.Err()
			}}
			objects := map[objectio.ObjectId]*ObjectWithTableInfo{id: {Stats: *stats, DBName: "db", TableName: "t"}}
			err := ApplyObjects(ctx, "task", 0, nil, objects, executor, nil, types.TS{}, nil, eng, nil, nil, nil, nil, nil, "acc", "pub", nil, nil, nil)
			if mode == "missing_owner" {
				require.ErrorContains(t, err, "requires CN memory admission")
				require.Zero(t, calls)
			} else {
				require.ErrorContains(t, err, "context canceled")
				require.Positive(t, calls)
			}
			require.Equal(t, total, owner.WorkingSetHeadroom().Available)
			require.Equal(t, total, other.WorkingSetHeadroom().Available)
		})
	}
}
