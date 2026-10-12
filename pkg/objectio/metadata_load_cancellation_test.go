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

package objectio

import (
	"context"
	"errors"
	"fmt"
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
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/stretchr/testify/require"
)

// Model a deadline's Done/Err transition at a channel barrier, not wall-clock time.
type metadataDeadlineContext struct{ context.Context }

func (c metadataDeadlineContext) Err() error {
	if c.Context.Err() != nil {
		return context.DeadlineExceeded
	}
	return nil
}

func TestDedupLoadCancellationOwnership(t *testing.T) {
	storageErr := errors.New("metadata storage failed")
	for _, tc := range []struct {
		name           string
		cancelOwner    bool
		ownerDeadline  bool
		cancelWaiter   bool
		waiterDeadline bool
		uncached       bool
		ownerError     func(context.Context) error
		waiterError    error
		wantTakeover   bool
	}{
		{name: "owner cancellation", cancelOwner: true, ownerError: context.Context.Err, wantTakeover: true},
		{name: "owner deadline", cancelOwner: true, ownerDeadline: true, ownerError: context.Context.Err, wantTakeover: true},
		{name: "takeover preserves its own failure", cancelOwner: true, ownerError: context.Context.Err, waiterError: storageErr, wantTakeover: true},
		{name: "wrapped cancellation", cancelOwner: true, wantTakeover: true, ownerError: func(ctx context.Context) error {
			return fmt.Errorf("metadata read: %w", ctx.Err())
		}},
		{name: "converted cancellation", cancelOwner: true, wantTakeover: true, ownerError: func(ctx context.Context) error {
			return moerr.ConvertGoError(ctx, ctx.Err())
		}},
		{name: "joined cancellation", cancelOwner: true, wantTakeover: true, ownerError: func(ctx context.Context) error {
			return errors.Join(ctx.Err(), fmt.Errorf("metadata read: %w", ctx.Err()))
		}},
		{name: "storage failure", ownerError: func(context.Context) error { return storageErr }},
		{name: "cancellation cannot hide storage failure", cancelOwner: true, ownerError: func(context.Context) error { return storageErr }},
		{name: "joined storage failure", cancelOwner: true, ownerError: func(ctx context.Context) error {
			return errors.Join(ctx.Err(), storageErr)
		}},
		{name: "independent deadline", cancelOwner: true, ownerError: func(context.Context) error { return context.DeadlineExceeded }},
		{name: "unattributed cancellation", ownerError: func(context.Context) error { return context.Canceled }},
		{name: "successful load despite owner cancellation", cancelOwner: true},
		{name: "uncached successful shared load", uncached: true},
		{name: "both canceled", cancelOwner: true, cancelWaiter: true, ownerError: context.Context.Err},
		{name: "waiter deadline", cancelWaiter: true, waiterDeadline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldCache := metaCache
			capacity := int64(1024)
			if tc.uncached {
				capacity = 1
			}
			metaCache = newMetaCache(fscache.ConstCapacity(capacity))
			t.Cleanup(func() { metaCache = oldCache })
			var key mataCacheKey
			key[0] = 101
			baseOwner, cancelOwner := context.WithCancel(context.Background())
			t.Cleanup(cancelOwner)
			var ownerCtx context.Context = baseOwner
			if tc.ownerDeadline {
				ownerCtx = metadataDeadlineContext{ownerCtx}
			}
			started, release, ownerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseOwner := func() { releaseOnce.Do(func() { close(release) }) }
			var ownerErr error
			go func() {
				defer close(ownerDone)
				_, ownerErr = dedupLoad(ownerCtx, key, func() ([]byte, error) {
					close(started)
					<-release
					if tc.ownerError != nil {
						return nil, tc.ownerError(ownerCtx)
					}
					return []byte("owner metadata"), nil
				})
			}()
			t.Cleanup(func() {
				cancelOwner()
				releaseOwner()
				require.True(t, waitForDedupLoadCompletion(ownerDone), "owner must terminate")
			})
			require.True(t, waitForDedupLoadCompletion(started))

			waiterCtx, cancelWaiter := newDedupLoadWaiterContext()
			if tc.waiterDeadline {
				waiterCtx.Context = metadataDeadlineContext{waiterCtx.Context}
			}
			waiterDone := make(chan struct{})
			var waiterValue []byte
			var waiterErr error
			var loads atomic.Int32
			go func() {
				defer close(waiterDone)
				waiterValue, waiterErr = dedupLoad(waiterCtx, key, func() ([]byte, error) {
					loads.Add(1)
					if tc.waiterError != nil {
						return nil, tc.waiterError
					}
					return []byte("waiter metadata"), nil
				})
			}()
			t.Cleanup(func() {
				cancelWaiter()
				require.True(t, waitForDedupLoadCompletion(waiterDone), "waiter must terminate")
			})
			waitForDedupLoadWaiterAdmission(t, waiterCtx, waiterDone, ownerDone, releaseOwner, cancelWaiter)
			if tc.cancelWaiter {
				cancelWaiter()
				// Prove the waiter can leave before the owner publishes any result.
				require.True(t, waitForDedupLoadCompletion(waiterDone))
			}
			if tc.cancelOwner {
				cancelOwner()
			}
			releaseOwner()
			require.True(t, waitForDedupLoadCompletion(ownerDone))
			require.True(t, waitForDedupLoadCompletion(waiterDone))
			switch {
			case tc.cancelWaiter:
				require.ErrorIs(t, waiterErr, waiterCtx.Err())
				require.Zero(t, loads.Load())
			case tc.wantTakeover:
				require.ErrorIs(t, ownerErr, ownerCtx.Err())
				require.NoError(t, waiterCtx.Err())
				if tc.waiterError != nil {
					require.ErrorIs(t, waiterErr, tc.waiterError)
					require.Nil(t, waiterValue)
				} else {
					require.NoError(t, waiterErr)
					require.Equal(t, []byte("waiter metadata"), waiterValue)
				}
				require.Equal(t, int32(1), loads.Load())
			case tc.ownerError != nil:
				require.Error(t, ownerErr)
				require.True(t, ownerErr == waiterErr, "a real load error is not retried or replaced")
				require.Zero(t, loads.Load())
			default:
				require.NoError(t, ownerErr)
				require.NoError(t, waiterErr)
				require.Equal(t, []byte("owner metadata"), waiterValue)
				require.Zero(t, loads.Load())
			}
			metaLoadMu.Lock()
			_, pending := metaLoadCalls[key]
			metaLoadMu.Unlock()
			require.False(t, pending, "no generation may remain after every caller exits")
		})
	}
}

func TestDedupLoadCanceledCallerDoesNotBecomeOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var loads int
	_, err := dedupLoad(ctx, mataCacheKey{}, func() ([]byte, error) {
		loads++
		return nil, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, loads)
}

type metadataEmptyJoinedError struct{}

func (metadataEmptyJoinedError) Error() string   { return "empty error tree" }
func (metadataEmptyJoinedError) Unwrap() []error { return nil }

type metadataEmptyWrappedError struct{}

func (metadataEmptyWrappedError) Error() string { return "empty wrapper" }
func (metadataEmptyWrappedError) Unwrap() error { return nil }

func TestMetadataLoadCancellationRequiresEveryLeaf(t *testing.T) {
	for _, tc := range []struct {
		err        error
		contextErr error
		want       bool
	}{
		{nil, context.Canceled, false},
		{context.Canceled, nil, false},
		{metadataEmptyJoinedError{}, context.Canceled, false},
		{metadataEmptyWrappedError{}, context.Canceled, false},
		{context.Canceled, context.Canceled, true},
		{context.DeadlineExceeded, context.DeadlineExceeded, true},
		{errors.Join(context.Canceled, context.DeadlineExceeded), context.Canceled, false},
		{errors.Join(context.Canceled, errors.New("I/O failure")), context.Canceled, false},
		{fmt.Errorf("outer: %w", errors.Join(context.Canceled, context.Canceled)), context.Canceled, true},
	} {
		require.Equal(t, tc.want, isMetadataLoadCancellation(tc.err, tc.contextErr), "%v / %v", tc.err, tc.contextErr)
	}
}

type metadataGenerationWaiter struct {
	context.Context
	admissions chan struct{}
}

func (c *metadataGenerationWaiter) Done() <-chan struct{} {
	select {
	case c.admissions <- struct{}{}:
	default:
	}
	return c.Context.Done()
}

func TestDedupLoadReelectsOneOwnerAcrossCanceledGenerations(t *testing.T) {
	oldCache := metaCache
	metaCache = newMetaCache(fscache.ConstCapacity(1024))
	t.Cleanup(func() { metaCache = oldCache })
	var key mataCacheKey
	key[0] = 102
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	started, ownerDone := make(chan struct{}), make(chan struct{})
	var ownerErr error
	go func() {
		defer close(ownerDone)
		_, ownerErr = dedupLoad(ownerCtx, key, func() ([]byte, error) {
			close(started)
			<-ownerCtx.Done()
			return nil, ownerCtx.Err()
		})
	}()
	t.Cleanup(func() { cancelOwner(); require.True(t, waitForDedupLoadCompletion(ownerDone)) })
	require.True(t, waitForDedupLoadCompletion(started))

	const waiterCount = 3
	var waiters [waiterCount]*metadataGenerationWaiter
	var cancels [waiterCount]context.CancelFunc
	var done [waiterCount]chan struct{}
	var values [waiterCount][]byte
	var errs [waiterCount]error
	var calls [waiterCount]atomic.Int32
	loaders := make(chan int, waiterCount)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseLoad := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseLoad)
	for i := range waiterCount {
		ctx, cancel := context.WithCancel(context.Background())
		waiters[i] = &metadataGenerationWaiter{Context: ctx, admissions: make(chan struct{}, waiterCount)}
		cancels[i], done[i] = cancel, make(chan struct{})
		go func() {
			defer close(done[i])
			values[i], errs[i] = dedupLoad(waiters[i], key, func() ([]byte, error) {
				calls[i].Add(1)
				loaders <- i
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-release:
					return []byte("shared metadata"), nil
				}
			})
		}()
		t.Cleanup(func() { cancel(); require.True(t, waitForDedupLoadCompletion(done[i])) })
		require.True(t, waitForDedupLoadCompletion(waiters[i].admissions))
	}
	nextLoader := func() int {
		t.Helper()
		timer := time.NewTimer(dedupLoadWaiterAdmissionTimeout)
		defer timer.Stop()
		select {
		case i := <-loaders:
			return i
		case <-timer.C:
			t.Fatal("no live waiter took ownership")
			return -1
		}
	}
	cancelOwner()
	require.True(t, waitForDedupLoadCompletion(ownerDone))
	require.ErrorIs(t, ownerErr, context.Canceled)
	second := nextLoader()
	for i := range waiterCount {
		if i != second {
			require.True(t, waitForDedupLoadCompletion(waiters[i].admissions), "peers must join the replacement generation")
		}
	}
	cancels[second]()
	require.True(t, waitForDedupLoadCompletion(done[second]))
	third := nextLoader()
	for i := range waiterCount {
		if i != second && i != third {
			require.True(t, waitForDedupLoadCompletion(waiters[i].admissions))
		}
	}
	releaseLoad()
	for i := range waiterCount {
		require.True(t, waitForDedupLoadCompletion(done[i]))
		if i == second {
			require.ErrorIs(t, errs[i], context.Canceled)
		} else {
			require.NoError(t, errs[i])
			require.Equal(t, []byte("shared metadata"), values[i])
		}
		if i == second || i == third {
			require.Equal(t, int32(1), calls[i].Load())
		} else {
			require.Zero(t, calls[i].Load(), "a healthy generation must still deduplicate peers")
		}
	}
	metaLoadMu.Lock()
	_, pending := metaLoadCalls[key]
	metaLoadMu.Unlock()
	require.False(t, pending)
}

// Pause after observing the abandoned generation and missing its cache result,
// but before the next election. Only the dedupLoad goroutine calls Err.
type metadataReelectionContext struct {
	*dedupLoadWaiterContext
	checks int
	paused chan struct{}
	resume chan struct{}
}

func (c *metadataReelectionContext) Err() error {
	c.checks++
	if c.checks == 3 {
		close(c.paused)
		<-c.resume
	}
	return c.Context.Err()
}

func TestDedupLoadLateWaiterSharesCompletedTakeover(t *testing.T) {
	storageErr := errors.New("takeover storage failure")
	for _, tc := range []struct {
		name       string
		capacity   int64
		loadErr    error
		newerOwner bool
	}{
		{name: "cached", capacity: 1024},
		{name: "cache admission rejected", capacity: 1},
		{name: "independent later owner", capacity: 1, newerOwner: true},
		{name: "takeover error", capacity: 1, loadErr: storageErr, newerOwner: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oldCache := metaCache
			metaCache = newMetaCache(fscache.ConstCapacity(tc.capacity))
			t.Cleanup(func() { metaCache = oldCache })
			var key mataCacheKey
			key[0] = 103
			ownerCtx, cancelOwner := context.WithCancel(context.Background())
			started, ownerDone := make(chan struct{}), make(chan struct{})
			var ownerErr error
			go func() {
				defer close(ownerDone)
				_, ownerErr = dedupLoad(ownerCtx, key, func() ([]byte, error) {
					close(started)
					<-ownerCtx.Done()
					return nil, ownerCtx.Err()
				})
			}()
			t.Cleanup(func() { cancelOwner(); require.True(t, waitForDedupLoadCompletion(ownerDone)) })
			require.True(t, waitForDedupLoadCompletion(started))

			fastCtx, cancelFast := newDedupLoadWaiterContext()
			fastDone, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseFast := func() { releaseOnce.Do(func() { close(release) }) }
			var fastValue []byte
			var fastErr error
			var fastLoads atomic.Int32
			value := []byte("takeover metadata")
			go func() {
				defer close(fastDone)
				fastValue, fastErr = dedupLoad(fastCtx, key, func() ([]byte, error) {
					fastLoads.Add(1)
					select {
					case <-release:
						return value, tc.loadErr
					case <-fastCtx.Context.Done():
						return nil, fastCtx.Err()
					}
				})
			}()
			t.Cleanup(func() {
				cancelFast()
				releaseFast()
				require.True(t, waitForDedupLoadCompletion(fastDone))
			})
			require.True(t, waitForDedupLoadCompletion(fastCtx.admitted))

			baseSlow, cancelSlow := newDedupLoadWaiterContext()
			slowCtx := &metadataReelectionContext{
				dedupLoadWaiterContext: baseSlow, paused: make(chan struct{}), resume: make(chan struct{}),
			}
			var resumeOnce sync.Once
			resumeSlow := func() { resumeOnce.Do(func() { close(slowCtx.resume) }) }
			slowDone := make(chan struct{})
			var slowValue []byte
			var slowErr error
			var slowLoads atomic.Int32
			go func() {
				defer close(slowDone)
				slowValue, slowErr = dedupLoad(slowCtx, key, func() ([]byte, error) {
					slowLoads.Add(1)
					return nil, errors.New("redundant late I/O must not run")
				})
			}()
			t.Cleanup(func() {
				cancelSlow()
				resumeSlow()
				require.True(t, waitForDedupLoadCompletion(slowDone))
			})
			require.True(t, waitForDedupLoadCompletion(slowCtx.admitted))
			cancelOwner()
			require.True(t, waitForDedupLoadCompletion(ownerDone))
			require.ErrorIs(t, ownerErr, context.Canceled)
			require.True(t, waitForDedupLoadCompletion(slowCtx.paused))
			releaseFast()
			require.True(t, waitForDedupLoadCompletion(fastDone))
			require.ErrorIs(t, fastErr, tc.loadErr)
			require.Equal(t, int32(1), fastLoads.Load())
			_, cached := metaCache.Get(context.Background(), key)
			require.Equal(t, tc.capacity > int64(len(value)) && tc.loadErr == nil, cached)

			// A terminal cohort must not retain a negative cache or let an old
			// participant's cleanup delete an independent new owner's record.
			laterCtx, cancelLater := context.WithCancel(context.Background())
			t.Cleanup(cancelLater)
			laterStarted, laterDone := make(chan struct{}), make(chan struct{})
			if tc.newerOwner {
				go func() {
					defer close(laterDone)
					_, _ = dedupLoad(laterCtx, key, func() ([]byte, error) {
						close(laterStarted)
						<-laterCtx.Done()
						return nil, laterCtx.Err()
					})
				}()
				t.Cleanup(func() { cancelLater(); require.True(t, waitForDedupLoadCompletion(laterDone)) })
				require.True(t, waitForDedupLoadCompletion(laterStarted))
			}
			metaLoadMu.Lock()
			laterCall := metaLoadCalls[key]
			metaLoadMu.Unlock()
			resumeSlow()
			require.True(t, waitForDedupLoadCompletion(slowDone))
			require.Zero(t, slowLoads.Load(), "a late waiter must consume the completed takeover, not perform I/O")
			require.ErrorIs(t, slowErr, tc.loadErr)
			if tc.loadErr == nil {
				require.Equal(t, value, fastValue)
				require.Equal(t, value, slowValue)
			}
			if tc.newerOwner {
				metaLoadMu.Lock()
				current := metaLoadCalls[key]
				metaLoadMu.Unlock()
				require.Same(t, laterCall, current)
				cancelLater()
				require.True(t, waitForDedupLoadCompletion(laterDone))
			}
			metaLoadMu.Lock()
			_, pending := metaLoadCalls[key]
			metaLoadMu.Unlock()
			require.False(t, pending)
		})
	}
}

func TestDedupLoadRechecksCacheBeforeElection(t *testing.T) {
	oldCache := metaCache
	metaCache = newMetaCache(fscache.ConstCapacity(1024))
	t.Cleanup(func() { metaCache = oldCache })
	var key mataCacheKey
	key[0] = 104
	ctx := context.Background()
	value := []byte("already cached metadata")
	metaCache.Set(ctx, key, value, int64(len(value)))
	loads := 0
	got, err := dedupLoad(ctx, key, func() ([]byte, error) {
		loads++
		return nil, errors.New("redundant I/O after an earlier cache miss")
	})
	require.Zero(t, loads)
	require.NoError(t, err)
	require.Equal(t, value, got)
}

type canceledMetadataReadFS struct {
	fileservice.FileService
	owner   context.Context
	started chan struct{}
	reads   atomic.Int32
}

func (fs *canceledMetadataReadFS) Read(ctx context.Context, v *fileservice.IOVector) error {
	fs.reads.Add(1)
	if ctx == fs.owner {
		close(fs.started)
		<-ctx.Done()
		return ctx.Err()
	}
	return fs.FileService.Read(ctx, v)
}

func TestMetadataReadersKeepLiveWaiterAfterOwnerCancellation(t *testing.T) {
	ctx := context.Background()
	base, err := fileservice.NewMemoryFS("metadata-cancellation", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { base.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	t.Cleanup(func() { bat.Clean(mp) })
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(42), false, mp))
	bat.SetRowCount(1)
	id := NewObjectid()
	name := BuildObjectNameWithObjectID(&id)
	writer, err := NewObjectWriter(name, base, 0, []uint16{0}, nil)
	require.NoError(t, err)
	_, err = writer.Write(bat)
	require.NoError(t, err)
	bloomBytes := []byte("test bloom payload")
	require.NoError(t, writer.WriteBF(0, 0, bloomBytes, 0))
	blocks, err := writer.WriteEnd(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	location := BuildLocation(name, blocks[0].GetExtent(), 1, 0)

	for _, bloom := range []bool{false, true} {
		t.Run(fmt.Sprintf("bloom=%t", bloom), func(t *testing.T) {
			oldCache := metaCache
			metaCache = newMetaCache(fscache.ConstCapacity(1 << 20))
			t.Cleanup(func() { metaCache = oldCache })
			if bloom {
				// Make only the BloomFilter cold so both readers share its real extent read.
				_, err := FastLoadObjectMeta(ctx, &location, false, base)
				require.NoError(t, err)
			}
			ownerCtx, cancelOwner := context.WithCancel(ctx)
			t.Cleanup(cancelOwner)
			fs := &canceledMetadataReadFS{FileService: base, owner: ownerCtx, started: make(chan struct{})}
			load := func(ctx context.Context) ([]byte, error) {
				if bloom {
					return FastLoadBF(ctx, location, false, fs)
				}
				return FastLoadObjectMeta(ctx, &location, false, fs)
			}
			ownerDone := make(chan struct{})
			var ownerErr error
			go func() { defer close(ownerDone); _, ownerErr = load(ownerCtx) }()
			t.Cleanup(func() { cancelOwner(); require.True(t, waitForDedupLoadCompletion(ownerDone)) })
			require.True(t, waitForDedupLoadCompletion(fs.started))
			waiterCtx, cancelWaiter := newDedupLoadWaiterContext()
			waiterDone := make(chan struct{})
			var value []byte
			var waiterErr error
			go func() { defer close(waiterDone); value, waiterErr = load(waiterCtx) }()
			t.Cleanup(func() { cancelWaiter(); require.True(t, waitForDedupLoadCompletion(waiterDone)) })
			waitForDedupLoadWaiterAdmission(t, waiterCtx, waiterDone, ownerDone, cancelOwner, cancelWaiter)
			cancelOwner()
			require.True(t, waitForDedupLoadCompletion(ownerDone))
			require.True(t, waitForDedupLoadCompletion(waiterDone))
			require.ErrorIs(t, ownerErr, context.Canceled)
			require.NoError(t, waiterCtx.Err())
			require.NoError(t, waiterErr)
			if bloom {
				require.Equal(t, bloomBytes, BloomFilter(value).GetBloomFilter(0))
			} else {
				meta := MustObjectMeta(value).MustDataMeta()
				require.Equal(t, uint32(1), meta.BlockCount())
				require.Equal(t, uint32(1), meta.GetBlockMeta(0).BlockHeader().Rows())
			}
			require.Equal(t, int32(2), fs.reads.Load(), "one canceled read, one live read")
			cached, err := load(ctx)
			require.NoError(t, err)
			require.Equal(t, value, cached)
			require.Equal(t, int32(2), fs.reads.Load(), "the takeover result must populate the cache")
		})
	}
}
