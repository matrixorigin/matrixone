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

package s3fifo

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// value counts its references and records a retain or release after its last release.
type value struct {
	id     int
	refs   atomic.Int32
	freed  atomic.Bool
	misuse *atomic.Int32
}

func newValue(id int, misuse *atomic.Int32) *value {
	v := &value{id: id, misuse: misuse}
	v.refs.Store(1)
	return v
}

func (v *value) Retain() {
	if v.freed.Load() {
		v.misuse.Add(1)
	}
	v.refs.Add(1)
}

func (v *value) Release() {
	n := v.refs.Add(-1)
	if n < 0 {
		v.misuse.Add(1)
	}
	if n == 0 {
		v.freed.Store(true)
	}
}

func hashInt(k int) uint64 { return uint64(k) * 0x9e3779b97f4a7c15 }

func newCache(capacity int64, opts ...func(*Options[int, *value])) *Cache[int, *value] {
	o := Options[int, *value]{Capacity: func() int64 { return capacity }, Hash: hashInt}
	for _, f := range opts {
		f(&o)
	}
	return New(o)
}

type blockingRetainValue struct {
	*value
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (v *blockingRetainValue) Retain() {
	if v.armed.CompareAndSwap(true, false) {
		close(v.entered)
		<-v.release
	}
	v.value.Retain()
}

func TestConcurrentRetainLookup(t *testing.T) {
	for _, shared := range []bool{false, true} {
		name := "default"
		if shared {
			name = "concurrent retain"
		}
		t.Run(name, func(t *testing.T) {
			var misuse atomic.Int32
			v := &blockingRetainValue{value: newValue(1, &misuse), entered: make(chan struct{}), release: make(chan struct{})}
			opts := Options[int, *blockingRetainValue]{Capacity: func() int64 { return 100 }, Hash: hashInt}
			var c *Cache[int, *blockingRetainValue]
			if shared {
				c = NewWithConcurrentRetain(opts)
			} else {
				c = New(opts)
			}
			joined := true
			t.Cleanup(func() {
				if !joined {
					return // A stuck worker may still own the cache and value.
				}
				c.Delete(1)
				v.Release()
				require.Zero(t, v.refs.Load())
				require.Zero(t, misuse.Load())
			})
			inserted, _ := c.Set(1, v, 10)
			require.True(t, inserted)
			v.armed.Store(true)
			type result struct {
				value *blockingRetainValue
				ok    bool
				err   error
			}
			done := make(chan result, 1)
			joined = false
			go func() {
				got, _, ok, err := c.Get(1, nil)
				done <- result{value: got, ok: ok, err: err}
			}()
			var held result
			defer func() {
				close(v.release)
				if !joined {
					select {
					case held = <-done:
						joined = true
					case <-time.After(5 * time.Second):
						t.Error("ordinary lookup did not exit")
						return
					}
				}
				if held.ok {
					held.value.Release()
				}
				if held.err != nil || !held.ok {
					t.Errorf("ordinary lookup failed: ok=%t err=%v", held.ok, held.err)
				}
			}()
			select {
			case <-v.entered:
			case held = <-done:
				joined = true
				t.Fatalf("ordinary lookup returned before Retain: ok=%t err=%v", held.ok, held.err)
			case <-time.After(5 * time.Second):
				t.Fatal("ordinary lookup did not enter Retain")
			}
			got, release, ok, err := c.TryGet(1, nil)
			if ok {
				defer got.Release()
			}
			require.NoError(t, err)
			require.Nil(t, release)
			require.Equal(t, shared, ok, "only opted-in admission-free readers may overlap")
			if shared {
				require.Same(t, v, got)
				require.Equal(t, int32(1), c.indexOf(hashInt(1)).index[1].freq.Load())
				for range maxFreq + 1 {
					hit, _, hitOK, hitErr := c.TryGet(1, nil)
					if hitOK {
						hit.Release()
					}
					require.NoError(t, hitErr)
					require.True(t, hitOK)
				}
				require.Equal(t, int32(maxFreq), c.indexOf(hashInt(1)).index[1].freq.Load())
			}
			called := false
			_, release, ok, err = c.TryGet(1, func(int64) (func(), error) {
				called = true
				return nil, nil
			})
			require.NoError(t, err)
			require.Nil(t, release)
			require.False(t, ok, "admission still needs exclusive access")
			require.False(t, called)
		})
	}
}

func TestConcurrentRetainFrequencyAndEviction(t *testing.T) {
	var misuse atomic.Int32
	v := newValue(1, &misuse)
	c := NewWithConcurrentRetain(Options[int, *value]{Capacity: func() int64 { return 100 }, Hash: hashInt})
	t.Cleanup(func() {
		c.Delete(1)
		v.Release()
		require.Zero(t, v.refs.Load())
		require.Zero(t, misuse.Load())
	})
	inserted, _ := c.Set(1, v, 1)
	require.True(t, inserted)
	for range maxFreq + 1 {
		got, _, ok, err := c.TryGet(1, nil)
		if ok {
			got.Release()
		}
		require.NoError(t, err)
		require.True(t, ok)
	}
	ix := c.indexOf(hashInt(1))
	entry := ix.index[1]
	require.Equal(t, int32(maxFreq), entry.freq.Load())
	ps := c.policyOf(hashInt(1))
	_, evicted := c.step(ps, true, ps.small.head.Load())
	require.False(t, evicted, "a hot small-queue entry moves to the main queue")
	for want := maxFreq - 1; want >= 0; want-- {
		_, evicted = c.step(ps, false, ps.main.head.Load())
		require.False(t, evicted)
		require.Equal(t, int32(want), entry.freq.Load())
	}
	removed, evicted := c.step(ps, false, ps.main.head.Load())
	if evicted {
		c.finish(removed)
	}
	require.True(t, evicted)
	require.False(t, c.Contains(1))
}

func TestConcurrentRetainDecayWaitsForReaders(t *testing.T) {
	var misuse atomic.Int32
	v := newValue(1, &misuse)
	c := NewWithConcurrentRetain(Options[int, *value]{Capacity: func() int64 { return 100 }, Hash: hashInt})
	t.Cleanup(func() {
		c.Delete(1)
		v.Release()
		require.Zero(t, v.refs.Load())
		require.Zero(t, misuse.Load())
	})
	inserted, _ := c.Set(1, v, 1)
	require.True(t, inserted)
	for range 2 {
		got, _, ok, err := c.Get(1, nil)
		if ok {
			got.Release()
		}
		require.NoError(t, err)
		require.True(t, ok)
	}
	ps := c.policyOf(hashInt(1))
	_, evicted := c.step(ps, true, ps.small.head.Load())
	require.False(t, evicted)
	ix := c.indexOf(hashInt(1))
	entry := ix.index[1]
	ix.mu.RLock()
	locked, joined := true, false
	done := make(chan bool, 1)
	go func() {
		removed, evicted := c.step(ps, false, ps.main.head.Load())
		if evicted {
			c.finish(removed)
		}
		done <- evicted
	}()
	defer func() {
		if locked {
			ix.mu.RUnlock()
		}
		if !joined {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("cache policy worker did not exit")
			}
		}
	}()
	guard := time.NewTimer(5 * time.Second)
	defer guard.Stop()
	// A queued writer prevents new readers; the current reader prevents it
	// from completing the decay until this test releases the lock.
	for ix.mu.TryRLock() {
		ix.mu.RUnlock()
		select {
		case <-guard.C:
			t.Fatal("frequency decay did not wait for the reader")
		default:
			runtime.Gosched()
		}
	}
	_, _, ok, err := c.TryGet(1, nil)
	require.NoError(t, err)
	require.False(t, ok, "a pending writer must remain a nonblocking miss")
	ix.mu.RUnlock()
	locked = false
	select {
	case evicted = <-done:
		joined = true
		require.False(t, evicted)
	case <-guard.C:
		t.Fatal("frequency decay did not finish")
	}
	require.Equal(t, int32(1), entry.freq.Load())
}

func TestConcurrentRetainSurvivesReplacement(t *testing.T) {
	var misuse atomic.Int32
	old := newValue(1, &misuse)
	replacement := newValue(2, &misuse)
	c := NewWithConcurrentRetain(Options[int, *value]{Capacity: func() int64 { return 100 }, Hash: hashInt})
	var held *value
	t.Cleanup(func() {
		if held != nil {
			held.Release()
		}
		c.Delete(1)
		old.Release()
		replacement.Release()
		require.Zero(t, old.refs.Load())
		require.Zero(t, replacement.refs.Load())
		require.Zero(t, misuse.Load())
	})
	inserted, _ := c.Set(1, old, 1)
	require.True(t, inserted)
	got, _, ok, err := c.TryGet(1, nil)
	if ok {
		held = got
	}
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, old, got)
	require.True(t, c.Replace(1, replacement, 1))
	require.Equal(t, int32(2), old.refs.Load(), "the caller still owns the returned value")
	require.False(t, old.freed.Load())
	held.Release()
	held = nil
	require.Equal(t, int32(1), old.refs.Load(), "replacement released only the cache reference")
}

func TestConcurrentRetainChurnKeepsReferences(t *testing.T) {
	var misuse atomic.Int32
	c := NewWithConcurrentRetain(Options[int, *value]{Capacity: func() int64 { return 100 }, Hash: hashInt})
	t.Cleanup(func() { c.Delete(1) })
	first := newValue(0, &misuse)
	inserted, _ := c.Set(1, first, 1)
	first.Release()
	require.True(t, inserted)
	all := []*value{first}
	start := make(chan struct{})
	var wg sync.WaitGroup
	const readers, iterations = 4, 200
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range iterations {
				lookup := c.Get
				if i%2 == 0 {
					lookup = c.TryGet
				}
				got, _, ok, _ := lookup(1, nil)
				if ok {
					if got.freed.Load() {
						misuse.Add(1)
					}
					got.Release()
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := range iterations {
			if i%3 == 0 {
				c.Delete(1)
			}
			v := newValue(i+1, &misuse)
			all = append(all, v)
			if i%3 == 1 {
				c.Replace(1, v, 1)
			} else {
				c.Set(1, v, 1)
			}
			v.Release()
		}
	}()
	close(start)
	wg.Wait()
	c.Delete(1)
	for _, v := range all {
		require.Zero(t, v.refs.Load())
	}
	require.Zero(t, misuse.Load())
}

func liveBytes(c *Cache[int, *value]) int64 {
	var indexed, queued int64
	for i := range c.policy {
		ps := &c.policy[i]
		ps.mu.Lock()
		queued += ps.small.bytes + ps.main.bytes
		ps.mu.Unlock()
	}
	for i := range c.index {
		ix := &c.index[i]
		ix.mu.Lock()
		for _, e := range ix.index {
			indexed += e.size
		}
		ix.mu.Unlock()
	}
	if indexed != queued {
		panic(fmt.Sprintf("index holds %d bytes, queues %d", indexed, queued))
	}
	return indexed
}

func TestTryGet(t *testing.T) {
	t.Run("lookup and admission", func(t *testing.T) {
		var misuse atomic.Int32
		c := newCache(100)
		v := newValue(1, &misuse)
		t.Cleanup(func() {
			c.Delete(1)
			v.Release()
			require.Zero(t, misuse.Load())
		})
		inserted, _ := c.Set(1, v, 10)
		require.True(t, inserted)
		ix := c.indexOf(hashInt(1))
		entry := ix.index[1]
		var admitted, released int
		admit := func(size int64) (func(), error) {
			admitted++
			require.EqualValues(t, 10, size)
			return func() { released++ }, nil
		}

		got, release, ok, err := c.TryGet(2, admit)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, got)
		require.Nil(t, release)
		func() {
			ix.mu.Lock()
			defer ix.mu.Unlock()
			got, release, ok, err = c.TryGet(1, admit)
			require.NoError(t, err)
			require.False(t, ok, "a contended lookup must return while the shard is held")
			require.Nil(t, got)
			require.Nil(t, release)
		}()
		require.Zero(t, admitted)
		require.Equal(t, int32(2), v.refs.Load())
		require.Zero(t, entry.freq.Load())

		got, release, ok, err = c.TryGet(1, admit)
		require.NoError(t, err)
		require.True(t, ok)
		require.Same(t, v, got)
		require.Equal(t, int32(3), v.refs.Load())
		require.Equal(t, int32(1), entry.freq.Load())
		require.NotNil(t, release)
		release()
		got.Release()
		require.Equal(t, 1, admitted)
		require.Equal(t, 1, released)

		got, release, ok, err = c.Get(1, nil)
		require.NoError(t, err)
		require.True(t, ok)
		require.Nil(t, release)
		got.Release()
		require.Equal(t, int32(2), entry.freq.Load(), "Get and TryGet update the same frequency")
		denied := errors.New("admission denied")
		got, release, ok, err = c.TryGet(1, func(int64) (func(), error) {
			return func() { released++ }, denied
		})
		require.ErrorIs(t, err, denied)
		require.False(t, ok)
		require.Nil(t, got)
		require.Nil(t, release)
		require.Equal(t, 2, released)
		require.Equal(t, int32(2), v.refs.Load())
		require.Equal(t, int32(2), entry.freq.Load(), "failed admission must not update frequency")
	})

	t.Run("ghost is a miss", func(t *testing.T) {
		var misuse atomic.Int32
		c := newCache(100)
		v := newValue(2, &misuse)
		t.Cleanup(func() {
			c.Delete(2)
			v.Release()
			require.Zero(t, misuse.Load())
		})
		inserted, _ := c.Set(2, v, 10)
		require.True(t, inserted)
		require.Zero(t, c.EvictTo(context.Background(), 0))
		ps := c.policyOf(hashInt(2))
		ps.mu.Lock()
		remembered := ps.ghost.contains(hashInt(2))
		ps.mu.Unlock()
		require.True(t, remembered)
		got, release, ok, err := c.TryGet(2, func(int64) (func(), error) {
			t.Fatal("admission must not run for a ghost")
			return nil, nil
		})
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, got)
		require.Nil(t, release)
		ps.mu.Lock()
		remembered = ps.ghost.contains(hashInt(2))
		ps.mu.Unlock()
		require.True(t, remembered, "a read miss must not consume the ghost")
		require.Equal(t, int32(1), v.refs.Load())
	})

	for _, operation := range []string{"delete", "replace", "evict"} {
		t.Run("retained after "+operation, func(t *testing.T) {
			var misuse atomic.Int32
			c := newCache(100)
			original, replacement := newValue(1, &misuse), newValue(2, &misuse)
			t.Cleanup(func() {
				c.EvictTo(context.Background(), 0)
				original.Release()
				replacement.Release()
				require.Zero(t, misuse.Load())
			})
			inserted, _ := c.Set(1, original, 10)
			require.True(t, inserted)
			got, release, ok, err := c.TryGet(1, nil)
			require.NoError(t, err)
			require.True(t, ok)
			require.Nil(t, release)
			t.Cleanup(got.Release)
			done := make(chan bool, 1)
			go func() {
				switch operation {
				case "delete":
					c.Delete(1)
					done <- true
				case "replace":
					done <- c.Replace(1, replacement, 10)
				case "evict":
					done <- c.EvictTo(context.Background(), 0) == 0
				}
			}()
			require.True(t, <-done)
			require.Same(t, original, got)
			require.False(t, got.freed.Load())
			require.Equal(t, int32(2), got.refs.Load(), "caller and initial owner still hold the original")
		})
	}
}

// The cache retains each value it caches and releases it once it leaves, whatever the way;
// a Get retains for the caller. Used is the bytes of the cached values at every step. None of
// it needs a notification.
func TestLifetimeAndAccounting(t *testing.T) {
	var misuse atomic.Int32
	c := newCache(1000)
	a, b, d := newValue(1, &misuse), newValue(2, &misuse), newValue(3, &misuse)

	inserted, rejected := c.Set(1, a, 100)
	require.True(t, inserted)
	require.False(t, rejected)
	require.Equal(t, int32(2), a.refs.Load(), "the cache holds its own reference")
	inserted, _ = c.Set(1, b, 100)
	require.False(t, inserted, "a cached key keeps its value")
	require.Equal(t, int32(1), b.refs.Load())
	require.Equal(t, int64(100), c.Used())

	got, release, ok, err := c.Get(1, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Nil(t, release)
	require.Same(t, a, got)
	require.Equal(t, int32(3), a.refs.Load(), "a Get retains for the caller")
	got.Release()

	require.Equal(t, c.Used(), liveBytes(c))

	c.Delete(1)
	require.Equal(t, int64(0), c.Used(), "Delete gives the bytes back at once")
	require.Equal(t, int32(1), a.refs.Load(), "Delete releases the value")
	require.False(t, c.Contains(1))
	c.Delete(1)
	inserted, _ = c.Set(1, d, 300)
	require.True(t, inserted)
	require.Equal(t, int64(300), c.Used())
	c.Delete(1)

	for i := 0; i < 20; i++ {
		c.Set(i, newValue(i, &misuse), 100)
		require.LessOrEqual(t, c.Used(), int64(1000))
		require.Equal(t, c.Used(), liveBytes(c))
	}
	require.Equal(t, int64(0), c.EvictTo(context.Background(), 0))
	require.Zero(t, misuse.Load())
	a.Release()
	b.Release()
	d.Release()
	require.True(t, a.freed.Load() && b.freed.Load() && d.freed.Load())
}

// A value bigger than the capacity is not cached; a Set that leaves the cache over capacity
// evicts before it returns, so the cache never stays over capacity.
func TestSetEvictsWithinCapacity(t *testing.T) {
	var misuse atomic.Int32
	c := newCache(1000)
	v := newValue(0, &misuse)
	inserted, _ := c.Set(0, v, 1001)
	require.False(t, inserted)
	require.Equal(t, int32(1), v.refs.Load())
	values := make([]*value, 50)
	for i := range values {
		values[i] = newValue(i, &misuse)
		inserted, _ := c.Set(i, values[i], 64)
		require.True(t, inserted)
		require.LessOrEqual(t, c.Used(), int64(1000))
	}
	cached := 0
	for i, v := range values {
		if c.Contains(i) {
			cached++
			require.Equal(t, int32(2), v.refs.Load())
		} else {
			require.Equal(t, int32(1), v.refs.Load(), "an evicted value is released")
		}
	}
	require.Equal(t, c.Used(), int64(cached*64))
	require.Zero(t, misuse.Load())
}

// S3-FIFO: a key read more than once survives a scan of keys read once, which leave first;
// the key evicted last goes straight to the main queue when it is set again.
func TestScanResistanceAndGhost(t *testing.T) {
	var misuse atomic.Int32
	const capacity = 64 << 20
	c := newCache(capacity)
	hot := make([]int, 64)
	for i := range hot {
		hot[i] = -1 - i
		c.Set(hot[i], newValue(hot[i], &misuse), 64<<10)
		for range 2 {
			v, _, _, _ := c.Get(hot[i], nil)
			v.Release()
		}
	}
	for i := 0; i < 4096; i++ {
		c.Set(i, newValue(i, &misuse), 64<<10)
	}
	for _, k := range hot {
		require.True(t, c.Contains(k), "hot key %d survives the scan", k)
	}
	require.False(t, c.Contains(0), "the oldest scanned key has left")

	k := -1
	for i := 4095; i >= 0 && k < 0; i-- {
		if !c.Contains(i) {
			k = i
		}
	}
	require.GreaterOrEqual(t, k, 0)
	ps, ix := c.policyOf(hashInt(k)), c.indexOf(hashInt(k))
	ps.mu.Lock()
	remembered := ps.ghost.contains(hashInt(k))
	ps.mu.Unlock()
	require.True(t, remembered)
	c.Set(k, newValue(k, &misuse), 64<<10)
	ps.mu.Lock()
	ix.mu.Lock()
	require.Equal(t, inMain, ix.index[k].queue)
	ix.mu.Unlock()
	ps.mu.Unlock()
	c.EvictTo(context.Background(), 0)
	require.Zero(t, misuse.Load())
}

// Admission rejects a new key that would take the cache over the target, admits a key the
// ghost remembers by evicting older ones, and leaves a cached key alone.
func TestAdmission(t *testing.T) {
	var misuse atomic.Int32
	var target atomic.Int64
	target.Store(-1)
	c := newCache(1000, func(o *Options[int, *value]) {
		o.Admission = func(int64) (int64, bool) { t := target.Load(); return t, t >= 0 }
	})
	for i := 0; i < 15; i++ {
		c.Set(i, newValue(i, &misuse), 100)
	}
	evicted := -1
	for i := 0; i < 15; i++ {
		if !c.Contains(i) {
			evicted = i
			break
		}
	}
	require.GreaterOrEqual(t, evicted, 0)
	target.Store(c.Used())

	v := newValue(100, &misuse)
	inserted, rejected := c.Set(100, v, 100)
	require.False(t, inserted)
	require.True(t, rejected)
	require.Equal(t, int32(1), v.refs.Load())

	inserted, rejected = c.Set(evicted, newValue(evicted, &misuse), 100)
	require.True(t, inserted, "a remembered key is admitted")
	require.False(t, rejected)
	require.LessOrEqual(t, c.Used(), target.Load())
	c.EvictTo(context.Background(), 0)
	require.Zero(t, misuse.Load())
}

// ChargeGuard is held while OnCharge runs, once per value admitted.
func TestChargeGuard(t *testing.T) {
	var misuse atomic.Int32
	var mu sync.Mutex
	charged := 0
	c := newCache(1000, func(o *Options[int, *value]) {
		o.ChargeGuard = &mu
		o.OnCharge = func(*value) {
			require.False(t, mu.TryLock(), "the guard is held")
			charged++
		}
	})
	c.Set(1, newValue(1, &misuse), 10)
	c.Set(1, newValue(1, &misuse), 10)
	c.Set(2, newValue(2, &misuse), 10)
	require.Equal(t, 2, charged)
}

// DeleteGroup removes exactly the keys of a group.
func TestDeleteGroup(t *testing.T) {
	var misuse atomic.Int32
	c := newCache(1<<20, func(o *Options[int, *value]) {
		o.Group = func(k int) string { return fmt.Sprint(k % 3) }
	})
	values := map[int]*value{}
	for i := 0; i < 300; i++ {
		values[i] = newValue(i, &misuse)
		c.Set(i, values[i], 10)
	}
	c.DeleteGroup("1")
	c.DeleteGroup("missing")
	for i, v := range values {
		require.Equal(t, i%3 != 1, c.Contains(i), i)
		if i%3 == 1 {
			require.Equal(t, int32(1), v.refs.Load())
		}
	}
	require.Equal(t, int64(200*10), c.Used())
	require.Equal(t, c.Used(), liveBytes(c))
}

// Notifications are optional: each cached value is notified inserted once and removed once,
// with the reason, while still referenced.
func TestNotifications(t *testing.T) {
	var misuse atomic.Int32
	inserts, removes := map[uint64]int{}, map[uint64]Reason{}
	c := newCache(250, func(o *Options[int, *value]) {
		o.OnInsert = func(_ int, v *value, _ int64, seq uint64) {
			require.False(t, v.freed.Load())
			inserts[seq]++
		}
		o.OnRemove = func(_ int, v *value, _ int64, seq uint64, reason Reason) {
			require.False(t, v.freed.Load())
			require.Equal(t, 1, inserts[seq], "removed after inserted")
			_, again := removes[seq]
			require.False(t, again)
			removes[seq] = reason
		}
	})
	for i := 0; i < 5; i++ {
		c.Set(i, newValue(i, &misuse), 100)
	}
	c.Delete(4)
	c.EvictTo(context.Background(), 0)
	require.Len(t, removes, len(inserts))
	reasons := map[Reason]int{}
	for _, r := range removes {
		reasons[r]++
	}
	require.Equal(t, map[Reason]int{Evicted: 4, Deleted: 1}, reasons)
	require.Zero(t, misuse.Load())
}

// Concurrent sets, gets, replaces, deletes and evictions never use a value after its last
// release, never leak a reference, and leave Used equal to the bytes of the cached values.
func TestConcurrentLifetime(t *testing.T) {
	var misuse atomic.Int32
	var notified atomic.Int64
	c := newCache(64<<10, func(o *Options[int, *value]) {
		o.Group = func(k int) string { return fmt.Sprint(k % 7) }
		o.OnInsert = func(k int, v *value, _ int64, seq uint64) {
			if v.freed.Load() || seq == 0 {
				misuse.Add(1)
			}
			notified.Add(1)
		}
		o.OnRemove = func(_ int, v *value, _ int64, _ uint64, _ Reason) {
			if v.freed.Load() {
				misuse.Add(1)
			}
			notified.Add(-1)
		}
	})
	var all sync.Map
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for i := 0; i < 20000; i++ {
				k := r.Intn(512)
				switch op := r.Intn(10); {
				case op < 4:
					v := newValue(k, &misuse)
					all.Store(v, struct{}{})
					c.Set(k, v, int64(64+r.Intn(1024)))
				case op < 8:
					if v, _, ok, _ := c.Get(k, nil); ok {
						if v.freed.Load() || v.id != k {
							misuse.Add(1)
						}
						v.Release()
					}
				case op == 8:
					c.EvictTo(context.Background(), int64(r.Intn(64<<10)))
				default:
					if r.Intn(20) == 0 {
						c.DeleteGroup(fmt.Sprint(k % 7))
					} else {
						c.Delete(k)
					}
				}
			}
		}(int64(g))
	}
	wg.Wait()
	require.Equal(t, c.Used(), liveBytes(c))
	require.LessOrEqual(t, c.Used(), int64(64<<10))
	require.Equal(t, int64(0), c.EvictTo(context.Background(), 0))
	require.Zero(t, notified.Load(), "every insertion notified is removed and notified once")
	all.Range(func(k, _ any) bool {
		v := k.(*value)
		require.Equal(t, int32(1), v.refs.Load(), "value %d keeps only its creator's reference", v.id)
		return true
	})
	require.Zero(t, misuse.Load())
}
