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

package s3fifo_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/fileservice/s3fifo"
	"github.com/stretchr/testify/require"
)

// Black-box checks of docs/design/20261106-fileservice-cache-ownership.md, memory and
// meta caches, through the public API only.

type refValue struct{ refs atomic.Int64 }

func (v *refValue) Retain()  { v.refs.Add(1) }
func (v *refValue) Release() { v.refs.Add(-1) }

func hashString(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h = (h ^ uint64(s[i])) * 1099511628211
	}
	return h
}

func newContractCache(capacity int64, opts func(*s3fifo.Options[string, *refValue])) *s3fifo.Cache[string, *refValue] {
	o := s3fifo.Options[string, *refValue]{Capacity: fscache.ConstCapacity(capacity), Hash: hashString}
	if opts != nil {
		opts(&o)
	}
	return s3fifo.New(o)
}

// "The cache retains a value when it inserts it and releases it after the value leaves
// the cache; a Get retains the value for its caller, who releases it."
func TestDesignS3FIFOCacheOwnsReferences(t *testing.T) {
	c := newContractCache(10, nil)
	v := new(refValue)
	inserted, _ := c.Set("a", v, 1)
	require.True(t, inserted)
	require.Equal(t, int64(1), v.refs.Load(), "the cache holds one reference")

	got, release, ok, err := c.Get("a", nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.Same(t, v, got)
	require.Equal(t, int64(2), v.refs.Load(), "Get retains for the caller")
	if release != nil {
		release()
	}
	got.Release()
	require.Equal(t, int64(1), v.refs.Load())

	c.Delete("a")
	require.Equal(t, int64(0), v.refs.Load(), "a removed value is released")

	values := make([]*refValue, 30)
	for i := range values {
		values[i] = new(refValue)
		c.Set(fmt.Sprint(i), values[i], 1)
	}
	var held int64
	for _, v := range values {
		held += v.refs.Load()
	}
	require.Equal(t, held, int64(c.Used()), "the cache holds exactly one reference per cached byte-unit value")
}

// "An insert that succeeds has made its value findable, counted it in used bytes and
// made it evictable. An insert that fails cached nothing."
func TestDesignS3FIFOPublication(t *testing.T) {
	c := newContractCache(10, nil)
	v := new(refValue)
	inserted, _ := c.Set("a", v, 4)
	require.True(t, inserted)
	require.True(t, c.Contains("a"))
	require.Equal(t, int64(4), c.Used())

	big := new(refValue)
	inserted, _ = c.Set("big", big, 11)
	require.False(t, inserted)
	require.False(t, c.Contains("big"))
	require.Equal(t, int64(0), big.refs.Load(), "a failed insert keeps no reference")
	require.Equal(t, int64(4), c.Used())
}

// "A value larger than the capacity replaced into an existing key deletes the key."
func TestDesignS3FIFOReplaceLargerThanCapacityDeletes(t *testing.T) {
	c := newContractCache(10, nil)
	old := new(refValue)
	c.Set("a", old, 2)
	huge := new(refValue)
	c.Replace("a", huge, 11)
	require.False(t, c.Contains("a"))
	require.Equal(t, int64(0), old.refs.Load())
	require.Equal(t, int64(0), huge.refs.Load())
	require.Equal(t, int64(0), c.Used())
}

// "Used bytes exceed the capacity only while inserts run, by at most a sixteenth plus the
// values being inserted."
func TestDesignS3FIFOUsedBound(t *testing.T) {
	const capacity, size, workers, perWorker = 1 << 20, 1 << 10, 16, 4000
	c := newContractCache(capacity, nil)
	var maxUsed atomic.Int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if u := c.Used(); u > maxUsed.Load() {
				maxUsed.Store(u)
			}
		}
	}()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				c.Set(fmt.Sprintf("%d-%d", w, i), new(refValue), size)
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	<-sampled
	require.LessOrEqual(t, maxUsed.Load(), int64(capacity+capacity/16+workers*size))
	require.LessOrEqual(t, c.Used(), int64(capacity), "once inserts stop, used bytes are within the capacity")
}

// "Notifications and releases run outside the cache's locks and may call into the cache."
func TestDesignS3FIFONotificationsMayUseTheCache(t *testing.T) {
	var c *s3fifo.Cache[string, *refValue]
	c = newContractCache(4, func(o *s3fifo.Options[string, *refValue]) {
		o.OnInsert = func(key string, _ *refValue, _ int64, _ uint64) { c.Contains(key) }
		o.OnRemove = func(key string, _ *refValue, _ int64, _ uint64, _ s3fifo.Reason) {
			c.Contains(key)
			if _, release, ok, _ := c.Get("pinned", nil); ok && release != nil {
				release()
			}
		}
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			c.Set(fmt.Sprint(i), new(refValue), 1)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("a notification calling into the cache deadlocked")
	}
}

// "A ghost queue of recently evicted keys whose re-insertion goes to main."
func TestDesignS3FIFOGhostReinsertionSurvivesNewKeys(t *testing.T) {
	c := newContractCache(20, nil)
	c.Set("hot", new(refValue), 1)
	for i := 0; i < 40; i++ {
		c.Set(fmt.Sprint("a", i), new(refValue), 1)
	}
	require.False(t, c.Contains("hot"), "evicted from the small queue")
	c.Set("hot", new(refValue), 1)
	for i := 0; i < 10; i++ {
		c.Set(fmt.Sprint("b", i), new(refValue), 1)
	}
	require.True(t, c.Contains("hot"), "re-inserted from the ghost queue into main")
}
