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
	"math/rand"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// model is S3-FIFO as libCacheSim implements it, in one shard, but for the order of insertion
// and eviction: a miss inserts the object into main when the ghost queue remembers it, taking it
// from the ghost queue, and into small otherwise, then evicts while the cache is over capacity.
// Eviction takes from small while small holds more than a tenth of the capacity or main is
// empty: a small object read more than once moves to main.
// From main, an object read since it was last passed over goes back with one read fewer. Any
// other is evicted, and remembered in the ghost queue when it has bytes, as the cache it
// replaces did for main too. A hit forgets its ghost entry, whose bytes count until it leaves
// the ghost queue, as in that cache. The ghost queue holds at most nine tenths of the capacity,
// and keeps at least one entry.
type model struct {
	capacity    int64
	small, main []*modelObject
	ghost       []modelGhost
	objects     map[int]*modelObject
}

type modelGhost struct {
	key        int
	size       int64
	remembered bool
}

type modelObject struct {
	key  int
	size int64
	freq int
}

func newModel(capacity int64) *model {
	return &model{capacity: capacity, objects: make(map[int]*modelObject)}
}

func bytesOf[T any](objects []T, size func(T) int64) (n int64) {
	for _, o := range objects {
		n += size(o)
	}
	return n
}

func (m *model) smallBytes() int64 {
	return bytesOf(m.small, func(o *modelObject) int64 { return o.size })
}

func (m *model) mainBytes() int64 {
	return bytesOf(m.main, func(o *modelObject) int64 { return o.size })
}

func (m *model) ghostBytes() int64 {
	return bytesOf(m.ghost, func(g modelGhost) int64 { return g.size })
}

func (m *model) used() int64 {
	return m.smallBytes() + m.mainBytes()
}

func (m *model) get(key int) {
	if o := m.objects[key]; o != nil {
		o.freq = min(o.freq+1, maxFreq)
	}
}

func (m *model) set(key int, size int64) {
	if size > m.capacity || m.objects[key] != nil {
		return
	}
	defer m.evictOver()
	o := &modelObject{key: key, size: size}
	m.objects[key] = o
	for i := range m.ghost {
		if g := &m.ghost[i]; g.key == key && g.remembered {
			g.remembered = false
			m.main = append(m.main, o)
			return
		}
	}
	m.small = append(m.small, o)
}

func (m *model) evictOver() {
	for m.used() > m.capacity {
		m.evict()
	}
}

// replace changes the size of a cached object, moving it to the back of its queue, then evicts
// while the cache is over capacity; a size over the capacity deletes the object.
func (m *model) replace(key int, size int64) bool {
	if size > m.capacity {
		m.delete(key)
		return false
	}
	o := m.objects[key]
	if o == nil {
		return false
	}
	for _, q := range []*[]*modelObject{&m.small, &m.main} {
		for i, x := range *q {
			if x == o {
				*q = append(append((*q)[:i:i], (*q)[i+1:]...), o)
				break
			}
		}
	}
	o.size = size
	m.evictOver()
	return true
}

func (m *model) delete(key int) {
	o := m.objects[key]
	if o == nil {
		return
	}
	delete(m.objects, key)
	for _, q := range []*[]*modelObject{&m.small, &m.main} {
		for i, x := range *q {
			if x == o {
				*q = append((*q)[:i:i], (*q)[i+1:]...)
			}
		}
	}
}

func (m *model) evict() {
	if m.smallBytes() > m.capacity/10 || m.mainBytes() == 0 {
		for len(m.small) > 0 {
			o := m.small[0]
			m.small = m.small[1:]
			if o.freq > 1 {
				m.main = append(m.main, o)
				continue
			}
			m.forget(o)
			return
		}
		return
	}
	for {
		o := m.main[0]
		m.main = m.main[1:]
		if o.freq > 0 {
			o.freq--
			m.main = append(m.main, o)
			continue
		}
		m.forget(o)
		return
	}
}

// forget evicts o, remembering it in the ghost queue when it has bytes.
func (m *model) forget(o *modelObject) {
	delete(m.objects, o.key)
	if o.size > 0 {
		for i := range m.ghost {
			if m.ghost[i].key == o.key {
				m.ghost[i].remembered = false
			}
		}
		m.ghost = append(m.ghost, modelGhost{key: o.key, size: o.size, remembered: true})
	}
	for m.ghostBytes() > m.capacity-m.capacity/10 && len(m.ghost) > 1 {
		m.ghost = m.ghost[1:]
	}
}

// Used sequentially, the cache keeps the keys, and fills its queues, as the model does.
func TestMatchesModel(t *testing.T) {
	var misuse atomic.Int32
	for _, capacity := range []int64{1, 1000, 20 << 10, 64 << 10} {
		c := newCache(capacity)
		m := newModel(capacity)
		r := rand.New(rand.NewSource(capacity + 7))
		for i := 0; i < 20000; i++ {
			k := r.Intn(400)
			switch op := r.Intn(20); {
			case op < 7:
				if v, _, ok, _ := c.Get(k, nil); ok {
					v.Release()
				}
				m.get(k)
			case op == 7:
				c.Delete(k)
				m.delete(k)
			case op == 8:
				size := int64(1 + r.Intn(int(min(capacity, 300))))
				require.Equal(t, m.replace(k, size), c.Replace(k, newValue(k, &misuse), size), "capacity %d, op %d", capacity, i)
			default:
				size := int64(1 + r.Intn(int(min(capacity, 300))))
				c.Set(k, newValue(k, &misuse), size)
				m.set(k, size)
			}
			require.Equal(t, m.used(), c.Used(), "capacity %d, op %d", capacity, i)
			require.Equal(t, m.smallBytes(), c.smallBytes.Load(), "capacity %d, op %d", capacity, i)
			require.Equal(t, m.mainBytes(), c.mainBytes.Load(), "capacity %d, op %d", capacity, i)
			require.Equal(t, m.ghostBytes(), c.ghostBytes.Load(), "capacity %d, op %d", capacity, i)
			for k := 0; k < 400; k++ {
				require.Equal(t, m.objects[k] != nil, c.Contains(k), "capacity %d, op %d, key %d", capacity, i, k)
			}
		}
	}
	require.Zero(t, misuse.Load())
}
