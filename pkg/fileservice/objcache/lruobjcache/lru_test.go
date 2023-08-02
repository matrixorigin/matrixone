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

package lruobjcache

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLRU(t *testing.T) {
	l := New(1, nil, nil)

	l.Set(1, []byte{42}, 1, false)
	_, ok := l.kv[1]
	assert.True(t, ok)
	_, ok = l.kv[2]
	assert.False(t, ok)
	val, _, _ := l.Get(1, false)
	assert.Equal(t, []byte{42}, val)

	l.Set(2, []byte{43}, 1, false)
	_, ok = l.kv[1]
	assert.False(t, ok)
	_, ok = l.kv[2]
	assert.True(t, ok)
	val, _, _ = l.Get(2, false)
	assert.Equal(t, []byte{43}, val)
}

func TestLRUCallbacks(t *testing.T) {

	isNewEntryMap := make(map[int]bool)
	postSetInvokedMap := make(map[int]bool)

	evictEntryMap := make(map[int][]byte)
	postEvictInvokedMap := make(map[int]bool)

	l := New(1,
		func(key any, value []byte, sz int64, isNewEntry bool) {
			_key := key.(int)
			isNewEntryMap[_key] = isNewEntry
			postSetInvokedMap[_key] = true
		},
		func(key any, value []byte, _ int64) {
			_key := key.(int)
			evictEntryMap[_key] = value
			postEvictInvokedMap[_key] = true
		})

	// PostSet
	l.Set(1, []byte{42}, 1, false)
	assert.True(t, postSetInvokedMap[1])
	postSetInvokedMap[1] = false // resetting
	assert.False(t, postEvictInvokedMap[1])
	assert.True(t, isNewEntryMap[1])

	l.Set(1, []byte{43}, 1, false)
	assert.True(t, postSetInvokedMap[1])
	assert.False(t, postEvictInvokedMap[1])
	assert.False(t, isNewEntryMap[1])

	// PostSet and PostEvict
	l.Set(2, []byte{44}, 1, false)
	assert.True(t, isNewEntryMap[2])              // isNewEntryMap is updated by PostSet
	assert.True(t, postEvictInvokedMap[1])        //postEvictInvokedMap is updated by PostEvict
	assert.Equal(t, []byte{43}, evictEntryMap[1]) //evictEntryMap is updated by PostEvict
}

func BenchmarkLRUSet(b *testing.B) {
	const capacity = 1024
	l := New(capacity, nil, nil)
	for i := 0; i < b.N; i++ {
		l.Set(i%capacity, []byte{byte(i)}, 1, false)
	}
}

func BenchmarkLRUParallelSet(b *testing.B) {
	const capacity = 1024
	l := New(capacity, nil, nil)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			l.Set(i%capacity, []byte{byte(i)}, 1, false)
		}
	})
}

func BenchmarkLRUParallelSetOrGet(b *testing.B) {
	const capacity = 1024
	l := New(capacity, nil, nil)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			l.Set(i%capacity, []byte{byte(i)}, 1, false)
			if i%2 == 0 {
				l.Get(i%capacity, false)
			}
		}
	})
}

func BenchmarkLRUParallelGet(b *testing.B) {
	const capacity = 1024
	l := New(capacity, nil, nil)
	l.Set(1, []byte{42}, 1, false)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Get(1, false)
		}
	})
}
