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

package lrucache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLRU(t *testing.T) {
	l := New[int, Bytes](1, nil, nil, nil)
	ctx := context.Background()

	l.Set(ctx, 1, []byte{42})
	_, ok := l.kv[1]
	assert.True(t, ok)
	_, ok = l.kv[2]
	assert.False(t, ok)
	val, _ := l.Get(ctx, 1)
	assert.Equal(t, Bytes([]byte{42}), val)

	l.Set(ctx, 2, []byte{43})
	_, ok = l.kv[1]
	assert.False(t, ok)
	_, ok = l.kv[2]
	assert.True(t, ok)
	val, _ = l.Get(ctx, 2)
	assert.Equal(t, Bytes([]byte{43}), val)
}

func TestLRUCallbacks(t *testing.T) {
	ctx := context.Background()

	postSetInvokedMap := make(map[int]bool)

	evictEntryMap := make(map[int][]byte)
	postEvictInvokedMap := make(map[int]bool)

	l := New(1,
		func(key int, _ Bytes) {
			postSetInvokedMap[key] = true
		},
		nil,
		func(key int, value Bytes) {
			evictEntryMap[key] = value
			postEvictInvokedMap[key] = true
		})

	// PostSet
	l.Set(ctx, 1, []byte{42})
	assert.True(t, postSetInvokedMap[1])
	postSetInvokedMap[1] = false // resetting
	assert.False(t, postEvictInvokedMap[1])

	l.Set(ctx, 1, []byte{43})
	assert.True(t, postSetInvokedMap[1])
	assert.True(t, postEvictInvokedMap[1]) // set on the same key, so evicted

	// PostSet and PostEvict
	l.Set(ctx, 2, []byte{44})
	assert.True(t, postEvictInvokedMap[1])        //postEvictInvokedMap is updated by PostEvict
	assert.Equal(t, []byte{43}, evictEntryMap[1]) //evictEntryMap is updated by PostEvict
}

func BenchmarkLRUSet(b *testing.B) {
	ctx := context.Background()
	const capacity = 1024
	l := New[int, Bytes](capacity, nil, nil, nil)
	for i := 0; i < b.N; i++ {
		l.Set(ctx, i%capacity, []byte{byte(i)})
	}
}

func BenchmarkLRUParallelSet(b *testing.B) {
	ctx := context.Background()
	const capacity = 1024
	l := New[int, Bytes](capacity, nil, nil, nil)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			l.Set(ctx, i%capacity, []byte{byte(i)})
		}
	})
}

func BenchmarkLRUParallelSetOrGet(b *testing.B) {
	ctx := context.Background()
	const capacity = 1024
	l := New[int, Bytes](capacity, nil, nil, nil)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			l.Set(ctx, i%capacity, []byte{byte(i)})
			if i%2 == 0 {
				l.Get(ctx, i%capacity)
			}
		}
	})
}
