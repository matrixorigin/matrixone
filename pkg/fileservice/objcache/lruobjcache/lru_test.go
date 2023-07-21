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
	kv := make(map[int][]byte)
	l := New(1, func(key any, value []byte, _ int64) {
		kv[key.(int)] = value
	})

	l.Set(1, []byte{42}, 1, false, nil)
	_, ok := l.kv[1]
	assert.True(t, ok)
	_, ok = l.kv[2]
	assert.False(t, ok)

	l.Set(2, []byte{42}, 1, false, nil)
	_, ok = l.kv[1]
	assert.False(t, ok)
	_, ok = l.kv[2]
	assert.True(t, ok)
	assert.Equal(t, []byte{42}, kv[1])

	// PostSet
	postSetInvoked := false
	l.Set(2, []byte{42}, 1, false, func(isNewEntry bool) {
		assert.Equal(t, false, isNewEntry)
		postSetInvoked = true
	})
	assert.Equal(t, true, postSetInvoked)
}

func BenchmarkLRUSet(b *testing.B) {
	const capacity = 1024
	l := New(capacity, nil)
	for i := 0; i < b.N; i++ {
		l.Set(i%capacity, []byte{byte(i)}, 1, false, nil)
	}
}

func BenchmarkLRUParallelSet(b *testing.B) {
	const capacity = 1024
	l := New(capacity, nil)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			l.Set(i%capacity, []byte{byte(i)}, 1, false, nil)
		}
	})
}

func BenchmarkLRUParallelSetOrGet(b *testing.B) {
	const capacity = 1024
	l := New(capacity, nil)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			l.Set(i%capacity, []byte{byte(i)}, 1, false, nil)
			if i%2 == 0 {
				l.Get(i%capacity, false)
			}
		}
	})
}
