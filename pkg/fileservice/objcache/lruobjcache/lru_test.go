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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/objcache"
	"github.com/stretchr/testify/assert"
)

func TestLRUReleasable(t *testing.T) {
	l := New(1)
	n := 0

	val := objcache.NewReleasableValue(1, func() {
		n++
	})
	l.Set(1, val, 1, false)

	l.Set(2, 42, 1, false)
	assert.Equal(t, 1, n)
}

func TestLRURefCount(t *testing.T) {
	l := New(1)

	r := objcache.NewRCValue(42)
	r.IncRef()
	l.Set(1, r, 1, false)
	_, ok := l.kv[1]
	assert.True(t, ok)

	l.Set(2, 42, 1, false)
	_, ok = l.kv[1]
	assert.True(t, ok)
	_, ok = l.kv[2]
	assert.False(t, ok)

	r.DecRef()
	l.Set(2, 42, 1, false)
	_, ok = l.kv[1]
	assert.False(t, ok)
	_, ok = l.kv[2]
	assert.True(t, ok)

	r2 := objcache.NewRCValue(42)
	r2.IncRef()
	l.Set(3, r2, 1, false)
	_, ok = l.kv[3]
	assert.True(t, ok)
	_, ok = l.kv[2]
	assert.False(t, ok)

}

func BenchmarkLRUSet(b *testing.B) {
	const capacity = 1024
	l := New(capacity)
	for i := 0; i < b.N; i++ {
		l.Set(i%capacity, i, 1, false)
	}
}

func BenchmarkLRUParallelSet(b *testing.B) {
	const capacity = 1024
	l := New(capacity)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			l.Set(i%capacity, i, 1, false)
		}
	})
}

func BenchmarkLRUParallelSetOrGet(b *testing.B) {
	const capacity = 1024
	l := New(capacity)
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			l.Set(i%capacity, i, 1, false)
			if i%2 == 0 {
				l.Get(i%capacity, false)
			}
		}
	})
}
