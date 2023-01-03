// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockservice

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	storages = map[string]func() LockStorage{
		"btree": newBtreeBasedStorage,
	}
)

func TestAdd(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory()

			k1 := []byte("k1")
			s.Add(k1, Lock{txnID: k1})
			assert.Equal(t, 1, s.Len())

			v, ok := s.Get(k1)
			assert.True(t, ok)
			assert.Equal(t, k1, v.txnID)
		})
	}
}

func TestDelete(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory()

			k1 := []byte("k1")
			k2 := []byte("k2")
			s.Add(k1, Lock{})
			s.Add(k2, Lock{})

			s.Delete(k1)
			assert.Equal(t, 1, s.Len())
			v, ok := s.Get(k1)
			assert.False(t, ok)
			assert.Empty(t, v)

			s.Delete(k2)
			assert.Equal(t, 0, s.Len())
			v, ok = s.Get(k2)
			assert.False(t, ok)
			assert.Empty(t, v)
		})
	}
}

func TestSeek(t *testing.T) {
	for name, factory := range storages {
		t.Run(name, func(t *testing.T) {
			s := factory()

			k1 := []byte("k1")
			k2 := []byte("k2")
			k3 := []byte("k3")

			checkSeek(t, s, k1, nil, nil)

			s.Add(k1, Lock{txnID: k1})
			checkSeek(t, s, k1, k1, k1)
			checkSeek(t, s, k2, nil, nil)
			checkSeek(t, s, k3, nil, nil)

			s.Add(k2, Lock{txnID: k2})
			checkSeek(t, s, k1, k1, k1)
			checkSeek(t, s, k2, k2, k2)
			checkSeek(t, s, k3, nil, nil)

			s.Add(k3, Lock{txnID: k3})
			checkSeek(t, s, k1, k1, k1)
			checkSeek(t, s, k2, k2, k2)
			checkSeek(t, s, k3, k3, k3)
		})
	}
}

func BenchmarkAdd(b *testing.B) {
	for name, factory := range storages {
		b.Run(name, func(b *testing.B) {
			s := factory()
			keys := make([][]byte, b.N)
			for i := 0; i < b.N; i++ {
				keys[i] = make([]byte, 4)
				binary.BigEndian.PutUint32(keys[i], rand.Uint32())
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Add(keys[i], Lock{txnID: keys[i]})
			}
		})
	}
}

func BenchmarkSeek(b *testing.B) {
	for name, factory := range storages {
		b.Run(name, func(b *testing.B) {
			s := factory()
			keys := make([][]byte, b.N)
			for i := 0; i < b.N; i++ {
				keys[i] = make([]byte, 4)
				binary.BigEndian.PutUint32(keys[i], rand.Uint32())
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Seek(keys[i])
			}
		})
	}
}

func BenchmarkDelete(b *testing.B) {
	for name, factory := range storages {
		b.Run(name, func(b *testing.B) {
			s := factory()
			keys := make([][]byte, b.N)
			for i := 0; i < b.N; i++ {
				keys[i] = make([]byte, 4)
				binary.BigEndian.PutUint32(keys[i], rand.Uint32())
				s.Add(keys[i], Lock{txnID: keys[i]})
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Delete(keys[i])
			}
		})
	}
}

func checkSeek(t *testing.T, s LockStorage, key []byte, expectKey, expectValue []byte) {
	k, v, ok := s.Seek(key)
	if len(expectKey) == 0 {
		assert.Empty(t, k)
		assert.Empty(t, v)
		assert.False(t, ok)
		return
	}
	assert.Equal(t, expectKey, k)
	assert.Equal(t, expectValue, v.txnID)
	assert.True(t, ok)
}
