// Copyright 2021 - 2022 Matrix Origin
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

package mem

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetAndGetAndLenAdnDelete(t *testing.T) {
	key := []byte("k1")
	kv := NewKV()
	assert.Equal(t, 0, kv.Len())

	v, ok := kv.Get(key)
	assert.False(t, ok)
	assert.Empty(t, v)

	kv.Set(key, key)
	assert.Equal(t, 1, kv.Len())

	v, ok = kv.Get(key)
	assert.True(t, ok)
	assert.Equal(t, key, v)

	kv.Delete(key)
	assert.Equal(t, 0, kv.Len())

	v, ok = kv.Get(key)
	assert.False(t, ok)
	assert.Empty(t, v)
}

func TestAscendRange(t *testing.T) {
	kv := NewKV()

	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("k%d", i))
		kv.Set(k, k)
	}

	cases := []struct {
		from, to   []byte
		expectKeys [][]byte
	}{
		{
			from:       []byte("k0"),
			to:         []byte("k5"),
			expectKeys: [][]byte{[]byte("k0"), []byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")},
		},
		{
			from:       []byte("k0"),
			to:         []byte("k6"),
			expectKeys: [][]byte{[]byte("k0"), []byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")},
		},
	}

	for idx, c := range cases {
		var keys [][]byte
		kv.AscendRange(c.from, c.to, func(key, _ []byte) bool {
			keys = append(keys, key)
			return true
		})
		assert.Equal(t, c.expectKeys, keys, "idx %d", idx)
	}
}
