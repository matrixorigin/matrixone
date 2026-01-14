// Copyright 2021 Matrix Origin
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

package bloomfilter

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestCBloomFilter(t *testing.T) {
	bf := NewCBloomFilter(1000)
	assert.NotNil(t, bf)
	defer bf.Free()

	key1 := []byte("hello")
	key2 := []byte("world")
	key3 := []byte("matrixone")

	bf.Add(key1)
	bf.Add(key2)

	assert.True(t, bf.Test(key1))
	assert.True(t, bf.Test(key2))
	// key3 might be a false positive, but with 1000 bits and 3 keys it's unlikely
	assert.False(t, bf.Test(key3))

	// Test Marshal/Unmarshal
	data := bf.Marshal()
	assert.NotNil(t, data)

	bf2 := UnmarshalCBloomFilter(data)
	assert.NotNil(t, bf2)
	defer bf2.Free()

	assert.True(t, bf2.Test(key1))
	assert.True(t, bf2.Test(key2))
	assert.False(t, bf2.Test(key3))
}
