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

	"github.com/stretchr/testify/require"
)

func TestCBloomFilter_EmptySlice(t *testing.T) {
	// Create a bloom filter
	bf := NewCBloomFilterWithProbaility(100, 0.01)
	defer bf.Free()

	emptySlice := []byte{}
	var nilSlice []byte

	// 1. Test Add and Test with empty slice
	bf.Add(emptySlice)

	// Should be true after fix
	exists := bf.Test(emptySlice)
	require.True(t, exists, "Empty slice should be found after Add")

	// 2. Test TestAndAdd
	bf2 := NewCBloomFilterWithProbaility(100, 0.01)
	defer bf2.Free()

	present := bf2.TestAndAdd(emptySlice)
	require.False(t, present, "First TestAndAdd should return false (not present)")

	presentAgain := bf2.TestAndAdd(emptySlice)
	require.True(t, presentAgain, "Second TestAndAdd should return true (already present)")

	// 3. Verify nil slice behavior (should remain ignored/false)
	bf2.Add(nilSlice)
	require.False(t, bf2.Test(nilSlice), "Nil slice should not be found")
}
