// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashtable

import (
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

var data = [][]byte{
	[]byte(""),
	[]byte("a"),
	[]byte("ab"),
	[]byte("abc"),
	[]byte("abcd"),
	[]byte("abcde"),
	[]byte("abcdef"),
	[]byte("abcdefg"),
	[]byte("abcdefgh"),
	[]byte("abcdefghi"),
	[]byte("abcdefghij"),
	[]byte("abcdefghijk"),
	[]byte("abcdefghijkl"),
	[]byte("abcdefghijklm"),
	[]byte("abcdefghijklmn"),
	[]byte("abcdefghijklmno"),
	[]byte("abcdefghijklmnop"),

	[]byte("aaaaaaaaaaaaaaaa"),
	[]byte("aaaaaaaaaaaaaaab"),
	[]byte("aaaaaaaaaaaaaabb"),
	[]byte("aaaaaaaaaaaaabbb"),
	[]byte("aaaaaaaaaaaabbbb"),
	[]byte("aaaaaaaaaaabbbbb"),
	[]byte("aaaaaaaaaabbbbbb"),
	[]byte("aaaaaaaaabbbbbbb"),
	[]byte("aaaaaaaabbbbbbbb"),
	[]byte("aaaaaaabbbbbbbbb"),
	[]byte("aaaaaabbbbbbbbbb"),
	[]byte("aaaaabbbbbbbbbbb"),
	[]byte("aaaabbbbbbbbbbbb"),
	[]byte("aaabbbbbbbbbbbbb"),
	[]byte("aabbbbbbbbbbbbbb"),
	[]byte("abbbbbbbbbbbbbbb"),
	[]byte("bbbbbbbbbbbbbbbb"),

	[]byte("Discard medicine more than two years old."),
	[]byte("He who has a shady past knows that nice guys finish last."),
	[]byte("I wouldn't marry him with a ten foot pole."),
	[]byte("Free! Free!/A trip/to Mars/for 900/empty jars/Burma Shave"),
	[]byte("The days of the digital watch are numbered.  -Tom Stoppard"),
	[]byte("Nepal premier won't resign."),
	[]byte("For every action there is an equal and opposite government program."),
	[]byte("His money is twice tainted: 'taint yours and 'taint mine."),
	[]byte("There is no reason for any individual to have a computer in their home. -Ken Olsen, 1977"),
	[]byte("It's a tiny change to the code and not completely disgusting. - Bob Manchek"),
	[]byte("size:  a.out:  bad magic"),
	[]byte("The major problem is with sendmail.  -Mark Horton"),
	[]byte("Give me a rock, paper and scissors and I will move the world.  CCFestoon"),
	[]byte("If the enemy is within range, then so are you."),
	[]byte("It's well we cannot hear the screams/That we create in others' dreams."),
	[]byte("You remind me of a TV show, but that's all right: I watch it anyway."),
	[]byte("C is as portable as Stonehedge!!"),
	[]byte("Even if I could be Shakespeare, I think I should still choose to be Faraday. - A. Huxley"),
	[]byte("The fugacity of a constituent in a mixture of gases at a given temperature is proportional to its mole fraction.  Lewis-Randall Rule"),
	[]byte("How can you write a big system without C++?  -Paul Glick"),
}

var golden = [][3]uint64{
	{0x57231643ef445469, 0x907b30e073265f45, 0x3dbe062192314be7},
	{0xac9e9519156d61b6, 0x907b30e0cb4e3795, 0x3dbe062192314be7},
	{0xfc172bf910cfb489, 0x907b30e0cb4e3795, 0xa92a358692314be7},
	{0xe380e8838517d5ad, 0x907b30e0cb4e3795, 0xa92a3586b2712bc7},
	{0xd6615c5690de91e7, 0xd3949c4ccb4e3795, 0xa92a3586b2712bc7},
	{0x664c36842476f444, 0x5c18104fcb4e3795, 0xa92a3586b2712bc7},
	{0xbc9df67caa16e686, 0x5c18104f7bfe4c5e, 0xa92a3586b2712bc7},
	{0x5286f5c9d84260e7, 0x5c18104f7bfe4c5e, 0x0d79c222b2712bc7},
	{0xcec5a6d57bc08698, 0x5c18104f7bfe4c5e, 0x0d79c222aa6527cb},
	{0x148024510ca2054d, 0x5c18104f7bfe4c5e, 0x0d79c2226ed092ba},
	{0x95c7ef559e3bf783, 0xb1f5d1637bfe4c5e, 0x0d79c2226ed092ba},
	{0x44cfbb1713178bd8, 0xb1f5d163234ea406, 0x0d79c2226ed092ba},
	{0xb3eb6026586bdac5, 0xb1f5d163234ea406, 0x8c350fef6ed092ba},
	{0xb47e6fe3421b9ad8, 0xb1f5d163234ea406, 0xdbf1cb7c6ed092ba},
	{0x0d58855ff3dd27e5, 0xb1f5d163234ea406, 0xdbf1cb7c07b94001},
	{0x86a874e7541aba8e, 0xb8e7ca6a234ea406, 0xdbf1cb7c07b94001},
	{0x178ce76052430bc8, 0xb8e7ca6a1ee03795, 0xdbf1cb7c07b94001},

	{0x12ad178c4d1ee33b, 0x124ef5b524b71cc7, 0x05243bcb3234f68d},
	{0x9e7bcd3bc92bdfd3, 0x124ef5b502820fd4, 0x05243bcb3234f68d},
	{0x4d703e8106cf3b74, 0x40ea03e702820fd4, 0x05243bcb3234f68d},
	{0x280c1b48b6c5177a, 0x40ea03e702820fd4, 0x05243bcba7a1c729},
	{0xc964aa8f22b4d041, 0x40ea03e702820fd4, 0xa5445b0ba7a1c729},
	{0xdfcd7ebebe26864b, 0x40ea03e702820fd4, 0xbd505707a7a1c729},
	{0x5d853b1cacc46e97, 0x40ea03e7008609d6, 0xbd505707a7a1c729},
	{0x5fad2c4d4bd151b3, 0xf75d7625008609d6, 0xbd505707a7a1c729},
	{0x32b4f455dcbf1a09, 0xf75d7625008609d6, 0xbd50570775ef89b5},
	{0xcbca8bcc5aa76096, 0xf75d7625008609d6, 0xbd5057071236370b},
	{0xf4487c5c7a272dff, 0xf75d7625008609d6, 0xe8faa8521236370b},
	{0x5527e6f273cfdf58, 0xf75d762544c2811a, 0xe8faa8521236370b},
	{0x7b43f98ad98f9496, 0xd7b49fec44c2811a, 0xe8faa8521236370b},
	{0x21653a5986192010, 0x925e304344c2811a, 0xe8faa8521236370b},
	{0x8a2be0cc55ee347e, 0x925e304344c2811a, 0xe8faa85260d2a179},
	{0x980b3114e71025f6, 0x925e304344c2811a, 0xf0e2987a60d2a179},
	{0xa7a33d25621c7562, 0x925e3043f85f1c3b, 0xf0e2987a60d2a179},

	{0xcff785df0b9b7d75, 0xfc0dd3539c80a99b, 0x61df14a1ab58ac9d},
	{0x96edc6e7be627a8f, 0x8df9e77e349fe316, 0xc87a0e284077850b},
	{0x66074051b892e14f, 0xd899939659baad31, 0x94a1828a15d7bb78},
	{0x206fb007ed76faf6, 0x58eecddfc68f169b, 0x33edca360805b586},
	{0xe8a83b6ba5105479, 0x8f33cf4f35c14090, 0x5a8cd440eae9b5f4},
	{0x909a31af7e26a48b, 0x559da96ebc928032, 0xb169282fbbbbf10d},
	{0xa802665115f68538, 0x280d3c804a4abb4f, 0x4aa68bb0f57a03da},
	{0x4a1925e05b4f60dc, 0x28ca4e72b699af7f, 0x6ef7148e66bc3ba0},
	{0xd970d0bd5edfe09d, 0x8975e8361e5c8613, 0x85066606aed75163},
	{0x18fcd7e6d51dfa35, 0xcf4540b6236bc67b, 0x5f28d41c24fd0ff4},
	{0xa4b08260426fd42b, 0xfd4d6f337735decc, 0x7612fdeeb417d78c},
	{0x25c2c14672724512, 0x545ad07a08a1a27f, 0x11ef0ad25b765a6c},
	{0x65590dcfdeb24a70, 0x926c6391b4edc372, 0x5ab9892ff2ec06f9},
	{0xb0b9c6ca8ddbecbd, 0xaffede505f89dcd6, 0x5c7504907eb2ae4b},
	{0x14a86da1e44ac18c, 0xbaffb312e4131a1f, 0xf20a893495216a46},
	{0x797ef5ae825df0fe, 0x23341f9f2abee954, 0x8d5c49cf932b17b8},
	{0x7c02cc3aa4da2f0d, 0x0e257b27eab088d3, 0xd07ca91c7439b4e4},
	{0x48d223630a9b37d5, 0xc37d3048537c6093, 0x902a07e6f97ce01d},
	{0xfd0df89810f88825, 0x85798a1253c0bbbb, 0x52df1e6ecf6d5505},
	{0x66fa326f6a2d8954, 0x98da25cf7a2638f8, 0x7b091be3b213701a},
}

func TestHashFn(t *testing.T) {
	fp1 := reflect.ValueOf(BytesBatchGenHashStates)
	fp2 := reflect.ValueOf(aesBytesBatchGenHashStates)
	if fp1.Pointer() != fp2.Pointer() {
		return
	}

	states := make([][3]uint64, len(data))
	for i := range data {
		if l := len(data[i]); l < 16 {
			data[i] = append(data[i], StrKeyPadding[l:]...)
		}
	}
	BytesBatchGenHashStates(&data[0], &states[0], len(data))
	for i := range data {
		if states[i] != golden[i] {
			t.Errorf("AesBytesHashState(%s) = {0x%016x, 0x%016x, 0x%016x} want {0x%016x, 0x%016x, 0x%016x}",
				data[i],
				states[i][0], states[i][1], states[i][2],
				golden[i][0], golden[i][1], golden[i][2])

		}
	}
}

func TestInt64HashMap_MarshalUnmarshal_Empty(t *testing.T) {
	originalMap := &Int64HashMap{}
	err := originalMap.Init(nil)
	require.NoError(t, err)
	defer originalMap.Free()

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &Int64HashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, DefaultAllocator())
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(0), unmarshaledMap.elemCnt)
	require.Equal(t, uint64(0), unmarshaledMap.Cardinality())
	require.Equal(t, uint64(kInitialCellCnt), unmarshaledMap.cellCnt)
}

func TestInt64HashMap_MarshalUnmarshal_SingleElement(t *testing.T) {
	originalMap := &Int64HashMap{}
	err := originalMap.Init(nil)
	require.NoError(t, err)
	defer originalMap.Free()

	key := uint64(12345)
	hashes := make([]uint64, 1)
	values := make([]uint64, 1)
	err = originalMap.InsertBatch(1, hashes, toUnsafePointer(&key), values)
	require.NoError(t, err)
	expectedMappedValue := values[0]

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &Int64HashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, DefaultAllocator())
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(1), unmarshaledMap.elemCnt)
	require.Equal(t, uint64(1), unmarshaledMap.Cardinality())

	// Verify by finding the key
	foundValues := make([]uint64, 1)
	unmarshaledMap.FindBatch(1, hashes, toUnsafePointer(&key), foundValues)
	require.Equal(t, expectedMappedValue, foundValues[0])
}

func TestInt64HashMap_MarshalUnmarshal_MultipleElementsNoResize(t *testing.T) {
	originalMap := &Int64HashMap{}
	err := originalMap.Init(nil)
	require.NoError(t, err)
	defer originalMap.Free()

	numElements := 50
	originalKeys := make([]uint64, numElements)
	originalMappedValues := make(map[uint64]uint64)

	for i := 0; i < numElements; i++ {
		key := uint64(rand.Int63())
		originalKeys[i] = key
		hashes := make([]uint64, 1)
		values := make([]uint64, 1)
		err = originalMap.InsertBatch(1, hashes, toUnsafePointer(&key), values)
		require.NoError(t, err)
		originalMappedValues[key] = values[0]
	}

	require.Equal(t, uint64(numElements), originalMap.elemCnt)

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &Int64HashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, DefaultAllocator())
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(numElements), unmarshaledMap.elemCnt)
	require.Equal(t, uint64(numElements), unmarshaledMap.Cardinality())

	// Verify all elements
	for _, key := range originalKeys {
		hashes := make([]uint64, 1)
		foundValues := make([]uint64, 1)
		unmarshaledMap.FindBatch(1, hashes, toUnsafePointer(&key), foundValues)
		require.Equal(t, originalMappedValues[key], foundValues[0], "key %d", key)
	}
}

func TestInt64HashMap_MarshalUnmarshal_MultipleElementsWithResize(t *testing.T) {
	originalMap := &Int64HashMap{}
	err := originalMap.Init(nil)
	require.NoError(t, err)
	defer originalMap.Free()

	numElements := 2000 // This should trigger multiple resizes
	originalKeys := make([]uint64, numElements)
	originalMappedValues := make(map[uint64]uint64)

	for i := 0; i < numElements; i++ {
		key := uint64(rand.Int63())
		originalKeys[i] = key
		hashes := make([]uint64, 1)
		values := make([]uint64, 1)
		err = originalMap.InsertBatch(1, hashes, toUnsafePointer(&key), values)
		require.NoError(t, err)
		originalMappedValues[key] = values[0]
	}

	require.Equal(t, uint64(numElements), originalMap.elemCnt)

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &Int64HashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, DefaultAllocator())
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(numElements), unmarshaledMap.elemCnt)
	require.Equal(t, uint64(numElements), unmarshaledMap.Cardinality())

	// Verify all elements
	for _, key := range originalKeys {
		hashes := make([]uint64, 1)
		foundValues := make([]uint64, 1)
		unmarshaledMap.FindBatch(1, hashes, toUnsafePointer(&key), foundValues)
		require.Equal(t, originalMappedValues[key], foundValues[0], "key %d", key)
	}
}

func TestStringHashMap_MarshalUnmarshal_Empty(t *testing.T) {
	originalMap := &StringHashMap{}
	err := originalMap.Init(nil)
	require.NoError(t, err)
	defer originalMap.Free()

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &StringHashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, DefaultAllocator())
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(0), unmarshaledMap.elemCnt)
	require.Equal(t, uint64(kInitialCellCnt), unmarshaledMap.cellCnt)
}

func TestStringHashMap_MarshalUnmarshal_SingleElement(t *testing.T) {
	originalMap := &StringHashMap{}
	err := originalMap.Init(nil)
	require.NoError(t, err)
	defer originalMap.Free()

	key := []byte("test_string_key")
	keys := [][]byte{key}
	states := make([][3]uint64, 1)
	values := make([]uint64, 1)

	err = originalMap.InsertStringBatch(states, keys, values)
	require.NoError(t, err)
	expectedMappedValue := values[0]

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &StringHashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, DefaultAllocator())
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(1), unmarshaledMap.elemCnt)

	// Verify by finding the key
	foundValues := make([]uint64, 1)
	unmarshaledMap.FindStringBatch(states, keys, foundValues)
	require.Equal(t, expectedMappedValue, foundValues[0])
}

func TestStringHashMap_MarshalUnmarshal_MultipleElementsNoResize(t *testing.T) {
	originalMap := &StringHashMap{}
	err := originalMap.Init(nil)
	require.NoError(t, err)
	defer originalMap.Free()

	numElements := 50
	originalKeys := make([][]byte, numElements)
	originalMappedValues := make(map[string]uint64)

	for i := 0; i < numElements; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		originalKeys[i] = key
		states := make([][3]uint64, 1)
		values := make([]uint64, 1)
		err = originalMap.InsertStringBatch(states, [][]byte{key}, values)
		require.NoError(t, err)
		originalMappedValues[string(key)] = values[0]
	}

	require.Equal(t, uint64(numElements), originalMap.elemCnt)

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &StringHashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, DefaultAllocator())
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(numElements), unmarshaledMap.elemCnt)

	// Verify all elements
	for _, key := range originalKeys {
		states := make([][3]uint64, 1)
		foundValues := make([]uint64, 1)
		unmarshaledMap.FindStringBatch(states, [][]byte{key}, foundValues)
		require.Equal(t, originalMappedValues[string(key)], foundValues[0], "key %s", string(key))
	}
}

func TestStringHashMap_MarshalUnmarshal_MultipleElementsWithResize(t *testing.T) {
	originalMap := &StringHashMap{}
	err := originalMap.Init(nil)
	require.NoError(t, err)
	defer originalMap.Free()

	numElements := 2000 // This should trigger multiple resizes
	originalKeys := make([][]byte, numElements)
	originalMappedValues := make(map[string]uint64)

	for i := 0; i < numElements; i++ {
		key := []byte(strconv.Itoa(i) + "_long_string_to_test_resize_behavior_and_hashing_across_blocks")
		originalKeys[i] = key
		states := make([][3]uint64, 1)
		values := make([]uint64, 1)
		err = originalMap.InsertStringBatch(states, [][]byte{key}, values)
		require.NoError(t, err)
		originalMappedValues[string(key)] = values[0]
	}

	require.Equal(t, uint64(numElements), originalMap.elemCnt)

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &StringHashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, DefaultAllocator())
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(numElements), unmarshaledMap.elemCnt)

	// Verify all elements
	for _, key := range originalKeys {
		states := make([][3]uint64, 1)
		foundValues := make([]uint64, 1)
		unmarshaledMap.FindStringBatch(states, [][]byte{key}, foundValues)
		require.Equal(t, originalMappedValues[string(key)], foundValues[0], "key %s", string(key))
	}
}

func TestWriteToError(t *testing.T) {
	t.Run("int64", func(t *testing.T) {
		m := new(Int64HashMap)
		require.NoError(t, m.Init(nil))
		defer m.Free()

		w := &errorAfterNWriter{
			N: -1,
		}
		_, err := m.WriteTo(w)
		require.Equal(t, io.ErrUnexpectedEOF, err)
		w.N = 0
		_, err = m.WriteTo(w)
		require.Equal(t, io.ErrUnexpectedEOF, err)
		w.N = 8
		_, err = m.WriteTo(w)
		require.Equal(t, io.ErrUnexpectedEOF, err)
		w.N = 16
		_, err = m.WriteTo(w)
		require.Equal(t, io.ErrUnexpectedEOF, err)
		w.N = 24
		_, err = m.WriteTo(w)
		require.Equal(t, io.ErrUnexpectedEOF, err)
	})

	t.Run("string", func(t *testing.T) {
		m := new(StringHashMap)
		require.NoError(t, m.Init(nil))
		defer m.Free()

		w := &errorAfterNWriter{
			N: -1,
		}
		_, err := m.WriteTo(w)
		require.Equal(t, io.ErrUnexpectedEOF, err)
		w.N = 0
		_, err = m.WriteTo(w)
		require.Equal(t, io.ErrUnexpectedEOF, err)
		w.N = 8
		_, err = m.WriteTo(w)
		require.Equal(t, io.ErrUnexpectedEOF, err)
		w.N = 16
		_, err = m.WriteTo(w)
		require.Equal(t, io.ErrUnexpectedEOF, err)
		w.N = 24
		_, err = m.WriteTo(w)
		require.Equal(t, io.ErrUnexpectedEOF, err)
	})
}

func TestInt64HashMap_BatchOperations(t *testing.T) {
	// Scenario 1: Basic Insertion and Verification
	t.Run("basic-insert-find", func(t *testing.T) {
		m := &Int64HashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		keys := []uint64{1, 2, 3, 4, 5}
		hashes := make([]uint64, len(keys))
		values := make([]uint64, len(keys))

		err = m.InsertBatch(len(keys), hashes, toUnsafePointer(&keys[0]), values)
		require.NoError(t, err)
		require.Equal(t, uint64(len(keys)), m.Cardinality())

		foundValues := make([]uint64, len(keys))
		m.FindBatch(len(keys), hashes, toUnsafePointer(&keys[0]), foundValues)
		require.Equal(t, values, foundValues)
		for _, v := range foundValues {
			require.NotEqual(t, uint64(0), v)
		}
	})

	// Scenario 2: Finding Non-existent Keys
	t.Run("find-non-existent", func(t *testing.T) {
		m := &Int64HashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		keys := []uint64{10, 11, 12}
		hashes := make([]uint64, len(keys))
		foundValues := make([]uint64, len(keys))
		m.FindBatch(len(keys), hashes, toUnsafePointer(&keys[0]), foundValues)
		for _, v := range foundValues {
			require.Equal(t, uint64(0), v)
		}
	})

	// Scenario 3: Handling Duplicate Keys
	t.Run("handle-duplicates", func(t *testing.T) {
		m := &Int64HashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		keys := []uint64{100, 101, 100, 102, 101}
		hashes := make([]uint64, len(keys))
		values := make([]uint64, len(keys))

		err = m.InsertBatch(len(keys), hashes, toUnsafePointer(&keys[0]), values)
		require.NoError(t, err)
		require.Equal(t, uint64(3), m.Cardinality()) // 100, 101, 102 are unique

		foundValues := make([]uint64, len(keys))
		m.FindBatch(len(keys), hashes, toUnsafePointer(&keys[0]), foundValues)
		require.Equal(t, values, foundValues)
		require.Equal(t, foundValues[0], foundValues[2]) // 100
		require.Equal(t, foundValues[1], foundValues[4]) // 101
		require.NotEqual(t, foundValues[0], foundValues[1])
		require.NotEqual(t, foundValues[1], foundValues[2])
	})

	// Scenario 4: Triggering Hash Map Resize
	t.Run("trigger-resize", func(t *testing.T) {
		m := &Int64HashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		numElements := 2000
		keys := make([]uint64, numElements)
		for i := 0; i < numElements; i++ {
			keys[i] = uint64(rand.Int63())
		}
		hashes := make([]uint64, len(keys))
		values := make([]uint64, len(keys))

		err = m.InsertBatch(len(keys), hashes, toUnsafePointer(&keys[0]), values)
		require.NoError(t, err)
		require.Equal(t, uint64(numElements), m.Cardinality())

		foundValues := make([]uint64, len(keys))
		m.FindBatch(len(keys), hashes, toUnsafePointer(&keys[0]), foundValues)
		require.Equal(t, values, foundValues)
	})

	// Scenario 5: Empty Batch
	t.Run("empty-batch", func(t *testing.T) {
		m := &Int64HashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		err = m.InsertBatch(0, nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.Cardinality())
		m.FindBatch(0, nil, nil, nil) // should not panic
	})

	// Test InsertBatchWithRing
	t.Run("insert-with-ring", func(t *testing.T) {
		m := &Int64HashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		keys := []uint64{200, 201, 202, 203, 204}
		zValues := []int64{1, 0, 1, 0, 1}
		hashes := make([]uint64, len(keys))
		values := make([]uint64, len(keys))

		err = m.InsertBatchWithRing(len(keys), zValues, hashes, toUnsafePointer(&keys[0]), values)
		require.NoError(t, err)
		require.Equal(t, uint64(3), m.Cardinality())

		foundValues := make([]uint64, len(keys))
		m.FindBatch(len(keys), hashes, toUnsafePointer(&keys[0]), foundValues)

		require.NotEqual(t, uint64(0), foundValues[0]) // key 200
		require.Equal(t, uint64(0), foundValues[1])    // key 201
		require.NotEqual(t, uint64(0), foundValues[2]) // key 202
		require.Equal(t, uint64(0), foundValues[3])    // key 203
		require.NotEqual(t, uint64(0), foundValues[4]) // key 204
	})

	t.Run("insert-with-ring-all-invalid", func(t *testing.T) {
		m := &Int64HashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		keys := []uint64{300, 301, 302}
		zValues := []int64{0, 0, 0}
		hashes := make([]uint64, len(keys))
		values := make([]uint64, len(keys))

		err = m.InsertBatchWithRing(len(keys), zValues, hashes, toUnsafePointer(&keys[0]), values)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.Cardinality())
	})
}

func TestStringHashMap_BatchOperations(t *testing.T) {
	// Scenario 1: Basic Insertion and Verification
	t.Run("basic-insert-find", func(t *testing.T) {
		m := &StringHashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		states := make([][3]uint64, len(keys))
		values := make([]uint64, len(keys))

		err = m.InsertStringBatch(states, keys, values)
		require.NoError(t, err)
		require.Equal(t, uint64(len(keys)), m.elemCnt)

		foundValues := make([]uint64, len(keys))
		m.FindStringBatch(states, keys, foundValues)
		require.Equal(t, values, foundValues)
		for _, v := range foundValues {
			require.NotEqual(t, uint64(0), v)
		}
	})

	// Scenario 2: Finding Non-existent Keys
	t.Run("find-non-existent", func(t *testing.T) {
		m := &StringHashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		keys := [][]byte{[]byte("x"), []byte("y"), []byte("z")}
		states := make([][3]uint64, len(keys))
		foundValues := make([]uint64, len(keys))
		m.FindStringBatch(states, keys, foundValues)
		for _, v := range foundValues {
			require.Equal(t, uint64(0), v)
		}
	})

	// Scenario 3: Handling Duplicate Keys
	t.Run("handle-duplicates", func(t *testing.T) {
		m := &StringHashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k1"), []byte("k3"), []byte("k2")}
		states := make([][3]uint64, len(keys))
		values := make([]uint64, len(keys))

		err = m.InsertStringBatch(states, keys, values)
		require.NoError(t, err)
		require.Equal(t, uint64(3), m.elemCnt) // k1, k2, k3 are unique

		foundValues := make([]uint64, len(keys))
		m.FindStringBatch(states, keys, foundValues)
		require.Equal(t, values, foundValues)
		require.Equal(t, foundValues[0], foundValues[2]) // k1
		require.Equal(t, foundValues[1], foundValues[4]) // k2
		require.NotEqual(t, foundValues[0], foundValues[1])
		require.NotEqual(t, foundValues[1], foundValues[2])
	})

	// Scenario 4: Triggering Hash Map Resize
	t.Run("trigger-resize", func(t *testing.T) {
		m := &StringHashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		numElements := 2000
		keys := make([][]byte, numElements)
		for i := 0; i < numElements; i++ {
			keys[i] = []byte(strconv.Itoa(i))
		}
		states := make([][3]uint64, len(keys))
		values := make([]uint64, len(keys))

		err = m.InsertStringBatch(states, keys, values)
		require.NoError(t, err)
		require.Equal(t, uint64(numElements), m.elemCnt)

		foundValues := make([]uint64, len(keys))
		m.FindStringBatch(states, keys, foundValues)
		require.Equal(t, values, foundValues)
	})

	// Scenario 5: Empty Batch
	t.Run("empty-batch", func(t *testing.T) {
		m := &StringHashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		err = m.InsertStringBatch(nil, nil, nil)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.elemCnt)
		m.FindStringBatch(nil, nil, nil) // should not panic
	})

	// Test InsertStringBatchWithRing
	t.Run("insert-with-ring", func(t *testing.T) {
		m := &StringHashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		keys := [][]byte{[]byte("s1"), []byte("s2"), []byte("s3"), []byte("s4"), []byte("s5")}
		zValues := []int64{1, 0, 1, 0, 1}
		states := make([][3]uint64, len(keys))
		values := make([]uint64, len(keys))

		err = m.InsertStringBatchWithRing(zValues, states, keys, values)
		require.NoError(t, err)
		require.Equal(t, uint64(3), m.elemCnt)

		foundValues := make([]uint64, len(keys))
		m.FindStringBatch(states, keys, foundValues)

		require.NotEqual(t, uint64(0), foundValues[0]) // s1
		require.Equal(t, uint64(0), foundValues[1])    // s2
		require.NotEqual(t, uint64(0), foundValues[2]) // s3
		require.Equal(t, uint64(0), foundValues[3])    // s4
		require.NotEqual(t, uint64(0), foundValues[4]) // s5
	})

	t.Run("insert-with-ring-all-invalid", func(t *testing.T) {
		m := &StringHashMap{}
		err := m.Init(nil)
		require.NoError(t, err)
		defer m.Free()

		keys := [][]byte{[]byte("s10"), []byte("s11"), []byte("s12")}
		zValues := []int64{0, 0, 0}
		states := make([][3]uint64, len(keys))
		values := make([]uint64, len(keys))

		err = m.InsertStringBatchWithRing(zValues, states, keys, values)
		require.NoError(t, err)
		require.Equal(t, uint64(0), m.elemCnt)
	})
}
