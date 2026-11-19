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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func BenchmarkInt64HashMapFindBatchLarge(b *testing.B) {
	const (
		numKeys   = 1 << 20
		batchSize = 8192
	)

	mp := mpool.MustNewZero()
	defer func() {
		if mp.CurrNB() != 0 {
			b.Fatalf("memory leak detected: %d bytes", mp.CurrNB())
		}
	}()

	keys := make([]uint64, numKeys)
	hashes := make([]uint64, numKeys)
	values := make([]uint64, numKeys)
	for i := range keys {
		keys[i] = uint64(i + 1)
	}
	Int64BatchHash(toUnsafePointer(&keys[0]), &hashes[0], len(keys))

	var ht Int64HashMap
	if err := ht.Init(mp); err != nil {
		b.Fatal(err)
	}
	defer ht.Free()
	if err := ht.InsertBatch(len(keys), hashes, toUnsafePointer(&keys[0]), values); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.SetBytes(batchSize * 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := (i * batchSize) & (numKeys - 1)
		ht.FindBatch(
			batchSize,
			hashes[offset:offset+batchSize],
			toUnsafePointer(&keys[offset]),
			values[offset:offset+batchSize],
		)
	}
}

// Keep scale-dependent throughput checks in benchmarks. Each timed operation
// plans a batch against the same immutable table, so growth and fixture setup
// do not obscure the cost of existing/missing/skewed probes.
func BenchmarkInt64HashMapPlanInsert(b *testing.B) {
	const batchSize = 256
	for _, cardinality := range []int{16, 1 << 15, 1 << 20, 1 << 22} {
		for _, distribution := range []string{"hits", "mixed", "misses", "skew", "window", "clustered", "shuffled"} {
			b.Run(fmt.Sprintf("groups=%d/%s", cardinality, distribution), func(b *testing.B) {
				mp := mpool.MustNewZero()
				var ht Int64HashMap
				if err := ht.Init(mp); err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					ht.Free()
					if mp.CurrNB() != 0 {
						b.Fatalf("retained hash memory: %d", mp.CurrNB())
					}
				})
				keys := make([]uint64, cardinality)
				hashes := make([]uint64, cardinality)
				values := make([]uint64, cardinality)
				for i := range keys {
					keys[i] = uint64(i + 1)
					hashes[i] = Int64HashWithFixedSeed(keys[i])
				}
				if err := ht.InsertBatch(len(keys), hashes, toUnsafePointer(&keys[0]), values); err != nil {
					b.Fatal(err)
				}
				inputSize := max(batchSize*16, cardinality)
				if distribution == "window" {
					inputSize = batchSize * 16
				}
				input, inputHashes := make([]uint64, inputSize), make([]uint64, inputSize)
				random := rand.New(rand.NewSource(42))
				for i := range input {
					input[i] = keys[random.Intn(cardinality)]
					if distribution == "misses" || distribution == "mixed" && i%5 == 0 {
						input[i] = uint64(cardinality + i + 1)
					}
					if (distribution == "clustered" || distribution == "shuffled") && i%8 != 0 {
						input[i] = input[i-i%8]
					}
					if distribution == "skew" && i%10 != 0 {
						input[i] = 1
					}
				}
				if distribution == "shuffled" {
					random.Shuffle(len(input), func(i, j int) { input[i], input[j] = input[j], input[i] })
				}
				for i, key := range input {
					inputHashes[i] = Int64HashWithFixedSeed(key)
				}
				var result, slots [batchSize]uint64
				var inserted [batchSize]uint8
				b.ReportAllocs()
				b.SetBytes(batchSize * 8)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					offset := i * batchSize % inputSize
					_, _, complete, err := ht.PlanInsertBatch(
						batchSize, uint64(cardinality), nil, inputHashes[offset:offset+batchSize],
						nil, result[:], slots[:], inserted[:], false, true)
					if err != nil || !complete {
						b.Fatalf("incomplete preview: %v", err)
					}
				}
			})
		}
	}
}

func BenchmarkStringHashMapPlanInsert(b *testing.B) {
	const batchSize = 256
	for _, cardinality := range []int{16, 1 << 15, 1 << 20, 1 << 22} {
		for _, distribution := range []string{"hits", "mixed", "misses", "skew", "window", "clustered", "shuffled"} {
			b.Run(fmt.Sprintf("groups=%d/%s", cardinality, distribution), func(b *testing.B) {
				mp := mpool.MustNewZero()
				var ht StringHashMap
				if err := ht.Init(mp); err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() {
					ht.Free()
					if mp.CurrNB() != 0 {
						b.Fatalf("retained hash memory: %d", mp.CurrNB())
					}
				})
				keys := make([]uint64, cardinality)

				for i := range keys {
					keys[i] = uint64(i + 1)
				}
				if err := ht.ResizeOnDemand(uint64(cardinality)); err != nil {
					b.Fatal(err)
				}
				for i, key := range keys {
					state := [3]uint64{Int64HashWithFixedSeed(key), key, key ^ m1}
					cell := ht.findCell(&state)
					cell.HashState, cell.Mapped = state, uint64(i+1)
				}
				ht.elemCnt = uint64(cardinality)
				inputSize := max(batchSize*16, cardinality)
				if distribution == "window" {
					inputSize = batchSize * 16
				}
				input, inputHashes := make([]uint64, inputSize), make([][3]uint64, inputSize)
				random := rand.New(rand.NewSource(42))
				for i := range input {
					input[i] = keys[random.Intn(cardinality)]
					if distribution == "misses" || distribution == "mixed" && i%5 == 0 {
						input[i] = uint64(cardinality + i + 1)
					}
					if (distribution == "clustered" || distribution == "shuffled") && i%8 != 0 {
						input[i] = input[i-i%8]
					}
					if distribution == "skew" && i%10 != 0 {
						input[i] = 1
					}
				}
				if distribution == "shuffled" {
					random.Shuffle(len(input), func(i, j int) { input[i], input[j] = input[j], input[i] })
				}
				for i, key := range input {
					inputHashes[i] = [3]uint64{Int64HashWithFixedSeed(key), key, key ^ m1}
				}
				var result, slots [batchSize]uint64
				var inserted [batchSize]uint8
				b.ReportAllocs()
				b.SetBytes(batchSize * 8)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					offset := i * batchSize % inputSize
					_, _, complete, err := ht.PlanInsertStringBatch(
						uint64(cardinality), nil, inputHashes[offset:offset+batchSize],
						result[:], slots[:], inserted[:], false)
					if err != nil || !complete {
						b.Fatalf("incomplete preview: %v", err)
					}
				}
			})
		}
	}
}

func TestBatchInsertPlansPreserveSequentialIdentity(t *testing.T) {
	for _, test := range []struct {
		rows    int
		reserve uint64
	}{
		{rows: 0}, {rows: 1}, {rows: 256}, {rows: 256, reserve: maxIntCellCntPerBlock},
	} {
		rows := test.rows
		t.Run(fmt.Sprintf("rows=%d/reserve=%d", rows, test.reserve), func(t *testing.T) {
			mp := mpool.MustNewZero()
			var ht Int64HashMap
			require.NoError(t, ht.Init(mp))
			t.Cleanup(func() {
				ht.Free()
				require.Zero(t, mp.CurrNB())
			})
			// Collide at the end to exercise wraparound, segment addressing
			// and tentative-slot conflicts without filling a large table.
			require.NoError(t, ht.ResizeWithPlan(ht.PlanResize(test.reserve)))
			hashFor := func(key int) uint64 {
				return ht.cellCntMask - 5 + uint64(key)*ht.cellCnt
			}
			const existing = 17
			initial := make([]uint64, existing)
			initialValues := make([]uint64, existing)
			model := make(map[uint64]uint64)
			for i := range initial {
				initial[i] = hashFor(i)
				model[initial[i]] = uint64(i + 1)
			}
			require.NoError(t, ht.InsertBatch(existing, initial, nil, initialValues))
			hashes, values, slots := make([]uint64, rows), make([]uint64, rows), make([]uint64, rows)
			flags, expectedFlags := make([]uint8, rows), make([]uint8, rows)
			zValues, expectedValues := make([]int64, rows), make([]uint64, rows)
			for i := range hashes {
				key := i / 3
				if i%4 == 0 {
					key = i % existing
				}
				hashes[i] = hashFor(key)
				if i%11 == 0 {
					continue
				}
				zValues[i] = 1
				id, found := model[hashes[i]]
				if !found {
					id = uint64(len(model) + 1)
					model[hashes[i]] = id
					expectedFlags[i] = 1
				}
				expectedValues[i] = id
			}
			added, version, complete, err := ht.PlanInsertBatch(
				rows, existing, zValues, hashes, nil, values, slots, flags, true, true)
			require.NoError(t, err)
			require.True(t, complete)
			require.Equal(t, uint64(existing), ht.Cardinality(), "preview must remain immutable")
			require.Equal(t, uint64(len(model)-existing), added)
			require.Equal(t, expectedValues, values)
			require.Equal(t, expectedFlags, flags)
			require.NoError(t, ht.CommitInsertBatchPlan(version, existing, hashes, values, slots, flags))
			require.Equal(t, uint64(len(model)), ht.Cardinality())
			clear(values)
			require.NoError(t, ht.FindPrehashedBatch(zValues, hashes, values, true))
			require.Equal(t, expectedValues, values)
		})
	}
}

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

func TestInt64HashMapFindBatchSegmentLayouts(t *testing.T) {
	for _, blockBits := range []uint8{4, 3} {
		t.Run(fmt.Sprintf("block-bits-%d", blockBits), func(t *testing.T) {
			// The same collision chain crosses the mask boundary in one block
			// and a physical block boundary in the segmented layout. Tiny Go-
			// owned cells exercise lookup without allocating native 4 MiB blocks.
			var cells [16]Int64HashMapCell
			cells[15] = Int64HashMapCell{Key: 15, Mapped: 1}
			cells[0] = Int64HashMapCell{Key: 31, Mapped: 2}
			cells[1] = Int64HashMapCell{Key: 47, Mapped: 3}
			before := cells
			ht := Int64HashMap{blockCellCntBits: blockBits, cellCntMask: 15, cellCnt: 16, elemCnt: 3}
			for start := 0; start < len(cells); start += 1 << blockBits {
				ht.cells = append(ht.cells, cells[start:start+(1<<blockBits)])
			}
			hashes := []uint64{15, 31, 47, 63, 15, 0}
			got := make([]uint64, len(hashes))
			ht.FindBatch(len(hashes), hashes, nil, got)
			require.Equal(t, []uint64{1, 2, 3, 0, 1, 0}, got)
			require.Equal(t, before, cells, "shared build-side lookup must be read-only")
		})
	}
}

func TestInt64HashMap_MarshalUnmarshal_Empty(t *testing.T) {
	m := mpool.MustNewZero()
	originalMap := &Int64HashMap{}
	err := originalMap.Init(m)
	require.NoError(t, err)
	defer originalMap.Free()

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &Int64HashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, m)
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(0), unmarshaledMap.elemCnt)
	require.Equal(t, uint64(0), unmarshaledMap.Cardinality())
	require.Equal(t, uint64(kInitialCellCnt), unmarshaledMap.cellCnt)
}

func TestInt64HashMap_MarshalUnmarshal_SingleElement(t *testing.T) {
	m := mpool.MustNewZero()
	originalMap := &Int64HashMap{}
	err := originalMap.Init(m)
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
	err = unmarshaledMap.UnmarshalBinary(marshaledData, m)
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
	m := mpool.MustNewZero()
	originalMap := &Int64HashMap{}
	err := originalMap.Init(m)
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
	err = unmarshaledMap.UnmarshalBinary(marshaledData, m)
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
	m := mpool.MustNewZero()
	originalMap := &Int64HashMap{}
	err := originalMap.Init(m)
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
	err = unmarshaledMap.UnmarshalBinary(marshaledData, m)
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
	m := mpool.MustNewZero()
	originalMap := &StringHashMap{}
	err := originalMap.Init(m)
	require.NoError(t, err)
	defer originalMap.Free()

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &StringHashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, m)
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(0), unmarshaledMap.elemCnt)
	require.Equal(t, uint64(kInitialCellCnt), unmarshaledMap.cellCnt)
}

func TestStringHashMap_MarshalUnmarshal_SingleElement(t *testing.T) {
	m := mpool.MustNewZero()
	originalMap := &StringHashMap{}
	err := originalMap.Init(m)
	require.NoError(t, err)
	defer originalMap.Free()

	key := bytes.Repeat([]byte("abc"), 32)
	keys := [][]byte{key}
	states := make([][3]uint64, 1)
	values := make([]uint64, 1)

	err = originalMap.InsertStringBatch(states, keys, values)
	require.NoError(t, err)
	expectedMappedValue := values[0]

	marshaledData, err := originalMap.MarshalBinary()
	require.NoError(t, err)

	unmarshaledMap := &StringHashMap{}
	err = unmarshaledMap.UnmarshalBinary(marshaledData, m)
	require.NoError(t, err)
	defer unmarshaledMap.Free()

	require.Equal(t, uint64(1), unmarshaledMap.elemCnt)

	// Verify by finding the key
	foundValues := make([]uint64, 1)
	unmarshaledMap.FindStringBatch(states, keys, foundValues)
	require.Equal(t, expectedMappedValue, foundValues[0])
}

func TestStringHashMap_MarshalUnmarshal_MultipleElementsNoResize(t *testing.T) {
	m := mpool.MustNewZero()
	originalMap := &StringHashMap{}
	err := originalMap.Init(m)
	require.NoError(t, err)
	defer originalMap.Free()

	numElements := 50
	originalKeys := make([][]byte, numElements)
	originalMappedValues := make(map[string]uint64)

	for i := 0; i < numElements; i++ {
		key := bytes.Repeat(fmt.Appendf(nil, "key_%d", i), 32)
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
	err = unmarshaledMap.UnmarshalBinary(marshaledData, m)
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
	m := mpool.MustNewZero()
	originalMap := &StringHashMap{}
	err := originalMap.Init(m)
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
	err = unmarshaledMap.UnmarshalBinary(marshaledData, m)
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
	memPool := mpool.MustNewZero()
	t.Run("int64", func(t *testing.T) {
		m := new(Int64HashMap)
		require.NoError(t, m.Init(memPool))
		defer m.Free()

		// Test error on empty map
		w := &errorAfterNWriter{N: 4} // Not enough for elemCnt
		_, err := m.WriteTo(w)
		require.Error(t, err)

		// Add an element
		key := uint64(1)
		hashes := make([]uint64, 1)
		values := make([]uint64, 1)
		err = m.InsertBatch(1, hashes, toUnsafePointer(&key), values)
		require.NoError(t, err)

		// Test error after writing elemCnt
		w = &errorAfterNWriter{N: 8} // Enough for elemCnt, but not for the first cell's key
		_, err = m.WriteTo(w)
		require.Error(t, err)

		// Test error after writing elemCnt and key
		w = &errorAfterNWriter{N: 16} // Enough for elemCnt and key, but not for mapped value
		_, err = m.WriteTo(w)
		require.Error(t, err)
	})

	t.Run("string", func(t *testing.T) {
		m := new(StringHashMap)
		require.NoError(t, m.Init(memPool))
		defer m.Free()

		// Test error on empty map
		w := &errorAfterNWriter{N: 4} // Not enough for elemCnt
		_, err := m.WriteTo(w)
		require.Error(t, err)

		// Add an element
		keys := [][]byte{
			bytes.Repeat([]byte("a"), 32),
		}
		states := make([][3]uint64, 1)
		values := make([]uint64, 1)
		err = m.InsertStringBatch(states, keys, values)
		require.NoError(t, err)

		// Test error after writing elemCnt
		w = &errorAfterNWriter{N: 8} // Enough for elemCnt, but not for HashState
		_, err = m.WriteTo(w)
		require.Error(t, err)

		// Test error after writing part of the cell
		w = &errorAfterNWriter{N: 8 + 8} // Enough for elemCnt and HashState
		_, err = m.WriteTo(w)
		require.Error(t, err)

		w = &errorAfterNWriter{N: 8 + 16} // Enough for elemCnt and HashState
		_, err = m.WriteTo(w)
		require.Error(t, err)

		w = &errorAfterNWriter{N: 8 + 24} // Enough for elemCnt and HashState
		_, err = m.WriteTo(w)
		require.Error(t, err)
	})
}

func TestUnmarshalRejectsCountLargerThanBoundedRecord(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	var count [8]byte
	binary.LittleEndian.PutUint64(count[:], 1<<40)

	t.Run("int64", func(t *testing.T) {
		var hashMap Int64HashMap
		_, err := hashMap.UnmarshalFrom(bytes.NewReader(count[:]), mp)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})
	t.Run("string", func(t *testing.T) {
		var hashMap StringHashMap
		_, err := hashMap.UnmarshalFrom(bytes.NewReader(count[:]), mp)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})
}

func TestCommitInsertPlanRejectsDuplicateSlotsBeforeMutation(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	t.Run("int64", func(t *testing.T) {
		var hashMap Int64HashMap
		require.NoError(t, hashMap.Init(mp))
		defer hashMap.Free()
		err := hashMap.CommitInsertBatchPlan(
			hashMap.Version(), 0,
			[]uint64{11, 22}, []uint64{1, 2},
			[]uint64{0, 0}, []uint8{1, 1})
		require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
		require.Zero(t, hashMap.Cardinality())
	})

	t.Run("string", func(t *testing.T) {
		var hashMap StringHashMap
		require.NoError(t, hashMap.Init(mp))
		defer hashMap.Free()
		err := hashMap.CommitInsertStringBatchPlan(
			hashMap.Version(), 0,
			[][3]uint64{{11, 1, 2}, {22, 3, 4}}, []uint64{1, 2},
			[]uint64{0, 0}, []uint8{1, 1})
		require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
		require.Zero(t, hashMap.Cardinality())
	})
}

func TestCommitInsertPlanRollsBackLateInvalidRow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	t.Run("int64", func(t *testing.T) {
		var hashMap Int64HashMap
		require.NoError(t, hashMap.Init(mp))
		defer hashMap.Free()
		err := hashMap.CommitInsertBatchPlan(
			hashMap.Version(), 0,
			[]uint64{11, 22, 33}, []uint64{1, 2, 3},
			[]uint64{0, 1, 2}, []uint8{1, 1, 2})
		require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
		require.Zero(t, hashMap.Cardinality())
		for slot := uint64(0); slot < 2; slot++ {
			require.Equal(t, Int64HashMapCell{}, *hashMap.cellAt(slot))
		}
	})

	t.Run("string", func(t *testing.T) {
		var hashMap StringHashMap
		require.NoError(t, hashMap.Init(mp))
		defer hashMap.Free()
		err := hashMap.CommitInsertStringBatchPlan(
			hashMap.Version(), 0,
			[][3]uint64{{11, 1, 2}, {22, 3, 4}, {33, 5, 6}},
			[]uint64{1, 2, 3}, []uint64{0, 1, 2}, []uint8{1, 1, 2})
		require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
		require.Zero(t, hashMap.Cardinality())
		for slot := uint64(0); slot < 2; slot++ {
			require.Equal(t, StringHashMapCell{}, *hashMap.cellAt(slot))
		}
	})
}
