// Copyright 2026 Matrix Origin
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

package hashtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestStableBytesHashGolden(t *testing.T) {
	keys := [][]byte{
		nil,
		{},
		{0},
		[]byte("a"),
		append([]byte("a"), 0),
		bytes.Repeat([]byte{'x'}, 15),
		bytes.Repeat([]byte{'x'}, 16),
		bytes.Repeat([]byte{'x'}, 17),
		bytes.Repeat([]byte{'x'}, 32),
		[]byte("共同前缀-共同后缀"),
		{0, 1, 2, 0, 255, 128},
	}
	expected := []uint64{
		0,
		0,
		0xf0635260750aae02,
		0x4203c943b09c06d1,
		0x4e710223778a597d,
		0xdf08f6992594623b,
		0x1b63ccbaa1e87af3,
		0xf78aa190e24dfeaa,
		0x5a0e200acd583247,
		0x703c56d5000e0b54,
		0xe099babb897435e2,
	}

	for i := range keys {
		require.Equal(t, expected[i], StableBytesHash(keys[i]))
	}
	require.NotEqual(t, expected[3], expected[4], "logical length must distinguish zero-padded keys")
}

func TestWyhashWrapperPreservesProcessLocalMixer(t *testing.T) {
	key := []byte("process-local-hash-contract")
	pointer := unsafe.Pointer(unsafe.SliceData(key))
	require.Equal(t,
		wyhashWithSecret(pointer, 42, uint64(len(key)), hashkey[0]),
		wyhash(pointer, 42, uint64(len(key))))
}

var stableBytesHashBenchmarkSink uint64

func BenchmarkStableBytesHashByKeyLength(b *testing.B) {
	for _, keyLength := range []int{8, 15, 16, 32, 64, 1024, 64 << 10, 1 << 20} {
		rows := min(256, max(1, (4<<20)/keyLength))
		keys := make([][]byte, rows)
		for row := range keys {
			keys[row] = make([]byte, keyLength)
			for i := range keys[row] {
				keys[row][i] = byte(i*131 + 17)
			}
			if keyLength >= 8 {
				binary.LittleEndian.PutUint64(keys[row][keyLength-8:], uint64(row))
			}
		}

		b.Run(fmt.Sprintf("%dB/%drows", keyLength, rows), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(rows * keyLength))
			for b.Loop() {
				for i := range keys {
					stableBytesHashBenchmarkSink = StableBytesHash(keys[i])
				}
			}
		})
	}
}
