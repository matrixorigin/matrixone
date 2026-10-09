// Copyright 2026 Matrix Origin
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

package shuffle

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestUnicodeCollationShuffleConsumerRelease(t *testing.T) {
	for _, charset := range []uint8{types.CharsetLegacy, types.CharsetBinary, types.CharsetUTF8MB4UnicodeCI} {
		for repeat := 0; repeat < 16; repeat++ {
			mp := mpool.MustNewZero()
			vec := vector.NewVec(types.NewWithCharset(types.T_varchar, 64, 0, charset))
			require.NoError(t, vector.AppendBytesList(vec, makeShuffleConsumerValues(64, 64), nil, mp))
			col, area := vector.MustVarlenaRawData(vec)
			arg := &Shuffle{}
			arg.ctr.stableStringHash = true
			sels := make([][]int32, 16)
			appendStringHashSels(arg, sels, vec, col, area, uint64(len(sels)), false)
			vec.Free(mp)
			require.Zero(t, mp.CurrNB(), "shuffle consumer retained bytes for charset %d", charset)
		}
	}
}

// BenchmarkUnicodeCollationShuffleConsumers measures the actual string-hash
// consumer used by the shuffle operator, rather than only the backend key
// builder. Both controls use the complete/stable hash path so the Unicode
// case includes its comparison-key materialization and bucket routing.
func BenchmarkUnicodeCollationShuffleConsumers(b *testing.B) {
	for _, size := range []int{8, 64, 1024} {
		rows := 256
		if size == 1024 {
			rows = 64
		}
		for _, tc := range []struct {
			name    string
			charset uint8
		}{
			{name: "legacy", charset: types.CharsetLegacy},
			{name: "binary", charset: types.CharsetBinary},
			{name: "unicode", charset: types.CharsetUTF8MB4UnicodeCI},
		} {
			b.Run(fmt.Sprintf("%s/%dB/%drows", tc.name, size, rows), func(b *testing.B) {
				mp := mpool.MustNewZero()
				vec := vector.NewVec(types.NewWithCharset(types.T_varchar, int32(size), 0, tc.charset))
				if err := vector.AppendBytesList(vec, makeShuffleConsumerValues(size, rows), nil, mp); err != nil {
					b.Fatal(err)
				}
				col, area := vector.MustVarlenaRawData(vec)
				arg := &Shuffle{}
				arg.ctr.stableStringHash = true
				sels := make([][]int32, 16)
				b.ReportAllocs()
				b.SetBytes(int64(size * rows))
				b.ResetTimer()
				for b.Loop() {
					for bucket := range sels {
						sels[bucket] = sels[bucket][:0]
					}
					appendStringHashSels(arg, sels, vec, col, area, uint64(len(sels)), false)
				}
				b.StopTimer()
				vec.Free(mp)
				if got := mp.CurrNB(); got != 0 {
					b.Fatalf("shuffle consumer retained %d bytes after vector release", got)
				}
			})
		}
	}
}

func makeShuffleConsumerValues(size, rows int) [][]byte {
	values := make([][]byte, rows)
	for row := range values {
		value := make([]byte, size)
		for i := range value {
			value[i] = byte('a' + (row+i)%26)
		}
		value[size-1] = byte('a' + row%26)
		values[row] = value
	}
	return values
}
