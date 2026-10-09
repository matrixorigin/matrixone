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

package sort

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestUnicodeCollationSortConsumerRelease(t *testing.T) {
	for _, charset := range []uint8{types.CharsetLegacy, types.CharsetBinary, types.CharsetUTF8MB4UnicodeCI} {
		for repeat := 0; repeat < 16; repeat++ {
			mp := mpool.MustNewZero()
			vec := vector.NewVec(types.NewWithCharset(types.T_varchar, 64, 0, charset))
			require.NoError(t, vector.AppendBytesList(vec, makeUnicodeConsumerValues(64, 64), nil, mp))
			selectors := make([]int64, 64)
			for i := range selectors {
				selectors[i] = int64(i)
			}
			SortForSQLOrder(false, false, false, selectors, vec)
			vec.Free(mp)
			require.Zero(t, mp.CurrNB(), "sort consumer retained bytes for charset %d", charset)
		}
	}
}

// BenchmarkUnicodeCollationSortConsumers is the consumer-side measurement
// required by the Unicode collation resource contract. The vector is built
// once per case, while each iteration resets only the row selection and runs
// the same SQL ORDER BY comparator for the legacy and Unicode types.
func BenchmarkUnicodeCollationSortConsumers(b *testing.B) {
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
				values := makeUnicodeConsumerValues(size, rows)
				if err := vector.AppendBytesList(vec, values, nil, mp); err != nil {
					b.Fatal(err)
				}
				selectors := make([]int64, rows)
				b.ReportAllocs()
				b.SetBytes(int64(size * rows))
				b.ResetTimer()
				for b.Loop() {
					for i := range selectors {
						selectors[i] = int64(i)
					}
					SortForSQLOrder(false, false, false, selectors, vec)
				}
				b.StopTimer()
				vec.Free(mp)
				if got := mp.CurrNB(); got != 0 {
					b.Fatalf("sort consumer retained %d bytes after vector release", got)
				}
			})
		}
	}
}

func makeUnicodeConsumerValues(size, rows int) [][]byte {
	values := make([][]byte, rows)
	for row := range values {
		value := make([]byte, size)
		for i := range value {
			value[i] = byte('a' + (row+i)%26)
		}
		// Keep the values distinct while retaining a valid UTF-8 payload for
		// both the legacy and Unicode comparison domains.
		value[size-1] = byte('a' + row%26)
		values[row] = value
	}
	return values
}
