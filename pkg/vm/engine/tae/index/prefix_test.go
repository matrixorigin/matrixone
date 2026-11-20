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

package index

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestPrefixInMatchesPrefixUnion(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	prefixSets := [][]string{
		nil, {""}, {"\x00x", "ab"}, {"a", "aa", "az"},
		{"az", "aa", "a"}, {"", "aaa", "zzz"}, {"b", "b", "a"},
		{"a", "b"}, {"missing"},
	}
	rows := []string{"", "\x00xy", "a", "aa", "ab", "az", "b", "ba", "zebra"}
	for _, prefixes := range prefixSets {
		func() {
			v := vector.NewVec(types.T_varchar.ToType())
			defer v.Free(mp)
			for _, prefix := range prefixes {
				require.NoError(t, vector.AppendBytes(v, []byte(prefix), false, mp))
			}
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			prepared, err := vector.UnmarshalPrefixInVector(data)
			defer prepared.Free(mp)
			require.NoError(t, err)
			for _, row := range rows {
				want := false
				for _, prefix := range prefixes {
					want = want || bytes.HasPrefix([]byte(row), []byte(prefix))
				}
				zm := BuildZM(types.T_varchar, []byte(row))
				require.Equal(t, want, zm.PrefixIn(prepared), "row=%q prefixes=%q", row, prefixes)
				for _, other := range rows {
					UpdateZM(zm, []byte(other))
					if want {
						require.True(t, zm.PrefixIn(prepared), "a range containing a match must be kept")
					}
				}
			}
		}()
	}
	require.Zero(t, mp.CurrNB())
}

func TestPrefixInUnknownBounds(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	v := vector.NewVec(types.T_varchar.ToType())
	defer v.Free(mp)
	require.NoError(t, vector.AppendBytes(v, []byte("z"), false, mp))
	unknown := NewZM(types.T_varchar, 0)
	truncated := BuildZM(types.T_varchar, bytes.Repeat([]byte{0xff}, 40))
	require.True(t, truncated.MaxTruncated())
	fixed := BuildZM(types.T_int64, types.EncodeInt64(new(int64)))
	for _, zm := range []ZM{nil, unknown, truncated, fixed} {
		require.True(t, zm.PrefixIn(v), "unknown or incompatible bounds cannot prove exclusion")
	}
	require.False(t, BuildZM(types.T_varchar, []byte("ab")).PrefixIn(v), "known disjoint zone still prunes")
	require.True(t, BuildZM(types.T_varchar, []byte("ab")).PrefixIn(nil))
	empty := vector.NewConstNull(types.T_varchar.ToType(), 0, mp)
	defer empty.Free(mp)
	require.False(t, unknown.PrefixIn(empty), "logical empty set has no physical candidate")
	v.GetNulls().Add(0)
	require.True(t, BuildZM(types.T_varchar, []byte("ab")).PrefixIn(v))
}

func BenchmarkPrefixInPrepared(b *testing.B) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for _, n := range []int{2, 128, 16384} {
		data := func() []byte {
			v := vector.NewVec(types.T_varchar.ToType())
			defer v.Free(mp)
			for i := range n {
				require.NoError(b, vector.AppendBytes(v, []byte(fmt.Sprintf("%08x", i*2)), false, mp))
			}
			data, err := v.MarshalBinary()
			require.NoError(b, err)
			return data
		}()
		prepared, err := vector.UnmarshalPrefixInVector(data)
		require.NoError(b, err)
		for _, hit := range []bool{false, true} {
			b.Run(fmt.Sprintf("n=%d/hit=%v", n, hit), func(b *testing.B) {
				key := n
				if !hit {
					key++
				}
				zm := BuildZM(types.T_varchar, []byte(fmt.Sprintf("%08xsuffix", key)))
				require.Equal(b, hit, zm.PrefixIn(prepared))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_ = zm.PrefixIn(prepared)
				}
			})
		}
		prepared.Free(mp)
	}
}
