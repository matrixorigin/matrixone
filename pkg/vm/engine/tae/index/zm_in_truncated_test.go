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

func TestZoneMapInTruncatedUpperBound(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	for _, typ := range []types.T{types.T_char, types.T_varchar, types.T_json, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink} {
		for _, size := range []int{29, 30, 31, 32} {
			t.Run(fmt.Sprintf("%s/bytes=%d", typ, size), func(t *testing.T) {
				value := bytes.Repeat([]byte{0xff}, size)
				zm := NewZM(typ, 0)
				UpdateZM(zm, value)
				require.Equal(t, size > 30, zm.MaxTruncated())
				data, err := zm.Marshal()
				require.NoError(t, err)
				persisted := NewZM(typ, 0)
				require.NoError(t, persisted.Unmarshal(data))
				require.Equal(t, zm, persisted)
				for _, nullable := range []bool{false, true} {
					func() {
						keys := vector.NewVec(typ.ToType())
						defer keys.Free(mp)
						// 排序向量超过3项，确保 SubVecIn 不走小列表直接返回路径。
						for _, key := range [][]byte{{0}, {1}, {2}, {3}, value} {
							require.NoError(t, vector.AppendBytes(keys, key, false, mp))
						}
						if nullable {
							require.NoError(t, vector.AppendBytes(keys, nil, true, mp))
						}
						require.True(t, persisted.AnyIn(keys), "持久化上界被截断时不能排除真实匹配")
						lo, hi := persisted.SubVecIn(keys)
						require.LessOrEqual(t, lo, 4)
						require.Greater(t, hi, 4, "BloomFilter 窗口不能丢掉真实候选值")
					}()
				}
				for _, nullable := range []bool{false, true} {
					func() {
						keys := vector.NewVec(typ.ToType())
						defer keys.Free(mp)
						for _, key := range [][]byte{{0}, {1}, {2}, {3}} {
							require.NoError(t, vector.AppendBytes(keys, key, false, mp))
						}
						if nullable {
							require.NoError(t, vector.AppendBytes(keys, nil, true, mp))
						}
						require.False(t, persisted.AnyIn(keys), "未知上界不影响已知下界的排除能力")
					}()
				}
				for _, kind := range []string{"empty", "all_null", "const_null"} {
					func() {
						keys := vector.NewVec(typ.ToType())
						if kind == "const_null" {
							keys = vector.NewConstNull(typ.ToType(), 1, mp)
						} else if kind == "all_null" {
							require.NoError(t, vector.AppendBytes(keys, nil, true, mp))
						}
						defer keys.Free(mp)
						require.False(t, persisted.AnyIn(keys), kind)
					}()
				}
			})
		}
	}
}
