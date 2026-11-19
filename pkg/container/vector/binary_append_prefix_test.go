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

package vector

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func binaryAppendDestination(t testing.TB, accounted bool) (*mpool.MPool, *Vector, testVectorAllocationAccount) {
	t.Helper()
	mp := mpool.MustNewZero()
	var state testVectorAllocationAccount
	dst := NewOffHeapVecWithType(types.T_varchar.ToType())
	if accounted {
		state = newTestVectorAllocationAccount(t, 1<<20, 32)
		require.NoError(t, dst.SetAllocationAccount(state.selection))
	}
	t.Cleanup(func() {
		dst.Free(mp)
		if accounted {
			owner, ok := state.account.OwnerUsage(testVectorAllocationOwner)
			require.True(t, ok)
			require.Zero(t, owner.Current)
			finalizeTestVectorAllocationAccount(t, state)
		}
		require.Zero(t, mp.CurrNB())
		mpool.DeleteMPool(mp)
	})
	return mp, dst, state
}

func TestUnionBatchBinaryDomainNullablePrefix(t *testing.T) {
	tests := []struct {
		name                      string
		prefix, source            types.RuntimeStringDomain
		shape                     string
		allNullPrefix, nullSource bool
		rowMetadata, wantRows     bool
	}{
		{name: "inherit", shape: "flat"},
		{name: "binary-constant", prefix: types.RuntimeStringBinary, source: types.RuntimeStringBinary, shape: "constant"},
		{name: "all-null-prefix-conflict", source: types.RuntimeStringBinary, shape: "flat", allNullPrefix: true},
		{name: "live-prefix-conflict-window", source: types.RuntimeStringBinary, shape: "window", wantRows: true},
		{name: "binary-prefix-conflict", prefix: types.RuntimeStringBinary, shape: "flat", wantRows: true},
		{name: "text", source: types.RuntimeStringText, shape: "flat", wantRows: true},
		{name: "row-metadata", shape: "flat", rowMetadata: true, wantRows: true},
		{name: "all-null-source", prefix: types.RuntimeStringBinary, shape: "flat", nullSource: true},
	}
	for _, accounted := range []bool{false, true} {
		for _, tc := range tests {
			t.Run(fmt.Sprintf("%s/accounted=%t", tc.name, accounted), func(t *testing.T) {
				mp, dst, _ := binaryAppendDestination(t, accounted)
				require.NoError(t, AppendBytesList(dst, [][]byte{nil, []byte("old-a"), []byte("old-b")},
					[]bool{true, tc.allNullPrefix, tc.allNullPrefix}, mp))
				if tc.rowMetadata {
					require.NoError(t, dst.SetRuntimeStringDomainsWithMP([]types.RuntimeStringDomain{
						types.RuntimeStringInherit, types.RuntimeStringBinary, types.RuntimeStringText,
					}, mp))
				} else {
					require.NoError(t, dst.SetRuntimeStringDomainWithMP(tc.prefix, mp))
				}
				var src *Vector
				if tc.shape == "constant" {
					var err error
					src, err = NewConstBytes(types.T_varchar.ToType(), []byte("constant"), 4, mp)
					require.NoError(t, err)
				} else {
					src = NewVec(types.T_varchar.ToType())
				}
				ownedSource := src
				t.Cleanup(func() { ownedSource.Free(mp) })
				if tc.shape != "constant" {
					require.NoError(t, AppendBytesList(src, [][]byte{[]byte("outside"), []byte("append-a"), nil, []byte("outside")},
						[]bool{false, tc.nullSource, true, false}, mp))
				}
				require.NoError(t, src.SetRuntimeStringDomainWithMP(tc.source, mp))
				offset := int64(1)
				if tc.shape == "window" {
					parent := src
					window, err := parent.Window(1, 3)
					require.NoError(t, err)
					t.Cleanup(func() { window.Free(mp) })
					src, offset = window, 0
				}
				require.NoError(t, dst.UnionBatch(src, offset, 2, nil, mp))
				require.Equal(t, 5, dst.Length())
				require.Equal(t, tc.wantRows, dst.HasBinaryStringRows())
				values := []string{"", "old-a", "old-b", "append-a", ""}
				if tc.shape == "constant" {
					values[3], values[4] = "constant", "constant"
				}
				for row := range 5 {
					isNull := row == 0 || (row < 3 && tc.allNullPrefix) ||
						(row == 3 && tc.nullSource) || (row == 4 && tc.shape != "constant")
					require.Equal(t, isNull, dst.IsNull(uint64(row)), "row %d", row)
					domain := tc.source
					if row < 3 {
						domain = tc.prefix
						if tc.rowMetadata && row == 1 {
							domain = types.RuntimeStringBinary
						} else if tc.rowMetadata && row == 2 {
							domain = types.RuntimeStringText
						}
					}
					if isNull {
						domain = types.RuntimeStringInherit
					} else {
						require.Equal(t, values[row], dst.GetStringAt(row), "row %d", row)
					}
					require.Equal(t, domain, dst.GetRuntimeStringDomainAt(row), "row %d", row)
				}
			})
		}
	}
}

func BenchmarkUnionBatchBinaryDomainNullablePrefix(b *testing.B) {
	for _, prefix := range []int{64, 4096} {
		for _, mode := range []string{"inherit", "binary", "conflict"} {
			b.Run(fmt.Sprintf("prefix=%d/%s", prefix, mode), func(b *testing.B) {
				const chunk, chunks = 32, 128
				mp := mpool.MustNewZero()
				template := NewVec(types.T_varchar.ToType())
				src := NewVec(types.T_varchar.ToType())
				defer func() {
					template.Free(mp)
					src.Free(mp)
					require.Zero(b, mp.CurrNB())
					mpool.DeleteMPool(mp)
				}()
				for row := range prefix {
					require.NoError(b, AppendBytes(template, []byte("payload-for-binary-domain-prefix"), row%3 == 0, mp))
				}
				for row := range chunk {
					require.NoError(b, AppendBytes(src, []byte("payload-for-binary-domain-append"), row%3 == 0, mp))
				}
				if mode == "binary" {
					require.NoError(b, template.SetRuntimeStringDomainWithMP(types.RuntimeStringBinary, mp))
				}
				if mode != "inherit" {
					require.NoError(b, src.SetRuntimeStringDomainWithMP(types.RuntimeStringBinary, mp))
				}
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					func() {
						dst, err := template.Dup(mp)
						require.NoError(b, err)
						defer dst.Free(mp)
						require.NoError(b, dst.PreExtendWithArea(prefix+chunk*chunks, (prefix+chunk*chunks)*32, mp))
						require.NoError(b, dst.PreExtendBitmap(prefix+chunk*chunks, mp))
						b.StartTimer()
						for range chunks {
							if err := dst.UnionBatch(src, 0, chunk, nil, mp); err != nil {
								b.Fatal(err)
							}
						}
						b.StopTimer()
						require.Equal(b, prefix+chunk*chunks, dst.Length())
					}()
					b.StartTimer()
				}
			})
		}
	}
}
