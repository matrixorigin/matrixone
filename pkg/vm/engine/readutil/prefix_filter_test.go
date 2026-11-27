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

package readutil

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func prefixFilterTable(sorted bool) *plan.TableDef {
	return &plan.TableDef{
		Name2ColIndex: map[string]int32{"k": 0},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"k"}, PkeyColName: "k"},
		Cols:          []*plan.ColDef{{Name: "k", Primary: sorted, Seqnum: 0, Typ: plan.Type{Id: int32(types.T_varchar)}}},
	}
}

func prefixFilterExpr(prefixes []string, mp *mpool.MPool) *plan.Expr {
	values := make([][]byte, len(prefixes))
	for i, prefix := range prefixes {
		values[i] = []byte(prefix)
	}
	expr := MakeInExprForTest(MakeColExprForTest(0, types.T_varchar, "k"), values, types.T_varchar, mp)
	expr.GetF().Func.ObjName = "prefix_in"
	expr.GetF().Func.Obj = function.PrefixInFunctionEncodedID
	return expr
}

func TestPrefixInReadFiltersPreserveMatches(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	encode := func(value int64) string {
		p := types.NewPacker()
		defer p.Close()
		p.EncodeInt64(value)
		return string(p.GetBuf())
	}
	for _, tc := range []struct {
		name           string
		prefixes, rows []string
	}{
		{"empty_set", nil, []string{"abc", "zebra"}},
		{"empty_prefix", []string{"", "zz"}, []string{"abc", "zebra"}},
		{"zero_leading", []string{"\x00x", "zz"}, []string{"\x00xy", "abc", "zebra"}},
		{"overlap", []string{"a", "aa", "az"}, []string{"aa", "ab", "az", "b"}},
		{"unsorted", []string{"az", "aa", "a", "a"}, []string{"aa", "ab", "az", "b"}},
		{"control", []string{"ab", "zz"}, []string{"aa", "abc", "zebra"}},
		// Causal simulation of serial_full over [1, 2, NULL], without bitmap NULL.
		{"encoded_order", []string{encode(1), encode(2), "\x00"}, []string{encode(1) + encode(9), encode(2) + encode(9), encode(3) + encode(9)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sort.Strings(tc.rows)
			expr := prefixFilterExpr(tc.prefixes, mp)
			before := bytes.Clone(expr.GetF().Args[1].GetVec().Data)
			rowVec := vector.NewVec(types.T_varchar.ToType())
			defer rowVec.Free(mp)
			meta := objectio.BuildMetaData(uint16(len(tc.rows)), 1)
			var wantRows []int64
			matches := make([]bool, len(tc.rows))
			for i, row := range tc.rows {
				require.NoError(t, vector.AppendBytes(rowVec, []byte(row), false, mp))
				meta.GetBlockMeta(uint32(i)).MustGetColumn(0).SetZoneMap(index.BuildZM(types.T_varchar, []byte(row)))
				for _, prefix := range tc.prefixes {
					matches[i] = matches[i] || bytes.HasPrefix([]byte(row), []byte(prefix))
				}
				if matches[i] {
					wantRows = append(wantRows, int64(i))
				}
			}
			pk, err := ConstructBasePKFilter(expr, prefixFilterTable(true), mp)
			require.NoError(t, err)
			defer pk.Cleanup()
			require.True(t, pk.Valid)
			sortedSearch, unsortedSearch, err := buildBlockPKSearchFuncs(pk, false)
			require.NoError(t, err)
			require.EqualValues(t, wantRows, sortedSearch(rowVec))
			require.EqualValues(t, wantRows, unsortedSearch(rowVec))
			for _, sorted := range []bool{false, true} {
				fast, _, _, block, seek, ok, _ := CompileFilterExpr(expr, prefixFilterTable(sorted), nil)
				require.True(t, ok)
				for i := range tc.rows {
					stop, keep, err := block(i, meta.GetBlockMeta(uint32(i)), nil)
					require.NoError(t, err)
					require.Equal(t, matches[i], keep, "row=%q", tc.rows[i])
					if stop {
						for _, remaining := range matches[i:] {
							require.False(t, remaining, "early stop must not lose any later match")
						}
					}
					if fast != nil {
						stats := objectio.NewObjectStats()
						require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(stats, meta.GetBlockMeta(uint32(i)).MustGetColumn(0).ZoneMap()))
						keep, err := fast(stats)
						require.NoError(t, err)
						require.Equal(t, matches[i], keep)
					}
				}
				if seek != nil {
					for _, skipped := range matches[:seek(meta)] {
						require.False(t, skipped, "seek must not skip a matching block")
					}
				}
			}
			require.Equal(t, before, expr.GetF().Args[1].GetVec().Data)
		})
	}
	require.Zero(t, mp.CurrNB())
}

func TestPrefixInSeekRetainsUnknownBlocks(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	expr := prefixFilterExpr([]string{"z"}, mp)
	_, _, _, block, seek, ok, _ := CompileFilterExpr(expr, prefixFilterTable(true), nil)
	require.True(t, ok)
	require.NotNil(t, seek)
	for _, truncated := range []bool{false, true} {
		for _, unknown := range []int{-1, 0, 1, 3} {
			meta := objectio.BuildMetaData(4, 1)
			for i, row := range []string{"a", "b", "c", "zz"} {
				zm := index.BuildZM(types.T_varchar, []byte(row))
				if i == unknown {
					if truncated {
						zm.SetMaxTruncated()
					} else {
						zm = index.NewZM(types.T_varchar, 0)
					}
				}
				meta.GetBlockMeta(uint32(i)).MustGetColumn(0).SetZoneMap(zm)
			}
			want := 3
			if unknown >= 0 {
				want = unknown
			}
			require.Equal(t, want, seek(meta), "unknown=%d truncated=%v", unknown, truncated)
			if unknown >= 0 {
				stop, keep, err := block(unknown, meta.GetBlockMeta(uint32(unknown)), nil)
				require.NoError(t, err)
				require.False(t, stop)
				require.True(t, keep)
			}
		}
	}
}

func TestPrefixInUnsafeVectorDisablesReadFilters(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	expr := prefixFilterExpr([]string{"ab"}, mp)
	valid := bytes.Clone(expr.GetF().Args[1].GetVec().Data)
	wire := func() []byte {
		nullable := vector.NewVec(types.T_varchar.ToType())
		defer nullable.Free(mp)
		require.NoError(t, vector.AppendBytes(nullable, []byte("ab"), false, mp))
		require.NoError(t, vector.AppendBytes(nullable, nil, true, mp))
		wire, err := nullable.MarshalBinary()
		require.NoError(t, err)
		return wire
	}()
	for _, data := range [][]byte{nil, valid[:len(valid)-1], wire} {
		expr.GetF().Args[1].GetVec().Data = data
		pk, err := ConstructBasePKFilter(expr, prefixFilterTable(true), mp)
		require.NoError(t, err)
		require.False(t, pk.Valid, "unsafe optional filter must leave the residual in control")
		_, _, _, _, _, ok, _ := CompileFilterExpr(expr, prefixFilterTable(true), nil)
		require.False(t, ok)
	}
	require.Zero(t, mp.CurrNB())
}

func BenchmarkPrefixInPrepare(b *testing.B) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for _, n := range []int{2, 128, 16384} {
		for _, sorted := range []bool{true, false} {
			b.Run(fmt.Sprintf("n=%d/sorted=%v", n, sorted), func(b *testing.B) {
				prefixes := make([]string, n)
				for i := range prefixes {
					pos := i
					if !sorted {
						pos = n - 1 - i
					}
					prefixes[i] = fmt.Sprintf("%08x", pos*2)
				}
				expr := prefixFilterExpr(prefixes, mp)
				// Exercise the fast path's claimed ordering as well as unsorted input.
				v := vector.NewVec(types.T_varchar.ToType())
				require.NoError(b, v.UnmarshalBinary(expr.GetF().Args[1].GetVec().Data))
				v.SetSorted(sorted)
				var err error
				expr.GetF().Args[1].GetVec().Data, err = v.MarshalBinary()
				require.NoError(b, err)
				v.Free(mp)
				table := prefixFilterTable(true)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					filter, err := ConstructBasePKFilter(expr, table, mp)
					if err != nil || !filter.Valid {
						b.Fatal("prefix filter preparation failed", err)
					}
					filter.Cleanup()
				}
			})
		}
	}
}

func BenchmarkPrefixInSeek(b *testing.B) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for _, n := range []int{128, 4096} {
		b.Run(fmt.Sprintf("blocks=%d", n), func(b *testing.B) {
			meta := objectio.BuildMetaData(uint16(n), 1)
			for i := range n {
				meta.GetBlockMeta(uint32(i)).MustGetColumn(0).SetZoneMap(index.BuildZM(types.T_varchar, []byte(fmt.Sprintf("%08d", i))))
			}
			expr := prefixFilterExpr([]string{fmt.Sprintf("%08d", n-1)}, mp)
			_, _, _, _, seek, ok, _ := CompileFilterExpr(expr, prefixFilterTable(true), nil)
			require.True(b, ok)
			require.NotNil(b, seek)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = seek(meta)
			}
		})
	}
}
