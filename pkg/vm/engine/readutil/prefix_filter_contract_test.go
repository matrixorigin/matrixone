// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package readutil

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

// Exercise the actual PK reader consumers, not just decoding or PK.Valid.
func TestPrefixInBlockFilterConstantSet(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k", ColPos: 0}}}
	table := &plan.TableDef{Name2ColIndex: map[string]int32{"k": 0}, Cols: []*plan.ColDef{{Name: "k", Typ: col.Typ}}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "k", Names: []string{"k"}}}
	for _, tc := range []struct {
		name     string
		constant bool
		length   int
		prefixes []string
	}{
		{"empty-set", false, 0, nil},
		{"constant-single", true, 1, []string{"abc"}},
		{"constant-two", true, 2, []string{"abc"}},
		{"constant-four", true, 4, []string{"abc"}},
		{"constant-empty-prefix", true, 4, []string{""}},
		{"flat-duplicates", false, 4, []string{"abc", "abc", "abc", "abc"}},
		{"flat-distinct", false, 2, []string{"abc", "zzz"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var src *vector.Vector
			if tc.constant {
				var err error
				src, err = vector.NewConstBytes(types.T_varchar.ToType(), []byte(tc.prefixes[0]), tc.length, mp)
				require.NoError(t, err)
			} else {
				src = vector.NewVec(types.T_varchar.ToType())
			}
			t.Cleanup(func() { src.Free(mp) })
			if !tc.constant {
				for _, prefix := range tc.prefixes {
					require.NoError(t, vector.AppendBytes(src, []byte(prefix), false, mp))
				}
			}
			src.SetSorted(true)
			payload, err := src.MarshalBinary()
			require.NoError(t, err)
			expr := plan2.MakeInExpr(context.Background(), col, int32(tc.length), payload, true)
			pk, err := ConstructBasePKFilter(expr, table, mp)
			require.NoError(t, err)
			if pk.Vec != nil {
				t.Cleanup(func() { pk.Vec.Free(mp) })
			}
			require.True(t, pk.Valid)
			require.NotNil(t, pk.Vec)
			filter, err := ConstructBlockPKFilter(false, pk, nil)
			require.NoError(t, err)
			if filter.Cleanup != nil {
				t.Cleanup(filter.Cleanup)
			}
			require.True(t, filter.Valid)
			for _, rows := range []struct {
				name string
				keys []string
			}{
				{"partial", []string{"abc0", "zzz0"}},
				{"all", []string{"abc0", "abc1"}},
				{"none", []string{"aaa0", "yyy0"}},
			} {
				for _, sorted := range []bool{true, false} {
					mode := "sorted"
					if !sorted {
						mode = "unsorted"
					}
					t.Run(rows.name+"/"+mode, func(t *testing.T) {
						keys := append([]string(nil), rows.keys...)
						if !sorted {
							keys[0], keys[1] = keys[1], keys[0]
						}
						data := vector.NewVec(types.T_varchar.ToType())
						t.Cleanup(func() { data.Free(mp) })
						var want []int64
						for row, key := range keys {
							require.NoError(t, vector.AppendBytes(data, []byte(key), false, mp))
							for _, prefix := range tc.prefixes {
								if bytes.HasPrefix([]byte(key), []byte(prefix)) {
									want = append(want, int64(row))
									break
								}
							}
						}
						search := filter.DecideSearchFunc(sorted)
						require.NotNil(t, search)
						var got []int64
						require.NotPanics(t, func() { got = search(containers.Vectors{*data}) })
						if len(want) == 0 {
							require.Empty(t, got)
						} else {
							require.Equal(t, want, got)
						}
					})
				}
			}
			wantLength := tc.length
			if tc.constant && wantLength > 1 {
				wantLength = 1
			}
			require.Equal(t, wantLength, pk.Vec.Length(), "membership sets must not expose repeated logical constant rows to PK search")
			// The decoded PK view belongs to this filter; normalization must not
			// rewrite the plan payload or change the generic decoder's contract.
			require.Equal(t, tc.length, src.Length())
			again := vector.TryUnmarshalBinaryVarlena(payload)
			require.NotNil(t, again)
			t.Cleanup(func() { again.Free(mp) })
			require.Equal(t, tc.length, again.Length())
		})
	}
}

func TestPrefixInRejectsIncompatibleVector(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	ints := vector.NewVec(types.T_int32.ToType())
	t.Cleanup(func() { ints.Free(mp) })
	require.NoError(t, vector.AppendFixed(ints, int32(7), false, mp))
	fixed, err := ints.MarshalBinary()
	require.NoError(t, err)
	strings := vector.NewVec(types.T_varchar.ToType())
	t.Cleanup(func() { strings.Free(mp) })
	require.NoError(t, vector.AppendBytes(strings, []byte("abc"), false, mp))
	valid, err := strings.MarshalBinary()
	require.NoError(t, err)
	values, _ := vector.MustVarlenaRawData(strings)
	values[0].SetOffsetLen(1<<31, 4096)
	badArea, err := strings.MarshalBinary()
	require.NoError(t, err)
	empty := vector.NewVec(types.T_varchar.ToType())
	emptyData, err := empty.MarshalBinary()
	require.NoError(t, err)
	miss, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("xyz"), 1, mp)
	require.NoError(t, err)
	t.Cleanup(func() { miss.Free(mp) })
	missData, err := miss.MarshalBinary()
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", mp)
	t.Cleanup(proc.Free)
	meta := objectio.NewBlock(objectio.NewSeqnums([]uint16{0}))
	zm := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(zm, []byte("abc0001"))
	index.UpdateZM(zm, []byte("abc9999"))
	meta.ColumnMeta(0).SetZoneMap(zm)
	require.True(t, meta.MustGetColumn(0).ZoneMap().IsInited(), "fallback must encounter a real initialized block zonemap")
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: "k", ColPos: 0}}}
	table := &plan.TableDef{Name2ColIndex: map[string]int32{"k": 0}, Cols: []*plan.ColDef{{Name: "k", Typ: plan.Type{Id: int32(types.T_varchar)}}}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "k", Names: []string{"k"}}}
	for _, tc := range []struct {
		name     string
		data     []byte
		card     int32
		want     bool
		selected bool
	}{
		{"fixed-width", fixed, 1, false, true},
		{"missing", nil, 0, false, true},
		{"truncated", valid[:len(valid)/2], 1, false, true},
		{"invalid-varlena-area", badArea, 1, false, true},
		{"valid-varlena", valid, 1, true, true},
		{"empty-varlena", emptyData, 0, true, false},
		{"nonmatching-varlena", missData, 1, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := plan2.MakeInExpr(context.Background(), col, tc.card, tc.data, true)
			count := plan2.AssignAuxIdForExpr(expr, 0)
			require.NotPanics(t, func() {
				fast, load, obj, block, seek, can, _ := CompileFilterExpr(expr, table, nil)
				require.Equal(t, tc.want, can)
				if !tc.want {
					require.Nil(t, fast)
					require.Nil(t, load)
					require.Nil(t, obj)
					require.Nil(t, block)
					require.Nil(t, seek)
				} else {
					require.NotNil(t, block)
					_, selected, err := block(0, meta, nil)
					require.NoError(t, err)
					require.Equal(t, tc.selected, selected)
				}
			}, "unsupported filter representations must decline the optimization without unsafe vector interpretation")
			zms := make([]objectio.ZoneMap, count)
			vecs := make([]*vector.Vector, count)
			t.Cleanup(func() {
				for _, v := range vecs {
					if v != nil {
						v.Free(mp)
					}
				}
			})
			require.NotPanics(t, func() {
				selected := colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
				require.Equal(t, tc.selected, selected, "fast-path rejection must remain safe and conservative in rangesOnePart's fallback")
			})
			if !tc.want {
				require.False(t, zms[expr.AuxId].IsInited(), "invalid payload must produce unknown rather than false")
			}
			require.NotPanics(t, func() {
				pk, err := ConstructBasePKFilter(expr, table, mp)
				require.NoError(t, err)
				if pk.Vec != nil {
					t.Cleanup(func() { pk.Vec.Free(mp) })
				}
				require.Equal(t, tc.want, pk.Valid, "PK-reader optimization must also reject incompatible prefix payloads")
			})
			if tc.name == "fixed-width" {
				cached := vector.NewVec(types.T_any.ToType())
				require.NoError(t, cached.UnmarshalBinary(tc.data))
				vecs[expr.GetF().Args[1].AuxId] = cached
				require.NotPanics(t, func() {
					require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs), "cached incompatible vectors must not prune or panic either")
				})
			}
		})
	}
}
