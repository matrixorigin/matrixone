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

package colexec_test

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestEvaluatePrefixInZoneMap(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, tc := range []struct {
		name              string
		prefixes          []string
		row               string
		unsafe, malformed bool
	}{
		{name: "overlap", prefixes: []string{"a", "aa", "az"}, row: "ab"},
		{name: "unsorted", prefixes: []string{"az", "aa", "a"}, row: "ab"},
		{name: "empty_prefix", prefixes: []string{"", "aaa", "zzz"}, row: "zebra"},
		{name: "zero_leading", prefixes: []string{"\x00x"}, row: "\x00xy"},
		{name: "disjoint", prefixes: []string{"az"}, row: "ab"},
		{name: "empty_set", row: "ab"},
		{name: "nullable", prefixes: []string{"z"}, row: "ab", unsafe: true},
		{name: "malformed", row: "ab", unsafe: true, malformed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := func() []byte {
				v := vector.NewVec(types.T_varchar.ToType())
				defer v.Free(proc.Mp())
				for _, prefix := range tc.prefixes {
					require.NoError(t, vector.AppendBytes(v, []byte(prefix), false, proc.Mp()))
				}
				if tc.unsafe {
					require.NoError(t, vector.AppendBytes(v, nil, true, proc.Mp()))
				}
				data, err := v.MarshalBinary()
				require.NoError(t, err)
				return data
			}()
			if tc.malformed {
				data = data[:len(data)-1]
			}
			before := bytes.Clone(data)
			expr := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "prefix_in", Obj: function.PrefixInFunctionEncodedID},
					Args: []*plan.Expr{makeVarcharColExpr(), {
						Typ:  plan.Type{Id: int32(types.T_varchar)},
						Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Data: data}},
					}},
				}},
			}
			want := tc.unsafe
			for _, prefix := range tc.prefixes {
				want = want || bytes.HasPrefix([]byte(tc.row), []byte(prefix))
			}
			zms, vecs := makeZoneMapEvalScratch(expr)
			defer freeZoneMapEvalVectors(proc, vecs)
			meta := makeVarcharBlockMeta(tc.row)
			var prepared *vector.Vector
			for range 2 {
				require.Equal(t, want, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs))
				require.Equal(t, before, data, "expression bytes must survive repeated block evaluation")
				rhs := vecs[expr.GetF().Args[1].AuxId]
				require.NotNil(t, rhs)
				if prepared != nil {
					require.Same(t, prepared, rhs, "prepare each immutable RHS once per scan")
				}
				prepared = rhs
			}
		})
	}
	require.Zero(t, proc.Mp().CurrNB())
}

func BenchmarkEvaluatePrefixInZoneMap(b *testing.B) {
	proc := testutil.NewProcess(b)
	defer proc.Free()
	for _, n := range []int{2, 128, 16384} {
		data := func() []byte {
			v := vector.NewVec(types.T_varchar.ToType())
			defer v.Free(proc.Mp())
			for i := range n {
				require.NoError(b, vector.AppendBytes(v, []byte(fmt.Sprintf("%08x", i*2)), false, proc.Mp()))
			}
			v.SetSorted(true)
			data, err := v.MarshalBinary()
			require.NoError(b, err)
			return data
		}()
		expr := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "prefix_in", Obj: function.PrefixInFunctionEncodedID},
				Args: []*plan.Expr{makeVarcharColExpr(), {
					Typ:  plan.Type{Id: int32(types.T_varchar)},
					Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Data: data}},
				}},
			}},
		}
		for _, hit := range []bool{false, true} {
			b.Run(fmt.Sprintf("n=%d/hit=%v", n, hit), func(b *testing.B) {
				key := n
				if !hit {
					key++
				}
				meta := makeVarcharBlockMeta(fmt.Sprintf("%08xsuffix", key))
				zms, vecs := makeZoneMapEvalScratch(expr)
				defer freeZoneMapEvalVectors(proc, vecs)
				columns := map[int]int{0: 0}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					_ = colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, columns, zms, vecs)
				}
			})
		}
	}
}

func TestZoneMapScratchSeparatesCandidatesAndTemporaries(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	concat, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "concat", []*plan.Expr{makeVarcharColExpr(), plan2.MakePlan2StringConstExprWithType("")})
	require.NoError(t, err)
	prefix := func(lhs *plan.Expr, values ...string) *plan.Expr {
		data := func() []byte {
			v := vector.NewVec(types.T_varchar.ToType())
			defer v.Free(proc.Mp())
			for _, value := range values {
				require.NoError(t, vector.AppendBytes(v, []byte(value), false, proc.Mp()))
			}
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			return data
		}()
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "prefix_in", Obj: function.PrefixInFunctionEncodedID},
			Args: []*plan.Expr{lhs, {Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Data: data}}}},
		}}}
	}
	left, right := prefix(makeVarcharColExpr(), "a", "aa", "az"), prefix(concat, "a", "b")
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "and", []*plan.Expr{left, right})
	require.NoError(t, err)
	zms, vecs := makeZoneMapEvalScratch(expr)
	defer freeZoneMapEvalVectors(proc, vecs)
	before, err := expr.Marshal()
	require.NoError(t, err)
	prepared := map[int32]*vector.Vector{}
	for _, tc := range []struct {
		row     string
		typ     types.T
		unknown bool
	}{
		{"ab", types.T_varchar, false}, {"zebra", types.T_varbinary, false},
		{"aa", types.T_char, false}, {"b", types.T_varchar, false},
		{"", types.T_varchar, true}, {"az", types.T_varchar, false},
	} {
		meta := makeVarcharBlockMeta()
		if !tc.unknown {
			meta.MustGetColumn(0).SetZoneMap(index.BuildZM(tc.typ, []byte(tc.row)))
		}
		want := tc.unknown || bytes.HasPrefix([]byte(tc.row), []byte("a"))
		got := colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
		require.Equal(t, want, got)
		freshZM, freshVec := makeZoneMapEvalScratch(expr)
		require.Equal(t, got, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, freshZM, freshVec))
		freeZoneMapEvalVectors(proc, freshVec)
		for _, p := range []*plan.Expr{left, right} {
			id := p.GetF().Args[1].AuxId
			if old := prepared[id]; old != nil {
				require.Same(t, old, vecs[id])
			}
			prepared[id] = vecs[id]
		}
		require.Zero(t, proc.Mp().CurrNB(), "per-block function temporaries must be released immediately")
	}
	after, err := expr.Marshal()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func BenchmarkEvaluatePrefixInScan(b *testing.B) {
	proc := testutil.NewProcess(b)
	defer proc.Free()
	for _, n := range []int{2, 128, 16384} {
		data := func() []byte {
			v := vector.NewVec(types.T_varchar.ToType())
			defer v.Free(proc.Mp())
			for i := range n {
				require.NoError(b, vector.AppendBytes(v, []byte(fmt.Sprintf("%08x", i*2)), false, proc.Mp()))
			}
			v.SetSorted(true)
			data, err := v.MarshalBinary()
			require.NoError(b, err)
			return data
		}()
		expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: "prefix_in", Obj: function.PrefixInFunctionEncodedID},
			Args: []*plan.Expr{makeVarcharColExpr(), {
				Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Data: data}},
			}},
		}}}
		count := plan2.AssignAuxIdForExpr(expr, 0)
		for _, blocks := range []int{1, 128, 4096} {
			for _, hit := range []bool{false, true} {
				b.Run(fmt.Sprintf("n=%d/blocks=%d/hit=%v", n, blocks, hit), func(b *testing.B) {
					key := n
					if !hit {
						key++
					}
					meta := makeVarcharBlockMeta(fmt.Sprintf("%08xsuffix", key))
					columns := map[int]int{0: 0}
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						zms := make([]objectio.ZoneMap, count)
						vecs := make([]*vector.Vector, count)
						for range blocks {
							_ = colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, columns, zms, vecs)
						}
						freeZoneMapEvalVectors(proc, vecs)
					}
				})
			}
		}
	}
}

func TestZoneMapConstantTemporaryOwnership(t *testing.T) {
	for _, constrained := range []bool{false, true} {
		t.Run(fmt.Sprintf("constrained=%v", constrained), func(t *testing.T) {
			mp, err := mpool.NewMPool(t.Name(), 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			defer mpool.DeleteMPool(mp)
			proc := testutil.NewProcessWithMPool(t, "", mp)
			defer proc.Free()
			left, right := "left", "right"
			if constrained {
				left, right = strings.Repeat("a", 700000), strings.Repeat("b", 700000)
			}
			expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "concat", []*plan.Expr{
				plan2.MakePlan2StringConstExprWithType(left), plan2.MakePlan2StringConstExprWithType(right),
			})
			require.NoError(t, err)
			zms, vecs := makeZoneMapEvalScratch(expr)
			defer freeZoneMapEvalVectors(proc, vecs)
			zm := colexec.GetExprZoneMap(proc.Ctx, proc, expr, nil, nil, zms, vecs)
			if constrained {
				require.False(t, zm.IsInited(), "allocation failure must leave pruning unknown")
			} else {
				require.True(t, zm.IsInited())
				require.Equal(t, []byte("leftright"), zm.GetMinBuf())
			}
			require.Zero(t, mp.CurrNB(), "constant executors and partial argument preparation must be released")
		})
	}
}
