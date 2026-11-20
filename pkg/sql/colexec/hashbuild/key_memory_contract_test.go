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

package hashbuild

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func contractCall(name string, result types.T, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(result)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: name}, Args: args}}}
}
func TestSpillKeyMemoryContracts(t *testing.T) {
	fixed := newExpr(0, types.T_int32.ToType())
	variable := newExpr(1, types.T_varchar.ToType())
	for _, name := range []string{"json_valid", "json_length", "length", "crc32"} {
		require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall(name, types.T_bool, variable)}), "fixed result does not bound hidden parse/hash scratch: %s", name)
	}
	require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall("+", types.T_int32, contractCall("json_length", types.T_int32, variable), fixed)}), "all intermediate functions must have a contract")
	require.True(t, SupportsBoundedSpillKeys([]*plan.Expr{{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "literal"}}}}}))
	fixedExpr := contractCall("+", types.T_int32, fixed, fixed)
	require.True(t, SupportsBoundedSpillKeys([]*plan.Expr{fixedExpr}))
	plain, ok := SpillKeyMemoryBound([]*plan.Expr{fixed}, 1<<20, 64)
	require.True(t, ok)
	derived, ok := SpillKeyMemoryBound([]*plan.Expr{fixedExpr}, 1<<20, 64)
	require.True(t, ok)
	require.Greater(t, derived.ExpressionBytes, plain.ExpressionBytes)
	require.Equal(t, plain.EncodedUnit, derived.EncodedUnit)
	for _, name := range []string{"serial", "serial_full"} {
		one := contractCall(name, types.T_varchar, variable, fixed)
		require.True(t, SupportsBoundedSpillKeys([]*plan.Expr{one}))
		single, ok := SpillKeyMemoryBound([]*plan.Expr{one}, 1<<20, 64)
		require.True(t, ok)
		repeated, ok := SpillKeyMemoryBound([]*plan.Expr{contractCall(name, types.T_varchar, variable, variable, fixed)}, 1<<20, 64)
		require.True(t, ok)
		require.Greater(t, repeated.EncodedUnit, single.EncodedUnit)
		require.Greater(t, repeated.ExpressionBytes, single.ExpressionBytes)
		require.True(t, single.StringKeys)
		_, ok = SpillKeyMemoryBound([]*plan.Expr{one}, math.MaxInt64, 8192)
		require.False(t, ok, "overflow must retire eligibility before allocation")
	}
	null := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	require.True(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall("serial_full", types.T_varchar, null, fixed)}))
}

func TestSerialFullLiteralCapacityContractMatchesEvaluation(t *testing.T) {
	for _, rows := range []int{1, hashmap.UnitLimit + 1} {
		t.Run(fmt.Sprintf("rows-%d", rows), func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			input := batch.NewOffHeapWithSize(1)
			input.Vecs[0] = testutil.MakeInt64Vector(make([]int64, rows), nil, proc.Mp())
			input.SetRowCount(rows)
			defer input.Clean(proc.Mp())
			// NUL bytes exercise packer's escaping bound; the literal is stored
			// once by the constant executor but serialized for every result row.
			literal := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: strings.Repeat("\x00x", 40)}}}}
			fn, err := function.GetFunctionByName(proc.Ctx, "serial_full", []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()})
			require.NoError(t, err)
			expr := contractCall("serial_full", fn.GetReturnType().Oid, literal, newExpr(0, types.T_int64.ToType()))
			expr.GetF().Func.Obj = fn.GetEncodedOverloadID()
			bound, ok := SpillKeyMemoryBound([]*plan.Expr{expr}, input.Vecs[0].WindowSize(0, rows), rows)
			require.True(t, ok)
			baseline := proc.Mp().AllocatedBytes()
			exec, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer func() {
				if exec != nil {
					exec.Free()
				}
			}()
			result, err := exec.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, rows, result.Length())
			var encoded int64
			for row := range min(rows, hashmap.UnitLimit) {
				encoded += int64(len(result.GetBytesAt(row))) + 3
			}
			require.LessOrEqual(t, encoded, bound.EncodedUnit)
			require.LessOrEqual(t, int64(result.Allocated()), bound.ResultBytes, "output bound includes every row beyond the hash unit")
			require.LessOrEqual(t, proc.Mp().AllocatedBytes()-baseline, bound.ExpressionBytes)
			exec.Free()
			exec = nil
			require.Equal(t, baseline, proc.Mp().AllocatedBytes())
		})
	}
	for _, typ := range []types.T{types.T_array_float32, types.T_array_float64, types.T_datalink, types.T_json} {
		literal := &plan.Expr{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "[1,2]"}}}}
		require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{literal}), "parsing/conversion needs a separate scratch contract: %s", typ)
	}
}

func TestNullPredicateContractMatchesEvaluationWithoutPayloadParsing(t *testing.T) {
	const rows = 3
	// Invalid binary JSON is intentional: null predicates must not decode any
	// payload. TEXT uses the same arbitrary bytes, including embedded NULs.
	payload := []byte(strings.Repeat("\xff{invalid-json\x00", 64))
	for _, typ := range []types.T{types.T_text, types.T_json} {
		for _, mode := range []string{"nullable", "non-null", "const", "const-null", "literal-null"} {
			for _, name := range []string{"isnull", "isnotnull"} {
				t.Run(fmt.Sprintf("%s/%s/%s", typ, mode, name), func(t *testing.T) {
					proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
					t.Cleanup(proc.Free)
					// Keep input ownership outside the executor's pool, so its high
					// water mark is an independent oracle for expression allocations.
					sourceMP := mpool.MustNewZero()
					t.Cleanup(func() { mpool.DeleteMPool(sourceMP) })
					input := batch.NewWithSize(1)
					t.Cleanup(func() { input.Clean(sourceMP) })
					arg := newExpr(0, typ.ToType())
					expectNull := []bool{false, false, false}
					switch mode {
					case "nullable", "non-null":
						input.Vecs[0] = vector.NewVec(typ.ToType())
						for row := range rows {
							isNull := mode == "nullable" && row == 1
							expectNull[row] = isNull
							require.NoError(t, vector.AppendBytes(input.Vecs[0], payload, isNull, sourceMP))
						}
					case "const":
						var err error
						input.Vecs[0], err = vector.NewConstBytes(typ.ToType(), payload, rows, sourceMP)
						require.NoError(t, err)
					case "const-null", "literal-null":
						input.Vecs[0] = vector.NewConstNull(typ.ToType(), rows, sourceMP)
						expectNull = []bool{true, true, true}
						if mode == "literal-null" {
							arg = &plan.Expr{Typ: plan.Type{Id: int32(typ)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
						}
					}
					input.SetRowCount(rows)
					fn, err := function.GetFunctionByName(proc.Ctx, name, []types.Type{typ.ToType()})
					require.NoError(t, err)
					expr := contractCall(name, fn.GetReturnType().Oid, arg)
					expr.GetF().Func.Obj = fn.GetEncodedOverloadID()
					bound, ok := SpillKeyMemoryBound([]*plan.Expr{expr}, input.Vecs[0].WindowSize(0, rows), rows)
					require.True(t, ok)
					largerInput, ok := SpillKeyMemoryBound([]*plan.Expr{expr}, 1<<30, rows)
					require.True(t, ok)
					require.Equal(t, bound, largerInput, "a borrowed payload's size must not inflate null-check scratch or BOOL output")
					baseline := proc.Mp().AllocatedBytes()
					exec, err := colexec.NewExpressionExecutor(proc, expr)
					require.NoError(t, err)
					t.Cleanup(func() {
						if exec != nil {
							exec.Free()
						}
					})
					result, err := exec.Eval(proc, []*batch.Batch{input}, nil)
					require.NoError(t, err, "null checks must accept malformed JSON without parsing it")
					require.Equal(t, rows, result.Length())
					for row, isNull := range expectNull {
						want := isNull
						if name == "isnotnull" {
							want = !want
						}
						require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, row), "row %d", row)
						require.False(t, result.IsNull(uint64(row)), "null predicates produce non-null BOOLs")
					}
					require.LessOrEqual(t, int64(result.Allocated()), bound.ResultBytes)
					require.LessOrEqual(t, proc.Mp().Stats().HighWaterMark.Load()-baseline, bound.ExpressionBytes)
					exec.Free()
					exec = nil
					require.Equal(t, baseline, proc.Mp().AllocatedBytes())
				})
			}
		}
	}
	variable := newExpr(0, types.T_json.ToType())
	for _, name := range []string{"isnull", "isnotnull"} {
		require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall(name, types.T_bool, contractCall("json_length", types.T_int32, variable))}), "a null check does not waive the child executor's allocation contract")
		require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall(name, types.T_bool)}))
		require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall(name, types.T_bool, variable, variable)}))
		require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall(name, types.T_varchar, variable)}))
	}
	require.False(t, SupportsBoundedSpillKeys([]*plan.Expr{contractCall("=", types.T_bool, variable, variable)}), "other fixed-result functions retain their fixed-input restriction")
}
