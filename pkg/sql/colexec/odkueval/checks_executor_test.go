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

package odkueval

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func checkTestAccount(t *testing.T) *mpool.AllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 4096)
	require.NoError(t, err)
	account, err := registry.Open(16 << 20)
	require.NoError(t, err)
	return account
}

func odkuTestAssertion(condition *plan.Expr, message string) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: int64(function.CHECK_CONSTRAINT_ASSERT) << 32, ObjName: "_check_constraint_assert"},
		Args: []*plan.Expr{condition, {Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: message}}}}},
	}}}
}
func odkuTestNotNull(typ types.Type, message string) *plan.Expr {
	input := &plan.Expr{Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}}
	return odkuTestAssertion(&plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: int64(function.ISNOTNULL) << 32, ObjName: "isnotnull"}, Args: []*plan.Expr{input}}}}, message)
}

func TestODKUNotNullCheckMatchesExecutor(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_varchar.ToType(), types.T_enum.ToType()} {
		for _, constant := range []bool{false, true} {
			for _, isNull := range []bool{false, true} {
				for _, message := range []string{"", "column with 100% preserved message"} {
					t.Run(fmt.Sprintf("%s/const=%t/null=%t/msg=%q", typ.Oid, constant, isNull, message), func(t *testing.T) {
						mp := mpool.MustNewZero()
						proc := testutil.NewProcessWithMPool(t, "", mp)
						account := checkTestAccount(t)
						var v *vector.Vector
						var generic []colexec.ExpressionExecutor
						t.Cleanup(func() {
							for _, e := range generic {
								e.Free()
							}
							if v != nil {
								v.Free(mp)
							}
							proc.Free()
							require.Zero(t, account.Snapshot().Used)
							require.Zero(t, mp.CurrNB())
						})
						var err error
						if constant && isNull {
							v = vector.NewConstNull(typ, 1, mp)
						} else if constant {
							switch typ.Oid {
							case types.T_int64:
								v, err = vector.NewConstFixed(typ, int64(7), 1, mp)
							case types.T_enum:
								v, err = vector.NewConstFixed(typ, types.Enum(2), 1, mp)
							default:
								v, err = vector.NewConstBytes(typ, []byte("a varlen value longer than the inline area"), 1, mp)
							}
							require.NoError(t, err)
						} else {
							v = vector.NewVec(typ)
							switch typ.Oid {
							case types.T_int64:
								err = vector.AppendFixed(v, int64(7), isNull, mp)
							case types.T_enum:
								err = vector.AppendFixed(v, types.Enum(2), isNull, mp)
							default:
								err = vector.AppendBytes(v, []byte("a varlen value longer than the inline area"), isNull, mp)
							}
							require.NoError(t, err)
						}
						expr := odkuTestNotNull(typ, message)
						checks, err := CompileChecks([]*plan.Expr{expr}, []types.Type{typ}, []types.Type{typ}, proc, account)
						require.NoError(t, err)
						require.Len(t, checks, 1)
						t.Cleanup(func() { FreeChecks(checks) })
						image := batch.NewWithSize(1)
						image.Vecs[0] = v
						image.SetRowCount(1)
						generic, err = hashbuild.NewExpressionExecutors(proc, []*plan.Expr{expr}, account)
						require.NoError(t, err)
						_, expected := generic[0].Eval(proc, []*batch.Batch{image}, nil)
						actual := EvaluateChecks(checks, image, image, proc)
						if expected == nil {
							require.NoError(t, actual)
						} else {
							require.Error(t, actual)
							require.True(t, moerr.IsMoErrCode(actual, moerr.ErrConstraintViolation))
							require.Equal(t, expected.Error(), actual.Error())
						}
					})
				}
			}
		}
	}
}

func TestODKUCheckOrderFallbackAndInvalidMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	typ := types.T_int64.ToType()
	account := checkTestAccount(t)
	v := vector.NewConstNull(typ, 1, mp)
	t.Cleanup(func() {
		v.Free(mp)
		proc.Free()
		require.Zero(t, account.Snapshot().Used)
		require.Zero(t, mp.CurrNB())
	})
	image := batch.NewWithSize(1)
	image.Vecs[0] = v
	image.SetRowCount(1)
	nn := odkuTestNotNull(typ, "NN first")
	complex := odkuTestAssertion(&plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}, "complex first")
	for _, list := range [][]*plan.Expr{{complex, nn}, {nn, complex}} {
		checks, e := CompileChecks(list, []types.Type{typ}, []types.Type{typ}, proc, account)
		require.NoError(t, e)
		e = EvaluateChecks(checks, image, image, proc)
		require.ErrorContains(t, e, list[0].GetF().Args[1].GetLit().GetSval())
		FreeChecks(checks)
		FreeChecks(checks) // Releasing the same compiled checks again is safe.
	}
	for _, change := range []func(*plan.Expr){
		func(e *plan.Expr) { e.GetF().Func.Obj++ },
		func(e *plan.Expr) { e.GetF().Args[0].GetF().Func.Obj++ },
		func(e *plan.Expr) { e.GetF().Args = nil },
		func(e *plan.Expr) { e.GetF().Args[0].GetF().Args[0].GetCol().ColPos = 2 },
	} {
		e := proto.Clone(nn).(*plan.Expr)
		change(e)
		_, err := CompileChecks([]*plan.Expr{e}, []types.Type{typ}, []types.Type{typ}, proc, account)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "%v", err)
	}
}
