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

package colexec

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// runtimeBinaryWarningSession records (code, message) diagnostics appended to
// the statement warning sink.
type runtimeBinaryWarningSession struct {
	warnings []struct {
		code uint16
		msg  string
	}
}

func (*runtimeBinaryWarningSession) GetTempTable(string, string) (string, bool) { return "", false }
func (*runtimeBinaryWarningSession) AddTempTable(string, string, string)        {}
func (*runtimeBinaryWarningSession) RemoveTempTable(string, string)             {}
func (*runtimeBinaryWarningSession) RemoveTempTableByRealName(string)           {}
func (*runtimeBinaryWarningSession) GetSqlModeNoAutoValueOnZero() (bool, bool) {
	return false, false
}
func (s *runtimeBinaryWarningSession) AppendWarningDiagnostic(code uint16, msg string) {
	s.warnings = append(s.warnings, struct {
		code uint16
		msg  string
	}{code, msg})
}

// #31921 runtime nullability counterexample (root tick123 review): the
// executor's binarySourceToText guard (func_cast.go, stringDomainMode/
// binaryStringAt) fires on LEGITIMATE runtime binary state carried by a
// statically UTF8 text-family vector (types.StringSemanticState allows it;
// producers write RuntimeStringBinary onto text-shaped results via
// setContributingStringResultDomain). The planner cannot prove NOT NULL for
// such sources, so the shared inference owner must stay conservative for any
// MySQL-string source of an explicit (overload 1) ->text CAST with a
// non-binary-charset target. This test wires the REAL bound CAST overload 1
// through the REAL compiler/eval path (NewExpressionExecutor/Eval) and
// asserts both the execution contract (NULL + Warning 1300) and the shared
// inference contract (DeduceNotNullable must not guarantee NOT NULL) on the
// same plan shape. No UNHEX masking: the source is a declared NOT NULL
// varchar with runtime-only binary state.
func TestExplicitCastRuntimeBinaryNullabilityEval(t *testing.T) {
	// Plain UTF8/legacy-charset text type, matching real catalog plan shapes
	// (mock relname binds with charset 0): no static binary domain, so the
	// runtime marker below is the ONLY binary evidence — exactly the gap.
	srcType := types.New(types.T_varchar, 16, 0)
	dstType := types.T_varchar.ToType()

	proc := testutil.NewProcess(t)
	fGet, err := function.GetFunctionByNameWithOverload(proc.Ctx, "cast", []types.Type{srcType, dstType}, 1)
	require.NoError(t, err)
	encoded := fGet.GetEncodedOverloadID()
	fid, oid := function.DecodeOverloadID(encoded)
	require.Equal(t, int32(function.CAST), fid)
	require.Equal(t, int32(1), oid, "explicit user CAST resolves to the castModeExplicit overload")

	srcPlanTyp := srcType.PlanType()
	srcPlanTyp.NotNullable = true // a declared NOT NULL text column
	dstPlanTyp := dstType.PlanType()
	// appendCastBeforeExprWithOverload mirrors the source NotNullable into the
	// target type BEFORE inference (base_binder.go toType.NotNullable =
	// expr.Typ.NotNullable), so at DeduceNotNullable time the TargetType arg
	// carries NotNullable=true for a NOT NULL source. Without this mirror the
	// allargsnotnull arm short-circuits on the target arg and masks the gap.
	dstPlanTyp.NotNullable = true

	build := func() *plan.Expr {
		colExpr := &plan.Expr{
			Typ:  srcPlanTyp,
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "c"}},
		}
		targetArg := &plan.Expr{
			Typ:  dstPlanTyp,
			Expr: &plan.Expr_T{T: &plan.TargetType{}},
		}
		return &plan.Expr{
			Typ: dstPlanTyp,
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{Obj: encoded, ObjName: "cast"},
				Args: []*plan.Expr{colExpr, targetArg},
			}},
		}
	}

	evalCase := func(t *testing.T, input *vector.Vector, wantNull []bool, wantWarnings int) {
		t.Helper()
		eproc := testutil.NewProcess(t)
		sink := &runtimeBinaryWarningSession{}
		eproc.WarningSink = sink
		executor, err := NewExpressionExecutor(eproc, build())
		require.NoError(t, err)
		defer executor.Free() // result is borrowed; only executor.Free releases it
		bat := batch.NewWithSize(1)
		bat.Vecs[0] = input
		bat.SetRowCount(input.Length())
		result, err := executor.Eval(eproc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		for i, w := range wantNull {
			require.Equal(t, w, result.IsNull(uint64(i)), "row %d nullability", i)
		}
		require.Len(t, sink.warnings, wantWarnings)
		for _, w := range sink.warnings {
			require.Equal(t, uint16(1300), w.code)
		}
	}

	t.Run("uniform_runtime_binary_marker_on_utf8_varchar", func(t *testing.T) {
		mproc := testutil.NewProcess(t)
		input := testutil.MakeVarlenaVector([][]byte{{0xFF}}, nil, srcType, mproc.Mp())
		t.Cleanup(func() { input.Free(mproc.Mp()) })
		input.SetIsBinaryString(true) // legal uniform runtime binary state
		evalCase(t, input, []bool{true}, 1)
		require.False(t, function.DeduceNotNullable(encoded, build().GetF().Args),
			"shared inference must not guarantee NOT NULL: a runtime-binary text source can become NULL")
	})

	t.Run("per_row_sidecar_on_utf8_varchar", func(t *testing.T) {
		mproc := testutil.NewProcess(t)
		input := testutil.MakeVarlenaVector([][]byte{{0xFF}, {0xC3, 0x28}}, nil, srcType, mproc.Mp())
		t.Cleanup(func() { input.Free(mproc.Mp()) })
		require.NoError(t, input.SetIsBinaryStringAt(0, true, mproc.Mp()))
		evalCase(t, input, []bool{true, false}, 1)
		require.False(t, function.DeduceNotNullable(encoded, build().GetF().Args),
			"shared inference must not guarantee NOT NULL: per-row binary sidecar can make rows NULL")
	})
}
