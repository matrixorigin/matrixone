// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// jsonBooleanConditionExpr builds a persisted-expression owner whose searched
// CASE evaluates a T_json condition in-domain - the #30932 conditional
// contract that the same-version cross-mode consumption gate detects.
func jsonBooleanConditionExpr() *planpb.Expr {
	jsonCond := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(62)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	thenValue := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(30)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: false}},
	}
	elseValue := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(30)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: false}},
	}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(30)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: encodeCaseFunctionID(), ObjName: "case"},
			Args: []*planpb.Expr{jsonCond, thenValue, elseValue},
		}},
	}
}

// enumStorageBoolCastExpr builds the #30931 contract node: the ENUM
// storage-truth CAST to BOOL, the new implicit boolean admission for persisted
// ENUM conditions.
func enumStorageBoolCastExpr() *planpb.Expr {
	enumStorage := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(66), Enumvalues: "'a','b'"},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	return castToBoolForTest(enumStorage)
}

func setStorageBoolCastExpr() *planpb.Expr {
	setStorage := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(28), Enumvalues: "'a','b'"},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	return castToBoolForTest(setStorage)
}

func castToBoolForTest(arg *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(10)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: function.EncodeOverloadID(function.CAST, 0), ObjName: "cast"},
			Args: []*planpb.Expr{arg, {
				Typ:  planpb.Type{Id: int32(10)},
				Expr: &planpb.Expr_T{T: &planpb.TargetType{}},
			}},
		}},
	}
}

// encodeCaseFunctionID mirrors registry id 71 into the walk's packed
// representation (high 32 bits carry the function identity).
func encodeCaseFunctionID() int64 {
	return int64(function.CASE) << 32
}

// tableDefOwner wraps expressions as a persisted catalog owner the shared
// walk accepts.
func tableDefOwner(exprs ...*planpb.Expr) *planpb.TableDef {
	cols := make([]*planpb.ColDef, 0, len(exprs))
	for _, expr := range exprs {
		cols = append(cols, &planpb.ColDef{Default: &planpb.Default{Expr: expr}})
	}
	return &planpb.TableDef{Cols: cols}
}

// TestBooleanConditionContractDetectionOnBoundExpressions pins the feature
// walk over REAL bound expressions: the in-domain T_json CASE/IFF condition
// slots and the ENUM storage-truth BOOL cast raise the
// JSONBooleanConditionContracts detection that the same-version cross-mode
// consumption gate consumes. Under the repository's shutdown-only upgrade
// contract (same-binary workers and rebinders) the detected contracts carry
// NO protocol requirement: RequiredPersistedExpressionProtocolVersion stays 0
// for every tagged form - the executable record that this feature adds no
// row to the durable feature->version table. Numeric controls raise nothing.
func TestBooleanConditionContractDetectionOnBoundExpressions(t *testing.T) {
	// ENUM storage (T_enum with declaration metadata), the raw storable the
	// generated-column binder binds for an ENUM column.
	bindEnumIFF, err := BindFuncExprImplByPlanExpr(context.Background(), "if", []*planpb.Expr{{
		Typ:  planpb.Type{Id: int32(types.T_enum), Enumvalues: "low,mid,high"},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}, MakePlan2Int64ConstExprWithType(1), MakePlan2Int64ConstExprWithType(0)})
	require.NoError(t, err, "the MySQL contracts bind IF(e,1,0) with the raw ENUM condition in-domain")
	require.Equal(t, int32(types.T_enum), bindEnumIFF.GetF().Args[0].Typ.Id,
		"iffCheck preserves the ENUM condition in-domain instead of scheduling a cast")

	jsonColumn := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_json)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	bindJSONCase, err := BindFuncExprImplByPlanExpr(context.Background(), "case", []*planpb.Expr{
		jsonColumn,
		MakePlan2Int64ConstExprWithType(1),
		MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		expr *planpb.Expr
	}{
		{"bound enum iff condition", bindEnumIFF},
		{"bound json case condition", bindJSONCase},
		{"stored json case condition", jsonBooleanConditionExpr()},
		{"stored enum storage bool cast", enumStorageBoolCastExpr()},
	} {
		features, walkErr := planpb.RequiredRemoteExpressionFeatures(tc.expr)
		require.NoError(t, walkErr, tc.name)
		require.True(t, features.JSONBooleanConditionContracts, tc.name,
			"the conditional boolean contract is detected for the cross-mode consumption gate")

		version, versionErr := RequiredPersistedExpressionProtocolVersion(tc.expr)
		require.NoError(t, versionErr, tc.name)
		require.Zero(t, version, tc.name,
			"the boolean-condition contracts add no row to the durable feature->version table")
	}

	// Negative controls: neither an ordinary integer->BOOL cast nor IF/CASE
	// over integer conditions is a boolean-condition contract.
	plainBoolCast := castToBoolForTest(&planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	})
	intCond := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	numericIf, err := BindFuncExprImplByPlanExpr(context.Background(), "if", []*planpb.Expr{
		intCond,
		MakePlan2Int64ConstExprWithType(1),
		MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		expr *planpb.Expr
	}{
		{"plain int->bool cast", plainBoolCast},
		{"numeric iff condition", numericIf},
	} {
		features, walkErr := planpb.RequiredRemoteExpressionFeatures(tc.expr)
		require.NoError(t, walkErr, tc.name)
		require.False(t, features.JSONBooleanConditionContracts, tc.name)

		version, versionErr := RequiredPersistedExpressionProtocolVersion(tc.expr)
		require.NoError(t, versionErr, tc.name)
		require.Zero(t, version, tc.name)
	}
}

// TestSetStorageConditionsExemptFromBooleanContracts is the design revision-8
// F8 oracle: SET storage conditions - an IF condition, a CASE condition and
// a CAST-to-BOOL source - execute on kernels that predate this change (the
// numeric iffCheck/caseCheck admission, the uint64 truth kernel and the
// numeric-to-BOOL cast), so they set NO JSONBooleanConditionContracts in any
// owner shape: the persisted authoring/read version gates return requirement
// 0 and the same-version cross-mode consumption gate stays open in BOTH
// session modes, keeping native sessions' base access to SET catalog
// definitions (base already serves them; probe r8-probe-setgen.log). The
// retained positive cases above (T_json condition, ENUM storage-truth cast)
// are the negative controls that keep the full contract.
func TestSetStorageConditionsExemptFromBooleanContracts(t *testing.T) {
	// T_uint64 with SET declaration metadata: the raw storage type the
	// generated-column binder binds for a SET('a','b') column.
	setIF := func() *planpb.Expr {
		cond := &planpb.Expr{
			Typ:  planpb.Type{Id: int32(28), Enumvalues: "'a','b'"},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
		}
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), "if", []*planpb.Expr{
			cond,
			MakePlan2Int64ConstExprWithType(1),
			MakePlan2Int64ConstExprWithType(0),
		})
		require.NoError(t, err)
		return bound
	}
	setCASE := func() *planpb.Expr {
		cond := &planpb.Expr{
			Typ:  planpb.Type{Id: int32(28), Enumvalues: "'a','b'"},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
		}
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), "case", []*planpb.Expr{
			cond,
			MakePlan2Int64ConstExprWithType(1),
			MakePlan2Int64ConstExprWithType(0),
		})
		require.NoError(t, err)
		return bound
	}

	for _, tc := range []struct {
		name  string
		owner any
	}{
		{"set if condition", setIF()},
		{"set case condition", setCASE()},
		{"set bool cast", setStorageBoolCastExpr()},
		{"set tabledef owner", tableDefOwner(setIF(), setStorageBoolCastExpr())},
	} {
		features, err := planpb.RequiredRemoteExpressionFeatures(tc.owner)
		require.NoError(t, err)
		require.False(t, features.JSONBooleanConditionContracts, tc.name)

		version, err := RequiredPersistedExpressionProtocolVersion(tc.owner)
		require.NoError(t, err)
		require.Equal(t, int64(0), version, tc.name)

		require.NoError(t, RequirePersistedProtocolVersionForAuthoring(context.Background(), nil, version), tc.name)
		require.NoError(t, RequirePersistedProtocolVersion(context.Background(), nil, version), tc.name)
	}

	// The consumption gate stays open in both session modes for a SET-only
	// catalog definition - the r8 parity contract for existing catalogs.
	mysqlProc := newPlanTestProcess(t)
	nativeProc := newPlanTestProcess(t)
	nativeProc.GetSessionInfo().MatrixOneNativeMode = true
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), mysqlProc, tableDefOwner(setIF(), setStorageBoolCastExpr())))
	require.NoError(t, RequireMySQLConditionalBooleanConsumption(context.Background(), nativeProc, tableDefOwner(setIF(), setStorageBoolCastExpr())))
}
