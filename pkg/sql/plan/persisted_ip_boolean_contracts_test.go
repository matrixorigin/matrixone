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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// jsonBooleanConditionExpr builds a persisted-expression owner whose searched
// CASE evaluates a T_json condition in-domain - the #30932 contract that
// requires the minted v135 floor.
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

// TestRequiredPersistedBooleanConditionContractsFloor is the H1 class (vii)(a)
// plus the class (v) version-table oracle: a persisted expression with an
// in-domain T_json CASE condition or an ENUM storage-truth BOOL cast raises
// the minted v135 floor, so the authoring and read gates REJECT below the
// floor before any expression evaluation. Plain CAST(int->bool) and CASE over
// ordinary conditions raise nothing, and SET storage conditions are exempt
// (TestSetStorageConditionsExemptFromBooleanContracts).
func TestRequiredPersistedBooleanConditionContractsFloor(t *testing.T) {
	for _, tc := range []struct {
		name string
		expr *planpb.Expr
	}{
		{"json case condition", jsonBooleanConditionExpr()},
		{"enum storage bool cast", enumStorageBoolCastExpr()},
	} {
		features, err := planpb.RequiredRemoteExpressionFeatures(tc.expr)
		require.NoError(t, err)
		require.True(t, features.JSONBooleanConditionContracts, tc.name)

		version, err := RequiredPersistedExpressionProtocolVersion(tc.expr)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion135, version, tc.name)

		// The authoring gate refuses below the minted floor with the typed
		// error, before any metadata is persisted.
		err = RequirePersistedProtocolVersionForAuthoring(context.Background(), nil, version)
		require.ErrorContains(t, err, "protocol version 135", tc.name)

		// The read/rebind gate refuses below the runtime floors before any
		// expression evaluation - the gate runs at the rebind boundary, not in
		// the executor.
		err = RequirePersistedProtocolVersion(context.Background(), nil, version)
		require.ErrorContains(t, err, "protocol version 135", tc.name)
	}

	// Negative controls: neither an ordinary integer->BOOL cast nor a CASE
	// over integer conditions is a boolean-condition contract.
	plainBoolCast := castToBoolForTest(&planpb.Expr{
		Typ:  planpb.Type{Id: int32(23)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	})
	features, err := planpb.RequiredRemoteExpressionFeatures(plainBoolCast)
	require.NoError(t, err)
	require.False(t, features.JSONBooleanConditionContracts)

	intCond := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(23)},
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
	plainCase := &planpb.Expr{
		Typ: planpb.Type{Id: int32(30)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: encodeCaseFunctionID(), ObjName: "case"},
			Args: []*planpb.Expr{intCond, thenValue, elseValue},
		}},
	}
	features, err = planpb.RequiredRemoteExpressionFeatures(plainCase)
	require.NoError(t, err)
	require.False(t, features.JSONBooleanConditionContracts)
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

// TestPersistedBooleanContractsPassThroughReaderLackingDetector is the H1
// class (vii)(b): the executable record that a reader whose feature walk lacks
// the minted detector (every binary compiled before this fix) computes
// requirement 0 for the same expression, and the read gate consequently
// returns nil - it cannot reject before execution. This is why the verified
// process-retirement rollout prerequisite (design Section 7, revision 7),
// not any per-expression gate, closes the expired-but-serving path.
func TestPersistedBooleanContractsPassThroughReaderLackingDetector(t *testing.T) {
	// (1) The HEAD-shaped feature value: none of the pre-contract features
	// detect a boolean-condition contract, so the same TABLE maps to floor 0.
	require.Equal(t, int64(0),
		RequiredPersistedExpressionProtocolVersionForFeatures(planpb.RemoteExpressionFeatures{}))

	// (2) A HEAD reader that only sees an older contract still raises that
	// contract - the parameterized mapping keeps old floors intact.
	require.Equal(t, int64(defines.MORPCVersion80),
		RequiredPersistedExpressionProtocolVersionForFeatures(
			planpb.RemoteExpressionFeatures{StringNumericResultContracts: true}))

	// (3) Requirement 0 bypasses the runtime checks entirely (persisted_ip
	// requirement-0 early return): the read gate returns nil for the reader
	// that detected nothing.
	err := RequirePersistedProtocolVersion(context.Background(), nil, 0)
	require.NoError(t, err)

	// (4) The mixed-owner proof: the SAME owner raises 135 for this build's
	// walk while the HEAD-shaped features value stays 0.
	owner := tableDefOwner(jsonBooleanConditionExpr(), enumStorageBoolCastExpr())
	version, err := RequiredPersistedExpressionProtocolVersion(owner)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion135, version)
}

// TestPersistedBooleanContractsGateSimulations is the H1 class (v) reader
// variant simulation set, labeled as gate-behavior proof (NOT rollback
// evidence): with a runtime whose session floors simulate a below-minted
// deployment, the authoring gate refuses (nothing gets persisted) and the
// read/rebind gate refuses with the typed error (no execution); with floors
// at or above the minted version both gates admit.
func TestPersistedBooleanContractsGateSimulations(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldProtocol, hadProtocol := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldReadFloor, hadReadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor)
	oldAuthoringFloor, hadAuthoringFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadProtocol {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldProtocol)
		} else {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
		}
		if hadReadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, oldReadFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
		}
		if hadAuthoringFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldAuthoringFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	const required = int64(defines.MORPCVersion135)

	// Simulated below-floor reader: the runtime protocol and the published
	// floors are all below the minted version.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion128)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion128))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion128))
	err := RequirePersistedProtocolVersion(context.Background(), proc, required)
	require.ErrorContains(t, err, "protocol version 135")
	err = RequirePersistedProtocolVersionForAuthoring(context.Background(), proc, required)
	require.ErrorContains(t, err, "protocol version 135")

	// The binary itself predates the minted version: the read floor is absent
	// but the runtime protocol is still below the requirement.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
	if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolFloor); ok {
		rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolFloor, current)
	}
	err = RequirePersistedProtocolVersion(context.Background(), proc, required)
	require.ErrorContains(t, err, "protocol version 135")

	// Admitted reader: an at-floor runtime passes both gates.
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolFloor, int64(defines.MORPCVersion135))
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, int64(defines.MORPCVersion135))
	require.NoError(t, RequirePersistedProtocolVersion(context.Background(), proc, required))
	require.NoError(t, RequirePersistedProtocolVersionForAuthoring(context.Background(), proc, required))
}
