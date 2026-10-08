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

package vectorscan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	searchplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/search"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func parameterizedScanTemplate(t *testing.T) *plan.IndexSearchScan {
	t.Helper()
	parameter := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
	}
	concat, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "concat", []*plan.Expr{
		parameter,
		plan2.MakePlan2StringConstExprWithType(""),
	})
	require.NoError(t, err)
	filter, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "=", []*plan.Expr{
		{
			Typ: plan.Type{Id: int32(types.T_varchar)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{
				ColPos: 2,
				Name:   "__mo_index_include_category",
			}},
		},
		concat,
	})
	require.NoError(t, err)
	return &plan.IndexSearchScan{
		Index:               &plan.IndexDef{IndexAlgo: "ivfflat"},
		SourceTable:         &plan.ObjectRef{},
		QueryPayload:        plan2.MakePlan2Vecf32ConstExprWithType("[1,2]", 2),
		CandidateLimit:      plan2.MakePlan2Uint64ConstExprWithType(2),
		AlgoExprs:           []*plan.Expr{plan2.MakePlan2Uint64ConstExprWithType(1)},
		AlgoExprNames:       []string{"first_round_limit"},
		PreFilters:          []*plan.Expr{filter},
		PostFilterOverFetch: true,
	}
}

func installStringParam(t *testing.T, proc *process.Process, value string) func() {
	t.Helper()
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte(value), false, proc.Mp()))
	proc.SetPrepareParams(params)
	return func() {
		proc.SetPrepareParams(nil)
		params.Free(proc.Mp())
	}
}

func foldedFilterValue(t *testing.T, spec *plan.IndexSearchScan) string {
	t.Helper()
	require.Len(t, spec.PreFilters, 1)
	right := spec.PreFilters[0].GetF().Args[1]
	require.NotNil(t, right.GetLit())
	return right.GetLit().GetSval()
}

func containsParam(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if containsParam(arg) {
				return true
			}
		}
	}
	return false
}

func TestExecutionKeepsTemplateImmutableAcrossPreparedGenerations(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	template := parameterizedScanTemplate(t)

	firstParamDone := installStringParam(t, proc, "x")
	first, err := PrepareCorrelatedExecution(template, proc)
	require.NoError(t, err)
	firstParamDone()
	require.Equal(t, "x", foldedFilterValue(t, first.Spec()))
	require.True(t, containsParam(template.PreFilters[0]))
	require.NoError(t, first.EvalBatch(batch.EmptyForConstFoldBatch, proc))
	req, hasQuery, err := first.RequestAt(0, searchIdentityForTest())
	require.NoError(t, err)
	require.True(t, hasQuery)
	require.Equal(t, uint64(2), req.ResultLimit)
	require.Equal(t, overfetch.FilteredPostModeLimit(2), req.CandidateBudget)
	requireAlgoU64(t, req, "first_round_limit", 1)
	first.Close()
	first.Close()

	secondParamDone := installStringParam(t, proc, "y")
	second, err := PrepareCorrelatedExecution(template, proc)
	require.NoError(t, err)
	secondParamDone()
	defer second.Close()
	require.Equal(t, "y", foldedFilterValue(t, second.Spec()))
	require.True(t, containsParam(template.PreFilters[0]))
}

func TestPrepareScalarKeepsTemplateImmutableAcrossParameters(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	template := parameterizedScanTemplate(t)

	firstParamDone := installStringParam(t, proc, "x")
	first, err := PrepareScalar(template, proc)
	require.NoError(t, err)
	firstParamDone()
	require.Equal(t, "x", foldedFilterValue(t, first))
	first.PreFilters[0] = &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_Bval{Bval: true},
		}},
	}

	secondParamDone := installStringParam(t, proc, "y")
	second, err := PrepareScalar(template, proc)
	require.NoError(t, err)
	secondParamDone()
	require.Equal(t, "y", foldedFilterValue(t, second))
	require.True(t, containsParam(template.PreFilters[0]))
	req, hasQuery, err := RequestFromScalar(second, searchIdentityForTest(), []byte{1, 2}, true, true)
	require.NoError(t, err)
	require.True(t, hasQuery)
	require.Equal(t, []byte{1, 2}, req.MembershipFilter)
	require.True(t, req.HasMembershipFilter)
	require.True(t, req.MembershipFilterRequired)
	require.Equal(t, uint64(2), req.ResultLimit)
	require.Equal(t, overfetch.FilteredPostModeLimit(2), req.CandidateBudget)
	requireAlgoU64(t, req, "first_round_limit", 1)
}

func TestIdentityUsesPublisherBeforeSnapshotTenant(t *testing.T) {
	currentSnapshot := timestamp.Timestamp{PhysicalTime: 10}
	spec := &plan.IndexSearchScan{
		SourceTable: &plan.ObjectRef{PubInfo: &plan.PubInfo{TenantId: 42}},
		ScanSnapshot: &plan.Snapshot{
			TS:     &timestamp.Timestamp{PhysicalTime: 8},
			Tenant: &plan.SnapshotTenant{TenantID: 99},
		},
	}
	identity, err := Identity(spec, currentSnapshot, 17, 3, 2)
	require.NoError(t, err)
	require.Equal(t, uint32(42), *identity.PhysicalAccountID)
	require.Equal(t, 17, identity.TxnOffset)
	require.Equal(t, int32(3), identity.PartitionCount)
	require.Equal(t, int32(2), identity.PartitionIndex)
	require.NotSame(t, spec.ScanSnapshot, identity.Snapshot)

	spec.SourceTable.PubInfo = nil
	identity, err = Identity(spec, currentSnapshot, 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, uint32(99), *identity.PhysicalAccountID)
	require.Equal(t, int32(1), identity.PartitionCount)

	spec.SourceTable.PubInfo = &plan.PubInfo{TenantId: -1}
	_, err = Identity(spec, currentSnapshot, 0, 1, 0)
	require.ErrorContains(t, err, "invalid publisher tenant")
}

func TestIdentityKeepsCurrentTxnForNonHistoricalSnapshot(t *testing.T) {
	currentSnapshot := timestamp.Timestamp{PhysicalTime: 10}
	for _, snapshotTS := range []timestamp.Timestamp{
		{},
		currentSnapshot,
		{PhysicalTime: 11},
	} {
		spec := &plan.IndexSearchScan{
			SourceTable: &plan.ObjectRef{},
			ScanSnapshot: &plan.Snapshot{
				TS:     &snapshotTS,
				Tenant: &plan.SnapshotTenant{TenantID: 99},
			},
		}
		identity, err := Identity(spec, currentSnapshot, 0, 1, 0)
		require.NoError(t, err)
		require.Nil(t, identity.Snapshot)
		require.Nil(t, identity.PhysicalAccountID)
	}

	publisherID := int32(42)
	identity, err := Identity(&plan.IndexSearchScan{
		SourceTable: &plan.ObjectRef{PubInfo: &plan.PubInfo{TenantId: publisherID}},
		ScanSnapshot: &plan.Snapshot{
			TS:     &timestamp.Timestamp{PhysicalTime: 11},
			Tenant: &plan.SnapshotTenant{TenantID: 99},
		},
	}, currentSnapshot, 0, 1, 0)
	require.NoError(t, err)
	require.Nil(t, identity.Snapshot)
	require.Equal(t, uint32(publisherID), *identity.PhysicalAccountID)
}

func TestExecutionRejectsInvalidRuntimeState(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	_, err := PrepareScalar(nil, proc)
	require.ErrorContains(t, err, "incomplete metadata")
	_, err = PrepareCorrelatedExecution(&plan.IndexSearchScan{}, proc)
	require.ErrorContains(t, err, "incomplete metadata")
	var nilExecution *Execution
	require.Error(t, nilExecution.EvalBatch(batch.EmptyForConstFoldBatch, proc))
	require.Nil(t, nilExecution.Spec())
	nilExecution.Close()
	_, err = Identity(nil, timestamp.Timestamp{}, 0, 1, 0)
	require.ErrorContains(t, err, "missing metadata")

	base := func() *plan.IndexSearchScan {
		return &plan.IndexSearchScan{
			Index:          &plan.IndexDef{IndexAlgo: "ivfflat"},
			SourceTable:    &plan.ObjectRef{},
			QueryPayload:   plan2.MakePlan2Vecf32ConstExprWithType("[1,2]", 2),
			CandidateLimit: plan2.MakePlan2Uint64ConstExprWithType(2),
		}
	}

	execution, err := PrepareCorrelatedExecution(base(), proc)
	require.NoError(t, err)
	_, _, err = execution.RequestAt(0, searchIdentityForTest())
	require.ErrorContains(t, err, "no evaluated provider batch")
	require.NoError(t, execution.EvalBatch(batch.EmptyForConstFoldBatch, proc))
	_, _, err = execution.RequestAt(1, searchIdentityForTest())
	require.ErrorContains(t, err, "out of range")
	execution.Close()

	nullQuery := base()
	nullQuery.QueryPayload = &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_array_float32), Width: 2},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Isnull: true,
			Value:  &plan.Literal_VecVal{},
		}},
	}
	execution, err = PrepareCorrelatedExecution(nullQuery, proc)
	require.NoError(t, err)
	require.NoError(t, execution.EvalBatch(batch.EmptyForConstFoldBatch, proc))
	_, hasQuery, err := execution.RequestAt(0, searchIdentityForTest())
	require.NoError(t, err)
	require.False(t, hasQuery)
	execution.Close()

	wrongLimit := base()
	wrongLimit.CandidateLimit = plan2.MakePlan2Int64ConstExprWithType(2)
	execution, err = PrepareCorrelatedExecution(wrongLimit, proc)
	require.NoError(t, err)
	require.NoError(t, execution.EvalBatch(batch.EmptyForConstFoldBatch, proc))
	_, _, err = execution.RequestAt(0, searchIdentityForTest())
	require.ErrorContains(t, err, "result limit did not evaluate to uint64")
	execution.Close()

	// an algorithm value is passed through as evaluated; its algorithm checks the type
	signedAlgo := base()
	signedAlgo.AlgoExprs = []*plan.Expr{plan2.MakePlan2Int64ConstExprWithType(-1)}
	signedAlgo.AlgoExprNames = []string{"first_round_limit"}
	execution, err = PrepareCorrelatedExecution(signedAlgo, proc)
	require.NoError(t, err)
	require.NoError(t, execution.EvalBatch(batch.EmptyForConstFoldBatch, proc))
	req, ok, err := execution.RequestAt(0, searchIdentityForTest())
	require.NoError(t, err)
	require.True(t, ok)
	lit, found := req.AlgoValue("first_round_limit")
	require.True(t, found)
	require.Equal(t, int64(-1), lit.GetI64Val())
	execution.Close()

	unnamed := base()
	unnamed.AlgoExprs = []*plan.Expr{plan2.MakePlan2Uint64ConstExprWithType(1)}
	_, err = PrepareCorrelatedExecution(unnamed, proc)
	require.ErrorContains(t, err, "algorithm expressions are not all named")
	_, err = PrepareScalar(unnamed, proc)
	require.ErrorContains(t, err, "algorithm expressions are not all named")
}

func TestRequestFromScalarRejectsMalformedBoundExpressions(t *testing.T) {
	identity := searchIdentityForTest()
	_, _, err := RequestFromScalar(nil, identity, nil, false, false)
	require.ErrorContains(t, err, "incomplete bound expressions")

	nonLiteralQuery := &plan.IndexSearchScan{QueryPayload: &plan.Expr{}}
	_, _, err = RequestFromScalar(nonLiteralQuery, identity, nil, false, false)
	require.ErrorContains(t, err, "query vector did not fold")

	nullQuery := &plan.IndexSearchScan{QueryPayload: &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}},
	}}
	_, hasQuery, err := RequestFromScalar(nullQuery, identity, nil, false, false)
	require.NoError(t, err)
	require.False(t, hasQuery)

	query := plan2.MakePlan2Vecf32ConstExprWithType("[1,2]", 2)
	_, _, err = RequestFromScalar(&plan.IndexSearchScan{QueryPayload: query}, identity, nil, false, false)
	require.ErrorContains(t, err, "result limit did not fold")
	_, _, err = RequestFromScalar(&plan.IndexSearchScan{
		QueryPayload: query,
		CandidateLimit: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Isnull: true,
		}}},
	}, identity, nil, false, false)
	require.ErrorContains(t, err, "result limit did not fold")
	_, _, err = RequestFromScalar(&plan.IndexSearchScan{
		QueryPayload:   query,
		CandidateLimit: plan2.MakePlan2Int64ConstExprWithType(1),
	}, identity, nil, false, false)
	require.ErrorContains(t, err, "result limit is not uint64")

	_, _, err = RequestFromScalar(&plan.IndexSearchScan{
		QueryPayload:   query,
		CandidateLimit: plan2.MakePlan2Uint64ConstExprWithType(1),
		AlgoExprs:      []*plan.Expr{{}},
		AlgoExprNames:  []string{"first_round_limit"},
	}, identity, nil, false, false)
	require.ErrorContains(t, err, "algorithm expression first_round_limit did not fold")
	_, _, err = RequestFromScalar(&plan.IndexSearchScan{
		QueryPayload:   query,
		CandidateLimit: plan2.MakePlan2Uint64ConstExprWithType(1),
		AlgoExprs:      []*plan.Expr{plan2.MakePlan2Uint64ConstExprWithType(1)},
	}, identity, nil, false, false)
	require.ErrorContains(t, err, "algorithm expressions are not all named")
	req, ok, err := RequestFromScalar(&plan.IndexSearchScan{
		QueryPayload:   query,
		CandidateLimit: plan2.MakePlan2Uint64ConstExprWithType(1),
		AlgoExprs:      []*plan.Expr{plan2.MakePlan2Uint64ConstExprWithType(7)},
		AlgoExprNames:  []string{"first_round_limit"},
	}, identity, nil, false, false)
	require.NoError(t, err)
	require.True(t, ok)
	requireAlgoU64(t, req, "first_round_limit", 7)
	_, found := req.AlgoValue("absent")
	require.False(t, found)
}

// requireAlgoU64 requires that req carries uint64 want as algorithm value name.
func requireAlgoU64(t *testing.T, req searchplugin.Request, name string, want uint64) {
	t.Helper()
	lit, ok := req.AlgoValue(name)
	require.True(t, ok)
	require.Equal(t, want, lit.GetU64Val())
}

func TestExplainDiagnosticsAreEnabledOnlyForScalarScans(t *testing.T) {
	proc := testutil.NewProcess(t)
	spec := parameterizedScanTemplate(t)
	cleanup := installStringParam(t, proc, "category")
	defer cleanup()

	scalar, err := PrepareScalar(spec, proc)
	require.NoError(t, err)
	scalarReq, ok, err := RequestFromScalar(scalar, searchIdentityForTest(), nil, false, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, scalarReq.CollectExplainDiagnostics)

	execution, err := PrepareCorrelatedExecution(spec, proc)
	require.NoError(t, err)
	defer execution.Close()
	require.NoError(t, execution.EvalBatch(batch.EmptyForConstFoldBatch, proc))
	correlatedReq, ok, err := execution.RequestAt(0, searchIdentityForTest())
	require.NoError(t, err)
	require.True(t, ok)
	require.False(t, correlatedReq.CollectExplainDiagnostics)
}

func searchIdentityForTest() searchplugin.ScanIdentity {
	return searchplugin.ScanIdentity{PartitionCount: 1}
}
