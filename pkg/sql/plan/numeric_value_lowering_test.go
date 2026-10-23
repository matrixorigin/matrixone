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

package plan

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestNumericValueKindDistinctLoweringPreservesEqualityTuple(t *testing.T) {
	const source = `SELECT CASE WHEN id=1 THEN X'31' ELSE '1' END AS v
		FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS source`

	t.Run("unaffected distinct keeps ordinary plan", func(t *testing.T) {
		p := buildNumericValuePlan(t, "SELECT DISTINCT v FROM ("+source+") AS d")
		for _, node := range p.GetQuery().Nodes {
			if node == nil {
				continue
			}
			require.Empty(t, node.PhysicalEqualityKeyList)
		}
	})

	t.Run("demanded passenger is excluded from dedup keys", func(t *testing.T) {
		p := buildNumericValuePlan(t, "SELECT ABS(v), SQRT(v) FROM (SELECT DISTINCT v FROM ("+source+") AS d) AS result")
		found := false
		for _, node := range p.GetQuery().Nodes {
			if node == nil || node.NodeType != planpb.Node_AGG || len(node.GroupByHashKey) == 0 {
				continue
			}
			// The source value and its BOOL passenger are both retained as
			// grouping payload. Only the original visible value is hashed.
			if len(node.GroupBy) == 2 && len(node.GroupByHashKey) == 1 {
				require.Equal(t, int32(0), node.GroupByHashKey[0])
				found = true
			}
		}
		require.True(t, found, "the rewritten DISTINCT aggregate must keep its original key subset")
	})
}

func TestNumericValueKindLoweringUsesOwnerSpecificCompanions(t *testing.T) {
	const source = `CASE WHEN id=1 THEN X'31' ELSE '1' END`
	for _, tc := range []struct {
		name  string
		query string
		check func(*testing.T, *planpb.Plan)
	}{
		{
			name:  "private integer argument",
			query: `SELECT REPEAT('a', ` + source + `) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`,
			check: func(t *testing.T, p *planpb.Plan) {
				found := findNumericValueFunction(p, function.CAST, function.IntegerArgumentCastOverload)
				require.NotNil(t, found)
				require.Len(t, found.Args, 3)
				require.Equal(t, int32(types.T_bool), found.Args[2].Typ.Id)
			},
		},
		{
			name:  "OCT",
			query: `SELECT OCT(` + source + `) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`,
			check: func(t *testing.T, p *planpb.Plan) {
				found := findNumericValueFunction(p, function.OCT, -1)
				require.NotNil(t, found)
				require.Len(t, found.Args, 2)
				require.Equal(t, int32(types.T_bool), found.Args[1].Typ.Id)
			},
		},
		{
			name:  "MAKETIME seconds",
			query: `SELECT MAKETIME(1, 0, ` + source + `) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`,
			check: func(t *testing.T, p *planpb.Plan) {
				found := findNumericValueFunction(p, function.MAKETIME, -1)
				require.NotNil(t, found)
				require.Len(t, found.Args, 4)
				require.Equal(t, int32(types.T_bool), found.Args[3].Typ.Id)
			},
		},
		{
			name:  "explicit numeric cast",
			query: `SELECT CAST(` + source + ` AS SIGNED) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`,
			check: func(t *testing.T, p *planpb.Plan) {
				found := findNumericValueFunction(p, function.CAST, function.NumericValueCastOverload)
				require.NotNil(t, found)
				require.Len(t, found.Args, 4)
				require.Equal(t, int32(types.T_bool), found.Args[2].Typ.Id)
			},
		},
		{
			name:  "IF condition",
			query: `SELECT IF(` + source + `, 1, 0) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`,
			check: func(t *testing.T, p *planpb.Plan) {
				found := findNumericValueFunction(p, function.IFF, -1)
				require.NotNil(t, found)
				require.Len(t, found.Args, 4)
				require.Equal(t, int32(types.T_bool), found.Args[3].Typ.Id)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := buildNumericValuePlan(t, tc.query)
			tc.check(t, p)
		})
	}
}

func TestNumericValueKindDependentScalarResultDescriptor(t *testing.T) {
	const source = `SELECT id, CASE WHEN id=1 THEN X'31' ELSE '1' END AS v
		FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS s`
	query := `SELECT id, v, ABS(v), SQRT(v) FROM (
		SELECT id, (SELECT (SELECT d.v)) AS v FROM (` + source + `) AS d
	) AS result ORDER BY id`
	p := buildNumericValuePlan(t, query)
	require.NotNil(t, p.GetQuery().GetCorrelation())

	found := false
	for _, program := range p.GetQuery().GetCorrelation().GetPrograms() {
		if program == nil || program.ScalarResultKindColumn != 1 {
			continue
		}
		found = true
		require.Len(t, program.OutputTypes, 2)
		require.Equal(t, int32(types.T_bool), program.OutputTypes[1].Id)
		require.GreaterOrEqual(t, int(program.RootNode), 0)
		require.Less(t, int(program.RootNode), len(p.GetQuery().Nodes))
		root := p.GetQuery().Nodes[program.RootNode]
		require.NotNil(t, root)
		require.Len(t, root.ProjectList, 2)
		require.Equal(t, int32(types.T_bool), root.ProjectList[1].Typ.Id)
	}
	require.True(t, found, "the nested dependent scalar returning the selected string kind must publish a BOOL result column")
}

func TestNumericValueKindCorrelationSlotPublishesRawAndKindTogether(t *testing.T) {
	ctx := context.Background()
	selector, err := BindFuncExprImplByPlanExpr(ctx, "case", []*Expr{
		makePlan2BoolConstExprWithType(true),
		makePlan2StringConstExprWithType("1", true),
		makePlan2StringConstExprWithType("1"),
	})
	require.NoError(t, err)
	require.True(t, types.T(selector.Typ.Id).IsMySQLString())
	textType := *DeepCopyType(&selector.Typ)

	firstCall := &planpb.DependentExpr{ProgramId: 1, Inputs: []*Expr{DeepCopyExpr(selector)}}
	secondCall := &planpb.DependentExpr{ProgramId: 1, Inputs: []*Expr{DeepCopyExpr(selector)}}
	query := &planpb.Query{
		Nodes: []*planpb.Node{
			{NodeId: 0, ProjectList: []*Expr{
				{Expr: &planpb.Expr_Dependent{Dependent: firstCall}},
				{Expr: &planpb.Expr_Dependent{Dependent: secondCall}},
			}},
			{NodeId: 1, ProjectList: []*Expr{{Typ: textType, Expr: &planpb.Expr_Slot{Slot: &planpb.CorrelationSlot{ProgramId: 1, Ordinal: 0}}}}},
		},
		Steps: []int32{0},
		Correlation: &planpb.CorrelationSpec{Programs: []*planpb.CorrelationProgram{{
			RootNode:    1,
			InputTypes:  []planpb.Type{textType},
			OutputTypes: []planpb.Type{makeSimplePlan2Type(types.T_int64)},
		}}},
	}
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	lowerer := &numericValueKindLowerer{
		builder:        &QueryBuilder{compCtx: &mock.ctxt, qry: query},
		slotKinds:      make(map[[2]uint32]uint32),
		slotKindActive: make(map[[2]uint32]bool),
	}
	kind, err := lowerer.kindForCorrelationSlot(&planpb.CorrelationSlot{ProgramId: 1, Ordinal: 0})
	require.NoError(t, err)
	require.Equal(t, uint32(1), kind.GetSlot().Ordinal)
	require.Equal(t, []planpb.Type{textType, makeSimplePlan2Type(types.T_bool)}, query.Correlation.Programs[0].InputTypes)

	for _, call := range []*planpb.DependentExpr{firstCall, secondCall} {
		require.Len(t, call.Inputs, 2)
		raw, passenger := call.Inputs[0], call.Inputs[1]
		require.Equal(t, textType, raw.Typ)
		require.Equal(t, int32(types.T_bool), passenger.Typ.Id)
		rawProgram, rawOK := numericProgramRefScope(raw)
		require.True(t, rawOK)
		kindProgram, kindSlot, kindOK := numericProgramRefIdentity(passenger)
		valueProgram, valueSlot, valueOK := numericProgramRefIdentity(raw)
		require.True(t, kindOK && valueOK)
		require.Equal(t, rawProgram, valueProgram)
		require.Equal(t, rawProgram, kindProgram)
		require.Equal(t, int64(0), valueSlot)
		require.Equal(t, int64(1), kindSlot)
		_, ok := numericProgramRawValue(raw)
		require.True(t, ok, "the raw slot must own the selector definition used by both outputs")
	}
}

func TestNumericValueLoweringKeepsStaticOrdinaryCastIdentities(t *testing.T) {
	for _, sql := range []string{
		`SELECT CAST(NULL AS int unsigned)`,
		`SELECT CAST('1' AS SIGNED)`,
		`SELECT CAST(split_part(upper('1'), ' ', 1) AS SIGNED)`,
		`SELECT ABS(split_part(upper('1'), ' ', 1))`,
	} {
		t.Run(sql, func(t *testing.T) {
			p := buildNumericValuePlan(t, sql)
			features, err := planpb.RequiredRemoteExpressionFeatures(p.GetQuery())
			require.NoError(t, err)
			require.False(t, features.NumericValuePrograms,
				"a source with a statically false kind must keep the original cast contract")
			require.Nil(t, findNumericValueFunction(p, function.CAST, function.NumericValueCastOverload))
		})
	}
}

func TestInferredStringDeclarationCastProofIsNarrowAndTransient(t *testing.T) {
	source := makePlan2StringConstExprWithType("1")
	target := types.T_text.ToType()
	cast, err := appendExplicitCastBeforeExpr(context.Background(), source, makePlan2Type(&target))
	require.NoError(t, err)
	require.True(t, markInferredStringDeclarationCast(cast))
	require.True(t, isInferredStringDeclarationCast(cast))

	clonedMetadata := copyPreparedNumericMetadata(cast.PreparedNumeric)
	require.True(t, clonedMetadata.GetInferredStringDeclarationCast())

	explicit := DeepCopyExpr(cast)
	markSyntaxExplicitCast(explicit)
	require.False(t, isInferredStringDeclarationCast(explicit), "SQL CAST syntax remains an authoritative boundary")

	legacy := DeepCopyExpr(cast)
	legacy.PreparedNumeric = nil
	require.False(t, isInferredStringDeclarationCast(legacy), "unmarked CAST1 keeps legacy boundary behavior")

	query := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*Expr{cast}}}}
	require.NoError(t, stripInferredStringDeclarationCastProofs(query))
	require.False(t, isInferredStringDeclarationCast(query.Nodes[0].ProjectList[0]))
	features, err := planpb.RequiredRemoteExpressionFeatures(query)
	require.NoError(t, err)
	require.False(t, features.NumericValuePrograms, "a proof on an unused string cast must not raise the protocol floor")
}

func TestInferredStringDeclarationCastProofCleanupVisitsWitnesses(t *testing.T) {
	makeCast := func() *Expr {
		target := types.T_text.ToType()
		cast, err := appendExplicitCastBeforeExpr(context.Background(), makePlan2StringConstExprWithType("1"), makePlan2Type(&target))
		require.NoError(t, err)
		require.True(t, markInferredStringDeclarationCast(cast))
		return cast
	}

	root, witness, nested := makeCast(), makeCast(), makeCast()
	root.PreparedNumeric.StringDomainSource = witness
	witness.PreparedNumeric.StringDomainSource = nested
	// Witness graphs can share or refer back to an expression. Cleanup must
	// cover the non-executable proof graph without looping on that cycle.
	nested.PreparedNumeric.StringDomainSource = root

	query := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*Expr{root}}}}
	require.NoError(t, stripInferredStringDeclarationCastProofs(query))
	for _, cast := range []*Expr{root, witness, nested} {
		require.False(t, isInferredStringDeclarationCast(cast))
	}
}

func TestNumericValueProgramSelectorOwnerRebindIsIdempotent(t *testing.T) {
	ctx := context.Background()
	hex := makePlan2StringConstExprWithType("1", true)
	target := types.T_varchar.ToType()
	inferred, err := appendExplicitCastBeforeExpr(ctx, hex, makePlan2Type(&target))
	require.NoError(t, err)
	require.True(t, markInferredStringDeclarationCast(inferred))
	selector, err := BindFuncExprImplByPlanExpr(ctx, "case", []*Expr{
		makePlan2BoolConstExprWithType(true), inferred, makePlan2StringConstExprWithType("1"),
	})
	require.NoError(t, err)

	valueRef, kindRef, err := makeNumericValueProgramOutputs(ctx, selector)
	require.NoError(t, err)
	duplicateRef := DeepCopyExpr(valueRef)
	initialScope, ok := numericProgramRefScope(valueRef)
	require.True(t, ok)
	kindScope, ok := numericProgramRefScope(kindRef)
	require.True(t, ok)
	require.Equal(t, initialScope, kindScope)

	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	lowerer := &numericValueKindLowerer{builder: &QueryBuilder{compCtx: &mock.ctxt}}
	require.NoError(t, lowerer.rewriteExpression(&valueRef))
	firstPass := DeepCopyExpr(valueRef)
	firstScopeCounter := nextNumericValueProgramScope.Load()
	require.NoError(t, lowerer.rewriteExpression(&duplicateRef))
	require.True(t, proto.Equal(valueRef, duplicateRef), "copies of one lexical owner must reuse identical paired child scopes")
	require.Equal(t, firstScopeCounter, nextNumericValueProgramScope.Load(), "a copied owner must reuse the first transformed definition")

	definition := valueRef.GetF().Args[0].GetF()
	require.NotNil(t, definition)
	require.Len(t, definition.Args, 2, "the validated owner keeps its original two-argument definition")
	pairedLeaf := false
	require.NoError(t, planpb.VisitExprTree(definition.Args[1], func(expr *Expr) error {
		if raw, ok := numericProgramRawValue(expr); ok && len(expr.GetF().Args[0].GetF().Args) == 3 {
			require.NotNil(t, raw)
			pairedLeaf = true
		}
		return nil
	}))
	require.True(t, pairedLeaf, "the selector leaf must be rebound to a same-owner value reference")
	require.True(t, proto.Equal(firstPass, valueRef))

	require.NoError(t, lowerer.rewriteExpression(&valueRef))
	require.True(t, proto.Equal(firstPass, valueRef), "a second lowering pass must preserve the complete owner tree")
	require.Equal(t, firstScopeCounter, nextNumericValueProgramScope.Load(), "a second pass must allocate no new program scopes")
}

func TestNumericValueLoweringUsesOnlyFrozenPreparedTextKind(t *testing.T) {
	for _, tc := range []struct {
		name                string
		bindings            []PreparedSourceBinding
		values              []any
		allowFrozenTextKind bool
		ordinary            bool
	}{
		{
			name:                "frozen text kind at comparison",
			bindings:            []PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}},
			values:              []any{ParamValue{Value: "16001", IsBinaryProtocol: true}},
			allowFrozenTextKind: true,
			ordinary:            true,
		},
		{
			name:                "ordinary text outside comparison",
			bindings:            []PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}},
			values:              []any{ParamValue{Value: "16001", IsBinaryProtocol: true}},
			allowFrozenTextKind: false,
		},
		{
			name:                "binary value kind",
			bindings:            []PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}},
			values:              []any{ParamValue{Value: "16001", IsBin: true, IsBinaryProtocol: true}},
			allowFrozenTextKind: true,
		},
		{
			name:                "unknown execution value",
			bindings:            []PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}},
			allowFrozenTextKind: true,
		},
		{
			name:                "unknown source binding",
			values:              []any{ParamValue{Value: "16001", IsBinaryProtocol: true}},
			allowFrozenTextKind: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.SetContext(withPreparedSourceBindings(context.Background(), tc.bindings, tc.values))
			state := preparedBindingState(mock.ctxt.GetContext())
			lowerer := &numericValueKindLowerer{builder: &QueryBuilder{compCtx: &mock.ctxt}}
			source := &Expr{
				Typ:  makeSimplePlan2Type(types.T_text),
				Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
			}
			raw, kind, err := lowerer.numericArgumentProgramOutputs(source, tc.allowFrozenTextKind)
			require.NoError(t, err)
			require.NotNil(t, raw)
			require.NotNil(t, kind)
			if tc.ordinary {
				require.True(t, isFalseBoolLiteral(kind))
				require.Equal(t, int32(0), raw.GetP().Pos)
				require.True(t, state.valueDependent,
					"the false-kind specialization must remain execution-specific")
				return
			}
			_, slot, ok := numericProgramRefIdentity(kind)
			require.True(t, ok, "unknown or binary kind must retain the paired program")
			require.Equal(t, int64(1), slot)
			_, ok = numericProgramRawValue(raw)
			require.True(t, ok, "the value ref must retain its validated lexical owner")
		})
	}
}

func findNumericValueFunction(p *planpb.Plan, functionID int32, overloadID int32) *planpb.Function {
	if p == nil || p.GetQuery() == nil {
		return nil
	}
	var visit func(*planpb.Expr) *planpb.Function
	visit = func(expr *planpb.Expr) *planpb.Function {
		if expr == nil {
			return nil
		}
		if f := expr.GetF(); f != nil {
			fid, oid := function.DecodeOverloadID(f.GetFunc().GetObj())
			if fid == functionID && (overloadID < 0 || oid == overloadID) {
				return f
			}
			for _, arg := range f.Args {
				if found := visit(arg); found != nil {
					return found
				}
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				if found := visit(item); found != nil {
					return found
				}
			}
		}
		if sub := expr.GetSub(); sub != nil {
			return visit(sub.Child)
		}
		return nil
	}
	for _, node := range p.GetQuery().Nodes {
		if node == nil {
			continue
		}
		for _, group := range [][]*planpb.Expr{node.ProjectList, node.GroupBy, node.AggList, node.FilterList, node.OnList} {
			for _, expr := range group {
				if found := visit(expr); found != nil {
					return found
				}
			}
		}
	}
	return nil
}

func buildNumericValuePlan(t *testing.T, sql string) *planpb.Plan {
	t.Helper()
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	statements, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, statement := range statements {
			statement.Free()
		}
	})
	p, err := BuildPlan(ctx, statements[0], false)
	require.NoError(t, err)
	return p
}
