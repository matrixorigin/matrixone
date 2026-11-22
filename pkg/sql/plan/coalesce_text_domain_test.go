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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// mixedCoalesce is binary-typed (an IMPLICIT binary cast ties with the text
// column and binary wins) but returns n_name's text value when it is not null.
const mixedCoalesce = "coalesce(n_name, cast(n_comment as varbinary))"

func buildTextDomainPlan(t *testing.T, sql string) *planpb.Plan {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	bound, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false, nil)
	require.NoError(t, err)
	return bound
}

func rootProjection(t *testing.T, bound *planpb.Plan) []*planpb.Expr {
	t.Helper()
	query := bound.GetQuery()
	require.NotEmpty(t, query.Steps)
	return query.Nodes[query.Steps[0]].ProjectList
}

// requireTextBranch checks a distributed consumer: a CASE whose first branch
// applies the consumer to a text-typed argument.
func requireTextBranch(t *testing.T, expr *planpb.Expr, consumer string) {
	t.Helper()
	require.Equal(t, "case", funcName(expr), expr.String())
	branch := expr.GetF().Args[1]
	found := findConsumer(branch, consumer)
	require.NotNil(t, found, branch.String())
	require.True(t, isStaticTextString(found.GetF().Args[0]), found.String())
}

func findConsumer(expr *planpb.Expr, name string) *planpb.Expr {
	if expr == nil {
		return nil
	}
	if funcName(expr) == name {
		return expr
	}
	for _, arg := range expr.GetF().GetArgs() {
		if found := findConsumer(arg, name); found != nil {
			return found
		}
	}
	return nil
}

func TestCoalesceTextDomainDistributesConsumers(t *testing.T) {
	for _, tc := range []struct {
		name, sql, consumer string
	}{
		{"direct", "select char_length(" + mixedCoalesce + ") from nation", "char_length"},
		{"forwarded through reverse", "select char_length(reverse(" + mixedCoalesce + ")) from nation", "char_length"},
		{"forwarded through nullif", "select char_length(nullif(" + mixedCoalesce + ", '')) from nation", "char_length"},
		{"left", "select hex(left(" + mixedCoalesce + ", 1)) from nation", "left"},
		{"lpad consumes but does not forward", "select char_length(lpad(" + mixedCoalesce + ", 3, 'z')) from nation", "lpad"},
		{"nested coalesce", "select char_length(coalesce(reverse(" + mixedCoalesce + "), '')) from nation", "char_length"},
		{"merged derived table", "select char_length(x) from (select " + mixedCoalesce + " x from nation) d", "char_length"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			projection := rootProjection(t, buildTextDomainPlan(t, tc.sql))
			expr := projection[0]
			if funcName(expr) != "case" {
				// The distributed consumer sits under a non-consumer such as HEX.
				expr = findConsumer(expr, "case")
			}
			require.NotNil(t, expr)
			requireTextBranch(t, expr, tc.consumer)
		})
	}
}

func TestCoalesceTextDomainThroughProjectColumn(t *testing.T) {
	// A flattened scalar subquery keeps its projection in its own Project, so
	// the consumer reads the value through a column of that Project.
	bound := buildTextDomainPlan(t,
		"select char_length((select "+mixedCoalesce+" from nation where n_nationkey = 1 limit 1))")
	consumer := rootProjection(t, bound)[0]
	require.Equal(t, "case", funcName(consumer), consumer.String())
	text := consumer.GetF().Args[1].GetF().Args[0]
	require.NotNil(t, text.GetCol(), text.String())
	require.True(t, isStaticTextString(text), text.String())
}

func TestCoalesceTextDomainLeavesOtherPlansUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
	}{
		{"text coalesce", "select char_length(coalesce(n_name, n_comment)) from nation"},
		{"binary coalesce", "select char_length(coalesce(cast(n_name as varbinary), cast(n_comment as varbinary))) from nation"},
		{"non-consumer", "select hex(" + mixedCoalesce + ") from nation"},
		{"byte-wise consumer", "select locate('a', " + mixedCoalesce + ") from nation"},
		{"materialized derived table", "select char_length(x) from (select " + mixedCoalesce + " x from nation limit 3) d"},
		{"volatile argument", "select char_length(coalesce(n_name, cast(uuid() as varbinary))) from nation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			projection := rootProjection(t, buildTextDomainPlan(t, tc.sql))
			require.Nil(t, findConsumer(projection[0], "case"), projection[0].String())
		})
	}
}

func TestMixedDomainCoalesceRecognition(t *testing.T) {
	text := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
	binary := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varbinary)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}}}
	coalesce := func(typ types.T, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(typ)},
			Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "coalesce"}, Args: args}},
		}
	}
	require.True(t, isMixedDomainCoalesce(coalesce(types.T_varbinary, text, binary)))
	require.False(t, isMixedDomainCoalesce(coalesce(types.T_varbinary, binary, binary)))
	require.False(t, isMixedDomainCoalesce(coalesce(types.T_varchar, text, text)))
	require.True(t, isMixedDomainCoalesce(coalesce(types.T_varbinary, coalesce(types.T_varbinary, text, binary), binary)))
	require.Nil(t, reachMixedDomainCoalesce(text))
	require.Nil(t, reachColumn(&planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{}}}))
	require.True(t, sameValueShape(text, DeepCopyExpr(text)))
	require.False(t, sameValueShape(text, binary))
	require.False(t, sameValueShape(text, nil))
}

func TestConditionalBinaryLiteralCoercibility(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		binary    bool
		probe     string
	}{
		// A binary literal is COERCIBLE and loses to an IMPLICIT text column.
		{"IF binary literal", "select reverse(if(n_nationkey > 0, n_name, _binary '')) from nation", false, ""},
		{"CASE hex literal", "select reverse(case when n_nationkey > 0 then n_name else X'ff' end) from nation", false, ""},
		{"COALESCE bit literal", "select reverse(coalesce(n_name, b'01100001')) from nation", false, ""},
		{"IFNULL binary literal", "select reverse(ifnull(n_name, _binary '')) from nation", false, ""},
		// Binary wins a tie, and an explicit CAST is IMPLICIT.
		{"binary cast ties", "select reverse(if(n_nationkey > 0, n_name, cast('' as binary(4)))) from nation", true, ""},
		{"binary column ties", "select hex(" + mixedCoalesce + ") from nation", true, "hex"},
		{"literal tie", "select reverse(if(n_nationkey > 0, 'abc', _binary '')) from nation", true, ""},
		{"explicit binary collation", "select reverse(if(n_nationkey > 0, n_name, _binary '' collate `binary`)) from nation", true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := tc.probe
			if probe == "" {
				probe = "reverse"
			}
			reverse := findPlanFunctionExpr(buildTextDomainPlan(t, tc.sql), probe)
			require.NotNil(t, reverse)
			require.Equal(t, tc.binary, isStaticBinaryString(reverse.GetF().Args[0]), reverse.String())
		})
	}
}

func TestStringDerivationRank(t *testing.T) {
	varchar := types.T_varchar.ToType()
	column := &planpb.Expr{Typ: makePlan2Type(&varchar), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
	param := &planpb.Expr{Typ: makePlan2Type(&varchar), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 1}}}
	null := makePlan2NullConstExprWithType()
	number := makePlan2Int64ConstExprWithType(1)
	cast := func(source *planpb.Expr, explicit bool) *planpb.Expr {
		return &planpb.Expr{
			Typ: makePlan2Type(&varchar),
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func:               &planpb.ObjectRef{ObjName: "cast"},
				Args:               []*planpb.Expr{source, {Typ: makePlan2Type(&varchar), Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}},
				SyntaxExplicitCast: explicit,
			}},
		}
	}
	lower := &planpb.Expr{
		Typ:  makePlan2Type(&varchar),
		Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "lower"}, Args: []*planpb.Expr{param}}},
	}
	explicitCollation := DeepCopyExpr(column)
	explicitCollation.Typ.CollationCoercibility, explicitCollation.Typ.CollationCoercibilitySet = 0, true

	require.Equal(t, uint32(2), stringDerivationRank(column))
	require.Equal(t, uint32(4), stringDerivationRank(param))
	require.Equal(t, uint32(0), stringDerivationRank(explicitCollation))
	require.Equal(t, uint32(2), stringDerivationRank(cast(param, true)))
	require.Equal(t, uint32(4), stringDerivationRank(cast(param, false)))
	require.Equal(t, uint32(6), stringDerivationRank(cast(null, false)))
	require.Equal(t, uint32(5), stringDerivationRank(cast(number, false)))
	require.Equal(t, uint32(2), stringDerivationRank(cast(column, false)))
	require.Equal(t, uint32(4), stringDerivationRank(lower))
}
