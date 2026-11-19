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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// MySQL evaluates ENUM and SET in boolean contexts (IF, CASE WHEN,
// WHERE/HAVING/ON predicates, AND/OR/NOT, IS TRUE/FALSE) by their numeric
// storage: the ENUM ordinal and the SET bitmap, never the display label.
// These tests pin the storage-typed plan for every boolean context and keep
// the display label in ordinary string comparisons.

// iffCondition returns the bound IF() condition argument after the binder has
// applied the ENUM/SET numeric-truth contract.
func iffCondition(p *Plan) (*planpb.Expr, bool) {
	fn := findPlanFunctionExpr(p, "if")
	if fn == nil {
		return nil, false
	}
	return fn.GetF().Args[0], true
}

// firstBoolCastSource returns the operand type of the first cast-to-BOOL
// expression found under expr. The display label would be re-parsed as text,
// so the operand type discriminates the numeric-truth fix on the base plan.
func firstBoolCastSource(expr *planpb.Expr) (*planpb.Type, bool) {
	if expr == nil {
		return nil, false
	}
	if fn := expr.GetF(); fn != nil {
		overload0 := fn.Func.GetObjName() == "cast" && len(fn.Args) == 2
		if overload0 {
			if target := fn.Args[1].GetT(); target != nil && types.T(fn.Args[1].Typ.Id) == types.T_bool {
				return &fn.Args[0].Typ, true
			}
		}
		for _, child := range fn.Args {
			if candidate, ok := firstBoolCastSource(child); ok {
				return candidate, true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, child := range list.List {
			if candidate, ok := firstBoolCastSource(child); ok {
				return candidate, true
			}
		}
	}
	return nil, false
}

// firstFilterBoolCastSource returns the operand type of the first cast-to-BOOL
// on any node's filter list.
func firstFilterBoolCastSource(p *Plan) (*planpb.Type, bool) {
	query := p.GetQuery()
	if query == nil {
		return nil, false
	}
	for _, node := range query.Nodes {
		for _, expr := range node.FilterList {
			if source, ok := firstBoolCastSource(expr); ok {
				return source, true
			}
		}
	}
	return nil, false
}

func requireNoDisplayValueFn(t *testing.T, p *Plan) {
	t.Helper()
	require.Nil(t, findPlanFunctionExpr(p, moEnumCastIndexToValueFun),
		"ENUM display label function must not remain in a pure boolean-context plan")
	require.Nil(t, findPlanFunctionExpr(p, moSetCastIndexToValueFun),
		"SET display function must not remain in a pure boolean-context plan")
}

func TestIfConditionBindsEnumSetStorage(t *testing.T) {
	p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, "select if(e, 1, 0) from enum_order_t")
	require.NoError(t, err)
	cond, ok := iffCondition(p)
	require.True(t, ok)
	require.Equal(t, int32(types.T_enum), cond.Typ.Id, "IF condition must consume the raw ENUM ordinal")
	require.Equal(t, "low,mid,high", cond.Typ.Enumvalues)
	requireNoDisplayValueFn(t, p)

	p, err = runOneStmt(newMySQLSpecialOrderMock(t), t, "select if(s, 1, 0) from enum_order_t")
	require.NoError(t, err)
	cond, ok = iffCondition(p)
	require.True(t, ok)
	require.Equal(t, int32(types.T_uint64), cond.Typ.Id, "IF condition must consume the raw SET bitmap")
	require.Empty(t, cond.Typ.Enumvalues,
		"the recovered operand is an ordinary bitmap, not a SQL SET value")
	requireNoDisplayValueFn(t, p)
}

func TestIfConditionAfterGroupByBindsStorage(t *testing.T) {
	for _, sql := range []string{
		"select if(e, 1, 0) from enum_order_t group by e",
		"select if(s, 1, 0) from enum_order_t group by s",
	} {
		p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, sql)
		require.NoError(t, err, sql)
		cond, ok := iffCondition(p)
		require.True(t, ok, sql)
		require.NotEqual(t, int32(types.T_varchar), cond.Typ.Id,
			"grouped IF condition must not fall back to the display label text")
	}
}

func TestWherePredicateCastsEnumSetStorage(t *testing.T) {
	t.Run("enum bare predicate", func(t *testing.T) {
		p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, "select id from enum_order_t where e")
		require.NoError(t, err)
		requireNoDisplayValueFn(t, p)
		source, ok := firstFilterBoolCastSource(p)
		require.True(t, ok, "the WHERE predicate must be a cast to BOOL")
		require.Equal(t, int32(types.T_enum), source.Id)
	})

	t.Run("set bare predicate", func(t *testing.T) {
		p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, "select id from enum_order_t where s")
		require.NoError(t, err)
		requireNoDisplayValueFn(t, p)
		source, ok := firstFilterBoolCastSource(p)
		require.True(t, ok, "the WHERE predicate must be a cast to BOOL")
		require.Equal(t, int32(types.T_uint64), source.Id)
		requireEmptySetMetadata(t, source)
	})
}

func requireEmptySetMetadata(t *testing.T, typ *planpb.Type) {
	t.Helper()
	require.Empty(t, typ.Enumvalues,
		"the SET predicate consumes the plain bitmap, not a SQL SET identity")
}

func TestLogicalOperatorsAndCaseCastEnumSetStorage(t *testing.T) {
	for name, sql := range map[string]string{
		"and":     "select id from enum_order_t where e and id > 0",
		"or-proj": "select e or s from enum_order_t",
		"not":     "select id from enum_order_t where not e",
		"case":    "select case when e then 1 else 0 end from enum_order_t",
		"istrue":  "select e is true from enum_order_t",
	} {
		t.Run(name, func(t *testing.T) {
			p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, sql)
			require.NoError(t, err, sql)
			requireNoDisplayValueFn(t, p)
		})
	}
}

func TestStringComparisonsKeepEnumSetDisplayLabels(t *testing.T) {
	// String comparisons keep the display value; the boolean-context rewrite
	// must not swallow the label semantics of an ordinary equality predicate.
	p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, "select id from enum_order_t where e = 'low'")
	require.NoError(t, err)
	require.NotNil(t, findPlanFunctionExpr(p, moEnumCastIndexToValueFun))

	p, err = runOneStmt(newMySQLSpecialOrderMock(t), t, "select if(e = 'low', 1, 0) from enum_order_t")
	require.NoError(t, err)
	require.NotNil(t, findPlanFunctionExpr(p, moEnumCastIndexToValueFun),
		"an IF condition that is itself a comparison keeps the comparison's display operand")
}

// firstPlanBoolCastSource returns the operand type of the first cast-to-BOOL
// across every node's filter and projection lists.
func firstPlanBoolCastSource(p *Plan) (*planpb.Type, bool) {
	query := p.GetQuery()
	if query == nil {
		return nil, false
	}
	for _, node := range query.Nodes {
		for _, expr := range node.FilterList {
			if source, ok := firstBoolCastSource(expr); ok {
				return source, true
			}
		}
		for _, expr := range node.ProjectList {
			if source, ok := firstBoolCastSource(expr); ok {
				return source, true
			}
		}
	}
	return nil, false
}

// Grouped boolean consumers (CASE WHEN, logical operators and bare HAVING
// predicates) must recover the stored ordinal/bitmap provenance instead of
// re-parsing the group's display label as unrelated text.
func TestGroupedBooleanConditionsBindStorage(t *testing.T) {
	// A stored operand decides grouped boolean truth: the recovered expression
	// is the group key's storage (the ENUM ordinal or SET bitmap operand behind
	// the display call), never the group's display label. Empty-label ENUM
	// declarations and empty-member SETs display their sentinel and that
	// member identically as text, so label-parsing recovery cannot be a
	// boolean mechanism for them.
	tests := []struct {
		name    string
		sql     string
		want    types.T
		enumish bool
	}{
		{name: "grouped-case-e", sql: "select case when e then 1 else 0 end from enum_order_t group by e", enumish: true},
		{name: "grouped-case-s", sql: "select case when s then 1 else 0 end from enum_order_t group by s", want: types.T_uint64},
		{name: "grouped-and-e", sql: "select e and 1 from enum_order_t group by e", enumish: true},
		{name: "grouped-or-s", sql: "select s or 0 from enum_order_t group by s", want: types.T_uint64},
		{name: "grouped-not-e", sql: "select not e from enum_order_t group by e", enumish: true},
		{name: "grouped-having-e", sql: "select count(*) from enum_order_t group by e having e", enumish: true},
		{name: "grouped-having-s", sql: "select count(*) from enum_order_t group by s having s", want: types.T_uint64},
		// ENUM('', 'a') and SET('', 'a') declarations: the empty label/member
		// displays like the zero sentinel, so truth must come from storage.
		{name: "grouped-case-empty-label-e", sql: "select case when e then 1 else 0 end from enum_empty_label_t group by e", enumish: true},
		{name: "grouped-having-empty-label-e", sql: "select count(*) from enum_empty_label_t group by e having e", enumish: true},
		{name: "grouped-not-empty-label-e", sql: "select not e from enum_empty_label_t group by e", enumish: true},
		{name: "grouped-case-empty-member-s", sql: "select case when s then 1 else 0 end from set_empty_member_t group by s", want: types.T_uint64},
		{name: "grouped-having-empty-member-s", sql: "select count(*) from set_empty_member_t group by s having s", want: types.T_uint64},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, tc.sql)
			require.NoError(t, err, tc.sql)
			// Grouped plans legitimately keep the display function as the
			// group output; the boolean truth must instead consume the
			// recovered storage operand, so the BOOL cast's source type
			// discriminates the fix.
			source, ok := firstPlanBoolCastSource(p)
			require.True(t, ok, tc.sql)
			if tc.enumish {
				// The ENUM ordinal storage carries its definition order, kept
				// either in the ENUM plan type or its scalar uint16 base.
				require.Contains(t, []int32{int32(types.T_enum), int32(types.T_uint16)}, source.Id, tc.sql)
				require.NotEmpty(t, source.Enumvalues, tc.sql)
			} else {
				require.Equal(t, int32(tc.want), source.Id, tc.sql)
				requireEmptySetMetadata(t, source)
			}
		})
	}
}

// The grouping boundary itself carries storage identity: beside every ENUM or
// SET display group key the binder registers a hidden storage-identity group
// key, so grouping follows MySQL's ordinal identity even for declarations
// whose zero sentinel and a valid member display identically as text.
func TestGroupedDisplayKeysGetStorageSiblingSlot(t *testing.T) {
	for _, sql := range []string{
		"select count(*) as cnt from enum_empty_label_t group by e",
		"select count(*) as cnt from enum_order_t group by e",
		"select count(*) as cnt from set_empty_member_t group by s",
		"select count(*) as cnt from enum_order_t group by e, s",
	} {
		p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, sql)
		require.NoError(t, err, sql)
		found := false
		for _, node := range p.GetQuery().GetNodes() {
			if node.NodeType != planpb.Node_AGG {
				continue
			}
			for i, group := range node.GroupBy {
				if !isEnumOrSetDisplayValueExpr(group) {
					continue
				}
				require.True(t, i+1 < len(node.GroupBy), sql)
				sibling := node.GroupBy[i+1]
				require.NotNil(t, sibling.GetCol(), sql)
				// The ENUM sibling keeps its typed ordinal; the SET sibling is
				// the physical bitmap (uint64 with cleared SET metadata).
				require.True(t, isEnumPlanType(&sibling.Typ) ||
					(sibling.Typ.Id == int32(types.T_uint64) && len(sibling.Typ.GetEnumvalues()) == 0), sql)
				found = true
			}
		}
		require.True(t, found, sql)
	}
}

// MySQL BIT (b'..') and HEX (x'..') literals decide truth numerically, while
// binary introducer strings keep ordinary string truthiness. A pending cast of
// such a literal to BOOL folds to the numeric truth instead of handing the raw
// bytes to the string boolean parser.
func TestBinaryLiteralBoolCastFoldsNumericTruth(t *testing.T) {
	ctx := context.Background()
	boolType := planpb.Type{Id: int32(types.T_bool)}
	makeLiteral := func(payload string, form planpb.StringLiteralForm) *planpb.Expr {
		expr := makePlan2StringConstExprWithType(payload, true)
		expr.GetLit().LiteralForm = form
		return expr
	}
	for name, tc := range map[string]struct {
		payload     string
		form        planpb.StringLiteralForm
		wantNonzero bool
	}{
		"bit-one":     {string([]byte{0x01}), planpb.StringLiteralForm_STRING_LITERAL_BIT, true},
		"bit-zero":    {string([]byte{0x00}), planpb.StringLiteralForm_STRING_LITERAL_BIT, false},
		"bit-128":     {string([]byte{0x80}), planpb.StringLiteralForm_STRING_LITERAL_BIT, true},
		"bit-empty":   {"", planpb.StringLiteralForm_STRING_LITERAL_BIT, false},
		"hex-one":     {string([]byte{0x01}), planpb.StringLiteralForm_STRING_LITERAL_HEX, true},
		"hex-zero":    {string([]byte{0x00}), planpb.StringLiteralForm_STRING_LITERAL_HEX, false},
		"hex-leading": {string([]byte{0x00, 0x01}), planpb.StringLiteralForm_STRING_LITERAL_HEX, true},
	} {
		t.Run(name, func(t *testing.T) {
			got, rewritten, err := rewriteMySQLSpecialTypeDisplayCast(ctx, makeLiteral(tc.payload, tc.form), boolType)
			require.NoError(t, err)
			require.True(t, rewritten, "BIT/HEX literal BOOL casts fold to the numeric truth")
			lit := got.GetLit()
			require.NotNil(t, lit)
			require.Equal(t, tc.wantNonzero, lit.GetBval())
		})
	}
	t.Run("binary introducer keeps string truthiness", func(t *testing.T) {
		literal := makeLiteral(string([]byte{0x01}), planpb.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER)
		got, rewritten, err := rewriteMySQLSpecialTypeDisplayCast(ctx, literal, boolType)
		require.NoError(t, err)
		require.False(t, rewritten, "a binary introducer string must keep the string BOOL cast")
		require.Equal(t, literal, got)
	})
	t.Run("ordinary text keeps string truthiness", func(t *testing.T) {
		literal := makePlan2StringConstExprWithType("1")
		got, rewritten, err := rewriteMySQLSpecialTypeDisplayCast(ctx, literal, boolType)
		require.NoError(t, err)
		require.False(t, rewritten, "an ordinary text literal must keep the string BOOL cast")
		require.Equal(t, literal, got)
	})
}
