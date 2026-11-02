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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// D10 complete-domain zones for boolean truth (review repair, finding 2):
// storage-carried references stay exact for every declaration (zone 1),
// cross-block reconstruction over an injective declaration stays exact
// (zone 2), and every ambiguous declaration - an ENUM with an empty label or
// duplicated label, a SET with an empty member - fails closed with the typed
// error instead of silently re-parsing a display label that displays exactly
// like the zero sentinel (the probe-2 counterexample: the valid ordinal-1
// '' row would otherwise evaluate FALSE).

// TestCrossBlockTruthRecoveryFailsClosedOnAmbiguousDeclarations covers every
// boolean consumer read across a grouped or re-materialized query boundary:
// the ambiguous declaration cannot decide truth from display text, so the
// derived/DISTINCT boundary rejects with the typed error.
func TestCrossBlockTruthRecoveryFailsClosedOnAmbiguousDeclarations(t *testing.T) {
	for _, sql := range []string{
		// ENUM('','a'): the empty label displays exactly like the ordinal-0
		// sentinel, so the group's '' output row is ambiguous cross-block.
		"select if(v, 1, 0) from (select e as v from enum_empty_label_t group by e) d",
		"select case when v then 1 else 0 end from (select e as v from enum_empty_label_t group by e) d",
		"select v and 1 from (select e as v from enum_empty_label_t group by e) d",
		"select not v from (select e as v from enum_empty_label_t group by e) d",
		"select v is true from (select e as v from enum_empty_label_t group by e) d",
		"select if(v, 1, 0) from (select distinct e as v from enum_empty_label_t) d",
		"select if(v, 1, 0) from (select distinct s as v from set_empty_member_t) d",
		// SET('','a'): an empty member collides with the zero bitmask.
		"select if(v, 1, 0) from (select s as v from set_empty_member_t group by s) d",
		"select v and 1 from (select s as v from set_empty_member_t group by s) d",
		"select case when v then 1 else 0 end from (select s as v from set_empty_member_t group by s) d",
		// Duplicated labels order-recover but cannot numerically decode any
		// stored member uniquely, so the whole declaration stays zone 3.
		"select if(v, 1, 0) from (select e as v from enum_duplicate_t group by e) d",
		"select if(v, 1, 0) from (select e as v from enum_equal_fold_t group by e) d",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := runOneStmt(newMySQLSpecialOrderMock(t), t, sql)
			require.Error(t, err, sql,
				"an ambiguous declaration must fail closed over a display-decoding boundary")
			require.ErrorContains(t, err, "cannot recover the stored value")
		})
	}
}

// requireEnumInDomainIFFCondition asserts that a bound IF() consumed a
// recovered ENUM storage condition in-domain (iffCheck schedules no cast, so
// the condition type itself is the discriminator).
func requireEnumInDomainIFFCondition(t *testing.T, p *Plan, sql string) {
	t.Helper()
	cond, ok := iffCondition(p)
	require.True(t, ok, sql)
	require.Contains(t, []int32{int32(types.T_enum), int32(types.T_uint16)}, cond.Typ.Id, sql)
	require.NotEmpty(t, cond.Typ.Enumvalues, sql)
}

// requireSetStorageIFFCondition asserts that a bound IF() consumed the raw SET
// bitmap: the physical uint64 domain with the declaration metadata cleared.
func requireSetStorageIFFCondition(t *testing.T, p *Plan, sql string) {
	t.Helper()
	cond, ok := iffCondition(p)
	require.True(t, ok, sql)
	require.Equal(t, int32(types.T_uint64), cond.Typ.Id, sql)
	require.Empty(t, cond.Typ.Enumvalues, sql)
}

// requireStorageBoolCast asserts the plan contains a cast-to-BOOL whose
// source is the recovered ENUM ordinal / SET bitmap.
func requireStorageBoolCast(t *testing.T, p *Plan, sql string, enumish bool) {
	t.Helper()
	source, ok := firstPlanBoolCastSource(p)
	require.True(t, ok, sql,
		"an injective declaration reconstructs the stored ordinal/bitmap and casts THAT to BOOL")
	if enumish {
		require.Contains(t, []int32{int32(types.T_enum), int32(types.T_uint16)}, source.Id, sql)
		require.NotEmpty(t, source.Enumvalues, sql)
	} else {
		require.Equal(t, int32(types.T_uint64), source.Id, sql)
		requireEmptySetMetadata(t, source)
	}
}

// TestCrossBlockTruthOverInjectiveDeclarationsStaysExact pins zone 2: for an
// injective declaration the grouped display maps back to the exact ordinal or
// bitmap, so the same boundary statements keep binding with storage truth,
// and DISTINCT re-materializations behave the same way.
func TestCrossBlockTruthOverInjectiveDeclarationsStaysExact(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		assert func(t *testing.T, p *Plan, sql string)
	}{
		{sql: "select if(v, 1, 0) from (select e as v from enum_order_t group by e) d",
			assert: requireEnumInDomainIFFCondition},
		{sql: "select if(v, 1, 0) from (select s as v from enum_order_t group by s) d",
			assert: requireSetStorageIFFCondition},
		{sql: "select case when v then 1 else 0 end from (select e as v from enum_order_t group by e) d",
			assert: requireFirstStorageBoolCast(true)},
		{sql: "select case when v then 1 else 0 end from (select s as v from enum_order_t group by s) d",
			assert: requireFirstStorageBoolCast(false)},
		{sql: "select v and 1 from (select s as v from enum_order_t group by s) d",
			assert: requireFirstStorageBoolCast(false)},
		{sql: "select if(v, 1, 0) from (select distinct e as v from enum_order_t) d",
			assert: requireEnumInDomainIFFCondition},
		{sql: "select if(v, 1, 0) from (select distinct s as v from enum_order_t) d",
			assert: requireSetStorageIFFCondition},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, tc.sql)
			require.NoError(t, err, tc.sql)
			tc.assert(t, p, tc.sql)
		})
	}
}

// requireFirstStorageBoolCast adapts requireStorageBoolCast to the table
// closures below.
func requireFirstStorageBoolCast(enumish bool) func(t *testing.T, p *Plan, sql string) {
	return func(t *testing.T, p *Plan, sql string) {
		requireStorageBoolCast(t, p, sql, enumish)
	}
}

// TestSameBlockTruthStaysExactForAmbiguousDeclarations pins zone 1 against
// the zone-3 fail-closure: within the same query block the hidden storage
// slot carries exact identity for every declaration, so the empty-label and
// empty-member fixtures keep their truthful same-block behavior. Grouped
// keys legitimately keep the display wrapper as the visible group output; the
// consumer's operand is what discriminates truth. This is the retained
// probe-2 pair - the same-block equivalent passes while the cross-block
// statement fails closed.
func TestSameBlockTruthStaysExactForAmbiguousDeclarations(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		assert func(t *testing.T, p *Plan, sql string)
	}{
		{sql: "select if(e, 1, 0) from enum_empty_label_t group by e",
			assert: requireEnumInDomainIFFCondition},
		{sql: "select if(s, 1, 0) from set_empty_member_t group by s",
			assert: requireSetStorageIFFCondition},
		{sql: "select case when e then 1 else 0 end from enum_empty_label_t group by e",
			assert: func(t *testing.T, p *Plan, sql string) {
				requireStorageBoolCast(t, p, sql, true)
			}},
		{sql: "select case when s then 1 else 0 end from set_empty_member_t group by s",
			assert: func(t *testing.T, p *Plan, sql string) {
				requireStorageBoolCast(t, p, sql, false)
			}},
		{sql: "select e and 1 from enum_empty_label_t group by e",
			assert: func(t *testing.T, p *Plan, sql string) {
				requireStorageBoolCast(t, p, sql, true)
			}},
		{sql: "select not e from enum_empty_label_t group by e",
			assert: func(t *testing.T, p *Plan, sql string) {
				requireStorageBoolCast(t, p, sql, true)
			}},
		{sql: "select count(*) from enum_empty_label_t group by e having e",
			assert: func(t *testing.T, p *Plan, sql string) {
				requireStorageBoolCast(t, p, sql, true)
			}},
		// Direct row references carry the display wrapper, whose storage is
		// unwrapped structurally: exact for every declaration, and no display
		// function may remain in these pure boolean-context plans.
		{sql: "select if(e, 1, 0) from enum_empty_label_t",
			assert: func(t *testing.T, p *Plan, sql string) {
				requireEnumInDomainIFFCondition(t, p, sql)
				requireNoDisplayValueFn(t, p)
			}},
		{sql: "select if(s, 1, 0) from set_empty_member_t",
			assert: func(t *testing.T, p *Plan, sql string) {
				requireSetStorageIFFCondition(t, p, sql)
				requireNoDisplayValueFn(t, p)
			}},
		{sql: "select not s from set_empty_member_t",
			assert: func(t *testing.T, p *Plan, sql string) {
				requireStorageBoolCast(t, p, sql, false)
				requireNoDisplayValueFn(t, p)
			}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, tc.sql)
			require.NoError(t, err, tc.sql,
				"the same-query-block storage identity stays exact for every declaration")
			tc.assert(t, p, tc.sql)
		})
	}
}

// TestExplicitBoolCastBindsGroupedStorage pins the explicit CAST ... AS BOOL
// seam: the cast's operand is the recovered stored ENUM ordinal / SET bitmap
// for every boundary the boolean-truth consumers already cover - a projected
// display label reached over GROUP BY / DISTINCT decodes back to its storage.
// The discriminator is argument-level: the display value function itself is
// ENUM-typed, so a type-only assertion cannot tell recovery from a label
// re-parse; the cast argument subtree must carry the display function no
// more, and its source type must be the storage domain.
func TestExplicitBoolCastBindsGroupedStorage(t *testing.T) {
	for _, tc := range []struct {
		sql     string
		enumish bool
	}{
		{sql: "select cast(v as bool) from (select e as v from enum_order_t group by e) d", enumish: true},
		{sql: "select cast(v as bool) from (select s as v from enum_order_t group by s) d", enumish: false},
		{sql: "select cast(v as bool) from (select distinct e as v from enum_order_t) d", enumish: true},
		{sql: "select cast(v as bool) from (select distinct s as v from enum_order_t) d", enumish: false},
		{sql: "select cast(e as bool) from enum_order_t group by e", enumish: true},
		{sql: "select cast(s as bool) from enum_order_t group by s", enumish: false},
		{sql: "select if(e, 1, 0), cast(e as bool) from enum_order_t group by e", enumish: true},
		{sql: "select e, cast(e as bool) from enum_order_t group by e", enumish: true},
		{sql: "select s, cast(s as bool) from enum_order_t group by s", enumish: false},
		{sql: "select count(*) from enum_order_t group by e having cast(e as bool)", enumish: true},
		{sql: "select cast(e as bool) from enum_order_t group by e order by cast(e as bool)", enumish: true},
		// The ungrouped direct reference keeps the direct storage unwrap.
		{sql: "select cast(e as bool) from enum_order_t", enumish: true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, tc.sql)
			require.NoError(t, err, tc.sql,
				"the explicit BOOL cast must bind the recovered storage value")
			requireBoolCastConsumesStorage(t, p, tc.sql, tc.enumish)
		})
	}
}

// firstPlanBoolCastArg returns the argument expression of the first
// cast-to-BOOL across every node's filter and projection lists.
func firstPlanBoolCastArg(p *Plan) (*Expr, bool) {
	query := p.GetQuery()
	if query == nil {
		return nil, false
	}
	for _, node := range query.Nodes {
		for _, expr := range node.FilterList {
			if arg, ok := firstBoolCastArg(expr); ok {
				return arg, true
			}
		}
		for _, expr := range node.ProjectList {
			if arg, ok := firstBoolCastArg(expr); ok {
				return arg, true
			}
		}
	}
	return nil, false
}

func firstBoolCastArg(expr *Expr) (*Expr, bool) {
	if expr == nil {
		return nil, false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func.GetObjName() == "cast" && len(fn.Args) == 2 &&
			types.T(fn.Args[1].Typ.Id) == types.T_bool {
			return fn.Args[0], true
		}
		for _, child := range fn.Args {
			if arg, ok := firstBoolCastArg(child); ok {
				return arg, true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, child := range list.List {
			if arg, ok := firstBoolCastArg(child); ok {
				return arg, true
			}
		}
	}
	return nil, false
}

// findExprFunctionExpr searches one expression tree for a function
// expression by object name.
func findExprFunctionExpr(expr *Expr, name string) *Expr {
	if expr == nil {
		return nil
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func.GetObjName() == name {
			return expr
		}
		for _, child := range fn.Args {
			if found := findExprFunctionExpr(child, name); found != nil {
				return found
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, child := range list.List {
			if found := findExprFunctionExpr(child, name); found != nil {
				return found
			}
		}
	}
	return nil
}

// requireBoolCastConsumesStorage asserts the first cast-to-BOOL argument
// subtree no longer carries any display value function and keeps the storage
// domain type: label decoding must never decide the cast.
func requireBoolCastConsumesStorage(t *testing.T, p *Plan, sql string, enumish bool) {
	t.Helper()
	arg, ok := firstPlanBoolCastArg(p)
	require.True(t, ok, sql, "the plan must contain the explicit BOOL cast")
	require.Nil(t, findExprFunctionExpr(arg, moEnumCastIndexToValueFun), sql,
		"the cast argument must not convert the ENUM display label")
	require.Nil(t, findExprFunctionExpr(arg, moSetCastIndexToValueFun), sql,
		"the cast argument must not convert the SET display label")
	if enumish {
		require.Contains(t, []int32{int32(types.T_enum), int32(types.T_uint16)}, arg.Typ.Id, sql)
		require.NotEmpty(t, arg.Typ.Enumvalues, sql)
	} else {
		require.Equal(t, int32(types.T_uint64), arg.Typ.Id, sql)
		requireEmptySetMetadata(t, &arg.Typ)
	}
}

// TestCrossBlockExplicitBoolCastFailsClosed extends the zone-3 fail-closure
// to the explicit CAST consumer: a display-decoding boundary cannot resolve
// truth for an ambiguous declaration, so the cast fails with the same typed
// recoverability error instead of deciding truth from label text.
func TestCrossBlockExplicitBoolCastFailsClosed(t *testing.T) {
	for _, sql := range []string{
		"select cast(v as bool) from (select e as v from enum_empty_label_t group by e) d",
		"select cast(v as bool) from (select distinct e as v from enum_empty_label_t) d",
		"select cast(v as bool) from (select e as v from enum_duplicate_t group by e) d",
		"select cast(v as bool) from (select s as v from set_empty_member_t group by s) d",
		"select cast(v as bool) from (select distinct s as v from set_empty_member_t) d",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := runOneStmt(newMySQLSpecialOrderMock(t), t, sql)
			require.Error(t, err, sql,
				"an ambiguous declaration must fail closed for the explicit BOOL cast too")
			require.ErrorContains(t, err, "cannot recover the stored value")
		})
	}
}

// TestNumericSetInListRecoversStorageAcrossBoundaries pins the r7 review
// repair: a numeric IN/NOT IN list over an empty-member SET keeps reading the
// stored bitmap through DISTINCT, GROUP BY and plain transparent derived
// boundaries. mysqlSpecialNumericOperand recovers those operands from
// canonical-materialized / transparent retained identity exactly - empty
// member or not - and fails closed with its typed error when no exact
// identity exists, so routing SET operands unconditionally through the
// helper is safe at every declaration; the narrow display-decode
// reversibility requirement stays the ENUM arm's policy. The recovery is
// mode-unconditional exactly like base: the numeric contract predates the
// boolean mode scalar, and the probes confirm base itself answers from the
// stored bitmap in native sessions too.
func TestNumericSetInListRecoversStorageAcrossBoundaries(t *testing.T) {
	storageAssert := func(t *testing.T, p *Plan, sql string) {
		operands := collectInListComparisonOperands(p)
		require.NotEmpty(t, operands, sql,
			"the plan must contain the IN/NOT IN member comparisons")
		for _, operand := range operands {
			require.Equal(t, int32(types.T_uint64), operand.Typ.Id, sql,
				"every IN-list member comparison must bind the recovered stored bitmap")
			require.Nil(t, findExprFunctionExpr(operand, moSetCastIndexToValueFun), sql,
				"the operand must not convert the SET display label")
		}
	}
	for _, sql := range []string{
		"select v in (2) from (select distinct s as v from set_empty_member_t) d",
		"select v not in (2) from (select distinct s as v from set_empty_member_t) d",
		"select v in (2) from (select s as v from set_empty_member_t group by s) d",
		"select v not in (2) from (select s as v from set_empty_member_t group by s) d",
		"select v in (1) from (select s as v from set_empty_member_t) d",
		"select v in (2, 3) from (select distinct s as v from set_empty_member_t) d",
		// Same-block control: the direct storage comparison already applies.
		"select s in (2) from set_empty_member_t",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, sql)
			require.NoError(t, err, sql,
				"the numeric IN-list contract binds without a fail-closed error")
			storageAssert(t, p, sql)
		})
	}
	t.Run("native session keeps base's unconditional numeric contract", func(t *testing.T) {
		sql := "select v in (2) from (select distinct s as v from set_empty_member_t) d"
		opt, _ := nativeModeOptimizer(t)
		p, err := runOneStmt(opt, t, sql)
		require.NoError(t, err, sql)
		storageAssert(t, p, sql)
	})
	t.Run("boolean truth bounds stay fail-closed separately", func(t *testing.T) {
		_, err := runOneStmt(newMySQLSpecialOrderMock(t), t,
			"select if(v, 1, 0) from (select distinct s as v from set_empty_member_t) d")
		require.ErrorContains(t, err, "cannot recover the stored value",
			"the same ambiguous boundary must keep failing closed for boolean truth")
	})
	t.Run("empty-label ENUM keeps the display-decode policy", func(t *testing.T) {
		sql := "select v in (1) from (select distinct e as v from enum_empty_label_t) d"
		p, err := runOneStmt(newMySQLSpecialOrderMock(t), t, sql)
		require.NoError(t, err, sql)
		operands := collectInListComparisonOperands(p)
		require.NotEmpty(t, operands, sql)
		for _, operand := range operands {
			require.NotEqual(t, int32(types.T_uint64), operand.Typ.Id, sql,
				"the ENUM arm keeps its reversible-only policy: the display operand keeps the ordinary mixed comparison")
		}
	})
}

// collectInListComparisonOperands returns the non-constant operand of every
// IN-list member comparison in the plan's projections. A single-member IN
// normalizes to `=` (NOT IN to `!=`) and multi-member lists normalize into
// or-chains of these comparisons, each carrying the integer member as a
// literal cast - the other side is the recovered or retained-display operand.
func collectInListComparisonOperands(p *Plan) []*Expr {
	query := p.GetQuery()
	if query == nil {
		return nil
	}
	var operands []*Expr
	isIntLiteralCast := func(e *Expr) bool {
		fn := e.GetF()
		if fn == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) != 2 {
			return false
		}
		return fn.Args[0].GetLit() != nil && fn.Args[0].Typ.Id == int32(types.T_int64)
	}
	var collect func(expr *Expr)
	collect = func(expr *Expr) {
		if expr == nil {
			return
		}
		if fn := expr.GetF(); fn != nil {
			name := fn.Func.GetObjName()
			if (name == "=" || name == "!=") && len(fn.Args) == 2 {
				switch {
				case isIntLiteralCast(fn.Args[1]):
					operands = append(operands, fn.Args[0])
				case isIntLiteralCast(fn.Args[0]):
					operands = append(operands, fn.Args[1])
				}
			}
			if (name == "in" || name == "not_in" || name == "partition_in") &&
				len(fn.Args) >= 2 && fn.Args[1].GetList() != nil {
				operands = append(operands, fn.Args[0])
			}
			for _, child := range fn.Args {
				if name != "=" && name != "!=" {
					collect(child)
				}
			}
		}
	}
	for _, node := range query.Nodes {
		for _, expr := range node.ProjectList {
			collect(expr)
		}
	}
	return operands
}
