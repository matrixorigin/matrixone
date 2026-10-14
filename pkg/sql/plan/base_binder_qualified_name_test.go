// Copyright 2022 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestQualifiedColumnCorrelationScope(t *testing.T) {
	for _, depth := range []int32{1, 2} {
		t.Run(string(rune('0'+depth)), func(t *testing.T) {
			outer := NewBindContext(nil, nil)
			outer.bindingByTable["records"] = &Binding{tag: 11, db: "main", table: "records", types: []*pb.Type{{Id: int32(types.T_float64)}}, colIdByName: map[string]int32{"id": 0}}
			outer.binder = NewWhereBinder(nil, outer)
			parent := outer
			if depth == 2 {
				parent = NewBindContext(nil, outer)
				parent.bindingByTable["records"] = &Binding{tag: 33, db: "middle", table: "records", types: []*pb.Type{{Id: int32(types.T_int32)}}, colIdByName: map[string]int32{"id": 0}}
				parent.binder = NewWhereBinder(nil, parent)
				parent.groupTag = 333
				parent.groupByAst["records.id"] = 0
				parent.groups = []*pb.Expr{GetColExpr(pb.Type{Id: int32(types.T_int32)}, 33, 0)}
			}
			inner := NewBindContext(nil, parent)
			inner.bindingByTable["records"] = &Binding{tag: 22, db: "stage", table: "records", types: []*pb.Type{{Id: int32(types.T_int32)}}, colIdByName: map[string]int32{"id": 0}}
			binder := NewWhereBinder(nil, inner)
			inner.binder = binder
			name := func(db string) *tree.UnresolvedName {
				return tree.NewUnresolvedName(tree.NewCStr(db, 1), tree.NewCStr("records", 1), tree.NewCStr("id", 1))
			}
			bound, err := binder.BindColRef(name("main"), 0, false)
			require.NoError(t, err)
			require.NotNil(t, bound.GetCorr())
			require.Equal(t, int32(11), bound.GetCorr().RelPos)
			require.Equal(t, depth, bound.GetCorr().Depth)
			require.True(t, inner.isCorrelated)
			typ, ok := binder.numericColumnType(name("main"))
			require.True(t, ok)
			require.Equal(t, int32(types.T_float64), typ.Id)
			local, err := binder.BindColRef(name("stage"), 0, false)
			require.NoError(t, err)
			require.Equal(t, int32(22), local.GetCol().RelPos)
			_, _, matched := resolveColumnIdentity(inner, name("main"))
			require.False(t, matched, "GROUP/HAVING identity must not capture another database")
			require.NotEqual(t, canonicalGroupByAstKey(inner, name("main")), canonicalGroupByAstKey(inner, name("stage")))
			require.Equal(t, canonicalGroupByAstKey(inner, name("stage")), canonicalGroupByAstKey(inner, tree.NewUnresolvedName(tree.NewCStr("records", 1), tree.NewCStr("id", 1))))
			require.False(t, sameHavingColumnReference(inner, name("main"), name("stage")))
			require.False(t, sameHavingColumnReference(inner, name("main"), tree.NewUnresolvedName(tree.NewCStr("records", 1), tree.NewCStr("id", 1))))
			shadow, err := binder.BindColRef(tree.NewUnresolvedName(tree.NewCStr("records", 1), tree.NewCStr("id", 1)), 0, false)
			require.NoError(t, err)
			require.Equal(t, int32(22), shadow.GetCol().RelPos)
			_, err = binder.BindColRef(name("missing"), 0, false)
			require.Error(t, err)
		})
	}
}

func TestQualifiedTableBindingNamespaces(t *testing.T) {
	for _, tc := range []struct {
		name, bindingDB, refDB string
		lower                  int64
		match                  bool
	}{
		{"unqualified", "stage", "", 0, true},
		{"same database", "stage", "stage", 0, true},
		{"other database", "stage", "main", 0, false},
		{"case sensitive", "Stage", "stage", 0, false},
		{"case insensitive", "Stage", "stage", 1, true},
		{"system database", "information_schema", "INFORMATION_SCHEMA", 0, true},
		{"schema-less unqualified", "", "", 1, true},
		{"schema-less qualified", "", "main", 1, false},
		{"malformed identifier", "St\xffge", "st\xffge", 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewBindContext(nil, nil)
			ctx.lower = tc.lower
			binding := &Binding{db: tc.bindingDB, table: "r"}
			ctx.bindingByTable["r"] = binding
			if tc.match {
				require.Same(t, binding, ctx.resolveTableBinding(tc.refDB, "r"))
			} else {
				require.Nil(t, ctx.resolveTableBinding(tc.refDB, "r"))
			}
		})
	}
}

func TestQualifiedTableBindingRemapAndErrors(t *testing.T) {
	// Ordinary view contexts can carry an empty saved default database. The
	// existing rewrite fallback must expose the effective session namespace
	// even when this table has no matching rewrite rule.
	opt := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, defaultDatabase := range []string{"tpch", ""} {
		builder := NewQueryBuilder(pb.Query_SELECT, opt.CurrentContext(), false, false)
		ctx := NewBindContext(builder, nil)
		ctx.defaultDatabase = defaultDatabase
		ctx.remapOption = &tree.RewriteOption{}
		_, _, err := builder.buildTable(&tree.AliasedTableExpr{Expr: &tree.TableName{ObjectName: "nation"}}, ctx, nil)
		require.NoError(t, err)
		_, err = NewWhereBinder(builder, ctx).BindColRef(tree.NewUnresolvedName(tree.NewCStr("tpch", 1), tree.NewCStr("nation", 1), tree.NewCStr("n_nationkey", 1)), 0, false)
		require.NoError(t, err)
	}

	builder, ctx := genBuilderAndCtx(t)
	binding := ctx.bindingByTable["bind_select"]
	ctx.remapOption = &tree.RewriteOption{}
	ctx.bindingByTable[builder.compCtx.DefaultDatabase()+".rewritten"] = binding
	binder := NewWhereBinder(builder, ctx)
	for _, parts := range [][]string{{"rewritten", "a"}, {builder.compCtx.DefaultDatabase(), "rewritten", "a"}} {
		name := tree.NewUnresolvedName()
		for i := len(parts) - 1; i >= 0; i-- {
			name.CStrParts[len(parts)-1-i] = tree.NewCStr(parts[i], ctx.lower)
		}
		name.NumParts = len(parts)
		expr, err := binder.BindColRef(name, 0, false)
		require.NoError(t, err)
		require.Equal(t, binding.tag, expr.GetCol().RelPos)
		typ, ok := binder.numericColumnType(name)
		require.True(t, ok)
		require.Equal(t, binding.types[0].Id, typ.Id)
	}
	// Ambiguity and missing columns remain errors after namespace matching.
	binding.colIdByName["ambiguous"] = AmbiguousName
	for _, col := range []string{"ambiguous", "missing"} {
		_, err := binder.BindColRef(tree.NewUnresolvedName(tree.NewCStr("select_test", 1), tree.NewCStr("bind_select", 1), tree.NewCStr(col, 1)), 0, false)
		require.Error(t, err)
	}
}

func TestQualifiedViewColumnBinding(t *testing.T) {
	f := newViewSchemaTestFixture(t)
	f.addView(t, "root_v", "select n_name from nation")
	f.compiler.namedSnapshot = &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 123}}
	for _, sql := range []string{"select tpch.root_v.n_name from tpch.root_v", "select tpch.nation.n_name from tpch.nation {snapshot='daily'}"} {
		_, err := buildOneQuery(t, f.compiler, sql, false)
		require.NoError(t, err)
	}
}

func TestQualifiedSubscriptionColumnBinding(t *testing.T) {
	opt, ctx := newSubscriptionMetadataTestOptimizer(t)
	physicalDB := ctx.tables["nation"].DbName
	_, err := runOneStmt(opt, t, "select sub_db.nation.n_name from sub_db.nation")
	require.NoError(t, err)
	require.Equal(t, physicalDB, ctx.tables["nation"].DbName)
	_, err = runOneStmt(opt, t, "select tpch.nation.n_name from sub_db.nation")
	require.Error(t, err)
}
