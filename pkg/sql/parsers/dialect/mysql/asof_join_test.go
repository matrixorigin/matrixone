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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestAsofJoinSyntaxRoundTrip(t *testing.T) {
	tests := []string{
		"select * from l l asof join r on l.k = r.k and l.ts >= r.ts",
		"select * from l AS l asof join r on l.k = r.k and l.ts >= r.ts",
		"select * from l AS l asof left join r on l.k = r.k and l.ts > r.ts tolerance interval 2 minute",
		"select * from l AS l asof left outer join r on l.k1 = r.k1 and l.k2 = r.k2 and r.ts <= l.ts",
	}

	for _, sql := range tests {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
		formatted := tree.String(stmt, dialect.MYSQL)
		stmt.Free()

		roundTrip, err := ParseOne(context.Background(), formatted, 1)
		require.NoError(t, err, formatted)
		require.Equal(t, formatted, tree.String(roundTrip, dialect.MYSQL))
		roundTrip.Free()
	}
}

func TestAsofJoinAfterAliasedTableFactorSuffix(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		joinType string
		alias    tree.Identifier
		cols     tree.IdentifierList
		hints    []*tree.IndexHint
	}{
		{
			name:     "derived table alias column list",
			sql:      "select * from (select 1 k, 2 ts) l(k, ts) asof join r r on l.k = r.k and l.ts >= r.ts",
			joinType: tree.JOIN_TYPE_ASOF,
			alias:    "l",
			cols:     tree.IdentifierList{"k", "ts"},
		},
		{
			name:     "derived table explicit quoted alias column list",
			sql:      "select * from (select 1 k, 2 ts) AS `l`(`k`, `ts`) asof left outer join r r on l.k = r.k and l.ts >= r.ts",
			joinType: tree.JOIN_TYPE_ASOF_LEFT,
			alias:    "l",
			cols:     tree.IdentifierList{"k", "ts"},
		},
		{
			name:     "table alias index hint",
			sql:      "select * from l l use index (idx) asof join r r on l.k = r.k and l.ts >= r.ts",
			joinType: tree.JOIN_TYPE_ASOF,
			alias:    "l",
			hints: []*tree.IndexHint{{
				IndexNames: []string{"idx"},
				HintType:   tree.HintUse,
				HintScope:  tree.HintForScan,
			}},
		},
		{
			name:     "table explicit alias multiple scoped index hints",
			sql:      "select * from l AS data use index (idx1) force index for join (idx2) asof left join r r on data.k = r.k and data.ts >= r.ts",
			joinType: tree.JOIN_TYPE_ASOF_LEFT,
			alias:    "data",
			hints: []*tree.IndexHint{
				{IndexNames: []string{"idx1"}, HintType: tree.HintUse, HintScope: tree.HintForScan},
				{IndexNames: []string{"idx2"}, HintType: tree.HintForce, HintScope: tree.HintForJoin},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), test.sql, 1)
			require.NoError(t, err, test.sql)
			defer stmt.Free()
			join := stmt.(*tree.Select).Select.(*tree.SelectClause).From.Tables[0].(*tree.JoinTableExpr)
			require.Equal(t, test.joinType, join.JoinType, test.sql)
			left := join.Left.(*tree.AliasedTableExpr)
			require.Equal(t, test.alias, left.As.Alias, test.sql)
			require.Equal(t, test.cols, left.As.Cols, test.sql)
			require.Equal(t, test.hints, left.IndexHints, test.sql)

			formatted := tree.String(stmt, dialect.MYSQL)
			roundTrip, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err, formatted)
			defer roundTrip.Free()
			require.Equal(t, formatted, tree.String(roundTrip, dialect.MYSQL))
		})
	}
}

func TestAsofNestedAliasKeepsLegacyInnerJoinSemantics(t *testing.T) {
	sql := "select * from (select s.k from s s) asof join u on asof.k = u.k"
	stmt, err := ParseOne(context.Background(), sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	join := stmt.(*tree.Select).Select.(*tree.SelectClause).From.Tables[0].(*tree.JoinTableExpr)
	require.Equal(t, tree.JOIN_TYPE_INNER, join.JoinType)
	left := join.Left.(*tree.AliasedTableExpr)
	require.Equal(t, tree.Identifier("asof"), left.As.Alias)
}

func TestAsofJoinStillRequiresAliasedLeftTableFactor(t *testing.T) {
	stmt, err := ParseOne(
		context.Background(),
		"select * from l use index (idx) asof join r r on l.k = r.k and l.ts >= r.ts",
		1,
	)
	if stmt != nil {
		defer stmt.Free()
	}
	require.ErrorContains(t, err, "requires an aliased left table factor")
}

func TestAsofRemainsAnIdentifierOutsideJoin(t *testing.T) {
	for _, sql := range []string{
		"select asof from t",
		"select t.asof from t",
		"select * from asof",
		"select * from t as asof",
		"create table asof (asof int)",
		"select * from `asof` join u on `asof`.k = u.k",
		"select * from t as asof join u on asof.k = u.k",
		"select * from db.`asof` join u on `asof`.k = u.k",
		"select * from /* c */ `asof` join u on `asof`.k = u.k",
		"select * from -- c\n  `asof` join u on `asof`.k = u.k",
		"select * from # c\n  `asof` join u on `asof`.k = u.k",
		"select * from // c\n  `asof` join u on `asof`.k = u.k",
		"select * from t, `asof` join u on `asof`.k = u.k",
		"select * from (`asof` join u on `asof`.k = u.k)",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
		stmt.Free()
	}
}

func TestAsofExplicitAliasKeepsInnerJoinSemantics(t *testing.T) {
	for _, sql := range []string{
		"select * from t AS asof join u on asof.k = u.k",
		"select * from t AS asof join u on asof.k = u.k and u.v = 1",
		"select * from t AS asof join u on asof.k = u.k and asof.ts > u.ts",
		"select * from t AS asof join u on asof.k = u.k and u.v > 1",
		"select * from t AS asof join u on asof.k = u.k and u.tolerance = 1",
		"select * from t AS asof join u on a = b and x > y",
		"select * from (select 1 as k) AS asof join (select 1 as k) u on asof.k = u.k",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
		join := stmt.(*tree.Select).Select.(*tree.SelectClause).From.Tables[0].(*tree.JoinTableExpr)
		require.Equal(t, tree.JOIN_TYPE_INNER, join.JoinType, sql)
		left := join.Left.(*tree.AliasedTableExpr)
		require.Equal(t, tree.Identifier("asof"), left.As.Alias, sql)
		stmt.Free()
	}
}

func TestAsofLegacyImplicitAliasKeepsInnerJoinSemantics(t *testing.T) {
	tests := []struct {
		sql       string
		leftAlias tree.Identifier
		joinType  string
	}{
		{sql: "select * from asof join u on asof.k = u.k", joinType: tree.JOIN_TYPE_INNER},
		{sql: "select * from t asof join u on asof.k = u.k", leftAlias: "asof", joinType: tree.JOIN_TYPE_INNER},
		{sql: "select * from t asof join u on asof.k = u.k and asof.ts > u.ts", leftAlias: "asof", joinType: tree.JOIN_TYPE_INNER},
		{sql: "select * from t asof join u on a = b and x > y", leftAlias: "asof", joinType: tree.JOIN_TYPE_INNER},
		{sql: "select * from t asof join u on asof.k = u.k and u.tolerance = 1", leftAlias: "asof", joinType: tree.JOIN_TYPE_INNER},
		{sql: "select * from t asof join u on a = b where x > y", leftAlias: "asof", joinType: tree.JOIN_TYPE_INNER},
		{sql: "select * from t asof left join u on asof.k = u.k", leftAlias: "asof", joinType: tree.JOIN_TYPE_LEFT},
		{sql: "select * from (select 1 as k) asof join (select 1 as k) u on asof.k = u.k", leftAlias: "asof", joinType: tree.JOIN_TYPE_INNER},
		{sql: "select * from (select 1 as k) asof(k) join u on asof.k = u.k", leftAlias: "asof", joinType: tree.JOIN_TYPE_INNER},
		{sql: "select * from t asof use index (idx) join u on asof.k = u.k", leftAlias: "asof", joinType: tree.JOIN_TYPE_INNER},
	}

	for _, test := range tests {
		stmt, err := ParseOne(context.Background(), test.sql, 1)
		require.NoError(t, err, test.sql)
		join := stmt.(*tree.Select).Select.(*tree.SelectClause).From.Tables[0].(*tree.JoinTableExpr)
		require.Equal(t, test.joinType, join.JoinType, test.sql)
		left := join.Left.(*tree.AliasedTableExpr)
		require.Equal(t, test.leftAlias, left.As.Alias, test.sql)
		stmt.Free()
	}
}

func TestAsofJoinProducesAsofAst(t *testing.T) {
	for _, sql := range []string{
		"select * from l l asof join r on l.k = r.k and l.ts >= r.ts",
		"select * from l data asof join r on data.k = r.k and data.ts >= r.ts",
		"select * from l AS count asof join r on count.k = r.k and count.ts >= r.ts",
		"select * from l 'left_alias' asof join r r on left_alias.k = r.k and left_alias.ts >= r.ts",
		"select * from f() f asof join r r on f.k = r.k and f.ts >= r.ts",
		"select * from l l join m m asof join r r on m.k = r.k and m.ts >= r.ts",
		"select * from l l cross apply f() f asof join r r on l.k = r.k and f.ts >= r.ts",
		"select * from (select 1 k, cast('2026-01-01' as timestamp) ts) l asof join r on l.k = r.k and l.ts >= r.ts",
		"select * from l AS l asof join r on l.k = r.k and l.ts >= r.ts",
		"select * from l AS l asof join r on l.k = r.k and r.ts <= l.ts",
		"select * from l AS l asof join r on lk = r.rk and event_ts >= r.effective_ts",
		"select * from l AS l asof join r on lk = r.rk and r.effective_ts <= event_ts",
		"select * from l AS l asof join r on lk = rk and event_ts >= effective_ts",
		"select * from l AS l asof join r on lk = rk and a >= b",
		"select * from l AS l asof join r on lk = rk and revision >= baseline",
		"select * from l AS l asof join r on l.k = r.k",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
		join := stmt.(*tree.Select).Select.(*tree.SelectClause).From.Tables[0].(*tree.JoinTableExpr)
		require.Equal(t, tree.JOIN_TYPE_ASOF, join.JoinType, sql)
		stmt.Free()
	}
}

func TestAsofJoinNamesDoNotChangeContext(t *testing.T) {
	for _, sql := range []string{
		"select * from l AS l asof join asof on l.k = asof.k and l.ts >= asof.ts",
		"select * from l AS l asof join r asof on l.k = r.k and l.ts >= r.ts",
		"select * from l AS l asof join r asof on l.k = asof.k and l.ts >= asof.ts",
		"select * from t asof asof join r r on asof.k = r.k and asof.ts >= r.ts",
		"select * from t as `l` asof join r on `l`.k = r.k and `l`.ts >= r.ts",
		"select * from l AS l asof join r AS asof on l.k = asof.k and l.ts >= asof.ts",
		"select * from l AS l asof join r AS `asof` on l.k = `asof`.k and l.ts >= `asof`.ts",
		"select * from l AS l asof join db.r AS asof on l.k = asof.k and l.ts >= asof.ts",
		"select * from l AS l asof join db.`r` AS `asof` on l.k = `asof`.k and l.ts >= `asof`.ts",
		"select * from l AS l asof join db.r `asof` on l.k = `asof`.k and l.ts >= `asof`.ts",
		"select * from `l` AS `l` asof join `db`.`r` AS `asof` on `l`.k = `asof`.k and `l`.ts >= `asof`.ts",
		"select * from l AS l asof join (select 1 k, ')' marker) AS asof on l.k = asof.k and l.ts >= asof.ts",
		"select * from l AS l asof join (select 1 k, now() ts /* ) */) AS asof on l.k = asof.k and l.ts >= asof.ts",
		"select * from l AS l /* left */ asof /* modifier */ join r on l.k = r.k and l.ts >= r.ts",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
		join := stmt.(*tree.Select).Select.(*tree.SelectClause).From.Tables[0].(*tree.JoinTableExpr)
		require.Equal(t, tree.JOIN_TYPE_ASOF, join.JoinType, sql)
		stmt.Free()
	}
}
