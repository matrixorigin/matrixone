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
