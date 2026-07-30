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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func buildMySQLDMLCompatibilityPlan(t *testing.T, sql string) (*Plan, error) {
	t.Helper()
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	return BuildPlan(ctx, stmt, false)
}

func requireMySQLDMLCompatibilityError(t *testing.T, sql string, code uint16, message string) {
	t.Helper()
	_, err := buildMySQLDMLCompatibilityPlan(t, sql)
	require.Error(t, err)
	moErr, ok := err.(*moerr.Error)
	require.True(t, ok, "unexpected error type %T: %v", err, err)
	require.Equal(t, code, moErr.MySQLCode())
	require.Equal(t, message, moErr.Error())
}

func TestMultiTableUpdateRejectsOrderByAndLimit(t *testing.T) {
	requireMySQLDMLCompatibilityError(
		t,
		"UPDATE nation JOIN region ON region.r_regionkey = nation.n_regionkey SET nation.n_name = region.r_name ORDER BY nation.n_nationkey",
		moerr.ER_WRONG_USAGE,
		"Incorrect usage of UPDATE and ORDER BY",
	)
	requireMySQLDMLCompatibilityError(
		t,
		"UPDATE nation JOIN region ON region.r_regionkey = nation.n_regionkey SET nation.n_name = region.r_name LIMIT 1",
		moerr.ER_WRONG_USAGE,
		"Incorrect usage of UPDATE and LIMIT",
	)
}

func TestUpdateRejectsDirectTargetTableSubqueries(t *testing.T) {
	tests := []string{
		"UPDATE nation SET n_name = 'x' WHERE n_nationkey IN (SELECT n_nationkey FROM nation)",
		"UPDATE nation SET n_name = (SELECT max(n_name) FROM nation)",
		"UPDATE nation SET n_name = 'x' WHERE EXISTS (SELECT 1 FROM region WHERE EXISTS (SELECT 1 FROM nation))",
		"UPDATE nation AS dst SET n_name = 'x' WHERE n_nationkey IN (SELECT n_nationkey FROM nation AS src)",
		"UPDATE nation JOIN region ON region.r_regionkey = nation.n_regionkey SET nation.n_name = region.r_name WHERE nation.n_nationkey IN (SELECT n_nationkey FROM nation)",
	}
	for _, sql := range tests {
		requireMySQLDMLCompatibilityError(
			t,
			sql,
			moerr.ER_UPDATE_TABLE_USED,
			"You can't specify target table 'nation' for update in FROM clause",
		)
	}
}

func TestDeleteRejectsDirectTargetTableSubquery(t *testing.T) {
	for _, sql := range []string{
		"DELETE FROM nation WHERE n_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey > 0)",
		"DELETE nation FROM nation JOIN region ON region.r_regionkey = nation.n_regionkey WHERE EXISTS (SELECT 1 FROM nation)",
	} {
		requireMySQLDMLCompatibilityError(
			t,
			sql,
			moerr.ER_UPDATE_TABLE_USED,
			"You can't specify target table 'nation' for update in FROM clause",
		)
	}
}

func TestMySQLDMLCompatibilityAllowsLegalShapes(t *testing.T) {
	tests := []string{
		"UPDATE nation SET n_name = 'x' ORDER BY n_nationkey LIMIT 1",
		"UPDATE nation SET n_name = 'x' WHERE n_regionkey IN (SELECT r_regionkey FROM region)",
		"UPDATE nation SET n_name = 'x' WHERE n_nationkey IN (SELECT n_nationkey FROM (SELECT n_nationkey FROM nation) AS materialized_nation)",
		"UPDATE nation AS dst JOIN nation AS src ON dst.n_nationkey = src.n_nationkey SET dst.n_name = src.n_name",
		"DELETE FROM nation WHERE n_nationkey IN (SELECT n_nationkey FROM (SELECT n_nationkey FROM nation) AS materialized_nation)",
	}
	for _, sql := range tests {
		_, err := buildMySQLDMLCompatibilityPlan(t, sql)
		require.NoError(t, err, sql)
	}
}
