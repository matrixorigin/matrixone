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

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestProtectedCatalogDMLTargets(t *testing.T) {
	for _, sql := range []string{
		"UPDATE mo_catalog.mo_database SET datname = NULL WHERE dat_id = 123",
		"DELETE FROM mo_catalog.mo_database WHERE dat_id = 123",
		"INSERT INTO mo_catalog.mo_database(dat_id,datname) VALUES (123,'x')",
		"REPLACE INTO mo_catalog.mo_database(dat_id,datname) VALUES (123,'x')",
		"UPDATE mo_catalog.mo_tables SET relname = 'x' WHERE rel_id = 123",
		"DELETE FROM mo_catalog.mo_tables WHERE rel_id = 123",
		"UPDATE mo_catalog.mo_columns SET att_is_unsigned = 1 WHERE account_id = 0",
		"DELETE FROM mo_catalog.mo_columns WHERE account_id = 0",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := buildMySQLDMLCompatibilityPlan(t, sql)
			require.ErrorContains(t, err, "direct DML on mo_catalog.")
		})
	}
}

func TestProtectedCatalogDMLReadSourceAndUpgradeCapability(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO nation(n_nationkey,n_name,n_regionkey) SELECT dat_id,datname,0 FROM mo_catalog.mo_database",
		"UPDATE nation n JOIN mo_catalog.mo_database d ON n.n_nationkey=d.dat_id SET n.n_name='x'",
	} {
		_, err := buildMySQLDMLCompatibilityPlan(t, sql)
		require.NoError(t, err)
	}

	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	sql := "UPDATE mo_catalog.mo_columns SET att_is_unsigned = 1 WHERE account_id = 0"
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, "direct DML on mo_catalog.mo_columns")

	ctx.SetContext(context.WithValue(ctx.GetContext(), defines.MoColumnsUpdateKey{}, true))
	_, err = BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	// The capability applies only to the internal UPDATE, never another table
	// or a different DML operation in the same planning context.
	for _, sql := range []string{
		"UPDATE mo_catalog.mo_database SET datname='x'",
		"DELETE FROM mo_catalog.mo_columns WHERE account_id=0",
	} {
		other, parseErr := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, parseErr)
		_, err = BuildPlan(ctx, other, false)
		other.Free()
		require.ErrorContains(t, err, "direct DML on mo_catalog.")
	}
	ctx.SetContext(context.Background())
	_, err = BuildPlan(ctx, stmt, false)
	require.ErrorContains(t, err, "direct DML on mo_catalog.mo_columns")

	require.NoError(t, checkCatalogDMLTarget(context.Background(), &ObjectRef{SchemaName: "user_db", ObjName: "mo_database"}, false))
}
