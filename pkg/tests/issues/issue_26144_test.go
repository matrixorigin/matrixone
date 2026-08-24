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

package issues

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue26144CloneAndBranchPreserveForeignKeyNames(t *testing.T) {
	runAuthenticatedClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		execSQLRequire(t, ctx, db, "set role moadmin")
		execSQLRequire(t, ctx, db, "select mo_feature_registry_upsert('branch', 'Branch feature', '{\"allowed_scope\":[]}', true)")

		const (
			sourceDB       = `issue_26144_src\part`
			branchDB       = "issue_26144_branch"
			cloneDB        = "issue_26144_clone"
			parentTable    = `parent\name`
			childTable     = `child\name`
			constraintName = `fk\name'one`
		)

		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, name := range []string{branchDB, cloneDB, sourceDB} {
				execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+quoteIssue26144Identifier(name))
			}
		}()

		execSQLRequire(t, ctx, db, "create database "+quoteIssue26144Identifier(sourceDB))
		execSQLRequire(t, ctx, db, "use "+quoteIssue26144Identifier(sourceDB))
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"create table %s (id int primary key)",
			quoteIssue26144Identifier(parentTable)))
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"create table %s (id int primary key, parent_id int, constraint %s foreign key (parent_id) references %s (id))",
			quoteIssue26144Identifier(childTable),
			quoteIssue26144Identifier(constraintName), quoteIssue26144Identifier(parentTable)))
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"insert into %s.%s values (1)",
			quoteIssue26144Identifier(sourceDB), quoteIssue26144Identifier(parentTable)))
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"insert into %s.%s values (1, 1)",
			quoteIssue26144Identifier(sourceDB), quoteIssue26144Identifier(childTable)))
		assertIssue26144ForeignKeyCatalog(t, ctx, db, sourceDB, childTable, constraintName, parentTable)

		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"data branch create database %s from %s",
			quoteIssue26144Identifier(branchDB), quoteIssue26144Identifier(sourceDB)))
		execSQLRequire(t, ctx, db, fmt.Sprintf(
			"create database %s clone %s",
			quoteIssue26144Identifier(cloneDB), quoteIssue26144Identifier(sourceDB)))

		for _, destination := range []string{branchDB, cloneDB} {
			assertIssue26144ForeignKeyCatalog(t, ctx, db, destination, childTable, constraintName, parentTable)
			_, err = db.ExecContext(ctx, fmt.Sprintf(
				"insert into %s.%s values (2, 999)",
				quoteIssue26144Identifier(destination), quoteIssue26144Identifier(childTable)))
			require.ErrorContains(t, err, "foreign key constraint fails")
		}

		for _, name := range []string{branchDB, cloneDB, sourceDB} {
			execSQLRequire(t, ctx, db, "drop database "+quoteIssue26144Identifier(name))
		}
	})
}

func assertIssue26144ForeignKeyCatalog(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	databaseName string,
	childTable string,
	constraintName string,
	parentTable string,
) {
	t.Helper()
	var gotChild, gotConstraint, gotParent string
	err := db.QueryRowContext(ctx,
		"select hex(table_name), hex(constraint_name), hex(refer_table_name) "+
			"from mo_catalog.mo_foreign_keys where db_name = ?",
		databaseName,
	).Scan(&gotChild, &gotConstraint, &gotParent)
	require.NoError(t, err)
	require.Equal(t, strings.ToUpper(hex.EncodeToString([]byte(childTable))), gotChild)
	require.Equal(t, strings.ToUpper(hex.EncodeToString([]byte(constraintName))), gotConstraint)
	require.Equal(t, strings.ToUpper(hex.EncodeToString([]byte(parentTable))), gotParent)
}

func quoteIssue26144Identifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}
