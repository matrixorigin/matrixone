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

package frontend

import (
	"context"
	"strings"
	"testing"

	icebergsql "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestIcebergCatalogSecretRefValidation(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, validateIcebergSecretRef(ctx, "secret://catalog/token"))
	err := validateIcebergSecretRef(ctx, "Bearer raw-token")
	require.Error(t, err)
	require.Contains(t, err.Error(), "secret://")
}

func TestIcebergCatalogStatementLoggingRedactsSecret(t *testing.T) {
	tests := []struct {
		name string
		stmt tree.Statement
		sql  string
	}{
		{
			name: "create catalog",
			stmt: &tree.CreateIcebergCatalog{
				Name: "ksa",
				Options: tree.IcebergOptions{
					tree.NewIcebergOption("uri", "https://catalog.example/rest"),
					tree.NewIcebergOption("token_secret", "secret://catalog/raw-token"),
				},
			},
			sql: `create iceberg catalog ksa with ("token_secret"="secret://catalog/raw-token")`,
		},
		{
			name: "alter catalog",
			stmt: &tree.AlterIcebergCatalog{
				Name: "ksa",
				Options: tree.IcebergOptions{
					tree.NewIcebergOption("token_secret", "secret://catalog/raw-token"),
				},
			},
			sql: `alter iceberg catalog ksa set ("token_secret"="secret://catalog/raw-token")`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			redacted := redactStatementTextForLogging(tt.stmt, tt.sql)
			require.Contains(t, redacted, "<redacted>")
			require.False(t, strings.Contains(redacted, "raw-token"), redacted)
		})
	}
}

func TestIcebergStatementsHavePrivilegeDefinitions(t *testing.T) {
	statements := []tree.Statement{
		&tree.CreateIcebergCatalog{Name: "ksa"},
		&tree.AlterIcebergCatalog{Name: "ksa"},
		&tree.DropIcebergCatalog{Name: "ksa"},
		&tree.ShowIcebergCatalogs{},
		&tree.ShowIcebergNamespaces{Catalog: "ksa"},
		&tree.ShowIcebergTables{Catalog: "ksa", Namespace: "sales"},
		&tree.Merge{
			Clauses: tree.MergeClauses{
				&tree.MergeClause{Matched: true, Action: tree.MergeActionUpdate},
				&tree.MergeClause{Matched: false, Action: tree.MergeActionInsert},
			},
		},
	}

	for _, stmt := range statements {
		require.NotPanics(t, func() {
			priv := determinePrivilegeSetOfStatement(stmt)
			require.NotNil(t, priv)
			_ = stmt.StmtKind()
		}, tree.String(stmt, dialect.MYSQL))
	}
}

func TestIcebergMergePrivilegeDefinitionRequiresAllActions(t *testing.T) {
	stmt := &tree.Merge{
		Clauses: tree.MergeClauses{
			&tree.MergeClause{Matched: true, Action: tree.MergeActionUpdate},
			&tree.MergeClause{Matched: false, Action: tree.MergeActionInsert},
		},
	}
	priv := determinePrivilegeSetOfStatement(stmt)
	require.Equal(t, objectTypeTable, priv.objectType())

	seenGeneral := make(map[PrivilegeType]bool)
	var actionCompound *compoundEntry
	for _, entry := range priv.entries {
		if entry.privilegeEntryTyp == privilegeEntryTypeGeneral {
			seenGeneral[entry.privilegeId] = true
		}
		if entry.privilegeEntryTyp == privilegeEntryTypeCompound {
			actionCompound = entry.compound
		}
	}
	require.True(t, seenGeneral[PrivilegeTypeTableAll])
	require.True(t, seenGeneral[PrivilegeTypeTableOwnership])
	require.False(t, seenGeneral[PrivilegeTypeInsert])
	require.False(t, seenGeneral[PrivilegeTypeUpdate])
	require.NotNil(t, actionCompound)

	seenCompound := make(map[PrivilegeType]bool)
	for _, item := range actionCompound.items {
		seenCompound[item.privilegeTyp] = true
	}
	require.True(t, seenCompound[PrivilegeTypeInsert])
	require.True(t, seenCompound[PrivilegeTypeUpdate])
	require.False(t, seenCompound[PrivilegeTypeDelete])
}

func TestShowIcebergRejectsParsedLikeWhereFilters(t *testing.T) {
	ctx := context.Background()
	if err := handleShowIcebergCatalogs(ctx, nil, &tree.ShowIcebergCatalogs{Where: &tree.Where{}}); err == nil ||
		!strings.Contains(err.Error(), "LIKE/WHERE") {
		t.Fatalf("expected SHOW ICEBERG CATALOGS WHERE rejection, got %v", err)
	}
	if err := handleShowIcebergNamespaces(ctx, nil, &tree.ShowIcebergNamespaces{Like: &tree.ComparisonExpr{}}); err == nil ||
		!strings.Contains(err.Error(), "LIKE/WHERE") {
		t.Fatalf("expected SHOW ICEBERG NAMESPACES LIKE rejection, got %v", err)
	}
	if err := handleShowIcebergTables(ctx, nil, &tree.ShowIcebergTables{Where: &tree.Where{}}); err == nil ||
		!strings.Contains(err.Error(), "LIKE/WHERE") {
		t.Fatalf("expected SHOW ICEBERG TABLES WHERE rejection, got %v", err)
	}
}

func TestIcebergP1P2SystemTablesAreInitializedForNewTenants(t *testing.T) {
	for _, table := range []struct {
		name string
		ddl  string
	}{
		{name: icebergsql.TablePublishJobs, ddl: icebergsql.PublishJobsDDL},
		{name: icebergsql.TableOrphanFiles, ddl: icebergsql.OrphanFilesDDL},
		{name: icebergsql.TableMaintenanceJobs, ddl: icebergsql.MaintenanceJobsDDL},
	} {
		require.Contains(t, predefinedTables, table.name)
		require.Contains(t, createSqls, table.ddl)
		require.NotContains(t, dropSqls, "drop table if exists mo_catalog."+table.name+";")
		require.Contains(t, dropIcebergSqls, "drop table if exists mo_catalog."+table.name+";")
	}
}
