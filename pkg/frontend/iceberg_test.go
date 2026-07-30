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
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

func TestIcebergCatalogSecretRefValidation(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, validateIcebergSecretRef(ctx, "secret://catalog/token"))
	err := validateIcebergSecretRef(ctx, "Bearer raw-token")
	require.Error(t, err)
	require.Contains(t, err.Error(), "secret://")
}

func TestIcebergParameterUnitForUninitializedService(t *testing.T) {
	require.Nil(t, icebergParameterUnitForService("iceberg-uninitialized-service"))
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

func TestIcebergCatalogDropBlocksAllCatalogScopedMetadata(t *testing.T) {
	ctx := context.Background()
	countByTable := map[string]uint64{
		icebergsql.TableTables:          1,
		icebergsql.TablePrincipalMap:    1,
		icebergsql.TableResidencyPolicy: 1,
		icebergsql.TableRefs:            1,
		icebergsql.TablePublishJobs:     1,
		icebergsql.TableOrphanFiles:     1,
		icebergsql.TableMaintenanceJobs: 1,
	}
	seen := make(map[string]string)
	stub := gostub.Stub(&ExeSqlInBgSes, func(_ context.Context, _ BackgroundExec, sql string) ([]ExecResult, error) {
		for table, count := range countByTable {
			if strings.Contains(sql, "mo_catalog."+table) {
				seen[table] = sql
				return []ExecResult{icebergCatalogCountResult{count: count}}, nil
			}
		}
		t.Fatalf("unexpected dependency query: %s", sql)
		return nil, nil
	})
	defer stub.Reset()

	deps, err := icebergCatalogBlockingDependencies(ctx, nil, 42, 7)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		"table mappings",
		"principal mappings",
		"residency policies",
		"ref cache entries",
		"publish jobs",
		"orphan file metadata",
		"maintenance jobs",
	}, deps)
	for table := range countByTable {
		require.Contains(t, seen, table)
		require.Contains(t, seen[table], "select count(*)")
	}
	require.Contains(t, seen[icebergsql.TablePrincipalMap], "account_id = 42 and catalog_id = 7")
	require.Contains(t, seen[icebergsql.TableResidencyPolicy], "(scope_type = 'cluster' or account_id = 42) and catalog_id = 7")
	require.Contains(t, seen[icebergsql.TablePublishJobs], "account_id = 42 and target_catalog_id = 7")
	require.Contains(t, seen[icebergsql.TableOrphanFiles], "account_id = 42 and catalog_id = 7")
}

func TestIcebergCatalogDropAllowsIdReuseOnlyWhenNoScopedMetadataRemains(t *testing.T) {
	ctx := context.Background()
	queried := 0
	stub := gostub.Stub(&ExeSqlInBgSes, func(_ context.Context, _ BackgroundExec, sql string) ([]ExecResult, error) {
		queried++
		if strings.Contains(sql, "mo_catalog."+icebergsql.TablePrincipalMap) ||
			strings.Contains(sql, "mo_catalog."+icebergsql.TableResidencyPolicy) {
			return []ExecResult{icebergCatalogCountResult{count: 0}}, nil
		}
		return []ExecResult{icebergCatalogCountResult{count: 0}}, nil
	})
	defer stub.Reset()

	deps, err := icebergCatalogBlockingDependencies(ctx, nil, 42, 7)
	require.NoError(t, err)
	require.Empty(t, deps)
	require.Equal(t, len(icebergCatalogDependencySpecs), queried)
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

type icebergCatalogCountResult struct {
	count uint64
}

func (r icebergCatalogCountResult) GetRowCount() uint64 {
	return 1
}

func (r icebergCatalogCountResult) GetColumnCount() uint64 {
	return 1
}

func (r icebergCatalogCountResult) GetString(context.Context, uint64, uint64) (string, error) {
	return "", nil
}

func (r icebergCatalogCountResult) GetUint64(context.Context, uint64, uint64) (uint64, error) {
	return r.count, nil
}

func (r icebergCatalogCountResult) GetInt64(context.Context, uint64, uint64) (int64, error) {
	return int64(r.count), nil
}

func (r icebergCatalogCountResult) ColumnIsNull(context.Context, uint64, uint64) (bool, error) {
	return false, nil
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
