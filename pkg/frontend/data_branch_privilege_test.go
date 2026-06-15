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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBranchRequirementToPrivilegeCopiesLightPrivilegeFields(t *testing.T) {
	req := branchWriteTableRequirement(
		catalog.MO_CATALOG,
		catalog.MO_TABLES,
		PrivilegeTypeDelete,
		clusterTableModify,
	)

	priv := branchRequirementToPrivilege(req)

	require.Equal(t, privilegeKindGeneral, priv.kind)
	require.Equal(t, objectTypeTable, priv.objType)
	require.True(t, priv.writeDatabaseAndTableDirectly)
	require.Equal(t, req.isClusterTable, priv.isClusterTable)
	require.Equal(t, clusterTableModify, priv.clusterTableOperation)
	require.Len(t, priv.entries, 3)

	require.Equal(t, []PrivilegeType{
		PrivilegeTypeDelete,
		PrivilegeTypeTableAll,
		PrivilegeTypeTableOwnership,
	}, []PrivilegeType{
		priv.entries[0].privilegeId,
		priv.entries[1].privilegeId,
		priv.entries[2].privilegeId,
	})
	for _, entry := range priv.entries {
		require.Equal(t, objectTypeTable, entry.objType)
		require.Equal(t, catalog.MO_CATALOG, entry.databaseName)
		require.Equal(t, catalog.MO_TABLES, entry.tableName)
		require.Equal(t, privilegeEntryTypeGeneral, entry.privilegeEntryTyp)
		require.Nil(t, entry.compound)
	}
}

func TestBranchDeleteDatabaseTableIDsSQLReusesCloneObjectFilter(t *testing.T) {
	const (
		accountID uint32 = 42
		dbName           = "db1"
	)
	expectedWhere := buildTableInfoListWhereClause(dbName, "", accountID) +
		fmt.Sprintf(" and relkind != %s", quoteSQLStringLiteral(catalog.SystemViewRel))
	expected := fmt.Sprintf(
		"select rel_id, relname from %s.%s where %s",
		catalog.MO_CATALOG,
		catalog.MO_TABLES,
		expectedWhere,
	)

	got := branchDeleteDatabaseTableIDsSQL(accountID, dbName)

	require.Equal(t, expected, got)
	require.Contains(t, got, "relname not like '\\\\_\\\\_mo\\\\_index\\\\_%' escape '\\\\'")
	require.Contains(t, got, "relname not like '\\\\_\\\\_mo\\\\_tmp\\\\_%' escape '\\\\'")
	require.Contains(t, got, "relkind != 'partition'")
	require.Contains(t, got, "relkind != 'S'")
	require.Contains(t, got, "relkind != 'v'")
}

func TestQuoteSQLLikePatternEscapesWildcardCharacters(t *testing.T) {
	require.Equal(t, "'a\\\\_b\\\\%c%'", quoteSQLLikePattern("a_b%c"))
}

func TestQuoteIdentifierForSQLEscapesBackticks(t *testing.T) {
	require.Equal(t, "`acc``branch`", quoteIdentifierForSQL("acc`branch"))
}

func TestDataBranchDiffOutputAsNotSupported(t *testing.T) {
	stmt := &tree.DataBranchDiff{
		OutputOpt: &tree.DiffOutputOpt{
			As: *tree.NewTableName(
				tree.Identifier("diff_out"),
				tree.ObjectNamePrefix{},
				nil,
			),
		},
	}

	err := validate(context.Background(), nil, stmt)

	require.Error(t, err)
	require.Contains(t, err.Error(), "DATA BRANCH DIFF OUTPUT AS")
}
