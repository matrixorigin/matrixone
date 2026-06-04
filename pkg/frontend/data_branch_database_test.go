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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestBuildDataBranchDatabaseTablePairs(t *testing.T) {
	srcInfos := []*tableInfo{
		{tblName: "labels", typ: tableType("BASE TABLE")},
		{tblName: "features", typ: tableType("BASE TABLE")},
		{tblName: "feature_view", typ: view},
	}
	dstInfos := []*tableInfo{
		{tblName: "features", typ: tableType("BASE TABLE")},
		{tblName: "feature_view", typ: view},
		{tblName: "labels", typ: tableType("BASE TABLE")},
	}

	pairs, err := buildDataBranchDatabaseTablePairs(
		tree.Identifier("src_db"),
		tree.Identifier("dst_db"),
		srcInfos,
		dstInfos,
	)
	require.NoError(t, err)
	require.Len(t, pairs, 2)

	require.Equal(t, "features", pairs[0].tableName)
	require.Equal(t, tree.Identifier("src_db"), pairs[0].srcTable.SchemaName)
	require.Equal(t, tree.Identifier("features"), pairs[0].srcTable.ObjectName)
	require.Equal(t, tree.Identifier("dst_db"), pairs[0].dstTable.SchemaName)
	require.Equal(t, tree.Identifier("features"), pairs[0].dstTable.ObjectName)

	require.Equal(t, "labels", pairs[1].tableName)
	require.Equal(t, tree.Identifier("src_db"), pairs[1].srcTable.SchemaName)
	require.Equal(t, tree.Identifier("labels"), pairs[1].srcTable.ObjectName)
	require.Equal(t, tree.Identifier("dst_db"), pairs[1].dstTable.SchemaName)
	require.Equal(t, tree.Identifier("labels"), pairs[1].dstTable.ObjectName)
}

func TestBuildDataBranchDatabaseTablePairsRejectsTableSetMismatch(t *testing.T) {
	srcInfos := []*tableInfo{
		{tblName: "features", typ: tableType("BASE TABLE")},
		{tblName: "labels", typ: tableType("BASE TABLE")},
	}
	dstInfos := []*tableInfo{
		{tblName: "features", typ: tableType("BASE TABLE")},
	}

	_, err := buildDataBranchDatabaseTablePairs(
		tree.Identifier("src_db"),
		tree.Identifier("dst_db"),
		srcInfos,
		dstInfos,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "table set mismatch")
	require.Contains(t, err.Error(), "labels")
}

func TestAppendDataBranchDatabaseDiffRowsSummary(t *testing.T) {
	final := &MysqlResultSet{}
	tableResult := &MysqlResultSet{
		Data: [][]interface{}{
			{"INSERTED", int64(2), int64(0)},
			{"DELETED", int64(0), int64(1)},
			{"UPDATED", int64(3), int64(4)},
		},
	}

	err := appendDataBranchDatabaseDiffRows(final, tableResult, "features", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"features", "INSERTED", int64(2), int64(0)},
		{"features", "DELETED", int64(0), int64(1)},
		{"features", "UPDATED", int64(3), int64(4)},
	}, final.Data)
}

func TestAppendDataBranchDatabaseDiffRowsCount(t *testing.T) {
	final := &MysqlResultSet{}
	tableResult := &MysqlResultSet{
		Data: [][]interface{}{
			{int64(7)},
		},
	}

	err := appendDataBranchDatabaseDiffRows(final, tableResult, "labels", &tree.DiffOutputOpt{Count: true})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"labels", int64(7)},
	}, final.Data)
}
