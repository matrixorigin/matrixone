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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func restoredLineageTestReceipt() *cloneReceipt {
	return &cloneReceipt{
		srcDb:      "branch_db",
		srcTbl:     "branch_t",
		dstDb:      "branch_db",
		dstTbl:     "branch_t",
		srcAccount: 42,
		toAccount:  42,
		srcTableID: 22,
		dstTableID: 33,
		snapshotTS: 123456,
	}
}

func restoredLineageTestMetadataRow(level string) []interface{} {
	return []interface{}{"11", "123000", "7", level}
}

func restoredLineageTestProtectionRowForTable(childID, parentID uint64, cloneTS int64) []interface{} {
	return []interface{}{
		"018f4f2a-6d8a-7c40-8f4d-8cb6b8b72f00",
		fmt.Sprintf("__mo_branch_%d", childID),
		fmt.Sprint(cloneTS),
		"table",
		"sys",
		"source_db",
		"parent_t",
		fmt.Sprint(parentID),
		"branch",
	}
}

func restoredLineageTestProtectionRow() []interface{} {
	return restoredLineageTestProtectionRowForTable(22, 11, 123000)
}

func restoredLineageMetadataResult(rows [][]interface{}) *MysqlResultSet {
	return newMrsForRestoreStringRows(
		[]string{"p_table_id", "clone_ts", "creator", "level"}, rows,
	)
}

func restoredLineageProtectionResult(rows [][]interface{}) *MysqlResultSet {
	return newMrsForRestoreStringRows(
		[]string{
			"snapshot_id", "sname", "ts", "level", "account_name",
			"database_name", "table_name", "obj_id", "kind",
		},
		rows,
	)
}

func restoredLineageCurrentMetadataResult(rows [][]interface{}) *MysqlResultSet {
	return newMrsForRestoreStringRows(
		[]string{"table_id", "p_table_id", "clone_ts", "creator", "level", "table_deleted"}, rows,
	)
}

func setRestoredLineageCurrentMetadata(bh *backgroundExecTest, rows ...[]interface{}) {
	bh.sql2result[historicalAlterLineageMetadataSQL()] = restoredLineageCurrentMetadataResult(rows)
}

func restoredLineageCurrentMetadataRow(
	tableID, parentID uint64,
	cloneTS int64,
	level string,
	deleted bool,
) []interface{} {
	deletedValue := "0"
	if deleted {
		deletedValue = "1"
	}
	return []interface{}{
		fmt.Sprint(tableID), fmt.Sprint(parentID), fmt.Sprint(cloneTS), "7", level, deletedValue,
	}
}

func newRestoredLineageTestExec(
	receipt *cloneReceipt,
	metadataRows, protectionRows [][]interface{},
) *backgroundExecTest {
	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[restoredBranchLineageMetadataSQL(receipt)] = restoredLineageMetadataResult(metadataRows)
	bh.sql2result[restoredBranchProtectionSQL(receipt)] = restoredLineageProtectionResult(protectionRows)
	// Existing direct-lineage fixtures use table 11 as an ordinary root.
	bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 11)] = restoredLineageMetadataResult(nil)
	bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 11)] = restoredLineageProtectionResult(nil)
	setRestoredLineageCurrentMetadata(bh)
	bh.sql2result[currentRestoredBranchProtectionSQL(receipt.dstTableID)] = restoredLineageProtectionResult(nil)
	return bh
}

func restoredLineageInsertSQL(receipt *cloneReceipt, level string) string {
	return fmt.Sprintf(
		"insert into mo_catalog.mo_branch_metadata values(%d, 123000, 11, 7, '%s', false)",
		receipt.dstTableID, level,
	)
}

func requireSystemAccountForEveryStatement(t *testing.T, bh *backgroundExecTest) {
	t.Helper()
	require.NotEmpty(t, bh.executionAccountIDs)
	for _, accountID := range bh.executionAccountIDs {
		require.Equal(t, uint32(sysAccountID), accountID)
	}
}

func requireRestoredProtectionInsertForTable(
	t *testing.T,
	sql string,
	childTableID, parentTableID uint64,
	cloneTS int64,
) {
	t.Helper()
	statements, err := mysql.Parse(context.Background(), sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	insert, ok := statements[0].(*tree.Insert)
	require.True(t, ok)
	values, ok := insert.Rows.Select.(*tree.ValuesClause)
	require.True(t, ok)
	require.Len(t, values.Rows, 1)
	require.Len(t, values.Rows[0], 9)

	row := values.Rows[0]
	requireSQLStringValue(t, row[1], branchSnapshotName(childTableID))
	requireSQLStringValue(t, row[3], dataBranchLevel_Table)
	requireSQLStringValue(t, row[4], sysAccountName)
	requireSQLStringValue(t, row[5], "source_db")
	requireSQLStringValue(t, row[6], "parent_t")
	requireSQLStringValue(t, row[8], branchSnapshotKind)

	ts, ok := row[2].(*tree.NumVal)
	require.True(t, ok)
	require.Equal(t, fmt.Sprint(cloneTS), ts.String())
	parentID, ok := row[7].(*tree.NumVal)
	require.True(t, ok)
	require.Equal(t, fmt.Sprint(parentTableID), parentID.String())
}

func requireRestoredProtectionInsert(
	t *testing.T,
	sql string,
	receipt *cloneReceipt,
) {
	t.Helper()
	requireRestoredProtectionInsertForTable(t, sql, receipt.dstTableID, 11, 123000)
}

func TestRestitchRestoredBranchLineageSkipsNonLogicalOwners(t *testing.T) {
	for _, test := range []struct {
		name         string
		metadataRows [][]interface{}
	}{
		{name: "ordinary table without lineage"},
		{name: "plain alter edge", metadataRows: [][]interface{}{
			restoredLineageTestMetadataRow(databranchutils.AlterLineageLevel),
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt := restoredLineageTestReceipt()
			bh := newRestoredLineageTestExec(receipt, test.metadataRows, nil)
			require.NoError(t, restitchRestoredBranchLineage(
				defines.AttachAccountId(context.Background(), 42), bh, receipt,
			))
			require.Equal(t, []string{restoredBranchLineageMetadataSQL(receipt)}, bh.executedSQLs)
			requireSystemAccountForEveryStatement(t, bh)
		})
	}
}

func TestRestitchRestoredBranchLineagePreservesLegacyLineageOnly(t *testing.T) {
	receipt := restoredLineageTestReceipt()
	bh := newRestoredLineageTestExec(receipt, [][]interface{}{
		restoredLineageTestMetadataRow(dataBranchLevel_Database),
	}, nil)

	require.NoError(t, restitchRestoredBranchLineage(
		defines.AttachAccountId(context.Background(), 42), bh, receipt,
	))
	require.Equal(t, []string{
		restoredBranchLineageMetadataSQL(receipt),
		restoredBranchProtectionSQL(receipt),
		restoredBranchAncestorLineageMetadataSQL(receipt, 11),
		restoredBranchAncestorProtectionSQL(receipt, 11),
		historicalAlterLineageMetadataSQL(),
		historicalAlterLineageMetadataSQL(),
		restoredLineageInsertSQL(receipt, dataBranchLevel_Database),
	}, bh.executedSQLs)
	requireSystemAccountForEveryStatement(t, bh)
}

func TestRestitchRestoredBranchLineagePreservesLogicalLevelAndProtection(t *testing.T) {
	for _, level := range []string{
		dataBranchLevel_Table,
		dataBranchLevel_Database,
		databranchutils.NextAlterLineageLevel(dataBranchLevel_Database),
	} {
		t.Run(level, func(t *testing.T) {
			receipt := restoredLineageTestReceipt()
			bh := newRestoredLineageTestExec(receipt, [][]interface{}{
				restoredLineageTestMetadataRow(level),
			}, [][]interface{}{restoredLineageTestProtectionRow()})

			require.NoError(t, restitchRestoredBranchLineage(
				defines.AttachAccountId(context.Background(), 42), bh, receipt,
			))
			require.Len(t, bh.executedSQLs, 9)
			require.Equal(t, []string{
				restoredBranchLineageMetadataSQL(receipt),
				restoredBranchProtectionSQL(receipt),
				restoredBranchAncestorLineageMetadataSQL(receipt, 11),
				restoredBranchAncestorProtectionSQL(receipt, 11),
				historicalAlterLineageMetadataSQL(),
				historicalAlterLineageMetadataSQL(),
				currentRestoredBranchProtectionSQL(receipt.dstTableID),
				restoredLineageInsertSQL(receipt, level),
			}, bh.executedSQLs[:8])
			requireRestoredProtectionInsert(t, bh.executedSQLs[8], receipt)
			requireSystemAccountForEveryStatement(t, bh)
		})
	}
}

func prepareRestoredLineageAncestorChain(receipt *cloneReceipt, bh *backgroundExecTest) {
	bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 11)] = restoredLineageMetadataResult(
		[][]interface{}{{"5", "122000", "7", dataBranchLevel_Database}},
	)
	bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 11)] = restoredLineageProtectionResult(
		[][]interface{}{restoredLineageTestProtectionRowForTable(11, 5, 122000)},
	)
	bh.sql2result[currentRestoredBranchProtectionSQL(11)] = restoredLineageProtectionResult(nil)
	bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 5)] = restoredLineageMetadataResult(nil)
	bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 5)] = restoredLineageProtectionResult(nil)
	setRestoredLineageCurrentMetadata(bh,
		restoredLineageCurrentMetadataRow(11, 5, 122000, dataBranchLevel_Database, true),
	)
}

func TestRestitchRestoredBranchLineageRestoresAncestorProtection(t *testing.T) {
	receipt := restoredLineageTestReceipt()
	bh := newRestoredLineageTestExec(receipt, [][]interface{}{
		restoredLineageTestMetadataRow(dataBranchLevel_Database),
	}, [][]interface{}{restoredLineageTestProtectionRow()})
	prepareRestoredLineageAncestorChain(receipt, bh)

	require.NoError(t, restitchRestoredBranchLineage(context.Background(), bh, receipt))
	require.NotContains(t, restoredBranchAncestorLineageMetadataSQL(receipt, 11), "table_deleted")
	require.Len(t, bh.executedSQLs, 13)
	require.Equal(t, []string{
		restoredBranchLineageMetadataSQL(receipt),
		restoredBranchProtectionSQL(receipt),
		restoredBranchAncestorLineageMetadataSQL(receipt, 11),
		restoredBranchAncestorProtectionSQL(receipt, 11),
		restoredBranchAncestorLineageMetadataSQL(receipt, 5),
		restoredBranchAncestorProtectionSQL(receipt, 5),
		historicalAlterLineageMetadataSQL(),
		historicalAlterLineageMetadataSQL(),
		currentRestoredBranchProtectionSQL(receipt.dstTableID),
		currentRestoredBranchProtectionSQL(11),
		restoredLineageInsertSQL(receipt, dataBranchLevel_Database),
	}, bh.executedSQLs[:11])
	requireRestoredProtectionInsert(t, bh.executedSQLs[11], receipt)
	requireRestoredProtectionInsertForTable(t, bh.executedSQLs[12], 11, 5, 122000)
	requireSystemAccountForEveryStatement(t, bh)
}

func TestRestitchRestoredBranchLineageHandlesExistingAlterAndLegacyAncestors(t *testing.T) {
	for _, test := range []struct {
		name                 string
		level                string
		historicalProtection []interface{}
		currentProtection    [][]interface{}
		expectCurrentRead    bool
	}{
		{
			name:                 "matching current ancestor protection",
			level:                dataBranchLevel_Database,
			historicalProtection: restoredLineageTestProtectionRowForTable(11, 5, 122000),
			currentProtection: [][]interface{}{
				restoredLineageTestProtectionRowForTable(11, 5, 122000),
			},
			expectCurrentRead: true,
		},
		{
			name:                 "ALTER ancestor protection remains ALTER owned",
			level:                databranchutils.NextAlterLineageLevel(dataBranchLevel_Database),
			historicalProtection: restoredLineageTestProtectionRowForTable(11, 5, 122000),
		},
		{
			name:  "legacy ancestor without protection",
			level: dataBranchLevel_Database,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt := restoredLineageTestReceipt()
			bh := newRestoredLineageTestExec(receipt, [][]interface{}{
				restoredLineageTestMetadataRow(dataBranchLevel_Database),
			}, [][]interface{}{restoredLineageTestProtectionRow()})
			bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 11)] = restoredLineageMetadataResult(
				[][]interface{}{{"5", "122000", "7", test.level}},
			)
			bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 11)] = restoredLineageProtectionResult(nil)
			if test.historicalProtection != nil {
				bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 11)] = restoredLineageProtectionResult(
					[][]interface{}{test.historicalProtection},
				)
			}
			bh.sql2result[currentRestoredBranchProtectionSQL(11)] = restoredLineageProtectionResult(test.currentProtection)
			setRestoredLineageCurrentMetadata(bh,
				restoredLineageCurrentMetadataRow(11, 5, 122000, test.level, true),
			)
			bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 5)] = restoredLineageMetadataResult(nil)
			bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 5)] = restoredLineageProtectionResult(nil)

			require.NoError(t, restitchRestoredBranchLineage(context.Background(), bh, receipt))
			var protectionInserts []string
			for _, sql := range bh.executedSQLs {
				if strings.HasPrefix(sql, "insert into mo_catalog.mo_snapshots") {
					protectionInserts = append(protectionInserts, sql)
				}
			}
			require.Len(t, protectionInserts, 1)
			requireRestoredProtectionInsert(t, protectionInserts[0], receipt)
			if test.expectCurrentRead {
				require.Contains(t, bh.executedSQLs, currentRestoredBranchProtectionSQL(11))
			} else {
				require.NotContains(t, bh.executedSQLs, currentRestoredBranchProtectionSQL(11))
			}
			requireSystemAccountForEveryStatement(t, bh)
		})
	}
}

func TestRestitchRestoredBranchLineageReconstructsMissingAlterAncestor(t *testing.T) {
	receipt := restoredLineageTestReceipt()
	bh := newRestoredLineageTestExec(receipt, [][]interface{}{
		restoredLineageTestMetadataRow(dataBranchLevel_Database),
	}, [][]interface{}{restoredLineageTestProtectionRow()})
	alterLevel := databranchutils.NextAlterLineageLevel(dataBranchLevel_Database)
	bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 11)] = restoredLineageMetadataResult(
		[][]interface{}{{"5", "122000", "7", alterLevel}},
	)
	bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 11)] = restoredLineageProtectionResult(
		[][]interface{}{restoredLineageTestProtectionRowForTable(11, 5, 122000)},
	)
	bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 5)] = restoredLineageMetadataResult(
		[][]interface{}{{"3", "121000", "7", dataBranchLevel_Database}},
	)
	bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 5)] = restoredLineageProtectionResult(
		[][]interface{}{restoredLineageTestProtectionRowForTable(5, 3, 121000)},
	)
	bh.sql2result[currentRestoredBranchProtectionSQL(5)] = restoredLineageProtectionResult(nil)
	bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 3)] = restoredLineageMetadataResult(nil)
	bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 3)] = restoredLineageProtectionResult(nil)
	setRestoredLineageCurrentMetadata(bh,
		restoredLineageCurrentMetadataRow(5, 3, 121000, dataBranchLevel_Database, true),
	)

	require.NoError(t, restitchRestoredBranchLineage(context.Background(), bh, receipt))
	require.Len(t, bh.executedSQLs, 16)
	alterInsert := restoredBranchLineageInsertSQL(11, &restoredBranchLineage{
		parentTableID: 5,
		cloneTS:       122000,
		creator:       7,
		level:         alterLevel,
	}, true)
	require.Equal(t, alterInsert, bh.executedSQLs[12])
	require.Equal(t, restoredLineageInsertSQL(receipt, dataBranchLevel_Database), bh.executedSQLs[13])
	requireRestoredProtectionInsert(t, bh.executedSQLs[14], receipt)
	requireRestoredProtectionInsertForTable(t, bh.executedSQLs[15], 5, 3, 121000)
	require.NotContains(t, bh.executedSQLs, currentRestoredBranchProtectionSQL(11))
	for _, sql := range bh.executedSQLs {
		require.NotContains(t, sql, "update mo_catalog.mo_branch_metadata")
	}
}

func TestRestitchRestoredBranchLineageRejectsInvalidAncestorHistory(t *testing.T) {
	for _, test := range []struct {
		name      string
		setup     func(*cloneReceipt, *backgroundExecTest)
		wantError string
	}{
		{
			name: "orphaned ancestor protection",
			setup: func(receipt *cloneReceipt, bh *backgroundExecTest) {
				bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 11)] = restoredLineageProtectionResult(
					[][]interface{}{restoredLineageTestProtectionRowForTable(11, 5, 122000)},
				)
			},
			wantError: "has no historical lineage",
		},
		{
			name: "duplicate ancestor lineage",
			setup: func(receipt *cloneReceipt, bh *backgroundExecTest) {
				bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 11)] = restoredLineageMetadataResult(
					[][]interface{}{{"5", "122000", "7", dataBranchLevel_Database}, {"5", "122000", "7", dataBranchLevel_Database}},
				)
			},
			wantError: "multiple historical rows",
		},
		{
			name: "ancestor protection mismatch",
			setup: func(receipt *cloneReceipt, bh *backgroundExecTest) {
				bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 11)] = restoredLineageMetadataResult(
					[][]interface{}{{"5", "122000", "7", dataBranchLevel_Database}},
				)
				bh.sql2result[restoredBranchAncestorProtectionSQL(receipt, 11)] = restoredLineageProtectionResult(
					[][]interface{}{restoredLineageTestProtectionRowForTable(11, 6, 122000)},
				)
			},
			wantError: "does not match its lineage",
		},
		{
			name: "ancestor cycle",
			setup: func(receipt *cloneReceipt, bh *backgroundExecTest) {
				bh.sql2result[restoredBranchAncestorLineageMetadataSQL(receipt, 11)] = restoredLineageMetadataResult(
					[][]interface{}{{"22", "122000", "7", dataBranchLevel_Database}},
				)
			},
			wantError: "ancestor cycle at table 22",
		},
		{
			name: "duplicate current ancestor protection",
			setup: func(receipt *cloneReceipt, bh *backgroundExecTest) {
				prepareRestoredLineageAncestorChain(receipt, bh)
				bh.sql2result[currentRestoredBranchProtectionSQL(11)] = restoredLineageProtectionResult(
					[][]interface{}{
						restoredLineageTestProtectionRowForTable(11, 5, 122000),
						restoredLineageTestProtectionRowForTable(11, 5, 122000),
					},
				)
			},
			wantError: "multiple protection rows",
		},
		{
			name: "current ancestor protection mismatch",
			setup: func(receipt *cloneReceipt, bh *backgroundExecTest) {
				prepareRestoredLineageAncestorChain(receipt, bh)
				bh.sql2result[currentRestoredBranchProtectionSQL(11)] = restoredLineageProtectionResult(
					[][]interface{}{restoredLineageTestProtectionRowForTable(11, 6, 122000)},
				)
			},
			wantError: "current protection for table 11",
		},
		{
			name: "duplicate current ancestor lineage",
			setup: func(receipt *cloneReceipt, bh *backgroundExecTest) {
				prepareRestoredLineageAncestorChain(receipt, bh)
				row := restoredLineageCurrentMetadataRow(11, 5, 122000, dataBranchLevel_Database, true)
				setRestoredLineageCurrentMetadata(bh, row, row)
			},
			wantError: "multiple current rows for table 11",
		},
		{
			name: "current ancestor lineage mismatch",
			setup: func(receipt *cloneReceipt, bh *backgroundExecTest) {
				prepareRestoredLineageAncestorChain(receipt, bh)
				setRestoredLineageCurrentMetadata(bh,
					restoredLineageCurrentMetadataRow(11, 6, 122000, dataBranchLevel_Database, true),
				)
			},
			wantError: "current lineage for table 11 does not match",
		},
		{
			name: "missing ordinary current ancestor",
			setup: func(receipt *cloneReceipt, bh *backgroundExecTest) {
				prepareRestoredLineageAncestorChain(receipt, bh)
				setRestoredLineageCurrentMetadata(bh)
			},
			wantError: "current lineage for ordinary ancestor 11 is missing",
		},
		{
			name: "unavailable later ancestor history",
			setup: func(receipt *cloneReceipt, bh *backgroundExecTest) {
				prepareRestoredLineageAncestorChain(receipt, bh)
				bh.sql2err[restoredBranchAncestorLineageMetadataSQL(receipt, 5)] = errors.New("history unavailable")
			},
			wantError: "history unavailable",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt := restoredLineageTestReceipt()
			bh := newRestoredLineageTestExec(receipt, [][]interface{}{
				restoredLineageTestMetadataRow(dataBranchLevel_Database),
			}, [][]interface{}{restoredLineageTestProtectionRow()})
			test.setup(receipt, bh)

			err := restitchRestoredBranchLineage(context.Background(), bh, receipt)
			require.ErrorContains(t, err, test.wantError)
			for _, sql := range bh.executedSQLs {
				require.False(t, strings.HasPrefix(sql, "insert into "), sql)
			}
		})
	}
}

func TestRestitchRestoredBranchLineageRejectsInvalidHistoricalState(t *testing.T) {
	for _, test := range []struct {
		name           string
		metadataRows   [][]interface{}
		protectionRows [][]interface{}
		wantError      string
	}{
		{
			name: "malformed lineage identity",
			metadataRows: [][]interface{}{
				{"not-a-table-id", "123000", "7", dataBranchLevel_Table},
			},
			wantError: "invalid syntax",
		},
		{
			name: "duplicate active lineage",
			metadataRows: [][]interface{}{
				restoredLineageTestMetadataRow(dataBranchLevel_Table),
				restoredLineageTestMetadataRow(dataBranchLevel_Table),
			},
			wantError: "multiple active rows",
		},
		{
			name:         "malformed protection identity",
			metadataRows: [][]interface{}{restoredLineageTestMetadataRow(dataBranchLevel_Table)},
			protectionRows: [][]interface{}{{
				"snapshot-id", "__mo_branch_22", "123000", "table", "sys",
				"source_db", "parent_t", "not-a-parent-id", "branch",
			}},
			wantError: "invalid syntax",
		},
		{
			name:         "duplicate protection",
			metadataRows: [][]interface{}{restoredLineageTestMetadataRow(dataBranchLevel_Table)},
			protectionRows: [][]interface{}{
				restoredLineageTestProtectionRow(), restoredLineageTestProtectionRow(),
			},
			wantError: "multiple protection rows",
		},
		{
			name:         "protection parent mismatch",
			metadataRows: [][]interface{}{restoredLineageTestMetadataRow(dataBranchLevel_Table)},
			protectionRows: [][]interface{}{{
				"snapshot-id", "__mo_branch_22", "123000", "table", "sys",
				"source_db", "parent_t", "12", "branch",
			}},
			wantError: "does not match its lineage",
		},
		{
			name:         "protection timestamp mismatch",
			metadataRows: [][]interface{}{restoredLineageTestMetadataRow(dataBranchLevel_Table)},
			protectionRows: [][]interface{}{{
				"snapshot-id", "__mo_branch_22", "123001", "table", "sys",
				"source_db", "parent_t", "11", "branch",
			}},
			wantError: "does not match its lineage",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt := restoredLineageTestReceipt()
			bh := newRestoredLineageTestExec(receipt, test.metadataRows, test.protectionRows)
			err := restitchRestoredBranchLineage(context.Background(), bh, receipt)
			require.ErrorContains(t, err, test.wantError)
			for _, sql := range bh.executedSQLs {
				require.False(t, strings.HasPrefix(sql, "insert into "), sql)
			}
		})
	}
}

func TestRestitchRestoredBranchLineagePropagatesReadAndLineageWriteErrors(t *testing.T) {
	wantErr := errors.New("injected restore lineage failure")
	for _, test := range []struct {
		name          string
		failingSQL    func(*cloneReceipt) string
		wantStatement int
	}{
		{
			name: "lineage historical read",
			failingSQL: func(receipt *cloneReceipt) string {
				return restoredBranchLineageMetadataSQL(receipt)
			},
			wantStatement: 1,
		},
		{
			name: "protection historical read",
			failingSQL: func(receipt *cloneReceipt) string {
				return restoredBranchProtectionSQL(receipt)
			},
			wantStatement: 2,
		},
		{
			name: "lineage publication",
			failingSQL: func(receipt *cloneReceipt) string {
				return restoredLineageInsertSQL(receipt, dataBranchLevel_Table)
			},
			wantStatement: 8,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt := restoredLineageTestReceipt()
			bh := newRestoredLineageTestExec(receipt, [][]interface{}{
				restoredLineageTestMetadataRow(dataBranchLevel_Table),
			}, [][]interface{}{restoredLineageTestProtectionRow()})
			bh.sql2err[test.failingSQL(receipt)] = wantErr

			err := restitchRestoredBranchLineage(context.Background(), bh, receipt)
			require.ErrorIs(t, err, wantErr)
			require.Len(t, bh.executedSQLs, test.wantStatement)
			require.Equal(t, test.failingSQL(receipt), bh.executedSQLs[len(bh.executedSQLs)-1])
		})
	}
}

type restoredLineageFailPrefixExec struct {
	*backgroundExecTest
	prefix  string
	err     error
	failAt  int
	matched int
}

func (bh *restoredLineageFailPrefixExec) Exec(ctx context.Context, sql string) error {
	if err := bh.backgroundExecTest.Exec(ctx, sql); err != nil {
		return err
	}
	if strings.HasPrefix(sql, bh.prefix) {
		bh.matched++
		if bh.failAt == 0 || bh.matched == bh.failAt {
			return bh.err
		}
	}
	return nil
}

func TestRestitchRestoredBranchLineagePropagatesProtectionWriteError(t *testing.T) {
	receipt := restoredLineageTestReceipt()
	backgroundExec := newRestoredLineageTestExec(receipt, [][]interface{}{
		restoredLineageTestMetadataRow(dataBranchLevel_Table),
	}, [][]interface{}{restoredLineageTestProtectionRow()})
	wantErr := errors.New("injected protection publication failure")
	bh := &restoredLineageFailPrefixExec{
		backgroundExecTest: backgroundExec,
		prefix:             "insert into mo_catalog.mo_snapshots",
		err:                wantErr,
	}

	err := restitchRestoredBranchLineage(context.Background(), bh, receipt)
	require.ErrorIs(t, err, wantErr)
	require.Len(t, backgroundExec.executedSQLs, 9)
	require.Equal(t,
		restoredLineageInsertSQL(receipt, dataBranchLevel_Table),
		backgroundExec.executedSQLs[7],
	)
	require.True(t, strings.HasPrefix(backgroundExec.executedSQLs[8], bh.prefix))
}

func TestRestitchRestoredBranchLineagePropagatesAncestorProtectionWriteError(t *testing.T) {
	receipt := restoredLineageTestReceipt()
	backgroundExec := newRestoredLineageTestExec(receipt, [][]interface{}{
		restoredLineageTestMetadataRow(dataBranchLevel_Database),
	}, [][]interface{}{restoredLineageTestProtectionRow()})
	prepareRestoredLineageAncestorChain(receipt, backgroundExec)
	wantErr := errors.New("injected ancestor protection publication failure")
	bh := &restoredLineageFailPrefixExec{
		backgroundExecTest: backgroundExec,
		prefix:             "insert into mo_catalog.mo_snapshots",
		err:                wantErr,
		failAt:             2,
	}

	err := restitchRestoredBranchLineage(context.Background(), bh, receipt)
	require.ErrorIs(t, err, wantErr)
	require.Len(t, backgroundExec.executedSQLs, 13)
	require.Equal(t,
		restoredLineageInsertSQL(receipt, dataBranchLevel_Database),
		backgroundExec.executedSQLs[10],
	)
	require.True(t, strings.HasPrefix(backgroundExec.executedSQLs[12], bh.prefix))
	require.Equal(t, 2, bh.matched)
}

func TestRestoreBranchLineageResolvesPhysicalIDsOnOwnerPath(t *testing.T) {
	receipt := restoredLineageTestReceipt()
	bh := newRestoredLineageTestExec(receipt, [][]interface{}{
		restoredLineageTestMetadataRow(dataBranchLevel_Database),
	}, [][]interface{}{restoredLineageTestProtectionRow()})
	sourceIDSQL := restoredBranchTableIDSQL(receipt, true)
	destinationIDSQL := restoredBranchTableIDSQL(receipt, false)
	require.Equal(t,
		"select rel_id from mo_catalog.mo_tables {MO_TS = 123456} where account_id = 42 and reldatabase = 'branch_db' and relname = 'branch_t'",
		sourceIDSQL,
	)
	require.Equal(t,
		"select rel_id from mo_catalog.mo_tables where account_id = 42 and reldatabase = 'branch_db' and relname = 'branch_t'",
		destinationIDSQL,
	)
	bh.sql2result[sourceIDSQL] = newMrsForRestoreStringRows([]string{"rel_id"}, [][]interface{}{{"22"}})
	bh.sql2result[destinationIDSQL] = newMrsForRestoreStringRows([]string{"rel_id"}, [][]interface{}{{"33"}})
	receipt.srcTableID = 0
	receipt.dstTableID = 0

	ctx := context.WithValue(
		defines.AttachAccountId(context.Background(), receipt.srcAccount),
		tree.CloneLevelCtxKey{}, tree.RestoreCloneLevelDatabase,
	)
	require.NoError(t, restoreBranchLineage(ctx, bh, receipt))
	require.Equal(t, uint64(22), receipt.srcTableID)
	require.Equal(t, uint64(33), receipt.dstTableID)
	require.Len(t, bh.executedSQLs, 11)
	require.Equal(t, []string{
		sourceIDSQL,
		destinationIDSQL,
		restoredBranchLineageMetadataSQL(receipt),
		restoredBranchProtectionSQL(receipt),
		restoredBranchAncestorLineageMetadataSQL(receipt, 11),
		restoredBranchAncestorProtectionSQL(receipt, 11),
		historicalAlterLineageMetadataSQL(),
		historicalAlterLineageMetadataSQL(),
		currentRestoredBranchProtectionSQL(receipt.dstTableID),
		restoredLineageInsertSQL(receipt, dataBranchLevel_Database),
	}, bh.executedSQLs[:10])
	requireRestoredProtectionInsert(t, bh.executedSQLs[10], receipt)
	require.Equal(t, []uint32{42, 42}, bh.executionAccountIDs[:2])
	for _, accountID := range bh.executionAccountIDs[2:] {
		require.Equal(t, uint32(sysAccountID), accountID)
	}
}

func TestRestoreBranchLineageRejectsPhysicalIDResolutionFailures(t *testing.T) {
	for _, test := range []struct {
		name            string
		sourceRows      [][]interface{}
		destinationRows [][]interface{}
		failSource      bool
		failDestination bool
		wantError       string
		wantStatements  int
	}{
		{
			name:           "source read error",
			failSource:     true,
			wantError:      "injected restore lineage failure",
			wantStatements: 1,
		},
		{
			name:            "destination read error",
			sourceRows:      [][]interface{}{{"22"}},
			failDestination: true,
			wantError:       "injected restore lineage failure",
			wantStatements:  2,
		},
		{
			name:           "missing source",
			wantError:      "cannot resolve physical ID",
			wantStatements: 1,
		},
		{
			name:           "missing destination",
			sourceRows:     [][]interface{}{{"22"}},
			wantError:      "cannot resolve physical ID",
			wantStatements: 2,
		},
		{
			name:           "malformed source ID",
			sourceRows:     [][]interface{}{{"not-a-table-id"}},
			wantError:      "invalid syntax",
			wantStatements: 1,
		},
		{
			name:           "duplicate source IDs",
			sourceRows:     [][]interface{}{{"22"}, {"22"}},
			wantError:      "multiple physical rows",
			wantStatements: 1,
		},
		{
			name:            "duplicate destination IDs",
			sourceRows:      [][]interface{}{{"22"}},
			destinationRows: [][]interface{}{{"33"}, {"33"}},
			wantError:       "multiple physical rows",
			wantStatements:  2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt := restoredLineageTestReceipt()
			bh := newRestoredLineageTestExec(receipt, nil, nil)
			sourceIDSQL := restoredBranchTableIDSQL(receipt, true)
			destinationIDSQL := restoredBranchTableIDSQL(receipt, false)
			bh.sql2result[sourceIDSQL] = newMrsForRestoreStringRows([]string{"rel_id"}, test.sourceRows)
			bh.sql2result[destinationIDSQL] = newMrsForRestoreStringRows([]string{"rel_id"}, test.destinationRows)
			wantErr := errors.New("injected restore lineage failure")
			if test.failSource {
				bh.sql2err[sourceIDSQL] = wantErr
			}
			if test.failDestination {
				bh.sql2err[destinationIDSQL] = wantErr
			}
			ctx := context.WithValue(context.Background(), tree.CloneLevelCtxKey{}, tree.RestoreCloneLevelTable)
			err := restoreBranchLineage(ctx, bh, receipt)
			require.ErrorContains(t, err, test.wantError)
			require.Len(t, bh.executedSQLs, test.wantStatements)
			for _, sql := range bh.executedSQLs {
				require.False(t, strings.HasPrefix(sql, "insert into "), sql)
			}
		})
	}
}

func TestRestoreBranchLineageIgnoresNonTableDatabaseRestoreScopes(t *testing.T) {
	for _, level := range []tree.CloneLevelType{
		tree.NormalCloneLevelTable,
		tree.NormalCloneLevelDatabase,
		tree.NormalCloneLevelAccount,
		tree.NormalCloneLevelCluster,
		tree.RestoreCloneLevelAccount,
		tree.RestoreCloneLevelCluster,
	} {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			ctx := context.WithValue(context.Background(), tree.CloneLevelCtxKey{}, level)
			require.NoError(t, restoreBranchLineage(ctx, nil, nil))
		})
	}
	require.NoError(t, restoreBranchLineage(context.Background(), nil, nil))
}
