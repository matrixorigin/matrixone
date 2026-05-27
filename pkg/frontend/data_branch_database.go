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
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type dataBranchDatabaseTablePair struct {
	tableName string
	srcTable  tree.TableName
	dstTable  tree.TableName
}

func isDataBranchDatabaseTableCandidate(info *tableInfo) bool {
	return info != nil && info.typ != view && info.typ != clusterTable
}

func dataBranchDatabaseTableName(db tree.Identifier, tbl string) tree.TableName {
	prefix := tree.ObjectNamePrefix{SchemaName: db, ExplicitSchema: true}
	return *tree.NewTableName(tree.Identifier(tbl), prefix, nil)
}

func dataBranchDatabaseTableMap(infos []*tableInfo) (map[string]*tableInfo, error) {
	tables := make(map[string]*tableInfo)
	for _, info := range infos {
		if !isDataBranchDatabaseTableCandidate(info) {
			continue
		}
		if _, ok := tables[info.tblName]; ok {
			return nil, moerr.NewInvalidInputNoCtxf("duplicate table %q in data branch database operation", info.tblName)
		}
		tables[info.tblName] = info
	}
	return tables, nil
}

func buildDataBranchDatabaseTablePairs(
	srcDb tree.Identifier,
	dstDb tree.Identifier,
	srcInfos []*tableInfo,
	dstInfos []*tableInfo,
) ([]dataBranchDatabaseTablePair, error) {
	srcTables, err := dataBranchDatabaseTableMap(srcInfos)
	if err != nil {
		return nil, err
	}
	dstTables, err := dataBranchDatabaseTableMap(dstInfos)
	if err != nil {
		return nil, err
	}

	var missingInDst, missingInSrc []string
	for name := range srcTables {
		if _, ok := dstTables[name]; !ok {
			missingInDst = append(missingInDst, name)
		}
	}
	for name := range dstTables {
		if _, ok := srcTables[name]; !ok {
			missingInSrc = append(missingInSrc, name)
		}
	}
	sort.Strings(missingInDst)
	sort.Strings(missingInSrc)
	if len(missingInDst) != 0 || len(missingInSrc) != 0 {
		return nil, moerr.NewInvalidInputNoCtxf(
			"DATA BRANCH DATABASE table set mismatch: missing in %s: %s; missing in %s: %s",
			dstDb,
			formatTableList(missingInDst),
			srcDb,
			formatTableList(missingInSrc),
		)
	}

	names := make([]string, 0, len(srcTables))
	for name := range srcTables {
		names = append(names, name)
	}
	sort.Strings(names)

	pairs := make([]dataBranchDatabaseTablePair, 0, len(names))
	for _, name := range names {
		pairs = append(pairs, dataBranchDatabaseTablePair{
			tableName: name,
			srcTable:  dataBranchDatabaseTableName(srcDb, name),
			dstTable:  dataBranchDatabaseTableName(dstDb, name),
		})
	}
	return pairs, nil
}

func formatTableList(names []string) string {
	if len(names) == 0 {
		return "none"
	}
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = fmt.Sprintf("%q", name)
	}
	return strings.Join(quoted, ", ")
}

func dataBranchDatabaseDiffOutputCount(outputOpt *tree.DiffOutputOpt) bool {
	return outputOpt != nil && outputOpt.Count
}

func dataBranchDatabaseDiffOutputSummary(outputOpt *tree.DiffOutputOpt) bool {
	return outputOpt == nil || outputOpt.Summary
}

func appendDataBranchDatabaseDiffRows(
	finalResult *MysqlResultSet,
	tableResult *MysqlResultSet,
	tableName string,
	outputOpt *tree.DiffOutputOpt,
) error {
	if finalResult == nil || tableResult == nil {
		return moerr.NewInternalErrorNoCtx("DATA BRANCH DATABASE diff result set is nil")
	}
	if dataBranchDatabaseDiffOutputCount(outputOpt) {
		for _, row := range tableResult.Data {
			if len(row) != 1 {
				return moerr.NewInternalErrorNoCtxf(
					"DATA BRANCH DATABASE diff count row shape mismatch: table=%s cols=%d",
					tableName, len(row),
				)
			}
			finalResult.AddRow([]interface{}{tableName, row[0]})
		}
		return nil
	}
	if dataBranchDatabaseDiffOutputSummary(outputOpt) {
		for _, row := range tableResult.Data {
			if len(row) != 3 {
				return moerr.NewInternalErrorNoCtxf(
					"DATA BRANCH DATABASE diff summary row shape mismatch: table=%s cols=%d",
					tableName, len(row),
				)
			}
			finalResult.AddRow([]interface{}{tableName, row[0], row[1], row[2]})
		}
		return nil
	}
	return moerr.NewNotSupportedNoCtx(fmt.Sprintf("%v", outputOpt))
}

func getDataBranchDatabaseTablePairs(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	srcDb tree.Identifier,
	dstDb tree.Identifier,
) ([]dataBranchDatabaseTablePair, error) {
	srcInfos, err := getTableInfos(ctx, ses.GetService(), bh, nil, string(srcDb), "")
	if err != nil {
		return nil, err
	}
	dstInfos, err := getTableInfos(ctx, ses.GetService(), bh, nil, string(dstDb), "")
	if err != nil {
		return nil, err
	}
	return buildDataBranchDatabaseTablePairs(srcDb, dstDb, srcInfos, dstInfos)
}

func dataBranchDatabaseTableDiffOutputOpt(outputOpt *tree.DiffOutputOpt) *tree.DiffOutputOpt {
	if dataBranchDatabaseDiffOutputCount(outputOpt) {
		return &tree.DiffOutputOpt{Count: true}
	}
	return &tree.DiffOutputOpt{Summary: true}
}

func buildDataBranchDatabaseDiffResultSet(
	ses *Session,
	stmt *tree.DataBranchDiffDatabase,
) (*MysqlResultSet, error) {
	if stmt.OutputOpt != nil &&
		!dataBranchDatabaseDiffOutputCount(stmt.OutputOpt) &&
		!dataBranchDatabaseDiffOutputSummary(stmt.OutputOpt) {
		return nil, moerr.NewNotSupportedNoCtx(fmt.Sprintf("%v", stmt.OutputOpt))
	}

	ses.ClearAllMysqlResultSet()
	ses.ClearResultBatches()

	mrs := &MysqlResultSet{}
	ses.SetMysqlResultSet(mrs)

	tableCol := new(MysqlColumn)
	tableCol.SetName("table_name")
	tableCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(tableCol)

	if dataBranchDatabaseDiffOutputCount(stmt.OutputOpt) {
		countCol := new(MysqlColumn)
		countCol.SetName("COUNT(*)")
		countCol.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		mrs.AddColumn(countCol)
		return mrs, nil
	}

	metricCol := new(MysqlColumn)
	metricCol.SetName("metric")
	metricCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mrs.AddColumn(metricCol)

	targetCol := new(MysqlColumn)
	targetCol.SetName(string(stmt.TargetDatabase))
	targetCol.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(targetCol)

	baseCol := new(MysqlColumn)
	baseCol.SetName(string(stmt.BaseDatabase))
	baseCol.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mrs.AddColumn(baseCol)

	return mrs, nil
}

func handleBranchDiffDatabase(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchDiffDatabase,
) (err error) {
	var (
		bh       BackgroundExec
		deferred func(error) error
		pairs    []dataBranchDatabaseTablePair
		final    *MysqlResultSet
	)

	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return
	}
	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	if pairs, err = getDataBranchDatabaseTablePairs(
		execCtx.reqCtx, ses, bh, stmt.TargetDatabase, stmt.BaseDatabase,
	); err != nil {
		return
	}
	if final, err = buildDataBranchDatabaseDiffResultSet(ses, stmt); err != nil {
		return
	}

	tableOutputOpt := dataBranchDatabaseTableDiffOutputOpt(stmt.OutputOpt)
	for _, pair := range pairs {
		tableResult := &MysqlResultSet{}
		ses.SetMysqlResultSet(tableResult)

		tableStmt := &tree.DataBranchDiff{
			TargetTable: pair.srcTable,
			BaseTable:   pair.dstTable,
			OutputOpt:   tableOutputOpt,
		}
		if err = diffMergeAgencyWithExecutor(ses, execCtx, bh, tableStmt); err != nil {
			ses.SetMysqlResultSet(final)
			return
		}
		ses.SetMysqlResultSet(final)
		if err = appendDataBranchDatabaseDiffRows(final, tableResult, pair.tableName, stmt.OutputOpt); err != nil {
			return
		}
	}

	return nil
}

func handleBranchMergeDatabase(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchMergeDatabase,
) (err error) {
	var (
		bh       BackgroundExec
		deferred func(error) error
		pairs    []dataBranchDatabaseTablePair
	)

	if stmt.ConflictOpt == nil {
		stmt.ConflictOpt = &tree.ConflictOpt{Opt: tree.CONFLICT_FAIL}
	}

	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return
	}
	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	if pairs, err = getDataBranchDatabaseTablePairs(
		execCtx.reqCtx, ses, bh, stmt.SrcDatabase, stmt.DstDatabase,
	); err != nil {
		return
	}

	for _, pair := range pairs {
		tableStmt := &tree.DataBranchMerge{
			SrcTable:    pair.srcTable,
			DstTable:    pair.dstTable,
			ConflictOpt: stmt.ConflictOpt,
		}
		if err = diffMergeAgencyWithExecutor(ses, execCtx, bh, tableStmt); err != nil {
			return
		}
	}
	return nil
}

func handleBranchPickDatabase(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchPickDatabase,
) (err error) {
	var (
		bh       BackgroundExec
		deferred func(error) error
		pairs    []dataBranchDatabaseTablePair
	)

	if ses.proc.GetTxnOperator().TxnOptions().ByBegin {
		return moerr.NewInternalError(execCtx.reqCtx, "DATA BRANCH PICK is not supported in explicit transactions")
	}
	if stmt.ConflictOpt == nil {
		stmt.ConflictOpt = &tree.ConflictOpt{Opt: tree.CONFLICT_FAIL}
	}
	if stmt.BetweenFrom == "" || stmt.BetweenTo == "" {
		return moerr.NewInvalidInputNoCtx("DATA BRANCH PICK DATABASE requires a BETWEEN SNAPSHOT clause")
	}

	if bh, deferred, err = getBackExecutor(execCtx.reqCtx, ses); err != nil {
		return
	}
	defer func() {
		if deferred != nil {
			err = deferred(err)
		}
	}()

	if pairs, err = getDataBranchDatabaseTablePairs(
		execCtx.reqCtx, ses, bh, stmt.SrcDatabase, stmt.DstDatabase,
	); err != nil {
		return
	}

	for _, pair := range pairs {
		tableStmt := &tree.DataBranchPick{
			SrcTable:    pair.srcTable,
			DstTable:    pair.dstTable,
			BetweenFrom: stmt.BetweenFrom,
			BetweenTo:   stmt.BetweenTo,
			ConflictOpt: stmt.ConflictOpt,
		}
		if err = diffMergeAgencyWithExecutor(ses, execCtx, bh, tableStmt); err != nil {
			return
		}
	}
	return nil
}
