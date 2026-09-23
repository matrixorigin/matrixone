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
	"crypto/sha256"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbstats "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type boundAnalyzeTable struct {
	ctx             context.Context
	columns         tree.IdentifierList
	relation        engine.AnalyzableRelation
	key             pbstats.StatsInfoKey
	tableDefVersion uint32
}

func handleAnalyzeStatsStmt(ses *Session, execCtx *ExecCtx, stmt *tree.AnalyzeStmt) error {
	if !analyzeStatsPublicationAllowed(execCtx) {
		return moerr.NewNotSupported(execCtx.reqCtx,
			"ANALYZE TABLE cannot run inside an active user transaction")
	}
	storage := getPu(ses.GetService()).StorageEngine
	publisher, ok := storage.(engine.AnalyzedStatsPublisher)
	if !ok {
		return moerr.NewNotSupported(execCtx.reqCtx,
			"ANALYZE TABLE is not supported by this storage engine")
	}
	tables, err := bindAnalyzeTables(ses, execCtx, stmt)
	if err != nil {
		return err
	}

	// ANALYZE historically received SELECT authorization through its derived
	// aggregate. Keep that security boundary while the dedicated privilege
	// descriptor is introduced: this zero-row probe binds every selected column
	// and performs ordinary SELECT authorization without reading table data.
	for _, table := range tables {
		if _, err = executeAnalyzeDerivedQuery(
			ses, execCtx, buildAnalyzeAuthorizationProbe(
				table.key.DbName, table.key.TableName, table.columns)); err != nil {
			return err
		}
	}

	// The authorization probes execute through the background executor and may
	// clear the statement result set. Install a fresh one for ANALYZE's summary.
	mrs := &MysqlResultSet{}
	ses.SetMysqlResultSet(mrs)
	addAnalyzeResultColumns(mrs)
	for _, table := range tables {
		result, err := analyzeBoundTable(ses, stmt.FullScan, table, publisher)
		if err != nil {
			return err
		}
		mrs.AddRow([]any{
			table.key.DbName + "." + table.key.TableName,
			result.Mode,
			result.Coverage,
			uint64(result.ColumnsAnalyzed),
			result.PopulationRows,
			result.PopulationExact,
			result.SampleRows,
			result.SampleBlocks,
			result.SampleBytes,
			"OK",
			fmt.Sprintf("q=%d/%d", result.SampleNumerator, result.SampleDenominator),
		})
	}
	if err := trySaveQueryResult(execCtx.reqCtx, ses, mrs); err != nil {
		return err
	}
	execCtx.results = []ExecResult{mrs}
	return nil
}

func analyzeBoundTable(
	ses *Session,
	fullScan bool,
	table boundAnalyzeTable,
	publisher engine.AnalyzedStatsPublisher,
) (*engine.AnalyzeTableResult, error) {
	tableKey := optimizerStatsTableKey{
		accountID: table.key.AccId,
		tableID:   table.key.TableID,
	}
	release, err := acquireOptimizerStatsPublisher(
		table.ctx, ses.GetService(), tableKey)
	if err != nil {
		return nil, err
	}
	defer release()

	seed := sha256.Sum256([]byte(fmt.Sprintf(
		"manual-analyze-v1/%d/%d/%d", table.key.AccId, table.key.DatabaseID, table.key.TableID)))
	result, err := table.relation.AnalyzeTable(table.ctx, engine.AnalyzeTableRequest{
		Process:  ses.proc,
		Columns:  identifiersToStrings(table.columns),
		FullScan: fullScan,
		Seed:     seed,
	})
	if err != nil {
		return nil, err
	}
	if result == nil || result.Stats == nil {
		return nil, moerr.NewInternalErrorNoCtxf(
			"ANALYZE TABLE did not collect statistics for %s.%s",
			table.key.DbName, table.key.TableName)
	}
	if err = publishCollectedAnalyzeStats(
		ses, table.ctx, table.key, table.tableDefVersion,
		result.Stats, publisher); err != nil {
		return nil, err
	}
	return result, nil
}

func bindAnalyzeTables(
	ses *Session,
	execCtx *ExecCtx,
	stmt *tree.AnalyzeStmt,
) ([]boundAnalyzeTable, error) {
	tcc := ses.GetTxnCompileCtx()
	if tcc == nil {
		return nil, moerr.NewInternalErrorNoCtx("ANALYZE TABLE requires a transaction compiler context")
	}
	tables := make([]boundAnalyzeTable, 0, len(stmt.Entries))
	seen := make(map[optimizerStatsTableKey]struct{}, len(stmt.Entries))
	for _, entry := range stmt.Entries {
		if entry == nil || entry.Table == nil {
			return nil, moerr.NewInvalidInputNoCtx("ANALYZE requires a table")
		}
		if entry.Table.AtTsExpr != nil {
			return nil, moerr.NewNotSupported(execCtx.reqCtx,
				"ANALYZE TABLE does not support historical snapshots")
		}
		columns := entry.Cols
		var err error
		if len(columns) == 0 {
			columns, err = resolveTableVisibleColumns(ses, execCtx.reqCtx, entry.Table)
			if err != nil {
				return nil, err
			}
		}
		dbName := resolveAnalyzeDatabase(tcc, entry.Table)
		if dbName == "" {
			return nil, moerr.NewNoDB(execCtx.reqCtx)
		}
		obj, tableDef, err := tcc.Resolve(dbName, string(entry.Table.Name()), nil)
		if err != nil {
			return nil, err
		}
		if obj == nil || tableDef == nil {
			return nil, moerr.NewNoSuchTable(
				execCtx.reqCtx, dbName, string(entry.Table.Name()))
		}
		if obj.PubInfo != nil || !analyzeTableOwnsPersistentStats(tableDef) ||
			tableDef.IsTemporary || tableDef.ViewSql != nil {
			return nil, moerr.NewNotSupported(execCtx.reqCtx,
				"ANALYZE TABLE supports only owned physical tables")
		}
		if util.BuildTableScanAccountFilter(
			ses.GetAccountId(), obj.SchemaName, obj.ObjName, tableDef.TableType,
		) != nil {
			return nil, moerr.NewNotSupported(execCtx.reqCtx,
				"ANALYZE TABLE cannot publish statistics for an account-filtered table")
		}
		physicalCtx, relation, err := tcc.getRelation(dbName, string(entry.Table.Name()), nil, nil)
		if err != nil {
			return nil, err
		}
		if relation == nil {
			return nil, moerr.NewNoSuchTable(
				execCtx.reqCtx, dbName, string(entry.Table.Name()))
		}
		analyzer, ok := relation.(engine.AnalyzableRelation)
		if !ok {
			return nil, moerr.NewNotSupported(execCtx.reqCtx,
				"ANALYZE TABLE is not supported by this relation")
		}
		accountID := tcc.resolvePhysicalObjectAccount(obj, tableDef, nil)
		databaseID := tableDef.DbId
		if databaseID == 0 {
			databaseID, err = tcc.GetDatabaseId(obj.SchemaName, nil)
			if err != nil {
				return nil, err
			}
		}
		identity := optimizerStatsTableKey{accountID: accountID, tableID: uint64(obj.Obj)}
		if _, duplicate := seen[identity]; duplicate {
			return nil, moerr.NewInvalidInputNoCtxf(
				"duplicate ANALYZE target %s.%s", obj.SchemaName, obj.ObjName)
		}
		seen[identity] = struct{}{}
		tables = append(tables, boundAnalyzeTable{
			ctx: physicalCtx, columns: columns, relation: analyzer,
			tableDefVersion: tableDef.Version,
			key: pbstats.StatsInfoKey{
				AccId: accountID, DatabaseID: databaseID, TableID: uint64(obj.Obj),
				DbName: obj.SchemaName, TableName: obj.ObjName,
			},
		})
	}
	return tables, nil
}

func publishCollectedAnalyzeStats(
	ses *Session,
	ctx context.Context,
	key pbstats.StatsInfoKey,
	tableDefVersion uint32,
	stats *pbstats.StatsInfo,
	publisher engine.AnalyzedStatsPublisher,
) error {
	tableKey := optimizerStatsTableKey{accountID: key.AccId, tableID: key.TableID}
	published, err := publisher.PublishAnalyzedStats(
		ctx, key, tableDefVersion, stats)
	if err != nil {
		return err
	}
	if published == nil {
		return moerr.NewInternalErrorNoCtxf(
			"ANALYZE TABLE did not publish statistics for %s.%s", key.DbName, key.TableName)
	}
	version := advanceOptimizerStatsVersion(ses.GetService(), tableKey)
	ses.cachePublishedStatsForTableDefVersion(
		tableKey, version, &tableDefVersion, published)
	return nil
}

func buildAnalyzeAuthorizationProbe(
	databaseName string,
	tableName string,
	columns tree.IdentifierList,
) string {
	ctx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteIdentifier())
	ctx.WriteString("select ")
	for i, column := range columns {
		if i > 0 {
			ctx.WriteByte(',')
		}
		ctx.WriteIdentifier(column)
	}
	ctx.WriteString(" from ")
	ctx.WriteIdentifier(tree.Identifier(databaseName))
	ctx.WriteByte('.')
	ctx.WriteIdentifier(tree.Identifier(tableName))
	ctx.WriteString(" where false")
	return ctx.String()
}

func identifiersToStrings(columns tree.IdentifierList) []string {
	result := make([]string, len(columns))
	for i := range columns {
		result[i] = string(columns[i])
	}
	return result
}

func addAnalyzeResultColumns(mrs *MysqlResultSet) {
	definitions := []struct {
		name string
		typ  defines.MysqlType
	}{
		{"table_name", defines.MYSQL_TYPE_VARCHAR},
		{"mode", defines.MYSQL_TYPE_VARCHAR},
		{"coverage", defines.MYSQL_TYPE_VARCHAR},
		{"columns_analyzed", defines.MYSQL_TYPE_LONGLONG},
		{"population_rows", defines.MYSQL_TYPE_LONGLONG},
		{"population_exact", defines.MYSQL_TYPE_TINY},
		{"sample_rows", defines.MYSQL_TYPE_LONGLONG},
		{"sample_blocks", defines.MYSQL_TYPE_LONGLONG},
		{"sample_bytes", defines.MYSQL_TYPE_LONGLONG},
		{"status", defines.MYSQL_TYPE_VARCHAR},
		{"message", defines.MYSQL_TYPE_VARCHAR},
	}
	for _, definition := range definitions {
		column := new(MysqlColumn)
		column.SetColumnType(definition.typ)
		column.SetName(definition.name)
		mrs.AddColumn(column)
	}
}
