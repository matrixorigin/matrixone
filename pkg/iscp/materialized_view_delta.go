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

package iscp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	// Keep enough rows in one delta statement to amortize parsing, planning and
	// target-table joins. The SQL-size guard below remains the hard safety bound
	// for wide rows.
	materializedViewDeltaBatchRows = 32768
	materializedViewDeltaMaxSQL    = 8 << 20
)

var errMaterializedViewDeltaSQLTooLarge = moerr.NewInternalErrorNoCtx("materialized view delta SQL is too large")

func execMaterializedViewDeltaSQL(
	ctx context.Context,
	sql string,
	service string,
	txn client.TxnOperator,
) (executor.Result, error) {
	v, ok := moruntime.ServiceRuntime(service).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing internal SQL executor")
	}

	// Every delta DML is a real statement in the refresh transaction. Advancing
	// the statement lifecycle merges the preceding DML's workspace writes before
	// the next UPDATE/INSERT scans the target. The generic ISCP executor keeps all
	// calls in one logical parent statement, which is only valid for sub-SQL and
	// can make successive missing-group inserts create duplicate target rows.
	opts := materializedViewDeltaExecOptions(txn)
	return v.(executor.SQLExecutor).Exec(ctx, sql, opts)
}

func materializedViewDeltaExecOptions(txn client.TxnOperator) executor.Options {
	return executor.Options{}.WithTxn(txn)
}

type materializedViewSignedRow struct {
	values map[string]any
	sign   int8
}

func decodeIncrementalDescription(encoded string) (*incrementalDescription, error) {
	b, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("invalid materialized view incremental specification encoding: %v", err)
	}
	var desc incrementalDescription
	if err := json.Unmarshal(b, &desc); err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("invalid materialized view incremental specification: %v", err)
	}
	if desc.Version == 0 {
		desc.Version = 1
	}
	if desc.Version < 1 || desc.Version > 2 {
		return nil, moerr.NewInternalErrorNoCtxf("unsupported materialized view incremental specification version %d", desc.Version)
	}
	if desc.SourceAlias == "" || len(desc.SourceColumns) == 0 || len(desc.Groups) == 0 ||
		desc.RowCountColumn == "" || len(desc.StateColumns) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("incomplete materialized view incremental specification")
	}
	for _, group := range desc.Groups {
		if group.Expression == "" || group.OutputColumn == "" {
			return nil, moerr.NewInternalErrorNoCtx("invalid materialized view incremental group")
		}
	}
	for _, agg := range desc.Aggregates {
		switch agg.Kind {
		case "count_star":
		case "count_column":
			if agg.InputExpression == "" {
				return nil, moerr.NewInternalErrorNoCtx("incremental COUNT requires an input")
			}
		case "sum":
			if agg.InputExpression == "" || agg.StateCountColumn == "" {
				return nil, moerr.NewInternalErrorNoCtx("incremental SUM requires input and state")
			}
			if desc.GroupKeyColumn != "" && agg.StateSumColumn == "" {
				return nil, moerr.NewInternalErrorNoCtx("incremental SUM with a group key requires sum state")
			}
		case "avg":
			if agg.InputExpression == "" || agg.StateSumColumn == "" || agg.StateCountColumn == "" {
				return nil, moerr.NewInternalErrorNoCtx("incremental AVG requires input and state")
			}
		case "min", "max":
			if agg.InputExpression == "" {
				return nil, moerr.NewInternalErrorNoCtxf("incremental %s requires an input", strings.ToUpper(agg.Kind))
			}
		case "count_distinct":
			if desc.Version < 2 || desc.StateTable == "" || agg.InputExpression == "" || agg.StateIndex <= 0 {
				return nil, moerr.NewInternalErrorNoCtx("incremental COUNT(DISTINCT) requires versioned auxiliary state")
			}
		default:
			return nil, moerr.NewInternalErrorNoCtxf("incremental aggregate %q is not supported", agg.Kind)
		}
	}
	return &desc, nil
}

func (c *MaterializedViewConsumer) consumeIncremental(ctx context.Context, r DataRetriever) (drained bool, err error) {
	desc, err := decodeIncrementalDescription(c.info.IncrementalSpec)
	if err != nil {
		return false, err
	}
	from, ok := r.(materializedViewFromBoundaryRetriever)
	if !ok {
		return false, moerr.NewInternalErrorNoCtx("materialized view retriever does not expose from boundary")
	}
	var insertRows, deleteRows int
	err = runTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID,
		r.GetAccountID(), 24*time.Hour, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, _ any) error {
			sqlctx := sqlproc.SqlCtx
			refreshCtx := context.WithValue(sqlproc.GetContext(), defines.MaterializedViewRefreshKey{}, true)
			db, err := c.cnEngine.Database(refreshCtx, c.jobID.DBName, sqlctx.Txn())
			if err != nil {
				return err
			}
			rel, err := db.Relation(refreshCtx, c.jobID.TableName, nil)
			if err != nil {
				return err
			}
			reader, ok := rel.(engine.RowIDReader)
			if !ok {
				return moerr.NewInternalErrorNoCtx("source relation does not support rowid lookup")
			}
			tableDef := rel.GetTableDef(refreshCtx)
			if tableDef == nil {
				return moerr.NewInternalErrorNoCtx("source relation has no table definition")
			}
			sourceTypes, err := materializedViewSourceColumnTypes(tableDef, desc.SourceColumns)
			if err != nil {
				return err
			}
			if err = resetMaterializedViewAffectedGroups(refreshCtx, sqlctx.GetService(), sqlctx.Txn(), c.info, desc); err != nil {
				return err
			}

			for {
				data := r.Next()
				if data == nil {
					break
				}
				if data.err != nil {
					drained = data.noMoreData
					data.Done()
					return data.err
				}
				inserts, decodeErr := materializedViewRowsFromBatch(data.insertBatch, true)
				if decodeErr != nil {
					data.Done()
					return decodeErr
				}
				deletes, decodeErr := materializedViewRowsFromBatch(data.deleteBatch, false)
				if decodeErr != nil {
					data.Done()
					return decodeErr
				}
				rows := make([]materializedViewSignedRow, 0, len(inserts)+len(deletes))
				insertRows += len(inserts)
				deleteRows += len(deletes)
				for _, row := range inserts {
					rows = append(rows, materializedViewSignedRow{values: row.Values, sign: 1})
				}
				if len(deletes) > 0 {
					oldRows, readErr := readMaterializedViewDeletedRows(
						refreshCtx, reader, deletes, from.GetFromTS(), desc.SourceColumns,
					)
					if readErr != nil {
						data.Done()
						return readErr
					}
					if len(oldRows) != len(deletes) {
						data.Done()
						return moerr.NewInternalErrorNoCtxf("rowid lookup returned %d rows for %d deletes", len(oldRows), len(deletes))
					}
					for i := range oldRows {
						values := make(map[string]any, len(desc.SourceColumns))
						for j, column := range desc.SourceColumns {
							values[strings.ToLower(column)] = oldRows[i][j]
						}
						rows = append(rows, materializedViewSignedRow{values: values, sign: -1})
					}
				}
				for start := 0; start < len(rows); start += materializedViewDeltaBatchRows {
					end := min(start+materializedViewDeltaBatchRows, len(rows))
					chunk := rows[start:end]
					if err := applyMaterializedViewDeltaRows(refreshCtx, sqlctx.GetService(), sqlctx.Txn(), c.info, desc, sourceTypes, chunk); err != nil {
						data.Done()
						return err
					}
					if err := applyMaterializedViewDistinctDeltas(refreshCtx, sqlctx.GetService(), sqlctx.Txn(), c.info, desc, sourceTypes, chunk); err != nil {
						data.Done()
						return err
					}
					if err := recordMaterializedViewAffectedGroups(refreshCtx, sqlctx.GetService(), sqlctx.Txn(), c.info, desc, sourceTypes, chunk); err != nil {
						data.Done()
						return err
					}
				}
				done := data.noMoreData
				data.Done()
				if done {
					drained = true
					break
				}
			}
			boundary, ok := r.(iterationBoundaryRetriever)
			if !ok {
				return moerr.NewInternalErrorNoCtx("materialized view retriever does not expose iteration boundary")
			}
			if err = recomputeMaterializedViewAffectedGroups(
				refreshCtx, sqlctx.GetService(), sqlctx.Txn(), c.info, desc, boundary.GetToTS(),
			); err != nil {
				return err
			}
			return r.UpdateWatermark(refreshCtx, sqlctx.GetService(), sqlctx.Txn())
		})
	if err == nil {
		metricv2.ISCPMaterializedViewRows.WithLabelValues("insert").Add(float64(insertRows))
		metricv2.ISCPMaterializedViewRows.WithLabelValues("delete").Add(float64(deleteRows))
	}
	return drained, err
}

// readMaterializedViewDeletedRows reads each row immediately before its own
// tombstone commit. Reading every delete at the iteration's fromTS is invalid
// when a row was inserted (or updated more than once) inside the same tail
// interval, because that row did not exist at fromTS yet.
func readMaterializedViewDeletedRows(
	ctx context.Context,
	reader engine.RowIDReader,
	deletes []materializedViewChangeRow,
	from types.TS,
	columns []string,
) ([][]any, error) {
	type snapshotGroup struct {
		indices []int
		rowids  []types.Rowid
	}
	groups := make(map[types.TS]*snapshotGroup)
	order := make([]types.TS, 0)
	for i := range deletes {
		snapshot := from
		if !deletes[i].CommitTS.IsEmpty() {
			snapshot = deletes[i].CommitTS.Prev()
		}
		group := groups[snapshot]
		if group == nil {
			group = &snapshotGroup{}
			groups[snapshot] = group
			order = append(order, snapshot)
		}
		group.indices = append(group.indices, i)
		group.rowids = append(group.rowids, deletes[i].RowID)
	}
	result := make([][]any, len(deletes))
	for _, snapshot := range order {
		group := groups[snapshot]
		rows, err := reader.ReadRowsByRowID(ctx, group.rowids, snapshot, columns, nil)
		if err != nil {
			return nil, err
		}
		if len(rows) != len(group.indices) {
			return nil, moerr.NewInternalErrorNoCtxf("rowid lookup returned %d rows for %d deletes", len(rows), len(group.indices))
		}
		for i := range rows {
			result[group.indices[i]] = rows[i]
		}
	}
	return result, nil
}

func materializedViewSourceColumnTypes(tableDef *planpb.TableDef, columns []string) ([]*types.Type, error) {
	byName := make(map[string]*types.Type, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		if col.Hidden || strings.EqualFold(col.Name, catalog.Row_ID) {
			continue
		}
		typ := &types.Type{Oid: types.T(col.Typ.Id), Width: col.Typ.Width, Scale: col.Typ.Scale}
		// Delta rows carry the already-stored temporal value. Preserve its full
		// internal microsecond precision in the generated VALUES CTE even when a
		// relation or change-batch descriptor reports a lower scale. Casting a
		// value such as 12:34:59.9 to DATETIME(0) rounds it into the next minute
		// and makes date_trunc grouping diverge from a scan of the source table.
		// Values from genuinely lower-scale columns have already been rounded at
		// source-table write time, so widening this transport type is lossless.
		switch typ.Oid {
		case types.T_time, types.T_datetime, types.T_timestamp:
			typ.Scale = 6
		}
		byName[strings.ToLower(col.Name)] = typ
	}
	result := make([]*types.Type, len(columns))
	for i, column := range columns {
		result[i] = byName[strings.ToLower(column)]
		if result[i] == nil {
			return nil, moerr.NewInternalErrorNoCtxf("incremental expression references unknown column %q", column)
		}
	}
	return result, nil
}

func applyMaterializedViewDeltaBatch(
	ctx context.Context,
	service string,
	txn client.TxnOperator,
	info *ConsumerInfo,
	desc *incrementalDescription,
	sourceTypes []*types.Type,
	rows []materializedViewSignedRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	needsDeleteCleanup := false
	for _, row := range rows {
		if row.sign < 0 {
			needsDeleteCleanup = true
			break
		}
	}
	target := sqlquote.QualifiedIdent(info.DBName, info.TableName)
	if materializedViewDeltaCanUpsert(desc) {
		if !needsDeleteCleanup {
			return execMaterializedViewDeltaUpsert(ctx, service, txn, info, desc, sourceTypes, rows)
		}
		negative := make([]materializedViewSignedRow, 0, len(rows))
		positive := make([]materializedViewSignedRow, 0, len(rows))
		for _, row := range rows {
			if row.sign < 0 {
				negative = append(negative, row)
			} else {
				positive = append(positive, row)
			}
		}
		// Validate both statements before executing either one so the adaptive
		// SQL-size split below never retries after a partial negative delta.
		for _, partition := range [][]materializedViewSignedRow{negative, positive} {
			if len(partition) == 0 {
				continue
			}
			if _, err := materializedViewDeltaCTE(ctx, desc, sourceTypes, partition); err != nil {
				return err
			}
		}
		if err := execMaterializedViewDeltaUpsert(ctx, service, txn, info, desc, sourceTypes, negative); err != nil {
			return err
		}
		if err := execMaterializedViewDeltaAndClose(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s <= 0", target, sqlquote.Ident(desc.RowCountColumn)), service, txn); err != nil {
			return err
		}
		return execMaterializedViewDeltaUpsert(ctx, service, txn, info, desc, sourceTypes, positive)
	}
	cte, err := materializedViewDeltaCTE(ctx, desc, sourceTypes, rows)
	if err != nil {
		return err
	}
	columns, values := materializedViewDeltaInsertProjection(desc, "d")
	join := materializedViewDeltaJoin(desc, "t", "d")
	sets := materializedViewDeltaUpdateSets(desc, "t", "d")
	if err := execMaterializedViewDeltaAndClose(ctx, fmt.Sprintf("%s UPDATE %s AS t JOIN delta AS d ON %s SET %s", cte, target, join, strings.Join(sets, ",")), service, txn); err != nil {
		return err
	}
	missingColumn := catalog.FakePrimaryKeyColName
	if desc.GroupKeyColumn != "" {
		missingColumn = desc.GroupKeyColumn
	}
	insert := fmt.Sprintf("%s INSERT INTO %s (%s) SELECT %s FROM delta AS d LEFT JOIN %s AS t ON %s WHERE t.%s IS NULL AND d.__mo_row_delta > 0",
		cte, target, strings.Join(columns, ","), strings.Join(values, ","), target, join, sqlquote.Ident(missingColumn))
	if err := execMaterializedViewDeltaAndClose(ctx, insert, service, txn); err != nil {
		return err
	}
	if !needsDeleteCleanup {
		return nil
	}
	return execMaterializedViewDeltaAndClose(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s <= 0", target, sqlquote.Ident(desc.RowCountColumn)), service, txn)
}

func execMaterializedViewDeltaUpsert(
	ctx context.Context,
	service string,
	txn client.TxnOperator,
	info *ConsumerInfo,
	desc *incrementalDescription,
	sourceTypes []*types.Type,
	rows []materializedViewSignedRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	cte, err := materializedViewDeltaCTE(ctx, desc, sourceTypes, rows)
	if err != nil {
		return err
	}
	columns, values := materializedViewDeltaInsertProjection(desc, "d")
	upsert := fmt.Sprintf("%s INSERT INTO %s (%s) SELECT %s FROM delta AS d ON DUPLICATE KEY UPDATE %s",
		cte, sqlquote.QualifiedIdent(info.DBName, info.TableName), strings.Join(columns, ","), strings.Join(values, ","), strings.Join(materializedViewDeltaUpsertSets(desc), ","))
	return execMaterializedViewDeltaAndClose(ctx, upsert, service, txn)
}

func materializedViewDeltaCanUpsert(desc *incrementalDescription) bool {
	return desc != nil && len(desc.Groups) > 0 && desc.GroupKeyColumn != ""
}

func materializedViewDeltaUpsertSets(desc *incrementalDescription) []string {
	sets := make([]string, 0, len(desc.Aggregates)*3+1)
	value := func(column string) string {
		quoted := sqlquote.Ident(column)
		return "VALUES(" + quoted + ")"
	}
	for _, agg := range desc.Aggregates {
		out := sqlquote.Ident(agg.OutputColumn)
		switch agg.Kind {
		case "count_star", "count_column":
			sets = append(sets, fmt.Sprintf("%s = %s + %s", out, out, value(agg.OutputColumn)))
		case "sum":
			stateSum := sqlquote.Ident(agg.StateSumColumn)
			stateCount := sqlquote.Ident(agg.StateCountColumn)
			sets = append(sets,
				fmt.Sprintf("%s = CASE WHEN %s + %s = 0 THEN NULL ELSE coalesce(%s,0) + coalesce(%s,0) END", out, stateCount, value(agg.StateCountColumn), stateSum, value(agg.StateSumColumn)),
				fmt.Sprintf("%s = coalesce(%s,0) + coalesce(%s,0)", stateSum, stateSum, value(agg.StateSumColumn)),
				fmt.Sprintf("%s = %s + %s", stateCount, stateCount, value(agg.StateCountColumn)))
		case "avg":
			stateSum := sqlquote.Ident(agg.StateSumColumn)
			stateCount := sqlquote.Ident(agg.StateCountColumn)
			sets = append(sets,
				fmt.Sprintf("%s = CASE WHEN %s + %s = 0 THEN NULL ELSE (coalesce(%s,0) + coalesce(%s,0)) / (%s + %s) END", out, stateCount, value(agg.StateCountColumn), stateSum, value(agg.StateSumColumn), stateCount, value(agg.StateCountColumn)),
				fmt.Sprintf("%s = coalesce(%s,0) + coalesce(%s,0)", stateSum, stateSum, value(agg.StateSumColumn)),
				fmt.Sprintf("%s = %s + %s", stateCount, stateCount, value(agg.StateCountColumn)))
		case "min":
			sets = append(sets, fmt.Sprintf("%s = CASE WHEN %s IS NULL THEN %s WHEN %s IS NULL THEN %s ELSE least(%s,%s) END",
				out, out, value(agg.OutputColumn), value(agg.OutputColumn), out, out, value(agg.OutputColumn)))
		case "max":
			sets = append(sets, fmt.Sprintf("%s = CASE WHEN %s IS NULL THEN %s WHEN %s IS NULL THEN %s ELSE greatest(%s,%s) END",
				out, out, value(agg.OutputColumn), value(agg.OutputColumn), out, out, value(agg.OutputColumn)))
		case "count_distinct":
			// Exact distinct transitions are applied against the auxiliary state
			// after the ordinary row-count delta has materialized missing groups.
		}
	}
	rowCount := sqlquote.Ident(desc.RowCountColumn)
	sets = append(sets, fmt.Sprintf("%s = %s + %s", rowCount, rowCount, value(desc.RowCountColumn)))
	return sets
}

// applyMaterializedViewDeltaRows preserves the configured row batch for the
// common case while splitting unusually wide rows before any DML is executed.
func applyMaterializedViewDeltaRows(
	ctx context.Context,
	service string,
	txn client.TxnOperator,
	info *ConsumerInfo,
	desc *incrementalDescription,
	sourceTypes []*types.Type,
	rows []materializedViewSignedRow,
) error {
	err := applyMaterializedViewDeltaBatch(ctx, service, txn, info, desc, sourceTypes, rows)
	if !errors.Is(err, errMaterializedViewDeltaSQLTooLarge) || len(rows) <= 1 {
		return err
	}
	middle := len(rows) / 2
	if err = applyMaterializedViewDeltaRows(ctx, service, txn, info, desc, sourceTypes, rows[:middle]); err != nil {
		return err
	}
	return applyMaterializedViewDeltaRows(ctx, service, txn, info, desc, sourceTypes, rows[middle:])
}

func materializedViewHasDistinctState(desc *incrementalDescription) bool {
	if desc == nil || desc.StateTable == "" {
		return false
	}
	for _, agg := range desc.Aggregates {
		if agg.Kind == "count_distinct" {
			return true
		}
	}
	return false
}

func materializedViewNeedsAffectedGroups(desc *incrementalDescription) bool {
	if desc == nil || desc.StateTable == "" {
		return false
	}
	for _, agg := range desc.Aggregates {
		if agg.Kind == "min" || agg.Kind == "max" || agg.Kind == "count_distinct" {
			return true
		}
	}
	return false
}

func materializedViewHasAuxiliaryState(desc *incrementalDescription) bool {
	return materializedViewHasDistinctState(desc) || materializedViewNeedsAffectedGroups(desc)
}

func ensureMaterializedViewStateTable(
	ctx context.Context,
	service string,
	txn client.TxnOperator,
	info *ConsumerInfo,
	desc *incrementalDescription,
) error {
	if !materializedViewHasAuxiliaryState(desc) {
		return nil
	}
	sql := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (aggregate_index INT NOT NULL, group_key VARBINARY(65535) NOT NULL, value_key VARBINARY(65535) NOT NULL, ref_count BIGINT NOT NULL, PRIMARY KEY (aggregate_index, group_key, value_key)) COMMENT = 'matrixone materialized view state'",
		sqlquote.QualifiedIdent(info.DBName, desc.StateTable),
	)
	return execMaterializedViewDeltaAndClose(ctx, sql, service, txn)
}

func resetMaterializedViewAffectedGroups(
	ctx context.Context,
	service string,
	txn client.TxnOperator,
	info *ConsumerInfo,
	desc *incrementalDescription,
) error {
	if !materializedViewNeedsAffectedGroups(desc) {
		return nil
	}
	return execMaterializedViewDeltaAndClose(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE aggregate_index = 0", sqlquote.QualifiedIdent(info.DBName, desc.StateTable)),
		service, txn)
}

func recordMaterializedViewAffectedGroups(
	ctx context.Context,
	service string,
	txn client.TxnOperator,
	info *ConsumerInfo,
	desc *incrementalDescription,
	sourceTypes []*types.Type,
	rows []materializedViewSignedRow,
) error {
	if !materializedViewNeedsAffectedGroups(desc) || len(rows) == 0 {
		return nil
	}
	hasDelete := false
	for _, row := range rows {
		if row.sign < 0 {
			hasDelete = true
			break
		}
	}
	if !hasDelete {
		return nil
	}
	sourceCTE, err := materializedViewDeltaSourceCTE(ctx, desc, sourceTypes, rows)
	if err != nil {
		return err
	}
	groups := make([]string, len(desc.Groups))
	for i := range desc.Groups {
		groups[i] = desc.Groups[i].Expression
	}
	where := "__mo_sign < 0"
	if desc.Filter != "" {
		where += " AND (" + desc.Filter + ")"
	}
	state := sqlquote.QualifiedIdent(info.DBName, desc.StateTable)
	sql := fmt.Sprintf(
		"WITH %s INSERT INTO %s (aggregate_index,group_key,value_key,ref_count) SELECT 0,CAST(serial_full(%s) AS VARBINARY(65535)),CAST('' AS VARBINARY(65535)),1 FROM src AS %s WHERE %s GROUP BY %s ON DUPLICATE KEY UPDATE ref_count = ref_count",
		sourceCTE, state, strings.Join(groups, ","), sqlquote.Ident(desc.SourceAlias), where, strings.Join(groups, ","))
	return execMaterializedViewDeltaAndClose(ctx, sql, service, txn)
}

func recomputeMaterializedViewAffectedGroups(
	ctx context.Context,
	service string,
	txn client.TxnOperator,
	info *ConsumerInfo,
	desc *incrementalDescription,
	boundary types.TS,
) error {
	if !materializedViewNeedsAffectedGroups(desc) {
		return nil
	}
	state := sqlquote.QualifiedIdent(info.DBName, desc.StateTable)
	target := sqlquote.QualifiedIdent(info.DBName, info.TableName)
	if err := execMaterializedViewDeltaAndClose(ctx,
		fmt.Sprintf("DELETE t FROM %s AS t JOIN %s AS s ON t.%s = s.group_key WHERE s.aggregate_index = 0",
			target, state, sqlquote.Ident(desc.GroupKeyColumn)), service, txn); err != nil {
		return err
	}
	refreshSQL, err := materializedViewRefreshAtSources(info.RefreshSQL, info.SourceTableInfos(), boundary)
	if err != nil {
		return err
	}
	targetColumns := append(append([]string(nil), info.Columns...), desc.StateColumns...)
	quoted := make([]string, len(targetColumns))
	selected := make([]string, len(targetColumns))
	for i := range targetColumns {
		quoted[i] = sqlquote.Ident(targetColumns[i])
		selected[i] = "r." + quoted[i]
	}
	insert := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM (%s) AS r JOIN %s AS s ON r.%s = s.group_key WHERE s.aggregate_index = 0",
		target, strings.Join(quoted, ","), strings.Join(selected, ","), refreshSQL, state, sqlquote.Ident(desc.GroupKeyColumn))
	if err = execMaterializedViewDeltaAndClose(ctx, insert, service, txn); err != nil {
		return err
	}
	return resetMaterializedViewAffectedGroups(ctx, service, txn, info, desc)
}

func materializedViewDistinctDeltaCTE(
	ctx context.Context,
	desc *incrementalDescription,
	agg incrementalAggregate,
	sourceTypes []*types.Type,
	rows []materializedViewSignedRow,
) (string, error) {
	sourceCTE, err := materializedViewDeltaSourceCTE(ctx, desc, sourceTypes, rows)
	if err != nil {
		return "", err
	}
	groups := make([]string, len(desc.Groups))
	for i := range desc.Groups {
		groups[i] = desc.Groups[i].Expression
	}
	where := fmt.Sprintf("(%s) IS NOT NULL", agg.InputExpression)
	if desc.Filter != "" {
		where = "(" + desc.Filter + ") AND " + where
	}
	cte := fmt.Sprintf(
		"WITH %s, distinct_delta AS (SELECT %d AS aggregate_index, CAST(serial_full(%s) AS VARBINARY(65535)) AS group_key, CAST(serial_full(%s) AS VARBINARY(65535)) AS value_key, sum(__mo_sign) AS ref_delta FROM src AS %s WHERE %s GROUP BY %s,%s)",
		sourceCTE,
		agg.StateIndex,
		strings.Join(groups, ","),
		agg.InputExpression,
		sqlquote.Ident(desc.SourceAlias),
		where,
		strings.Join(groups, ","),
		agg.InputExpression,
	)
	if len(cte) > materializedViewDeltaMaxSQL {
		return "", errMaterializedViewDeltaSQLTooLarge
	}
	return cte, nil
}

func applyMaterializedViewDistinctDeltas(
	ctx context.Context,
	service string,
	txn client.TxnOperator,
	info *ConsumerInfo,
	desc *incrementalDescription,
	sourceTypes []*types.Type,
	rows []materializedViewSignedRow,
) error {
	if !materializedViewHasDistinctState(desc) || len(rows) == 0 {
		return nil
	}
	state := sqlquote.QualifiedIdent(info.DBName, desc.StateTable)
	target := sqlquote.QualifiedIdent(info.DBName, info.TableName)
	for _, agg := range desc.Aggregates {
		if agg.Kind != "count_distinct" {
			continue
		}
		cte, err := materializedViewDistinctDeltaCTE(ctx, desc, agg, sourceTypes, rows)
		if err != nil {
			return err
		}
		for _, sql := range materializedViewDistinctDeltaStatements(desc, agg, cte, state, target) {
			if err = execMaterializedViewDeltaAndClose(ctx, sql, service, txn); err != nil {
				return err
			}
		}
	}
	return nil
}

func materializedViewDistinctDeltaStatements(
	desc *incrementalDescription,
	agg incrementalAggregate,
	cte, state, target string,
) []string {
	return []string{
		fmt.Sprintf(
			"%s, visible_delta AS (SELECT d.group_key, sum(CASE WHEN coalesce(s.ref_count,0) = 0 AND d.ref_delta > 0 THEN 1 WHEN coalesce(s.ref_count,0) > 0 AND coalesce(s.ref_count,0) + d.ref_delta <= 0 THEN -1 ELSE 0 END) AS value_delta FROM distinct_delta AS d LEFT JOIN %s AS s ON s.aggregate_index = d.aggregate_index AND s.group_key = d.group_key AND s.value_key = d.value_key GROUP BY d.group_key) UPDATE %s AS t JOIN visible_delta AS d ON t.%s = d.group_key SET t.%s = t.%s + d.value_delta",
			cte, state, target, sqlquote.Ident(desc.GroupKeyColumn), sqlquote.Ident(agg.OutputColumn), sqlquote.Ident(agg.OutputColumn)),
		fmt.Sprintf(
			"%s UPDATE %s AS s JOIN distinct_delta AS d ON s.aggregate_index = d.aggregate_index AND s.group_key = d.group_key AND s.value_key = d.value_key SET s.ref_count = s.ref_count + d.ref_delta",
			cte, state),
		fmt.Sprintf(
			"%s INSERT INTO %s (aggregate_index,group_key,value_key,ref_count) SELECT d.aggregate_index,d.group_key,d.value_key,d.ref_delta FROM distinct_delta AS d LEFT JOIN %s AS s ON s.aggregate_index = d.aggregate_index AND s.group_key = d.group_key AND s.value_key = d.value_key WHERE s.aggregate_index IS NULL AND d.ref_delta > 0",
			cte, state, state),
		fmt.Sprintf("DELETE FROM %s WHERE aggregate_index = %d AND ref_count <= 0", state, agg.StateIndex),
	}
}

func rebuildMaterializedViewDistinctState(
	ctx context.Context,
	service string,
	txn client.TxnOperator,
	info *ConsumerInfo,
	desc *incrementalDescription,
	boundary types.TS,
) error {
	if !materializedViewHasDistinctState(desc) {
		return nil
	}
	state := sqlquote.QualifiedIdent(info.DBName, desc.StateTable)
	if err := execMaterializedViewDeltaAndClose(ctx, "DELETE FROM "+state+" WHERE aggregate_index > 0", service, txn); err != nil {
		return err
	}
	groups := make([]string, len(desc.Groups))
	for i := range desc.Groups {
		groups[i] = desc.Groups[i].Expression
	}
	for _, agg := range desc.Aggregates {
		if agg.Kind != "count_distinct" {
			continue
		}
		where := fmt.Sprintf("(%s) IS NOT NULL", agg.InputExpression)
		if desc.Filter != "" {
			where = "(" + desc.Filter + ") AND " + where
		}
		query := fmt.Sprintf(
			"SELECT %d, CAST(serial_full(%s) AS VARBINARY(65535)), CAST(serial_full(%s) AS VARBINARY(65535)), count(*) FROM %s AS %s WHERE %s GROUP BY %s,%s",
			agg.StateIndex,
			strings.Join(groups, ","),
			agg.InputExpression,
			info.SourceSQL,
			sqlquote.Ident(desc.SourceAlias),
			where,
			strings.Join(groups, ","),
			agg.InputExpression,
		)
		atBoundary, err := materializedViewRefreshAtSources(query, info.SourceTableInfos(), boundary)
		if err != nil {
			return err
		}
		insert := fmt.Sprintf("INSERT INTO %s (aggregate_index,group_key,value_key,ref_count) %s", state, atBoundary)
		if err = execMaterializedViewDeltaAndClose(ctx, insert, service, txn); err != nil {
			return err
		}
	}
	return nil
}

func execMaterializedViewDeltaAndClose(
	ctx context.Context,
	sql string,
	service string,
	txn client.TxnOperator,
) error {
	res, err := execMaterializedViewDeltaSQL(ctx, sql, service, txn)
	if err == nil {
		res.Close()
	}
	return err
}

func materializedViewDeltaCTE(
	ctx context.Context,
	desc *incrementalDescription,
	sourceTypes []*types.Type,
	rows []materializedViewSignedRow,
) (string, error) {
	sourceCTE, err := materializedViewDeltaSourceCTE(ctx, desc, sourceTypes, rows)
	if err != nil {
		return "", err
	}

	projection := make([]string, 0, len(desc.Groups)+len(desc.Aggregates)*2+1)
	groupBy := make([]string, 0, len(desc.Groups))
	for i, group := range desc.Groups {
		projection = append(projection, fmt.Sprintf("%s AS %s", group.Expression, materializedViewDeltaGroupAlias(i)))
		groupBy = append(groupBy, group.Expression)
	}
	projection = append(projection, "sum(__mo_sign) AS __mo_row_delta")
	for i, agg := range desc.Aggregates {
		countAlias := materializedViewDeltaCountAlias(i)
		sumAlias := materializedViewDeltaSumAlias(i)
		switch agg.Kind {
		case "count_star":
			projection = append(projection, fmt.Sprintf("sum(__mo_sign) AS %s", countAlias))
		case "count_column":
			projection = append(projection, fmt.Sprintf("sum(CASE WHEN (%s) IS NULL THEN 0 ELSE __mo_sign END) AS %s", agg.InputExpression, countAlias))
		case "sum":
			projection = append(projection,
				fmt.Sprintf("sum(CASE WHEN (%s) IS NULL THEN 0 ELSE __mo_sign * (%s) END) AS %s", agg.InputExpression, agg.InputExpression, sumAlias),
				fmt.Sprintf("sum(CASE WHEN (%s) IS NULL THEN 0 ELSE __mo_sign END) AS %s", agg.InputExpression, countAlias))
		case "avg":
			projection = append(projection,
				fmt.Sprintf("sum(CASE WHEN (%s) IS NULL THEN 0 ELSE __mo_sign * (%s) END) AS %s", agg.InputExpression, agg.InputExpression, sumAlias),
				fmt.Sprintf("sum(CASE WHEN (%s) IS NULL THEN 0 ELSE __mo_sign END) AS %s", agg.InputExpression, countAlias))
		case "min":
			projection = append(projection, fmt.Sprintf("min(CASE WHEN __mo_sign > 0 THEN (%s) ELSE NULL END) AS %s", agg.InputExpression, sumAlias))
		case "max":
			projection = append(projection, fmt.Sprintf("max(CASE WHEN __mo_sign > 0 THEN (%s) ELSE NULL END) AS %s", agg.InputExpression, sumAlias))
		case "count_distinct":
			// Its value delta depends on the persisted old refcount, so it is
			// computed by applyMaterializedViewDistinctDeltas.
		}
	}
	where := ""
	if desc.Filter != "" {
		where = " WHERE (" + desc.Filter + ")"
	}
	cte := fmt.Sprintf("WITH %s, delta AS (SELECT %s FROM src AS %s%s GROUP BY %s)",
		sourceCTE, strings.Join(projection, ","), sqlquote.Ident(desc.SourceAlias), where, strings.Join(groupBy, ","))
	if len(cte) > materializedViewDeltaMaxSQL {
		return "", errMaterializedViewDeltaSQLTooLarge
	}
	return cte, nil
}

func materializedViewDeltaSourceCTE(
	ctx context.Context,
	desc *incrementalDescription,
	sourceTypes []*types.Type,
	rows []materializedViewSignedRow,
) (string, error) {
	values := make([]byte, 0, len(rows)*128)
	for i, row := range rows {
		if i > 0 {
			values = append(values, ',')
		}
		values = append(values, "ROW("...)
		for colIdx, column := range desc.SourceColumns {
			if colIdx > 0 {
				values = append(values, ',')
			}
			value, exists := row.values[strings.ToLower(column)]
			if !exists {
				return "", moerr.NewInternalErrorNoCtxf("incremental row is missing column %q", column)
			}
			// VALUES determines a common column type before the outer SELECT is
			// evaluated. In particular, mixing an integral-looking float literal
			// with a typed NULL can corrupt the inferred value. Cast every cell at
			// the VALUES boundary so delta arithmetic sees the source type.
			values = append(values, "CAST("...)
			var err error
			values, err = convertColIntoSql(ctx, value, sourceTypes[colIdx], values)
			if err != nil {
				return "", err
			}
			values = append(values, " AS "...)
			values = append(values, materializedViewDeltaSQLType(sourceTypes[colIdx])...)
			values = append(values, ')')
		}
		values = append(values, ',')
		values = append(values, "CAST("...)
		values = append(values, fmt.Sprint(row.sign)...)
		values = append(values, " AS BIGINT)"...)
		values = append(values, ')')
	}
	columns := make([]string, 0, len(desc.SourceColumns)+1)
	for i, column := range desc.SourceColumns {
		columns = append(columns, fmt.Sprintf("CAST(column_%d AS %s) AS %s", i, materializedViewDeltaSQLType(sourceTypes[i]), sqlquote.Ident(column)))
	}
	columns = append(columns, fmt.Sprintf("CAST(column_%d AS BIGINT) AS __mo_sign", len(desc.SourceColumns)))

	cte := fmt.Sprintf("src AS (SELECT %s FROM (VALUES %s) AS __mo_mv_values)", strings.Join(columns, ","), string(values))
	if len(cte) > materializedViewDeltaMaxSQL {
		return "", errMaterializedViewDeltaSQLTooLarge
	}
	return cte, nil
}

func materializedViewDeltaSQLType(typ *types.Type) string {
	switch typ.Oid {
	case types.T_time, types.T_datetime, types.T_timestamp:
		return fmt.Sprintf("%s(%d)", typ.Oid.String(), typ.Scale)
	default:
		return typ.DescString()
	}
}

func materializedViewDeltaGroupAlias(i int) string { return fmt.Sprintf("__mo_g_%d", i) }
func materializedViewDeltaCountAlias(i int) string { return fmt.Sprintf("__mo_a_%d_count", i) }
func materializedViewDeltaSumAlias(i int) string   { return fmt.Sprintf("__mo_a_%d_sum", i) }

func materializedViewDeltaJoin(desc *incrementalDescription, targetAlias, deltaAlias string) string {
	predicates := make([]string, len(desc.Groups))
	for i, group := range desc.Groups {
		target := targetAlias + "." + sqlquote.Ident(group.OutputColumn)
		delta := deltaAlias + "." + materializedViewDeltaGroupAlias(i)
		if group.NotNullable {
			// Keep this as a plain equality so the optimizer can use a hash join.
			// Null-safe equality is only required for nullable GROUP BY keys and
			// can prevent the plain-equality hash-join optimization.
			predicates[i] = target + " = " + delta
		} else {
			predicates[i] = target + " <=> " + delta
		}
	}
	return strings.Join(predicates, " AND ")
}

func materializedViewDeltaUpdateSets(desc *incrementalDescription, targetAlias, deltaAlias string) []string {
	sets := make([]string, 0, len(desc.Aggregates)*3+1)
	for i, agg := range desc.Aggregates {
		out := targetAlias + "." + sqlquote.Ident(agg.OutputColumn)
		countDelta := deltaAlias + "." + materializedViewDeltaCountAlias(i)
		sumDelta := deltaAlias + "." + materializedViewDeltaSumAlias(i)
		switch agg.Kind {
		case "count_star", "count_column":
			sets = append(sets, fmt.Sprintf("%s = %s + %s", out, out, countDelta))
		case "sum":
			stateCount := targetAlias + "." + sqlquote.Ident(agg.StateCountColumn)
			if agg.StateSumColumn == "" {
				sets = append(sets,
					fmt.Sprintf("%s = CASE WHEN %s + %s = 0 THEN NULL ELSE coalesce(%s,0) + coalesce(%s,0) END", out, stateCount, countDelta, out, sumDelta),
					fmt.Sprintf("%s = %s + %s", stateCount, stateCount, countDelta))
			} else {
				stateSum := targetAlias + "." + sqlquote.Ident(agg.StateSumColumn)
				sets = append(sets,
					fmt.Sprintf("%s = CASE WHEN %s + %s = 0 THEN NULL ELSE coalesce(%s,0) + coalesce(%s,0) END", out, stateCount, countDelta, stateSum, sumDelta),
					fmt.Sprintf("%s = coalesce(%s,0) + coalesce(%s,0)", stateSum, stateSum, sumDelta),
					fmt.Sprintf("%s = %s + %s", stateCount, stateCount, countDelta))
			}
		case "avg":
			stateSum := targetAlias + "." + sqlquote.Ident(agg.StateSumColumn)
			stateCount := targetAlias + "." + sqlquote.Ident(agg.StateCountColumn)
			sets = append(sets,
				fmt.Sprintf("%s = CASE WHEN %s + %s = 0 THEN NULL ELSE (coalesce(%s,0) + coalesce(%s,0)) / (%s + %s) END", out, stateCount, countDelta, stateSum, sumDelta, stateCount, countDelta),
				fmt.Sprintf("%s = coalesce(%s,0) + coalesce(%s,0)", stateSum, stateSum, sumDelta),
				fmt.Sprintf("%s = %s + %s", stateCount, stateCount, countDelta))
		case "min":
			sets = append(sets, fmt.Sprintf("%s = CASE WHEN %s IS NULL THEN %s WHEN %s IS NULL THEN %s ELSE least(%s,%s) END",
				out, out, sumDelta, sumDelta, out, out, sumDelta))
		case "max":
			sets = append(sets, fmt.Sprintf("%s = CASE WHEN %s IS NULL THEN %s WHEN %s IS NULL THEN %s ELSE greatest(%s,%s) END",
				out, out, sumDelta, sumDelta, out, out, sumDelta))
		case "count_distinct":
		}
	}
	rowCount := targetAlias + "." + sqlquote.Ident(desc.RowCountColumn)
	sets = append(sets, fmt.Sprintf("%s = %s + %s.__mo_row_delta", rowCount, rowCount, deltaAlias))
	return sets
}

func materializedViewDeltaInsertProjection(desc *incrementalDescription, deltaAlias string) ([]string, []string) {
	columns := make([]string, 0, len(desc.Groups)+len(desc.Aggregates)*3+1)
	values := make([]string, 0, cap(columns))
	for i, group := range desc.Groups {
		columns = append(columns, sqlquote.Ident(group.OutputColumn))
		values = append(values, deltaAlias+"."+materializedViewDeltaGroupAlias(i))
	}
	if desc.GroupKeyColumn != "" {
		groupArgs := make([]string, len(desc.Groups))
		for i := range desc.Groups {
			groupArgs[i] = deltaAlias + "." + materializedViewDeltaGroupAlias(i)
		}
		columns = append(columns, sqlquote.Ident(desc.GroupKeyColumn))
		values = append(values, "serial_full("+strings.Join(groupArgs, ",")+")")
	}
	for i, agg := range desc.Aggregates {
		countDelta := deltaAlias + "." + materializedViewDeltaCountAlias(i)
		sumDelta := deltaAlias + "." + materializedViewDeltaSumAlias(i)
		columns = append(columns, sqlquote.Ident(agg.OutputColumn))
		switch agg.Kind {
		case "count_star", "count_column":
			values = append(values, countDelta)
		case "sum":
			values = append(values, fmt.Sprintf("CASE WHEN %s = 0 THEN NULL ELSE %s END", countDelta, sumDelta))
			if agg.StateSumColumn != "" {
				columns = append(columns, sqlquote.Ident(agg.StateSumColumn))
				values = append(values, sumDelta)
			}
			columns = append(columns, sqlquote.Ident(agg.StateCountColumn))
			values = append(values, countDelta)
		case "avg":
			values = append(values, fmt.Sprintf("CASE WHEN %s = 0 THEN NULL ELSE %s / %s END", countDelta, sumDelta, countDelta))
			columns = append(columns, sqlquote.Ident(agg.StateSumColumn), sqlquote.Ident(agg.StateCountColumn))
			values = append(values, sumDelta, countDelta)
		case "min", "max":
			values = append(values, sumDelta)
		case "count_distinct":
			values = append(values, "CAST(0 AS BIGINT)")
		}
	}
	columns = append(columns, sqlquote.Ident(desc.RowCountColumn))
	values = append(values, deltaAlias+".__mo_row_delta")
	return columns, values
}
