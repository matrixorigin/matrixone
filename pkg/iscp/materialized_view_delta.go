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
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
	materializedViewDeltaBatchRows = 512
	materializedViewDeltaMaxSQL    = 8 << 20
)

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
		return nil, fmt.Errorf("invalid materialized view incremental specification encoding: %w", err)
	}
	var desc incrementalDescription
	if err := json.Unmarshal(b, &desc); err != nil {
		return nil, fmt.Errorf("invalid materialized view incremental specification: %w", err)
	}
	if desc.SourceAlias == "" || len(desc.SourceColumns) == 0 || len(desc.Groups) == 0 ||
		len(desc.Aggregates) == 0 || desc.RowCountColumn == "" || len(desc.StateColumns) == 0 {
		return nil, fmt.Errorf("incomplete materialized view incremental specification")
	}
	for _, group := range desc.Groups {
		if group.Expression == "" || group.OutputColumn == "" {
			return nil, fmt.Errorf("invalid materialized view incremental group")
		}
	}
	for _, agg := range desc.Aggregates {
		switch agg.Kind {
		case "count_star":
		case "count_column":
			if agg.InputExpression == "" {
				return nil, fmt.Errorf("incremental COUNT requires an input")
			}
		case "sum":
			if agg.InputExpression == "" || agg.StateCountColumn == "" {
				return nil, fmt.Errorf("incremental SUM requires input and state")
			}
		case "avg":
			if agg.InputExpression == "" || agg.StateSumColumn == "" || agg.StateCountColumn == "" {
				return nil, fmt.Errorf("incremental AVG requires input and state")
			}
		default:
			return nil, fmt.Errorf("incremental aggregate %q is not supported", agg.Kind)
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
		return false, fmt.Errorf("materialized view retriever does not expose from boundary")
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
				return fmt.Errorf("source relation does not support rowid lookup")
			}
			tableDef := rel.GetTableDef(refreshCtx)
			if tableDef == nil {
				return fmt.Errorf("source relation has no table definition")
			}
			sourceTypes, err := materializedViewSourceColumnTypes(tableDef, desc.SourceColumns)
			if err != nil {
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
					rowids := make([]types.Rowid, len(deletes))
					for i := range deletes {
						rowids[i] = deletes[i].RowID
					}
					oldRows, readErr := reader.ReadRowsByRowID(refreshCtx, rowids, from.GetFromTS(), desc.SourceColumns, nil)
					if readErr != nil {
						data.Done()
						return readErr
					}
					if len(oldRows) != len(deletes) {
						data.Done()
						return fmt.Errorf("rowid lookup returned %d rows for %d deletes", len(oldRows), len(deletes))
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
					if err := applyMaterializedViewDeltaBatch(refreshCtx, sqlctx.GetService(), sqlctx.Txn(), c.info, desc, sourceTypes, rows[start:end]); err != nil {
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
			return r.UpdateWatermark(refreshCtx, sqlctx.GetService(), sqlctx.Txn())
		})
	if err == nil {
		metricv2.ISCPMaterializedViewRows.WithLabelValues("insert").Add(float64(insertRows))
		metricv2.ISCPMaterializedViewRows.WithLabelValues("delete").Add(float64(deleteRows))
	}
	return drained, err
}

func materializedViewSourceColumnTypes(tableDef *planpb.TableDef, columns []string) ([]*types.Type, error) {
	byName := make(map[string]*types.Type, len(tableDef.Cols))
	for _, col := range tableDef.Cols {
		if col.Hidden || strings.EqualFold(col.Name, catalog.Row_ID) {
			continue
		}
		byName[strings.ToLower(col.Name)] = &types.Type{Oid: types.T(col.Typ.Id), Width: col.Typ.Width, Scale: col.Typ.Scale}
	}
	result := make([]*types.Type, len(columns))
	for i, column := range columns {
		result[i] = byName[strings.ToLower(column)]
		if result[i] == nil {
			return nil, fmt.Errorf("incremental expression references unknown column %q", column)
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
	cte, err := materializedViewDeltaCTE(ctx, desc, sourceTypes, rows)
	if err != nil {
		return err
	}
	target := sqlquote.QualifiedIdent(info.DBName, info.TableName)
	join := materializedViewDeltaJoin(desc, "t", "d")
	sets := materializedViewDeltaUpdateSets(desc, "t", "d")
	if err := execMaterializedViewDeltaAndClose(ctx, fmt.Sprintf("%s UPDATE %s AS t JOIN delta AS d ON %s SET %s", cte, target, join, strings.Join(sets, ",")), service, txn); err != nil {
		return err
	}
	columns, values := materializedViewDeltaInsertProjection(desc, "d")
	insert := fmt.Sprintf("%s INSERT INTO %s (%s) SELECT %s FROM delta AS d LEFT JOIN %s AS t ON %s WHERE t.%s IS NULL AND d.__mo_row_delta > 0",
		cte, target, strings.Join(columns, ","), strings.Join(values, ","), target, join, sqlquote.Ident(catalog.FakePrimaryKeyColName))
	if err := execMaterializedViewDeltaAndClose(ctx, insert, service, txn); err != nil {
		return err
	}
	return execMaterializedViewDeltaAndClose(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s <= 0", target, sqlquote.Ident(desc.RowCountColumn)), service, txn)
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
				return "", fmt.Errorf("incremental row is missing column %q", column)
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
			values = append(values, sourceTypes[colIdx].DescString()...)
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
		columns = append(columns, fmt.Sprintf("CAST(column_%d AS %s) AS %s", i, sourceTypes[i].DescString(), sqlquote.Ident(column)))
	}
	columns = append(columns, fmt.Sprintf("CAST(column_%d AS BIGINT) AS __mo_sign", len(desc.SourceColumns)))

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
		}
	}
	where := ""
	if desc.Filter != "" {
		where = " WHERE (" + desc.Filter + ")"
	}
	cte := fmt.Sprintf("WITH src AS (SELECT %s FROM (VALUES %s) AS __mo_mv_values), delta AS (SELECT %s FROM src AS %s%s GROUP BY %s)",
		strings.Join(columns, ","), string(values), strings.Join(projection, ","), sqlquote.Ident(desc.SourceAlias), where, strings.Join(groupBy, ","))
	if len(cte) > materializedViewDeltaMaxSQL {
		return "", fmt.Errorf("materialized view delta SQL exceeds %d bytes", materializedViewDeltaMaxSQL)
	}
	return cte, nil
}

func materializedViewDeltaGroupAlias(i int) string { return fmt.Sprintf("__mo_g_%d", i) }
func materializedViewDeltaCountAlias(i int) string { return fmt.Sprintf("__mo_a_%d_count", i) }
func materializedViewDeltaSumAlias(i int) string   { return fmt.Sprintf("__mo_a_%d_sum", i) }

func materializedViewDeltaJoin(desc *incrementalDescription, targetAlias, deltaAlias string) string {
	predicates := make([]string, len(desc.Groups))
	for i, group := range desc.Groups {
		target := targetAlias + "." + sqlquote.Ident(group.OutputColumn)
		delta := deltaAlias + "." + materializedViewDeltaGroupAlias(i)
		predicates[i] = fmt.Sprintf("(%s = %s OR (%s IS NULL AND %s IS NULL))", target, delta, target, delta)
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
			sets = append(sets,
				fmt.Sprintf("%s = CASE WHEN %s + %s = 0 THEN NULL ELSE coalesce(%s,0) + coalesce(%s,0) END", out, stateCount, countDelta, out, sumDelta),
				fmt.Sprintf("%s = %s + %s", stateCount, stateCount, countDelta))
		case "avg":
			stateSum := targetAlias + "." + sqlquote.Ident(agg.StateSumColumn)
			stateCount := targetAlias + "." + sqlquote.Ident(agg.StateCountColumn)
			sets = append(sets,
				fmt.Sprintf("%s = CASE WHEN %s + %s = 0 THEN NULL ELSE (coalesce(%s,0) + coalesce(%s,0)) / (%s + %s) END", out, stateCount, countDelta, stateSum, sumDelta, stateCount, countDelta),
				fmt.Sprintf("%s = coalesce(%s,0) + coalesce(%s,0)", stateSum, stateSum, sumDelta),
				fmt.Sprintf("%s = %s + %s", stateCount, stateCount, countDelta))
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
	for i, agg := range desc.Aggregates {
		countDelta := deltaAlias + "." + materializedViewDeltaCountAlias(i)
		sumDelta := deltaAlias + "." + materializedViewDeltaSumAlias(i)
		columns = append(columns, sqlquote.Ident(agg.OutputColumn))
		switch agg.Kind {
		case "count_star", "count_column":
			values = append(values, countDelta)
		case "sum":
			values = append(values, fmt.Sprintf("CASE WHEN %s = 0 THEN NULL ELSE %s END", countDelta, sumDelta))
			columns = append(columns, sqlquote.Ident(agg.StateCountColumn))
			values = append(values, countDelta)
		case "avg":
			values = append(values, fmt.Sprintf("CASE WHEN %s = 0 THEN NULL ELSE %s / %s END", countDelta, sumDelta, countDelta))
			columns = append(columns, sqlquote.Ident(agg.StateSumColumn), sqlquote.Ident(agg.StateCountColumn))
			values = append(values, sumDelta, countDelta)
		}
	}
	columns = append(columns, sqlquote.Ident(desc.RowCountColumn))
	values = append(values, deltaAlias+".__mo_row_delta")
	return columns, values
}
