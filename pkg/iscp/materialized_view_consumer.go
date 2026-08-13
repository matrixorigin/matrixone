// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// MaterializedViewConsumer owns one refresh transaction per ISCP iteration.
// It intentionally does not use InitSQL: the initial snapshot and every tail
// iteration enter through the same consumer lifecycle.
type MaterializedViewConsumer struct {
	cnUUID      string
	cnEngine    engine.Engine
	cnTxnClient client.TxnClient
	jobID       JobID
	info        *ConsumerInfo
}

type iterationBoundaryRetriever interface {
	GetToTS() types.TS
}

type materializedViewFromBoundaryRetriever interface {
	GetFromTS() types.TS
}

type incrementalAggregate struct {
	Kind         string `json:"kind"`
	InputColumn  string `json:"input_column,omitempty"`
	OutputColumn string `json:"output_column"`
}

type incrementalDescription struct {
	GroupColumns []string               `json:"group_columns"`
	GroupOutputs []string               `json:"group_outputs"`
	Aggregates   []incrementalAggregate `json:"aggregates"`
}

type materializedViewChangeRow struct {
	Values map[string]any
	RowID  types.Rowid
}

type incrementalDelta struct {
	groupValues map[string]any
	countDelta  map[string]int64
	sumTerms    map[string][]string
}

var _ Consumer = (*MaterializedViewConsumer)(nil)

func NewMaterializedViewConsumer(
	cnUUID string,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	jobID JobID,
	info *ConsumerInfo,
) (Consumer, error) {
	if info == nil || info.DBName == "" || info.TableName == "" || info.RefreshSQL == "" || info.SourceSQL == "" {
		return nil, fmt.Errorf("invalid materialized view consumer specification")
	}
	return &MaterializedViewConsumer{
		cnUUID: cnUUID, cnEngine: cnEngine, cnTxnClient: cnTxnClient,
		jobID: jobID, info: info,
	}, nil
}

func (c *MaterializedViewConsumer) Consume(ctx context.Context, r DataRetriever) error {
	var inserts, deletes []materializedViewChangeRow
	collectChanges := true
	for {
		data := r.Next()
		if data == nil {
			break
		}
		if data.err != nil {
			data.Done()
			return data.err
		}
		if collectChanges {
			if data.insertBatch != nil {
				rows, err := materializedViewRowsFromBatch(data.insertBatch, true)
				if err != nil {
					logutil.Warnf("materialized view incremental batch fallback: mv=%s.%s err=%v", c.info.DBName, c.info.TableName, err)
					collectChanges = false
				} else {
					inserts = append(inserts, rows...)
				}
			}
			if data.deleteBatch != nil {
				rows, err := materializedViewRowsFromBatch(data.deleteBatch, false)
				if err != nil {
					logutil.Warnf("materialized view incremental delete batch fallback: mv=%s.%s err=%v", c.info.DBName, c.info.TableName, err)
					collectChanges = false
				} else {
					deletes = append(deletes, rows...)
				}
			}
		}
		done := data.noMoreData
		data.Done()
		if done {
			break
		}
	}

	if r.GetDataType() == ISCPDataType_Tail && collectChanges && c.info.IncrementalSpec != "" {
		if err := c.consumeIncremental(ctx, r, inserts, deletes); err == nil {
			return nil
		} else {
			logutil.Warnf("materialized view incremental refresh fallback: mv=%s.%s err=%v", c.info.DBName, c.info.TableName, err)
		}
		// Incremental refresh is deliberately fail-closed: the fallback starts
		// a new transaction, so a partial delta can never be committed.
	}

	return c.consumeFullRefresh(ctx, r)
}

func (c *MaterializedViewConsumer) consumeFullRefresh(ctx context.Context, r DataRetriever) error {
	return runTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID,
		r.GetAccountID(), 24*time.Hour, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, _ any) error {
			sqlctx := sqlproc.SqlCtx
			refreshCtx := context.WithValue(sqlproc.GetContext(), defines.MaterializedViewRefreshKey{}, true)
			// Keep this as a row DELETE. A predicate is required because a
			// predicate-free DELETE is optimized to TRUNCATE, which replaces the
			// physical relation and unregisters the source ISCP job as a side
			// effect. The fake primary key is present on every MV result row.
			deleteSQL := fmt.Sprintf("delete from `%s`.`%s` where `__mo_fake_pk_col` is not null", c.info.DBName, c.info.TableName)
			res, err := ExecWithResult(refreshCtx, deleteSQL, sqlctx.GetService(), sqlctx.Txn())
			if err != nil {
				return err
			}
			res.Close()
			boundary, ok := r.(iterationBoundaryRetriever)
			if !ok {
				return fmt.Errorf("materialized view retriever does not expose iteration boundary")
			}
			refreshSQL, err := materializedViewRefreshAtSources(c.info.RefreshSQL, c.info.SourceTableInfos(), boundary.GetToTS())
			if err != nil {
				return err
			}
			insertSQL := fmt.Sprintf("insert into `%s`.`%s` %s", c.info.DBName, c.info.TableName, refreshSQL)
			if len(c.info.Columns) > 0 {
				columns := make([]string, 0, len(c.info.Columns)+1)
				selectColumns := make([]string, 0, len(c.info.Columns))
				for _, column := range c.info.Columns {
					quoted := "`" + strings.ReplaceAll(column, "`", "``") + "`"
					columns = append(columns, quoted)
					selectColumns = append(selectColumns, quoted)
				}
				columns = append(columns, "`__mo_fake_pk_col`")
				insertSQL = fmt.Sprintf("insert into `%s`.`%s` (%s) select %s, row_number() over () from (%s) as `__mo_mv_refresh`", c.info.DBName, c.info.TableName, strings.Join(columns, ","), strings.Join(selectColumns, ","), refreshSQL)
			}
			res, err = ExecWithResult(refreshCtx, insertSQL, sqlctx.GetService(), sqlctx.Txn())
			if err != nil {
				return err
			}
			res.Close()
			return r.UpdateWatermark(refreshCtx, sqlctx.GetService(), sqlctx.Txn())
		})
}

func materializedViewRowsFromBatch(bat *AtomicBatch, insert bool) ([]materializedViewChangeRow, error) {
	if bat == nil || bat.Rows == nil {
		return nil, nil
	}
	iter := bat.GetRowIterator().(*atomicBatchRowIter)
	defer iter.Close()
	rows := make([]materializedViewChangeRow, 0, bat.Rows.Len())
	for iter.Next() {
		item := iter.Item()
		values := make([]any, len(item.Src.Vecs))
		if err := extractRowFromEveryVector(context.Background(), item.Src, item.Offset, values); err != nil {
			return nil, err
		}
		if !insert {
			rowid, ok := values[0].(types.Rowid)
			if !ok {
				return nil, fmt.Errorf("materialized view delete batch does not retain rowid")
			}
			rows = append(rows, materializedViewChangeRow{RowID: rowid})
			continue
		}
		row := materializedViewChangeRow{Values: make(map[string]any)}
		if len(values) > 0 {
			if rowid, ok := values[0].(types.Rowid); ok {
				row.RowID = rowid
			}
		}
		for i, attr := range item.Src.Attrs {
			if i >= len(values) || strings.EqualFold(attr, catalog.Row_ID) || strings.EqualFold(attr, "commit_ts") || strings.EqualFold(attr, "__mo_commit_ts") {
				continue
			}
			row.Values[strings.ToLower(attr)] = values[i]
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (c *MaterializedViewConsumer) consumeIncremental(ctx context.Context, r DataRetriever, inserts, deletes []materializedViewChangeRow) error {
	var desc incrementalDescription
	spec, err := base64.StdEncoding.DecodeString(c.info.IncrementalSpec)
	if err != nil {
		return fmt.Errorf("invalid materialized view incremental specification encoding: %w", err)
	}
	if err := json.Unmarshal(spec, &desc); err != nil || len(desc.GroupColumns) == 0 || len(desc.GroupColumns) != len(desc.GroupOutputs) {
		return fmt.Errorf("invalid materialized view incremental specification")
	}
	for _, agg := range desc.Aggregates {
		if agg.Kind != "count_star" && agg.Kind != "count_column" && agg.Kind != "sum" {
			return fmt.Errorf("incremental aggregate %q is not supported", agg.Kind)
		}
	}
	from, ok := r.(materializedViewFromBoundaryRetriever)
	if !ok {
		return fmt.Errorf("materialized view retriever does not expose from boundary")
	}
	return runTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID, r.GetAccountID(), 24*time.Hour, nil, nil,
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
			scales := make(map[string]int32)
			if tableDef := rel.GetTableDef(refreshCtx); tableDef != nil {
				for _, col := range tableDef.Cols {
					scales[strings.ToLower(col.Name)] = col.Typ.Scale
				}
			}
			attrs := make([]string, 0, len(desc.GroupColumns)+len(desc.Aggregates))
			seen := make(map[string]struct{})
			addAttr := func(name string) {
				name = strings.ToLower(name)
				if _, exists := seen[name]; !exists {
					seen[name] = struct{}{}
					attrs = append(attrs, name)
				}
			}
			for _, name := range desc.GroupColumns {
				addAttr(name)
			}
			for _, agg := range desc.Aggregates {
				if agg.InputColumn != "" {
					addAttr(agg.InputColumn)
				}
			}
			rowids := make([]types.Rowid, 0, len(deletes))
			for _, row := range deletes {
				rowids = append(rowids, row.RowID)
			}
			oldRows, err := reader.ReadRowsByRowID(refreshCtx, rowids, from.GetFromTS(), attrs, nil)
			if err != nil {
				return err
			}
			deltas := make(map[string]*incrementalDelta)
			normalize := func(row map[string]any) {
				for name, value := range row {
					row[name] = normalizeMaterializedViewValue(value, scales[name])
				}
			}
			apply := func(row map[string]any, sign int64) error {
				normalize(row)
				keyParts := make([]string, 0, len(desc.GroupColumns))
				groupValues := make(map[string]any, len(desc.GroupColumns))
				for _, col := range desc.GroupColumns {
					value, exists := row[strings.ToLower(col)]
					if !exists {
						return fmt.Errorf("missing group column %q", col)
					}
					groupValues[strings.ToLower(col)] = value
					keyParts = append(keyParts, fmt.Sprintf("%#v", value))
				}
				key := strings.Join(keyParts, "\x00")
				delta := deltas[key]
				if delta == nil {
					delta = &incrementalDelta{groupValues: groupValues, countDelta: make(map[string]int64), sumTerms: make(map[string][]string)}
					deltas[key] = delta
				}
				for _, agg := range desc.Aggregates {
					if agg.Kind == "count_star" {
						delta.countDelta[agg.OutputColumn] += sign
						continue
					}
					value := row[strings.ToLower(agg.InputColumn)]
					if value == nil {
						continue
					}
					if agg.Kind == "count_column" {
						delta.countDelta[agg.OutputColumn] += sign
						continue
					}
					literal, err := materializedViewSQLNumericLiteral(value)
					if err != nil {
						return err
					}
					term := literal
					if sign < 0 {
						term = "- (" + literal + ")"
					}
					delta.sumTerms[agg.OutputColumn] = append(delta.sumTerms[agg.OutputColumn], term)
				}
				return nil
			}
			for _, row := range inserts {
				if err := apply(row.Values, 1); err != nil {
					return err
				}
			}
			for i := range deletes {
				if i >= len(oldRows) {
					return fmt.Errorf("rowid lookup returned too few rows")
				}
				old := make(map[string]any, len(attrs))
				for j, name := range attrs {
					old[strings.ToLower(name)] = oldRows[i][j]
				}
				if err := apply(old, -1); err != nil {
					return err
				}
			}
			for _, delta := range deltas {
				if err := applyIncrementalDelta(refreshCtx, sqlctx.GetService(), sqlctx.Txn(), c.info, &desc, delta); err != nil {
					return err
				}
			}
			return r.UpdateWatermark(refreshCtx, sqlctx.GetService(), sqlctx.Txn())
		})
}

func applyIncrementalDelta(ctx context.Context, service string, txn client.TxnOperator, info *ConsumerInfo, desc *incrementalDescription, delta *incrementalDelta) error {
	where := make([]string, 0, len(desc.GroupColumns))
	for i, col := range desc.GroupColumns {
		value, err := materializedViewSQLLiteral(delta.groupValues[strings.ToLower(col)])
		if err != nil {
			return err
		}
		out := desc.GroupOutputs[i]
		where = append(where, fmt.Sprintf("((`%s` = %s) or (`%s` is null and %s is null))", strings.ReplaceAll(out, "`", "``"), value, strings.ReplaceAll(out, "`", "``"), value))
	}
	predicate := strings.Join(where, " and ")
	selectSQL := fmt.Sprintf("select count(*) from `%s`.`%s` where %s", info.DBName, info.TableName, predicate)
	res, err := ExecWithResult(ctx, selectSQL, service, txn)
	if err != nil {
		return err
	}
	exists := false
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows > 0 && cols[0].Length() > 0 {
			value := vector.GetAny(cols[0], 0, false)
			exists = fmt.Sprint(value) != "0"
		}
		return false
	})
	res.Close()
	if !exists {
		countDelta := int64(0)
		for _, agg := range desc.Aggregates {
			if agg.Kind == "count_star" || agg.Kind == "count_column" {
				countDelta = delta.countDelta[agg.OutputColumn]
				break
			}
		}
		if countDelta <= 0 {
			return fmt.Errorf("cannot apply a delete-only delta to a missing materialized-view group")
		}
		columns, values := make([]string, 0), make([]string, 0)
		for i, col := range desc.GroupOutputs {
			columns = append(columns, "`"+strings.ReplaceAll(col, "`", "``")+"`")
			value, err := materializedViewSQLLiteral(delta.groupValues[strings.ToLower(desc.GroupColumns[i])])
			if err != nil {
				return err
			}
			values = append(values, value)
		}
		for _, agg := range desc.Aggregates {
			columns = append(columns, "`"+strings.ReplaceAll(agg.OutputColumn, "`", "``")+"`")
			if agg.Kind == "count_star" || agg.Kind == "count_column" {
				values = append(values, fmt.Sprint(delta.countDelta[agg.OutputColumn]))
			} else {
				values = append(values, incrementalSumExpression(delta.sumTerms[agg.OutputColumn]))
			}
		}
		return execAndClose(ctx, fmt.Sprintf("insert into `%s`.`%s` (%s) values (%s)", info.DBName, info.TableName, strings.Join(columns, ","), strings.Join(values, ",")), service, txn)
	}
	sets := make([]string, 0, len(desc.Aggregates))
	for _, agg := range desc.Aggregates {
		col := "`" + strings.ReplaceAll(agg.OutputColumn, "`", "``") + "`"
		if agg.Kind == "count_star" || agg.Kind == "count_column" {
			sets = append(sets, fmt.Sprintf("%s = %s + %d", col, col, delta.countDelta[agg.OutputColumn]))
		} else if terms := delta.sumTerms[agg.OutputColumn]; len(terms) > 0 {
			sets = append(sets, fmt.Sprintf("%s = coalesce(%s, 0) + (%s)", col, col, incrementalSumExpression(terms)))
		}
	}
	if len(sets) == 0 {
		return nil
	}
	if err := execAndClose(ctx, fmt.Sprintf("update `%s`.`%s` set %s where %s", info.DBName, info.TableName, strings.Join(sets, ","), predicate), service, txn); err != nil {
		return err
	}
	for _, agg := range desc.Aggregates {
		if agg.Kind != "count_star" && agg.Kind != "count_column" {
			continue
		}
		countCol := "`" + strings.ReplaceAll(agg.OutputColumn, "`", "``") + "`"
		if err := execAndClose(ctx, fmt.Sprintf("delete from `%s`.`%s` where %s and %s <= 0", info.DBName, info.TableName, predicate, countCol), service, txn); err != nil {
			return err
		}
		break
	}
	return nil
}

func incrementalSumExpression(terms []string) string {
	if len(terms) == 0 {
		return "0"
	}
	return strings.Join(terms, " + ")
}

func execAndClose(ctx context.Context, sql, service string, txn client.TxnOperator) error {
	res, err := ExecWithResult(ctx, sql, service, txn)
	if err == nil {
		res.Close()
	}
	return err
}

func materializedViewSQLLiteral(value any) (string, error) {
	if value == nil {
		return "null", nil
	}
	if s, ok := value.(string); ok {
		return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
	}
	if b, ok := value.([]byte); ok {
		return "'" + strings.ReplaceAll(string(b), "'", "''") + "'", nil
	}
	rv := reflect.ValueOf(value)
	if rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Float64 {
		return fmt.Sprint(value), nil
	}
	if s, ok := value.(fmt.Stringer); ok {
		return "'" + strings.ReplaceAll(s.String(), "'", "''") + "'", nil
	}
	return "", fmt.Errorf("unsupported incremental value type %T", value)
}

func materializedViewSQLNumericLiteral(value any) (string, error) {
	if s, ok := value.(string); ok {
		if _, err := strconv.ParseFloat(strings.TrimSpace(s), 64); err != nil {
			return "", fmt.Errorf("incremental SUM value %q is not numeric", s)
		}
		return strings.TrimSpace(s), nil
	}
	if b, ok := value.([]byte); ok {
		s := strings.TrimSpace(string(b))
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return "", fmt.Errorf("incremental SUM value %q is not numeric", s)
		}
		return s, nil
	}
	return materializedViewSQLLiteral(value)
}

func normalizeMaterializedViewValue(value any, scale int32) any {
	switch v := value.(type) {
	case types.Decimal64:
		return types.Decimal64ToFloat64(v, scale)
	case types.Decimal128:
		return types.Decimal128ToFloat64(v, scale)
	case types.Decimal256:
		return types.Decimal256ToFloat64(v, scale)
	default:
		return value
	}
}

func materializedViewRefreshAt(query, source string, ts types.TS) (string, error) {
	needle := "from " + source
	replacement := fmt.Sprintf("from %s{MO_TS = '%s'}", source, ts.ToString())
	refresh := strings.Replace(query, needle, replacement, 1)
	if refresh == query {
		return "", fmt.Errorf("materialized view source %q not found in refresh query", source)
	}
	return refresh, nil
}

func materializedViewRefreshAtInDatabase(query, source, database string, ts types.TS) (string, error) {
	needle := "from " + source
	qualified := fmt.Sprintf("`%s`.`%s`", database, source)
	replacement := fmt.Sprintf("from %s{MO_TS = '%s'}", qualified, ts.ToString())
	refresh := strings.Replace(query, needle, replacement, 1)
	if refresh == query {
		return "", fmt.Errorf("materialized view source %q not found in refresh query", source)
	}
	return refresh, nil
}

// materializedViewRefreshAtSources adds the same snapshot boundary to every
// direct source relation in a refresh query. The planner only emits direct
// base-table FROM/JOIN expressions for MVs, so keeping this rewrite lexical
// avoids reparsing SQL in the background consumer.
func materializedViewRefreshAtSources(query string, sources []TableInfo, ts types.TS) (string, error) {
	if len(sources) == 0 {
		return "", fmt.Errorf("materialized view has no source tables")
	}
	refresh := query
	for _, source := range sources {
		if source.DBName == "" || source.TableName == "" {
			return "", fmt.Errorf("materialized view has incomplete source table")
		}
		qualified := fmt.Sprintf("`%s`.`%s`", strings.ReplaceAll(source.DBName, "`", "``"), strings.ReplaceAll(source.TableName, "`", "``"))
		replacement := fmt.Sprintf("${1} %s{MO_TS = '%s'}", qualified, ts.ToString())
		name := regexp.QuoteMeta(source.TableName)
		pattern := regexp.MustCompile(`(?i)(\bfrom\b|\bjoin\b|,)\s+(?:` + regexp.QuoteMeta(source.DBName) + `\s*\.\s*)?` + name + `\b`)
		updated := pattern.ReplaceAllString(refresh, replacement)
		if updated == refresh {
			return "", fmt.Errorf("materialized view source %q not found in refresh query", source.TableName)
		}
		refresh = updated
	}
	return refresh, nil
}
