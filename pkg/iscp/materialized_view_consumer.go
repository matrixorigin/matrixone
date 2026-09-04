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
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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
	Kind             string `json:"kind"`
	InputExpression  string `json:"input_expression,omitempty"`
	OutputColumn     string `json:"output_column"`
	StateSumColumn   string `json:"state_sum_column,omitempty"`
	StateCountColumn string `json:"state_count_column,omitempty"`
	StateIndex       int    `json:"state_index,omitempty"`
}

type incrementalGroup struct {
	Expression   string `json:"expression"`
	OutputColumn string `json:"output_column"`
	NotNullable  bool   `json:"not_nullable,omitempty"`
}

type incrementalDescription struct {
	Version        int                    `json:"version,omitempty"`
	Strategy       string                 `json:"strategy,omitempty"`
	SourceAlias    string                 `json:"source_alias"`
	SourceColumns  []string               `json:"source_columns"`
	Filter         string                 `json:"filter,omitempty"`
	Having         string                 `json:"having,omitempty"`
	Groups         []incrementalGroup     `json:"groups"`
	Aggregates     []incrementalAggregate `json:"aggregates"`
	GroupKeyColumn string                 `json:"group_key_column,omitempty"`
	RowCountColumn string                 `json:"row_count_column"`
	StateColumns   []string               `json:"state_columns"`
	StateTable     string                 `json:"state_table,omitempty"`
	BranchID       int                    `json:"branch_id,omitempty"`
	SourceDatabase string                 `json:"source_database,omitempty"`
	SourceTable    string                 `json:"source_table,omitempty"`
	Branches       []incrementalBranch    `json:"branches,omitempty"`
}

type incrementalBranch struct {
	Description *incrementalDescription `json:"description"`
}

type materializedViewChangeRow struct {
	Values   map[string]any
	RowID    types.Rowid
	CommitTS types.TS
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
		return nil, moerr.NewInternalErrorNoCtx("invalid materialized view consumer specification")
	}
	return &MaterializedViewConsumer{
		cnUUID: cnUUID, cnEngine: cnEngine, cnTxnClient: cnTxnClient,
		jobID: jobID, info: info,
	}, nil
}

func (c *MaterializedViewConsumer) Consume(ctx context.Context, r DataRetriever) error {
	drained := false
	if r.GetDataType() == ISCPDataType_Tail && c.info.IncrementalSpec != "" {
		started := time.Now()
		var incrementalErr error
		drained, incrementalErr = c.consumeIncremental(ctx, r)
		if incrementalErr == nil {
			metricv2.ISCPMaterializedViewRefreshDuration.WithLabelValues("incremental", "success").Observe(time.Since(started).Seconds())
			observeMaterializedViewWatermarkLag(r)
			return nil
		} else {
			metricv2.ISCPMaterializedViewRefreshDuration.WithLabelValues("incremental", "error").Observe(time.Since(started).Seconds())
			if strings.EqualFold(c.info.RefreshMethod, "fast") {
				return incrementalErr
			}
			metricv2.ISCPMaterializedViewFallback.Inc()
			logutil.Warnf("materialized view incremental refresh fallback: mv=%s.%s err=%v", c.info.DBName, c.info.TableName, incrementalErr)
		}
		// Incremental refresh is deliberately fail-closed: the fallback starts
		// a new transaction, so a partial delta can never be committed. Drain any
		// remaining payload before evaluating the definition at the boundary.
	}
	if !drained {
		if err := c.drainChanges(r); err != nil {
			return err
		}
	}

	started := time.Now()
	err := c.consumeFullRefresh(ctx, r)
	result := "success"
	if err != nil {
		result = "error"
	} else {
		observeMaterializedViewWatermarkLag(r)
	}
	metricv2.ISCPMaterializedViewRefreshDuration.WithLabelValues("full", result).Observe(time.Since(started).Seconds())
	return err
}

func observeMaterializedViewWatermarkLag(r DataRetriever) {
	boundary, ok := r.(iterationBoundaryRetriever)
	if !ok {
		return
	}
	lag := time.Since(time.Unix(0, boundary.GetToTS().Physical())).Seconds()
	if lag >= 0 {
		metricv2.ISCPMaterializedViewWatermarkLag.Observe(lag)
	}
}

// NeedsChangePayload reports whether this iteration can use row deltas. The
// initial snapshot and full-refresh-only tails only need the consistent toTS
// boundary; their definition query reads source tables at that timestamp.
func (c *MaterializedViewConsumer) NeedsChangePayload(dataType int8) bool {
	return dataType == ISCPDataType_Tail && c.info.IncrementalSpec != ""
}

// drainChanges advances a snapshot or full-refresh-only stream to its
// iteration boundary without retaining its table-sized row payload.
func (c *MaterializedViewConsumer) drainChanges(r DataRetriever) error {
	for {
		data := r.Next()
		if data == nil {
			break
		}
		if data.err != nil {
			data.Done()
			return data.err
		}
		done := data.noMoreData
		data.Done()
		if done {
			break
		}
	}
	return nil
}

func (c *MaterializedViewConsumer) consumeFullRefresh(ctx context.Context, r DataRetriever) error {
	return runTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID,
		r.GetAccountID(), 24*time.Hour, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, _ any) error {
			sqlctx := sqlproc.SqlCtx
			refreshCtx := context.WithValue(sqlproc.GetContext(), defines.MaterializedViewRefreshKey{}, true)
			boundary, ok := r.(iterationBoundaryRetriever)
			if !ok {
				return moerr.NewInternalErrorNoCtx("materialized view retriever does not expose iteration boundary")
			}
			toTS := boundary.GetToTS()
			if err := RefreshMaterializedView(refreshCtx, sqlctx.GetService(), sqlctx.Txn(), c.info, &toTS); err != nil {
				return err
			}
			return r.UpdateWatermark(refreshCtx, sqlctx.GetService(), sqlctx.Txn())
		})
}

// RefreshMaterializedView atomically replaces a materialized view in txn. A
// non-nil boundary is used by ISCP to read every source at the same watermark;
// nil reads the caller transaction snapshot for an ON DEMAND refresh.
func RefreshMaterializedView(ctx context.Context, service string, txn client.TxnOperator, info *ConsumerInfo, boundary *types.TS) error {
	if info == nil || info.DBName == "" || info.TableName == "" || info.RefreshSQL == "" {
		return moerr.NewInternalErrorNoCtx("invalid materialized view refresh specification")
	}
	refreshCtx := context.WithValue(ctx, defines.MaterializedViewRefreshKey{}, true)
	var incrementalDesc *incrementalDescription
	if info.IncrementalSpec != "" {
		var err error
		incrementalDesc, err = decodeIncrementalDescription(info.IncrementalSpec)
		if err != nil {
			return err
		}
		if boundary == nil {
			return moerr.NewInternalErrorNoCtx("incremental materialized view state requires an ISCP boundary")
		}
		if err = ensureMaterializedViewStateTable(refreshCtx, service, txn, info, incrementalDesc); err != nil {
			return err
		}
		if err = rebuildMaterializedViewDistinctState(refreshCtx, service, txn, info, incrementalDesc, *boundary); err != nil {
			return err
		}
	}
	deleteColumn := catalog.FakePrimaryKeyColName
	if materializedViewDeltaCanUpsert(incrementalDesc) {
		deleteColumn = incrementalDesc.GroupKeyColumn
	}
	deleteSQL := fmt.Sprintf("delete from `%s`.`%s` where %s is not null", info.DBName, info.TableName, sqlquote.Ident(deleteColumn))
	res, err := ExecWithResult(refreshCtx, deleteSQL, service, txn)
	if err != nil {
		return err
	}
	res.Close()
	var refreshSQL string
	if boundary != nil {
		refreshSQL, err = materializedViewRefreshAtSources(info.RefreshSQL, info.SourceTableInfos(), *boundary)
	} else {
		refreshSQL, err = materializedViewRefreshAtCurrentSources(info.RefreshSQL, info.SourceTableInfos())
	}
	if err != nil {
		return err
	}
	insertSQL := fmt.Sprintf("insert into `%s`.`%s` %s", info.DBName, info.TableName, refreshSQL)
	if len(info.Columns) > 0 {
		targetColumns := append([]string(nil), info.Columns...)
		if incrementalDesc != nil {
			targetColumns = append(targetColumns, incrementalDesc.StateColumns...)
		}
		columns := make([]string, 0, len(targetColumns)+1)
		selectColumns := make([]string, 0, len(targetColumns))
		for _, column := range targetColumns {
			quoted := sqlquote.Ident(column)
			columns = append(columns, quoted)
			selectColumns = append(selectColumns, quoted)
		}
		if materializedViewDeltaCanUpsert(incrementalDesc) {
			insertSQL = fmt.Sprintf("insert into `%s`.`%s` (%s) select %s from (%s) as `__mo_mv_refresh`", info.DBName, info.TableName, strings.Join(columns, ","), strings.Join(selectColumns, ","), refreshSQL)
		} else {
			columns = append(columns, sqlquote.Ident(catalog.FakePrimaryKeyColName))
			insertSQL = fmt.Sprintf("insert into `%s`.`%s` (%s) select %s, row_number() over () from (%s) as `__mo_mv_refresh`", info.DBName, info.TableName, strings.Join(columns, ","), strings.Join(selectColumns, ","), refreshSQL)
		}
	}
	res, err = ExecWithResult(refreshCtx, insertSQL, service, txn)
	if err != nil {
		return err
	}
	res.Close()
	return nil
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
		if err := extractRowFromEveryVector(context.Background(), item.Src, item.Offset, values, ReprSQLString); err != nil {
			return nil, err
		}
		if !insert {
			rowid, ok := values[0].(types.Rowid)
			if !ok {
				return nil, moerr.NewInternalErrorNoCtx("materialized view delete batch does not retain rowid")
			}
			row := materializedViewChangeRow{RowID: rowid}
			for i, attr := range item.Src.Attrs {
				if i < len(values) && (strings.EqualFold(attr, objectio.DefaultCommitTS_Attr) ||
					strings.EqualFold(attr, "commit_ts") || strings.EqualFold(attr, "__mo_commit_ts")) {
					var commitOK bool
					row.CommitTS, commitOK = values[i].(types.TS)
					if !commitOK {
						return nil, moerr.NewInternalErrorNoCtxf("materialized view delete batch has invalid commit timestamp %T", values[i])
					}
					break
				}
			}
			rows = append(rows, row)
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

func materializedViewRefreshAt(query, source string, ts types.TS) (string, error) {
	needle := "from " + source
	replacement := fmt.Sprintf("from %s{MO_TS = '%s'}", source, ts.ToString())
	refresh := strings.Replace(query, needle, replacement, 1)
	if refresh == query {
		return "", moerr.NewInternalErrorNoCtxf("materialized view source %q not found in refresh query", source)
	}
	return refresh, nil
}

// materializedViewRefreshAtSources adds the same snapshot boundary to every
// direct source relation in a refresh query. The MV planner only accepts a
// top-level SelectClause whose FROM tree contains direct base tables, so the
// parsed FROM tree is the authoritative place to rewrite JOIN and comma-join
// forms without mistaking qualified column references for table references.
func materializedViewRefreshAtSources(query string, sources []TableInfo, ts types.TS) (string, error) {
	return materializedViewRefreshAtSourcesWithBoundary(query, sources, &ts)
}

func materializedViewRefreshAtCurrentSources(query string, sources []TableInfo) (string, error) {
	return materializedViewRefreshAtSourcesWithBoundary(query, sources, nil)
}

func materializedViewRefreshAtSourcesWithBoundary(query string, sources []TableInfo, ts *types.TS) (string, error) {
	if len(sources) == 0 {
		return "", moerr.NewInternalErrorNoCtx("materialized view has no source tables")
	}
	stmt, err := mysql.ParseOne(context.Background(), query, 1)
	if err != nil {
		return "", moerr.NewInternalErrorNoCtxf("parse materialized view refresh query: %v", err)
	}
	defer stmt.Free()
	selectStmt, ok := stmt.(*tree.Select)
	if !ok {
		return "", moerr.NewInternalErrorNoCtxf("materialized view refresh query is %T, expected select", stmt)
	}
	type sourceKey struct {
		database string
		table    string
	}
	sourceByKey := make(map[sourceKey]TableInfo, len(sources))
	found := make(map[sourceKey]bool, len(sources))
	for _, source := range sources {
		if source.DBName == "" || source.TableName == "" {
			return "", moerr.NewInternalErrorNoCtx("materialized view has incomplete source table")
		}
		key := sourceKey{database: strings.ToLower(source.DBName), table: strings.ToLower(source.TableName)}
		sourceByKey[key] = source
		found[key] = false
	}

	var rewriteTableExpr func(tree.TableExpr) error
	rewriteTableExpr = func(expr tree.TableExpr) error {
		switch node := expr.(type) {
		case *tree.AliasedTableExpr:
			return rewriteTableExpr(node.Expr)
		case *tree.JoinTableExpr:
			if err := rewriteTableExpr(node.Left); err != nil {
				return err
			}
			// The MySQL parser represents a single table reference as a
			// degenerate join whose right side and condition are nil.
			if node.Right == nil && node.Cond == nil {
				return nil
			}
			return rewriteTableExpr(node.Right)
		case *tree.ParenTableExpr:
			return rewriteTableExpr(node.Expr)
		case *tree.TableName:
			tableName := strings.ToLower(string(node.ObjectName))
			var matchKey sourceKey
			matches := 0
			for key := range sourceByKey {
				if key.table != tableName {
					continue
				}
				if node.ExplicitSchema && key.database != strings.ToLower(string(node.SchemaName)) {
					continue
				}
				matchKey = key
				matches++
			}
			if matches == 0 {
				return moerr.NewInternalErrorNoCtxf("materialized view source %q not found in refresh metadata", node.ObjectName)
			}
			if matches > 1 {
				return moerr.NewInternalErrorNoCtxf("materialized view source %q is ambiguous without a database qualifier", node.ObjectName)
			}
			source := sourceByKey[matchKey]
			node.SchemaName = tree.Identifier(source.DBName)
			node.ExplicitSchema = true
			if ts != nil {
				node.AtTsExpr = &tree.AtTimeStamp{
					Type: tree.ATMOTIMESTAMP,
					Expr: tree.NewStrVal("'" + ts.ToString() + "'"),
				}
			}
			found[matchKey] = true
			return nil
		default:
			return moerr.NewInternalErrorNoCtxf("materialized view source must be a direct base table (table=%T)", expr)
		}
	}
	var rewriteSelect func(tree.SelectStatement) error
	rewriteSelect = func(selection tree.SelectStatement) error {
		switch node := selection.(type) {
		case *tree.Select:
			return rewriteSelect(node.Select)
		case *tree.ParenSelect:
			if node.Select == nil {
				return moerr.NewInternalErrorNoCtx("materialized view refresh query has an empty parenthesized select")
			}
			return rewriteSelect(node.Select)
		case *tree.UnionClause:
			if node.Type != tree.UNION || !node.All || node.Distinct {
				return moerr.NewInternalErrorNoCtx("materialized view refresh query contains an unsupported set operation")
			}
			if err := rewriteSelect(node.Left); err != nil {
				return err
			}
			return rewriteSelect(node.Right)
		case *tree.SelectClause:
			if node.From == nil || len(node.From.Tables) == 0 {
				return moerr.NewInternalErrorNoCtx("materialized view refresh query has no direct source tables")
			}
			for _, expr := range node.From.Tables {
				if err := rewriteTableExpr(expr); err != nil {
					return err
				}
			}
			return nil
		default:
			return moerr.NewInternalErrorNoCtxf("materialized view refresh query contains unsupported select %T", selection)
		}
	}
	if err := rewriteSelect(selectStmt); err != nil {
		return "", err
	}
	for key, wasFound := range found {
		if !wasFound {
			return "", moerr.NewInternalErrorNoCtxf("materialized view source %q not found in refresh query", key.table)
		}
	}
	return tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithQuoteIdentifier(), tree.WithSingleQuoteString()), nil
}
