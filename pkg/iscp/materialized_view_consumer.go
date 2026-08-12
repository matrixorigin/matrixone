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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
	// Drain the complete iteration first. A refresh is deliberately coalesced
	// to one operation even when CollectChanges produces many batches.
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

	return runTxnWithSqlContext(ctx, c.cnEngine, c.cnTxnClient, c.cnUUID,
		r.GetAccountID(), 24*time.Hour, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, _ any) error {
			sqlctx := sqlproc.SqlCtx
			refreshCtx := context.WithValue(sqlproc.GetContext(), defines.MaterializedViewRefreshKey{}, true)
			deleteSQL := fmt.Sprintf("delete from `%s`.`%s`", c.info.DBName, c.info.TableName)
			res, err := ExecWithResult(refreshCtx, deleteSQL, sqlctx.GetService(), sqlctx.Txn())
			if err != nil {
				return err
			}
			res.Close()
			boundary, ok := r.(iterationBoundaryRetriever)
			if !ok {
				return fmt.Errorf("materialized view retriever does not expose iteration boundary")
			}
			refreshSQL, err := materializedViewRefreshAtInDatabase(c.info.RefreshSQL, c.info.SourceSQL, c.jobID.DBName, boundary.GetToTS())
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
