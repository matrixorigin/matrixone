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

package plan

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const showIndexStatusColumns = "`Key_name`, `Table`, `State`, `Error_message`, `Ignored_rows`, `Max_bad_records`, `Synced_at`"

const showIndexStatusEmpty = "SELECT CAST('' AS VARCHAR) AS `Key_name`, CAST('' AS VARCHAR) AS `Table`, CAST('' AS VARCHAR) AS `State`, CAST('' AS VARCHAR) AS `Error_message`, CAST(0 AS BIGINT) AS `Ignored_rows`, CAST(0 AS BIGINT) AS `Max_bad_records`, FROM_UNIXTIME(0) AS `Synced_at` WHERE FALSE"

// indexJob is the sync job of an index, as mo_iscp_log records it.
type indexJob struct {
	id        uint64
	state     int8
	watermark string
	errCode   int64
	errMsg    string
}

// indexJobKey names the sync job of an index of a table.
type indexJobKey struct {
	tableID uint64
	name    string
}

// buildShowIndexStatus shows, for each index of a table kept in sync, the state of its sync job,
// its error, the rows it leaves out and its limit, and the time it is synced to.
func buildShowIndexStatus(stmt *tree.ShowIndexStatus, ctx CompilerContext) (*Plan, error) {
	dbName, err := databaseIsValid(stmt.TableName.GetDBName(), ctx, nil)
	if err != nil {
		return nil, err
	}
	tblName := stmt.TableName.GetTableName()
	objRef, tableDef, err := ctx.Resolve(dbName, tblName, nil)
	if err != nil {
		return nil, err
	}
	if tableDef == nil {
		return nil, moerr.NewNoSuchTable(ctx.GetContext(), dbName, tblName)
	}
	defs, names, err := tableWithPartitions(ctx, dbName, tableDef)
	if err != nil {
		return nil, err
	}
	jobs, err := loadIndexJobs(ctx, defs)
	if err != nil {
		return nil, err
	}

	sql, ignoredTables, err := showIndexStatusSql(dbName, defs, names, jobs)
	if err != nil {
		return nil, err
	}
	newStmt, err := getRewriteSQLStmt(ctx, "SELECT "+showIndexStatusColumns+" FROM ("+sql+") t ORDER BY `Key_name`, `Table`")
	if err != nil {
		return nil, err
	}
	defer newStmt.Free()
	if stmt.Where != nil {
		newStmt.(*tree.Select).Select.(*tree.SelectClause).Where = stmt.Where
	}
	p, err := getReturnDdlBySelectStmt(ctx, newStmt, plan.DataDefinition_SHOW_INDEX)
	if err != nil {
		return nil, err
	}
	readOnBehalfOf(p, ignoredTables, objRef)
	return p, nil
}

// showIndexStatusSql returns the rows of SHOW INDEX STATUS of the indexes of the tables defs of
// database dbName, named tableNames, whose sync jobs are jobs, and the ignored tables the rows
// read.
func showIndexStatusSql(dbName string, defs []*TableDef, tableNames []string, jobs map[indexJobKey]indexJob) (string, map[string]struct{}, error) {
	var selects []string
	ignoredTables := make(map[string]struct{})
	for d, def := range defs {
		var names []string
		ignored := make(map[string]*plan.IndexDef)
		for _, idx := range def.Indexes {
			if _, seen := ignored[idx.IndexName]; !seen {
				names = append(names, idx.IndexName)
				ignored[idx.IndexName] = nil
			}
			if idx.IndexAlgoTableType == catalog.IndexIgnored_TblType && idx.TableExist {
				ignored[idx.IndexName] = idx
			}
		}
		for _, name := range names {
			job, hasJob := jobs[indexJobKey{tableID: def.TblId, name: indexJobName(name)}]
			idx := ignored[name]
			if !hasJob && idx == nil {
				continue
			}
			state, errMsg, syncedAt := "CAST(NULL AS VARCHAR)", "CAST(NULL AS VARCHAR)", "FROM_UNIXTIME(NULL)"
			if hasJob {
				state = sqlquote.String(job.stateName())
				if job.errMsg != "" {
					errMsg = sqlquote.String(job.errMsg)
				}
				if physical := watermarkPhysical(job.watermark); physical > 0 {
					syncedAt = fmt.Sprintf("FROM_UNIXTIME(%d DIV 1000000000)", physical)
				}
			}
			ignoredRows, maxBadRecords := "CAST(NULL AS BIGINT)", "CAST(NULL AS BIGINT)"
			if idx != nil {
				limit, err := catalog.IndexParamMaxBadRecords(idx.IndexAlgoParams)
				if err != nil {
					return "", nil, err
				}
				ignoredRows = fmt.Sprintf("(SELECT COUNT(*) FROM %s)", sqlquote.QualifiedIdent(dbName, idx.IndexTableName))
				maxBadRecords = fmt.Sprintf("CAST(%d AS BIGINT)", limit)
				ignoredTables[idx.IndexTableName] = struct{}{}
			}
			selects = append(selects, fmt.Sprintf(
				"SELECT CAST(%s AS VARCHAR) AS `Key_name`, CAST(%s AS VARCHAR) AS `Table`, %s AS `State`, %s AS `Error_message`, %s AS `Ignored_rows`, %s AS `Max_bad_records`, %s AS `Synced_at`",
				sqlquote.String(name), sqlquote.String(tableNames[d]), state, errMsg, ignoredRows, maxBadRecords, syncedAt))
		}
	}
	sql := showIndexStatusEmpty
	if len(selects) > 0 {
		sql = strings.Join(selects, " UNION ALL ")
	}
	return sql, ignoredTables, nil
}

// watermarkPhysical returns the physical time of a watermark "physical-logical", or 0 when it
// is not a time the index is synced to: a job stopped before its first sync records the
// largest timestamp.
func watermarkPhysical(watermark string) int64 {
	physical, _, ok := strings.Cut(watermark, "-")
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(physical, 10, 64)
	if err != nil || n == math.MaxInt64 {
		return 0
	}
	return n
}

// indexJobName is the name of the sync job of index name.
func indexJobName(name string) string {
	return "index_" + name
}

// stateName is how SHOW INDEX STATUS shows the state of a job.
func (j indexJob) stateName() string {
	switch {
	case j.state == catalog.MoISCPJobStateError:
		return "stopped"
	case j.errCode != 0:
		return "retrying"
	}
	return "running"
}

// loadIndexJobs reads the latest sync job of each index of the tables defs from mo_iscp_log,
// which the system account keeps for every account.
func loadIndexJobs(ctx CompilerContext, defs []*TableDef) (map[indexJobKey]indexJob, error) {
	jobs := make(map[indexJobKey]indexJob)
	ids := make([]string, 0, len(defs))
	for _, def := range defs {
		ids = append(ids, strconv.FormatUint(def.TblId, 10))
	}
	accountID, err := defines.GetAccountId(ctx.GetContext())
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(
		"SELECT table_id, job_name, job_id, job_state, watermark, "+
			"CAST(json_unquote(json_extract(job_status, '$.ErrorCode')) AS SIGNED), json_unquote(json_extract(job_status, '$.ErrorMsg')) "+
			"FROM %s.%s WHERE account_id = %d AND table_id IN (%s) AND drop_at IS NULL",
		catalog.MO_CATALOG, catalog.MO_ISCP_LOG, accountID, strings.Join(ids, ","))
	res, err := runSqlWithSnapshot(ctx, sql, &Snapshot{Tenant: &plan.SnapshotTenant{TenantID: catalog.System_Account}})
	if err != nil {
		return nil, err
	}
	defer res.Close()
	for _, bat := range res.Batches {
		for i := 0; i < bat.RowCount(); i++ {
			key := indexJobKey{
				tableID: vector.GetFixedAtNoTypeCheck[uint64](bat.Vecs[0], i),
				name:    bat.Vecs[1].GetStringAt(i),
			}
			job := indexJob{
				id:        vector.GetFixedAtNoTypeCheck[uint64](bat.Vecs[2], i),
				state:     vector.GetFixedAtNoTypeCheck[int8](bat.Vecs[3], i),
				watermark: bat.Vecs[4].GetStringAt(i),
			}
			if !bat.Vecs[5].IsNull(uint64(i)) {
				job.errCode = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[5], i)
			}
			if !bat.Vecs[6].IsNull(uint64(i)) {
				job.errMsg = bat.Vecs[6].GetStringAt(i)
			}
			if old, ok := jobs[key]; !ok || job.id > old.id {
				jobs[key] = job
			}
		}
	}
	return jobs, nil
}
