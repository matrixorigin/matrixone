// Copyright 2022 Matrix Origin
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

package idxcron

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// findReindexAlgo locates the plugin whose SyncDescriptor.IdxcronAction
// matches the cron task's action string. Returns (plugin, true) on a
// hit; the caller reads IdxcronAlgoToken / IdxcronListsAware off
// plugin.Catalog().SyncDescriptor() and dispatches the per-algo
// rebuild gate via plugin.Idxcron().Updatable(). Replaces the
// hardcoded action switch — new algorithms participate in idxcron by
// setting their SyncDescriptor fields and providing an Idxcron hook,
// no edits needed here.
func findReindexAlgo(action string) (indexplugin.AlgoPlugin, bool) {
	for _, p := range indexplugin.All() {
		if p.Catalog().SyncDescriptor().IdxcronAction == action {
			return p, true
		}
	}
	return nil, false
}

/*
+----------------+---------------------+------+------+---------+-------+---------+
| Field          | Type                | Null | Key  | Default | Extra | Comment |
+----------------+---------------------+------+------+---------+-------+---------+
| account_id     | INT UNSIGNED(32)    | NO   | PRI  | NULL    |       |         |
| table_id       | BIGINT UNSIGNED(64) | NO   | PRI  | NULL    |       |         |
| db_name        | VARCHAR(65535)      | NO   | PRI  | NULL    |       |         |
| table_name     | VARCHAR(65535)      | NO   | PRI  | NULL    |       |         |
| index_name     | VARCHAR(65535)      | NO   | PRI  | NULL    |       |         |
| action         | VARCHAR(65535)      | NO   | PRI  | NULL    |       |         |
| metadata       | JSON(0)             | NO   |      | NULL    |       |         |
| status         | JSON(0)             | NO   |      | NULL    |       |         |
| create_at      | TIMESTAMP(0)        | NO   |      | NULL    |       |         |
| last_update_at | TIMESTAMP(0)        | YES  |      | NULL    |       |         |
+----------------+---------------------+------+------+---------+-------+---------+
*/
const (
	Action_Ivfflat_Reindex = "ivfflat_reindex"
	Action_Wildcard        = "*"

	Status_Error   = "error"
	Status_Ok      = "ok"
	Status_Skipped = "skipped"

	OneWeek = 24 * 7 * time.Hour

	Reason_Skipped = "skipped"
)

var (
	runSaveStatusSql     = sqlexec.RunSql
	runGetTasksSql       = sqlexec.RunSql
	runReindexSql        = sqlexec.RunSql
	runTxnWithSqlContext = sqlexec.RunTxnWithSqlContext
	runCmdSql            = sqlexec.RunSql

	getTableDef = getTableDefFunc
	getTasks    = getTasksFunc
)

var running atomic.Bool

type IndexUpdateTaskInfo struct {
	DbName       string
	TableName    string
	IndexName    string
	Action       string
	AccountId    uint32
	TableId      uint64
	Metadata     *sqlexec.Metadata
	Status       []byte
	CreatedAt    types.Timestamp
	LastUpdateAt *types.Timestamp
}

type IndexUpdateStatus struct {
	Status string    `json:"status"`
	Msg    string    `json:"msg,omitempty"`
	Time   time.Time `json:"time,omitempty"`
}

func (t *IndexUpdateTaskInfo) saveStatus(sqlproc *sqlexec.SqlProcess, updated bool, reason string, err error) error {

	statussqlfmt := "UPDATE mo_catalog.mo_index_update SET status = '%s' WHERE table_id = %d AND account_id = %d AND action = '%s'"
	updatesqlfmt := "UPDATE mo_catalog.mo_index_update SET last_update_at = now(), status = '%s' WHERE table_id = %d AND account_id = %d AND action = '%s'"

	// skip update status if index is NOT updated
	status := IndexUpdateStatus{Time: time.Now()}
	var sqlfmt string

	if err != nil {
		// save error status column to mo_index_update
		status.Status = Status_Error
		status.Msg = err.Error()
		sqlfmt = statussqlfmt
	} else if !updated {
		status.Status = Status_Skipped
		status.Msg = reason
		sqlfmt = statussqlfmt
	} else {
		status.Status = Status_Ok
		status.Msg = "reindex success"
		sqlfmt = updatesqlfmt
	}

	bytes, err := sonic.Marshal(&status)
	if err != nil {
		return err
	}

	// update status
	sql := fmt.Sprintf(sqlfmt,
		string(bytes), t.TableId, t.AccountId, t.Action)
	res, err2 := runSaveStatusSql(sqlproc, sql)
	if err2 != nil {
		return err2
	}
	res.Close()

	return nil
}

type IndexUpdateTaskExecutor struct {
	ctx         context.Context
	cancelFunc  context.CancelFunc
	cnUUID      string
	txnEngine   engine.Engine
	cnTxnClient client.TxnClient
	mp          *mpool.MPool
}

func IndexUpdateTaskExecutorFactory(
	cnUUID string,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	mp *mpool.MPool,
) func(ctx context.Context, task task.Task) (err error) {
	return func(ctx context.Context, task task.Task) (err error) {

		if !running.CompareAndSwap(false, true) {
			return nil
		}

		defer func() {
			running.Store(false)
		}()

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
		exec, err := NewIndexUpdateTaskExecutor(
			ctx,
			cnUUID,
			txnEngine,
			cnTxnClient,
			mp,
		)
		if err != nil {
			return
		}

		err = exec.run(ctx)
		return
	}
}

func NewIndexUpdateTaskExecutor(
	ctx context.Context,
	cnUUID string,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	mp *mpool.MPool) (*IndexUpdateTaskExecutor, error) {

	exec := &IndexUpdateTaskExecutor{
		cnUUID:      cnUUID,
		txnEngine:   txnEngine,
		cnTxnClient: cnTxnClient,
		mp:          mp,
	}
	exec.ctx, exec.cancelFunc = context.WithCancel(ctx)
	return exec, nil
}

func getTasksFunc(
	ctx context.Context,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cnUUID string) ([]*IndexUpdateTaskInfo, error) {

	tasks := make([]*IndexUpdateTaskInfo, 0, 16)

	err := runTxnWithSqlContext(ctx, txnEngine, cnTxnClient, cnUUID,
		catalog.System_Account, 5*time.Minute, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, data any) error {

			sql := "SELECT db_name, table_name, index_name, action, account_id, table_id, metadata, last_update_at, create_at from mo_catalog.mo_index_update"
			res, err := runGetTasksSql(sqlproc, sql)
			if err != nil {
				return err
			}
			defer res.Close()

			for _, bat := range res.Batches {
				dbvec := bat.Vecs[0]
				tblvec := bat.Vecs[1]
				idxvec := bat.Vecs[2]
				actionvec := bat.Vecs[3]
				accountvec := bat.Vecs[4]
				tblidvec := bat.Vecs[5]
				metavec := bat.Vecs[6]
				lastupdatevec := bat.Vecs[7]
				createdAtVec := bat.Vecs[8]

				for i := 0; i < bat.RowCount(); i++ {
					dbname := dbvec.GetStringAt(i)
					tblname := tblvec.GetStringAt(i)
					idxname := idxvec.GetStringAt(i)
					action := actionvec.GetStringAt(i)
					accountId := vector.GetFixedAtWithTypeCheck[uint32](accountvec, i)
					tableId := vector.GetFixedAtWithTypeCheck[uint64](tblidvec, i)
					createdAt := vector.GetFixedAtWithTypeCheck[types.Timestamp](createdAtVec, i)

					metadata := (*sqlexec.Metadata)(nil)
					lastupdate := (*types.Timestamp)(nil)

					if !metavec.IsNull(uint64(i)) {
						bytes := metavec.CloneBytesAt(i)
						metadata, err = sqlexec.NewMetadata(bytes)
						if err != nil {
							return err
						}
					}

					if !lastupdatevec.IsNull(uint64(i)) {
						ts := vector.GetFixedAtWithTypeCheck[types.Timestamp](lastupdatevec, i)
						lastupdate = &ts
					}

					tasks = append(tasks, &IndexUpdateTaskInfo{DbName: dbname,
						TableName:    tblname,
						IndexName:    idxname,
						Action:       action,
						AccountId:    accountId,
						TableId:      tableId,
						Metadata:     metadata,
						LastUpdateAt: lastupdate,
						CreatedAt:    createdAt})

				}
			}

			return nil
		})

	return tasks, err
}

func getTableDefFunc(sqlproc *sqlexec.SqlProcess, txnEngine engine.Engine, dbname string, tablename string) (tableDef *plan.TableDef, err error) {

	sqlCtx := sqlproc.SqlCtx
	txnOp := sqlCtx.Txn()

	// get indexdef
	db, err := txnEngine.Database(sqlproc.GetContext(), dbname, txnOp)
	if err != nil {
		return
	}

	rel, err := db.Relation(sqlproc.GetContext(), tablename, nil)
	if err != nil {
		return
	}

	tableDef = rel.CopyTableDef(sqlproc.GetContext())
	return
}

// runReindex is the shared per-task body the cron executor invokes for
// any algorithm whose SyncDescriptor declares an IdxcronAction. The
// descriptor's IdxcronAlgoToken decides which keyword the eventual
// ALTER REINDEX SQL uses, and IdxcronListsAware gates the IVF-FLAT
// nlist / training-sample heuristic.
//
// Lifted from the IVF-FLAT-specific runIvfflatReindex; the body is
// algorithm-agnostic apart from the listsAware branches.
func runReindex(ctx context.Context,
	txnEngine engine.Engine,
	txnClient client.TxnClient,
	cnUUID string,
	task *IndexUpdateTaskInfo,
	currentHour int,
	p indexplugin.AlgoPlugin) (updated bool, reason string, err error) {

	d := p.Catalog().SyncDescriptor()

	if len(task.IndexName) == 0 {
		err = moerr.NewInternalErrorNoCtx("table index name is empty string. skip reindex.")
		return
	}
	if d.IdxcronAlgoToken == "" {
		err = moerr.NewInternalErrorNoCtxf("idxcron: empty IdxcronAlgoToken for action %q", task.Action)
		return
	}

	resolveVariableFunc := (func(string, bool, bool) (any, error))(nil)
	if task.Metadata != nil {
		resolveVariableFunc = task.Metadata.ResolveVariableFunc
	}

	err = runTxnWithSqlContext(ctx, txnEngine, txnClient, cnUUID,
		task.AccountId, 24*time.Hour, resolveVariableFunc, nil,
		func(sqlproc *sqlexec.SqlProcess, data any) (err2 error) {

			tableDef, err2 := getTableDef(sqlproc, txnEngine, task.DbName, task.TableName)
			if err2 != nil {
				return
			}

			if tableDef.TblId != task.TableId {
				return moerr.NewInternalErrorNoCtx("table id mimstach")
			}

			// Read cadence knobs from indexAlgoParams. Re-read every
			// tick so users can ALTER the index to change
			// auto_update/day/hour without re-registering the cron task.
			auto_update := false
			interval := OneWeek
			hour := int64(0)
			for _, idx := range tableDef.Indexes {
				if idx.IndexName != task.IndexName {
					continue
				}
				autoUpdateAst, err2 := sonic.Get([]byte(idx.IndexAlgoParams), catalog.AutoUpdate)
				if err2 == nil {
					auto_update_str, err2 := autoUpdateAst.StrictString()
					if err2 != nil {
						return err2
					}
					if auto_update_str == "true" {
						auto_update = true
					}
				}

				dayAst, err2 := sonic.Get([]byte(idx.IndexAlgoParams), catalog.Day)
				if err2 == nil {
					day, err2 := dayAst.Int64()
					if err2 != nil {
						return err2
					}
					if day > 0 {
						interval = time.Duration(day) * 24 * time.Hour
					}
				}

				hourAst, err2 := sonic.Get([]byte(idx.IndexAlgoParams), catalog.Hour)
				if err2 == nil {
					hour, err2 = hourAst.Int64()
					if err2 != nil {
						return err2
					}
				}
				break
			}

			if !auto_update || interval == 0 || currentHour != int(hour) {
				reason = Reason_Skipped
				return
			}

			// Universal createdAt + interval gate. The per-algo hook
			// owns everything beyond (algorithm-specific cadence,
			// minimum-data checks, metadata mutation).
			now := time.Now()
			createdAt := time.Unix(task.CreatedAt.Unix(), 0)
			if createdAt.Add(interval).After(now) {
				reason = fmt.Sprintf("current time < interval after createdAt (%v < %v)",
					createdAt.Format("2006-01-02 15:04:05"),
					createdAt.Add(interval).Format("2006-01-02 15:04:05"))
				return
			}

			ok, reason2, err2 := p.Idxcron().Updatable(idxcronplugin.UpdatableInput{
				Sqlproc:      sqlproc,
				TableDef:     tableDef,
				IndexName:    task.IndexName,
				Metadata:     task.Metadata,
				CreatedAt:    task.CreatedAt,
				LastUpdateAt: task.LastUpdateAt,
				Interval:     interval,
			})
			if err2 != nil {
				return
			}
			reason = reason2
			if !ok {
				return
			}

			// run alter table alter reindex in force synchronous mode to make sure to build index in single transaction
			sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` ALTER REINDEX `%s` %s FORCE_SYNC",
				task.DbName, task.TableName, task.IndexName, d.IdxcronAlgoToken)
			res, err2 := runReindexSql(sqlproc, sql)
			if err2 != nil {
				return
			}
			res.Close()

			// mark reindex is performed
			updated = true
			return
		})

	return
}

func (e *IndexUpdateTaskExecutor) run(ctx context.Context) (err error) {

	logutil.Infof("IndexUpdateTaskExecutor START")
	currentHour := time.Now().Hour()

	defer func() {
		logutil.Infof("IndexUpdateTaskExecutor END")
	}()

	tasks, err := getTasks(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID)
	if err != nil {
		return err
	}

	// do the maintenance such as ivfflat re-index, fulltext batch_delete
	for _, t := range tasks {
		var (
			err2    error
			updated bool
			reason  string
		)

		select {
		case <-ctx.Done():
			return moerr.NewInternalError(ctx, "context cancelled")
		default:
		}

		p, ok := findReindexAlgo(t.Action)
		if !ok {
			err2 = moerr.NewInternalErrorNoCtxf("idxcron: no plugin registered for action %q (task=%v)", t.Action, t)
		} else {
			updated, reason, err2 = runReindex(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID, t, currentHour, p)
		}

		if !updated && reason == Reason_Skipped {
			// skip save Status when update was skipped
			continue
		}

		runTxnWithSqlContext(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID,
			catalog.System_Account, 5*time.Minute, nil, nil,
			func(sqlproc *sqlexec.SqlProcess, data any) error {

				return t.saveStatus(sqlproc, updated, reason, err2)
			})
	}

	return nil
}

var IndexUpdateTaskCronExpr = "0 0 * * * *" // run once an hour, beginning of hour
// var IndexUpdateTaskCronExpr = "0 55 23 * * *" // 23:55:00 everyday

// var IndexUpdateTaskCronExpr = "0 */5 * * * *" // every 5 minutes
// var IndexUpdateTaskCronExpr = "*/15 * * * * *" // every 15 seconds

const ParamSeparator = " "

func IndexUpdateTaskMetadata(id task.TaskCode, args ...string) task.TaskMetadata {
	return task.TaskMetadata{
		ID:       "IndexUpdateTask",
		Executor: id,
		Context:  []byte(strings.Join(args, ParamSeparator)),
		Options:  task.TaskOptions{Concurrency: 1},
	}
}
