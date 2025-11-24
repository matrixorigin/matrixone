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
	"os"
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

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
	Action_Ivfflat_Reindex      = "ivfflat_reindex"
	Action_Fulltext_BatchDelete = "fulltext_batch_delete"
	Action_Wildcard             = "*"

	Status_Error = "error"
	Status_Ok    = "ok"

	OneWeek                 = 24 * 7 * time.Hour
	KmeansTrainPercentParam = "kmeans_train_percent"
	KmeansMaxIterationParam = "kmeans_max_iteration"
)

var (
	runSaveStatusSql     = sqlexec.RunSql
	runGetTasksSql       = sqlexec.RunSql
	runReindexSql        = sqlexec.RunSql
	runGetCountSql       = sqlexec.RunSql
	runTxnWithSqlContext = sqlexec.RunTxnWithSqlContext

	// createdAt Delay update duration
	createdAtDelay = 2 * 24 * time.Hour

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
	Status string `json:"status"`
	Msg    string `json:"msg,omitempty"`
}

// The optimal number of LISTS is estimated the the formula below:
// For datasets with less than one million rows, use lists = rows / 1000.
// For datasets with more than one million rows, use lists = sqrt(rows).
//
// Faiss guidelines suggest using between 30 * nlist and 256 * nlist vectors for training, ideally from a representative sample of your data.
//
// Case 1: ivf_train_percent * dsize < 30 * nlist, always re-index
// Case 2: 30 * nlist < ivf_train_percent * dsize < 256 * nlist, re-index every week
// Case 3:  dsize > 256 * nlist, re-index every 1 week and ivf_train_percent = (256 * nlist) / dsize
func (t *IndexUpdateTaskInfo) checkIndexUpdatable(ctx context.Context, dsize uint64, nlist int64) (ok bool, err error) {

	now := time.Now()
	createdAt := time.Unix(t.CreatedAt.Unix(), 0)
	if createdAt.Add(createdAtDelay).After(now) {
		// skip update when createdAt + delay is after current time
		return false, nil
	}

	// If data size is smaller than nlist, skip the reindex
	if dsize < uint64(nlist) {
		return false, nil
	}

	lower := float64(30 * nlist)
	upper := float64(256 * nlist)

	if t.Metadata == nil {
		return true, nil
	}

	v, err := t.Metadata.ResolveVariableFunc(KmeansTrainPercentParam, false, true)
	if err != nil {
		return false, err
	}
	ivf_train_percent := v.(float64)

	nsample := float64(dsize) * (ivf_train_percent / 100)

	if nsample < lower {
		return true, nil
	} else if nsample < upper {
		// reindex every week
		if t.LastUpdateAt == nil {
			return true, nil
		}

		ts := time.Unix(t.LastUpdateAt.Unix(), 0)
		ts = ts.Add(OneWeek)
		if ts.After(now) {
			return false, nil
		} else {
			// update
			return true, nil
		}

	} else {
		// reindex every week
		if t.LastUpdateAt != nil {
			ts := time.Unix(t.LastUpdateAt.Unix(), 0)
			ts = ts.Add(OneWeek)
			if ts.After(now) {
				return false, nil
			}
		}

		// reindex every week and limit nsample to upper bound
		ratio := (upper / float64(dsize)) * 100
		err = t.Metadata.Modify(KmeansTrainPercentParam, ratio)
		if err != nil {
			return false, err
		}

		return true, nil
	}
}

func (t *IndexUpdateTaskInfo) saveStatus(sqlproc *sqlexec.SqlProcess, updated bool, err error) error {

	var status IndexUpdateStatus

	if err != nil {
		// save error status column to mo_index_update
		status.Status = Status_Error
		status.Msg = err.Error()

		os.Stderr.WriteString(fmt.Sprintf("save Status error %v\n", err))
		return nil
	} else {
		status.Status = Status_Ok
	}

	bytes, err := sonic.Marshal(&status)
	if err != nil {
		return err
	}

	// run status sql
	if updated {
		sql := fmt.Sprintf("UPDATE mo_catalog.mo_index_update SET last_update_at = now(), status = '%s' WHERE table_id = %d AND account_id = %d AND action = '%s'",
			string(bytes), t.TableId, t.AccountId, t.Action)
		res, err2 := runSaveStatusSql(sqlproc, sql)
		if err2 != nil {
			return err2
		}
		res.Close()
	}

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

			//tasks := data.([]*IndexUpdateTaskInfo)
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

	os.Stderr.WriteString(fmt.Sprintf("table id %d\n", rel.GetTableID(sqlproc.GetContext())))
	tableDef = rel.CopyTableDef(sqlproc.GetContext())
	return
}

// return status as SQL to update mo_index_update
func runIvfflatReindex(ctx context.Context,
	txnEngine engine.Engine,
	txnClient client.TxnClient,
	cnUUID string,
	task *IndexUpdateTaskInfo) (updated bool, err error) {

	if len(task.IndexName) == 0 {
		err = moerr.NewInternalErrorNoCtx("table index name is empty string. skip reindex.")
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

			// get number of list from indexDef
			lists := int64(0)
			for _, idx := range tableDef.Indexes {
				if idx.IndexName == task.IndexName {
					listsAst, err2 := sonic.Get([]byte(idx.IndexAlgoParams), catalog.IndexAlgoParamLists)
					if err2 != nil {
						return err2
					}
					lists, err2 = listsAst.Int64()
					if err2 != nil {
						return err2
					}
					break
				}
			}

			if lists == 0 {
				return moerr.NewInternalErrorNoCtx("IVFFLAT index parameter LISTS not found")
			}

			// get number of rows from source table
			cntsql := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", task.DbName, task.TableName)
			res, err2 := runGetCountSql(sqlproc, cntsql)
			if err2 != nil {
				return
			}
			defer res.Close()

			dsize := uint64(0)
			if len(res.Batches) > 0 {
				bat := res.Batches[0]
				if bat.RowCount() > 0 {
					cntvec := bat.Vecs[0]
					dsize = vector.GetFixedAtWithTypeCheck[uint64](cntvec, 0)
				}
			}

			ok, err := task.checkIndexUpdatable(ctx, dsize, lists)
			if err != nil {
				return err
			}
			if !ok {
				// skip the update
				return nil
			}

			// run alter table alter reindex
			sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` ALTER REINDEX `%s` IVFFLAT LISTS=%d", task.DbName, task.TableName, task.IndexName, lists)
			res, err2 = runReindexSql(sqlproc, sql)
			if err2 != nil {
				return
			}
			res.Close()

			os.Stderr.WriteString(sql)
			os.Stderr.WriteString(fmt.Sprintf("\ndsize = %d\n", dsize))
			// mark reindex is performed
			updated = true
			return
		})

	return
}

func runFulltextBatchDelete(ctx context.Context, txnEngine engine.Engine, txnClient client.TxnClient, cnUUID string, task *IndexUpdateTaskInfo) (updated bool, err error) {
	return updated, moerr.NewInternalErrorNoCtx("fulltext batch delete not implemented yet")
}

func (e *IndexUpdateTaskExecutor) run(ctx context.Context) (err error) {

	os.Stderr.WriteString("IndexUpdateTaskExecutor RUN TASK\n")

	tasks, err := getTasks(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID)
	if err != nil {
		return err
	}
	os.Stderr.WriteString(fmt.Sprintf("RUN OUT TASKS %v\n", len(tasks)))
	for _, t := range tasks {
		os.Stderr.WriteString(fmt.Sprintf("RUN OUT TASKS %v\n", t.Metadata))
	}

	// do the maintenance such as ivfflat re-index, fulltext batch_delete
	for _, t := range tasks {
		var err2 error
		var updated bool

		switch t.Action {
		case Action_Ivfflat_Reindex:
			updated, err2 = runIvfflatReindex(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID, t)
		case Action_Fulltext_BatchDelete:
			updated, err2 = runFulltextBatchDelete(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID, t)
		default:
			err2 = moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid index update action %v", t))
		}

		runTxnWithSqlContext(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID,
			catalog.System_Account, 5*time.Minute, nil, nil,
			func(sqlproc *sqlexec.SqlProcess, data any) error {

				return t.saveStatus(sqlproc, updated, err2)
			})
	}

	return nil
}

// var IndexUpdateTaskCronExpr = "0 0 24 * * *"
var IndexUpdateTaskCronExpr = "*/15 * * * * *"

const ParamSeparator = " "

func IndexUpdateTaskMetadata(id task.TaskCode, args ...string) task.TaskMetadata {
	return task.TaskMetadata{
		ID:       "IndexUpdateTask",
		Executor: id,
		Context:  []byte(strings.Join(args, ParamSeparator)),
		Options:  task.TaskOptions{Concurrency: 1},
	}
}
