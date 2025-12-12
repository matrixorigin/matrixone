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
	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	Action_Ivfflat_Reindex = "ivfflat_reindex"
	Action_Wildcard        = "*"

	Status_Error   = "error"
	Status_Ok      = "ok"
	Status_Skipped = "skipped"

	OneWeek                 = 24 * 7 * time.Hour
	KmeansTrainPercentParam = "kmeans_train_percent"
	KmeansMaxIterationParam = "kmeans_max_iteration"

	Reason_Skipped = "skipped"
)

var (
	runSaveStatusSql     = sqlexec.RunSql
	runGetTasksSql       = sqlexec.RunSql
	runReindexSql        = sqlexec.RunSql
	runGetCountSql       = sqlexec.RunSql
	runTxnWithSqlContext = sqlexec.RunTxnWithSqlContext
	runCmdSql            = sqlexec.RunSql

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
	Status string    `json:"status"`
	Msg    string    `json:"msg,omitempty"`
	Time   time.Time `json:"time,omitempty"`
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
func (t *IndexUpdateTaskInfo) checkIndexUpdatable(ctx context.Context, dsize uint64, nlist int64, interval time.Duration) (ok bool, reason string, err error) {
	now := time.Now()
	createdAt := time.Unix(t.CreatedAt.Unix(), 0)
	ts := createdAt.Add(interval)
	if ts.After(now) {
		// skip update when createdAt + delay is after current time
		reason = fmt.Sprintf("current time < interval after createdAt (%v < %v)", createdAt.Format("2006-01-02 15:04:05"), ts.Format("2006-01-02 15:04:05"))
		return
	}

	// If data size is smaller than nlist, skip the reindex
	if dsize < uint64(nlist) {
		reason = fmt.Sprintf("source data size < Nlist (%d < %d)", dsize, nlist)
		return
	}

	lower := float64(30 * nlist)
	upper := float64(256 * nlist)

	if t.Metadata == nil {
		ok = true
		return
	}

	v, err := t.Metadata.ResolveVariableFunc(KmeansTrainPercentParam, false, true)
	if err != nil {
		return
	}
	ivf_train_percent := v.(float64)

	nsample := float64(dsize) * (ivf_train_percent / 100)

	if nsample < lower {
		ok = true
		return
	} else if nsample < upper {
		// reindex every week
		if t.LastUpdateAt == nil {
			ok = true
			return
		}

		ts = time.Unix(t.LastUpdateAt.Unix(), 0)
		ts = ts.Add(interval)
		if ts.After(now) {
			reason = fmt.Sprintf("training sample size in between lower and upper limit (%f < %f < %f) AND current time < interval after lastUpdatedAt (%v < %v)",
				lower, nsample, upper, now.Format("2006-01-02 15:04:05"), ts.Format("2006-01-02 15:04:05"))
			return
		} else {
			// update
			ok = true
			return
		}

	} else {
		// reindex every week
		if t.LastUpdateAt != nil {
			ts = time.Unix(t.LastUpdateAt.Unix(), 0)
			ts = ts.Add(2 * interval)
			if ts.After(now) {
				reason = fmt.Sprintf("training sample size > upper limit ( %f > %f) AND current time < 2*interval after lastUpdatedAt (%v < %v)",
					nsample, upper, now.Format("2006-01-02 15:04:05"), ts.Format("2006-01-02 15:04:05"))
				return
			}
		}

		// reindex every week and limit nsample to upper bound
		ratio := (upper / float64(dsize)) * 100
		err = t.Metadata.Modify(KmeansTrainPercentParam, ratio)
		if err != nil {
			return
		}

		ok = true
		return
	}
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

// return status as SQL to update mo_index_update
func runIvfflatReindex(ctx context.Context,
	txnEngine engine.Engine,
	txnClient client.TxnClient,
	cnUUID string,
	task *IndexUpdateTaskInfo,
	currentHour int) (updated bool, reason string, err error) {

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
			auto_update := false
			interval := OneWeek
			hour := int64(0)
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
						day := int64(0)
						day, err2 = dayAst.Int64()
						if err2 != nil {
							return err2
						}

						// interval in Day
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
			}

			if lists == 0 {
				return moerr.NewInternalErrorNoCtx("IVFFLAT index parameter LISTS not found")
			}

			if !auto_update || interval == 0 || currentHour != int(hour) {
				reason = Reason_Skipped
				return
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

			ok := false
			ok, reason, err2 = task.checkIndexUpdatable(ctx, dsize, lists, interval)
			if err2 != nil {
				return
			}
			if !ok {
				// skip the update
				return
			}

			// run alter table alter reindex in force synchronous mode to make sure to build index in single transaction
			sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` ALTER REINDEX `%s` IVFFLAT FORCE_SYNC", task.DbName, task.TableName, task.IndexName)
			res, err2 = runReindexSql(sqlproc, sql)
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

		switch t.Action {
		case Action_Ivfflat_Reindex:
			updated, reason, err2 = runIvfflatReindex(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID, t, currentHour)
		default:
			err2 = moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid index update action %v", t))
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
