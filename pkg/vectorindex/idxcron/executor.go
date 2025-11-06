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
	LastUpdateAt types.Timestamp
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

		return exec.run(ctx)
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

func (e *IndexUpdateTaskExecutor) getTasks(ctx context.Context) ([]IndexUpdateTaskInfo, error) {

	tasks := make([]IndexUpdateTaskInfo, 0, 16)

	err := sqlexec.RunTxnWithSqlContext(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID,
		catalog.System_Account, 5*time.Minute, nil, nil,
		func(sqlproc *sqlexec.SqlProcess, data any) error {

			sql := "SELECT db_name, table_name, index_name, action, account_id, table_id, metadata from mo_catalog.mo_index_update"
			res, err := sqlexec.RunSql(sqlproc, sql)
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

				for i := 0; i < bat.RowCount(); i++ {
					dbname := dbvec.GetStringAt(i)
					tblname := tblvec.GetStringAt(i)
					idxname := idxvec.GetStringAt(i)
					action := actionvec.GetStringAt(i)
					accountId := vector.GetFixedAtWithTypeCheck[uint32](accountvec, i)
					tableId := vector.GetFixedAtWithTypeCheck[uint64](tblidvec, i)
					metadata := (*sqlexec.Metadata)(nil)
					if !metavec.IsNull(uint64(i)) {
						bytes := metavec.GetRawBytesAt(i)
						metadata, err = sqlexec.NewMetadata(bytes)
						if err != nil {
							return err
						}
					}

					tasks = append(tasks, IndexUpdateTaskInfo{DbName: dbname,
						TableName: tblname,
						IndexName: idxname,
						Action:    action,
						AccountId: accountId,
						TableId:   tableId,
						Metadata:  metadata})
				}
			}

			os.Stderr.WriteString(fmt.Sprintf("TASKS %v\n", tasks))

			return nil
		})

	return tasks, err
}

// The optimal number of LISTS is estimated the the formula below:
// For datasets with less than one million rows, use lists = rows / 1000.
// For datasets with more than one million rows, use lists = sqrt(rows).
//
// Faiss guidelines suggest using between 30 * nlist and 256 * nlist vectors for training, ideally from a representative sample of your data.
//
// we should estimate with the parameter kmeans_train_percent
// say kmeans_train_percent is 1% (default) LISTS=1000 and Recommended samples count is between 30000 and 256000,
//
// Case 1:
// e.g. 1 million vectors and last iteration is 10K vectors
// #training_data = 1% x 1 million = 10000.  Not enough training data and perform re-index
//
// Case 2:
// e.g. 5 million vectors and last iteration is 10K vectors
// #training_data = 1% x 5 million = 50000.  Minimum requirement 30000 samples met but last iteration didn't meet requirement, so perform re-index
//
// Case 3:
// e.g. 5 million vectors and last iteration is 3 million vectors
// #training_data = 1% x 5 million = 50000.  Minimum requirement 30000 samples met and last iteration also meet requirement, so skip re-index
//
// Case 4:
// e.g. 30 million vectors
// #training_data = 1% x 30 million= 300000.  Exceed Faiss recommendation, skip re-index if last iteration also meet requirement.
func runIvfflatReindex(ctx context.Context, txnEngine engine.Engine, txnClient client.TxnClient, cnUUID string, task IndexUpdateTaskInfo) (err error) {

	if len(task.IndexName) == 0 {
		return moerr.NewInternalErrorNoCtx("table index name is empty string. skip reindex.")
	}

	resolveVariableFunc := (func(string, bool, bool) (any, error))(nil)
	if task.Metadata != nil {
		resolveVariableFunc = task.Metadata.ResolveVariableFunc
	}

	err = sqlexec.RunTxnWithSqlContext(ctx, txnEngine, txnClient, cnUUID,
		task.AccountId, 24*time.Hour, resolveVariableFunc, nil,
		func(sqlproc *sqlexec.SqlProcess, data any) (err2 error) {

			sqlCtx := sqlproc.SqlCtx
			txnOp := sqlCtx.Txn()

			// get indexdef
			db, err2 := txnEngine.Database(sqlproc.GetContext(), task.DbName, txnOp)
			if err2 != nil {
				return
			}

			rel, err2 := db.Relation(sqlproc.GetContext(), task.TableName, nil)
			if err2 != nil {
				return
			}

			tableDef := rel.CopyTableDef(sqlproc.GetContext())
			if rel.GetTableID(sqlproc.GetContext()) != task.TableId {
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

			// run alter table alter reindex
			sql := fmt.Sprintf("ALTER TABLE `%s`.`%s` ALTER REINDEX `%s` IVFFLAT LISTS=%d", task.DbName, task.TableName, task.IndexName, lists)
			res, err2 := sqlexec.RunSql(sqlproc, sql)
			if err2 != nil {
				return
			}
			defer res.Close()

			return
		})

	return
}

func runFulltextBatchDelete(ctx context.Context, txnEngine engine.Engine, txnClient client.TxnClient, cnUUID string, task IndexUpdateTaskInfo) (err error) {
	return moerr.NewInternalErrorNoCtx("fulltext batch delete not implemented yet")
}

func (e *IndexUpdateTaskExecutor) run(ctx context.Context) (err error) {

	os.Stderr.WriteString("IndexUpdateTaskExecutor RUN TASK\n")

	tasks, err := e.getTasks(ctx)
	if err != nil {
		return err
	}

	// do the maintenance such as ivfflat re-index, fulltext batch_delete
	for _, t := range tasks {
		var err2 error

		switch t.Action {
		case Action_Ivfflat_Reindex:
			err2 = runIvfflatReindex(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID, t)
		case Action_Fulltext_BatchDelete:
			err2 = runFulltextBatchDelete(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID, t)
		default:
			err2 = moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid index update action %v", t))
		}

		sqlexec.RunTxnWithSqlContext(ctx, e.txnEngine, e.cnTxnClient, e.cnUUID,
			catalog.System_Account, 5*time.Minute, nil, nil,
			func(sqlproc *sqlexec.SqlProcess, data any) error {
				if err2 != nil {
					// save error status to db

				} else {
					// save success status to db

				}
				return nil
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
