package idxcron

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
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
| cfg            | JSON(0)             | NO   |      | NULL    |       |         |
| status         | JSON(0)             | NO   |      | NULL    |       |         |
| create_at      | TIMESTAMP(0)        | NO   |      | NULL    |       |         |
| last_update_at | TIMESTAMP(0)        | YES  |      | NULL    |       |         |
+----------------+---------------------+------+------+---------+-------+---------+
*/
const (
	Action_Ivfflat_Reindex      = "ivfflat_reindex"
	Action_Fulltext_BatchDelete = "fulltext_batch_delete"
)

var running atomic.Bool

type IndexUpdateTaskInfo struct {
	DbName    string
	TableName string
	IndexName string
	Action    string
	AccountId uint32
	TableId   int64
	Config    []byte
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
			insertsql := `REPLACE INTO mo_catalog.mo_index_update values (0, 1, "db", "table", "idx", "ivfflat_reindex", '{"kmeans_train_percent":10, "kmeans_max_iteration":4, "ivf_threads_build":23}', '{}', NOW(), NOW())`
			_, err := sqlexec.RunSql(sqlproc, insertsql)
			if err != nil {
				return err
			}

			sql := "SELECT db_name, table_name, index_name, action, account_id, table_id, cfg from mo_catalog.mo_index_update"
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
				cfgvec := bat.Vecs[6]

				for i := 0; i < bat.RowCount(); i++ {
					dbname := dbvec.GetStringAt(i)
					tblname := tblvec.GetStringAt(i)
					idxname := idxvec.GetStringAt(i)
					action := actionvec.GetStringAt(i)
					accountId := vector.GetFixedAtWithTypeCheck[uint32](accountvec, i)
					tableId := vector.GetFixedAtWithTypeCheck[int64](tblidvec, i)
					config := []byte(nil)
					if !cfgvec.IsNull(uint64(i)) {
						config = cfgvec.GetRawBytesAt(i)
					}

					tasks = append(tasks, IndexUpdateTaskInfo{DbName: dbname,
						TableName: tblname,
						IndexName: idxname,
						Action:    action,
						AccountId: accountId,
						TableId:   tableId,
						Config:    config})
				}
			}

			os.Stderr.WriteString(fmt.Sprintf("TASKS %v\n", tasks))

			return nil
		})

	return tasks, err
}

func convertByteJson2ResolveVariableFunc(config []byte) func(string, bool, bool) (any, error) {

	return func(varName string, isSystemVar, isGlobalVar bool) (any, error) {

		if config == nil {
			return nil, nil
		}

		var bj bytejson.ByteJson
		if err := bj.Unmarshal(config); err != nil {
			return nil, err
		}

		path, err := bytejson.ParseJsonPath("$." + varName)
		if err != nil {
			return nil, err
		}

		out := bj.QuerySimple([]*bytejson.Path{&path})
		if out.IsNull() {
			return nil, moerr.NewInternalErrorNoCtx("value is null")
		}

		// assume all value are int64
		value := out.GetInt64()
		return value, nil
	}
}

func runIvfflatReindex(ctx context.Context, txnEngine engine.Engine, txnClient client.TxnClient, cnUUID string, task IndexUpdateTaskInfo) (err error) {
	resolveVariableFunc := convertByteJson2ResolveVariableFunc(task.Config)
	val, err := resolveVariableFunc("kmeans_train_percent", false, false)
	if err != nil {
		return err
	}

	os.Stderr.WriteString(fmt.Sprintf("kmeans_train_percent %d\n", val.(int64)))

	val, err = resolveVariableFunc("kmeans_max_iteration", false, false)
	if err != nil {
		return err
	}
	os.Stderr.WriteString(fmt.Sprintf("kmeans_max_iteration %d\n", val.(int64)))

	val, err = resolveVariableFunc("ivf_threads_build", false, false)
	if err != nil {
		return err
	}
	os.Stderr.WriteString(fmt.Sprintf("ivf_threads_build %d\n", val.(int64)))

	return nil
}

func runFulltextBatchDelete(ctx context.Context, txnEngine engine.Engine, txnClient client.TxnClient, cnUUID string, task IndexUpdateTaskInfo) (err error) {
	return nil
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

		if err2 != nil {
			// save error status to db

		} else {
			// save success status to db

		}
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
