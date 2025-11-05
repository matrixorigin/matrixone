// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func Test_newCdcSqlFormat(t *testing.T) {
	id, _ := uuid.Parse("019111fd-aed1-70c0-8760-9abadd8f0f4a")
	d := time.Date(2024, 8, 2, 15, 20, 0, 0, time.UTC)
	sql := cdc.CDCSQLBuilder.InsertTaskSQL(
		3,
		id.String(),
		"task1",
		"src uri",
		"123",
		"dst uri",
		"mysql",
		"456",
		"ca path",
		"cert path",
		"key path",
		"db1:t1",
		"xfilter",
		"op filters",
		"error",
		"common",
		"",
		"",
		"conf path",
		d,
		"running",
		125,
		true,
		"yyy",
		"{}",
	)
	wantSql := "INSERT INTO mo_catalog.mo_cdc_task VALUES(3,\"019111fd-aed1-70c0-8760-9abadd8f0f4a\",\"task1\",\"src uri\",\"123\",\"dst uri\",\"mysql\",\"456\",\"ca path\",\"cert path\",\"key path\",\"db1:t1\",\"xfilter\",\"op filters\",\"error\",\"common\",\"\",\"\",\"conf path\",\"2024-08-02 15:20:00\",\"running\",125,\"125\",\"true\",\"yyy\",'{}',\"\",\"\",\"\",\"\")"
	assert.Equal(t, wantSql, sql)

	sql2 := cdc.CDCSQLBuilder.GetTaskSQL(3, id.String())
	wantSql2 := "SELECT sink_uri, sink_type, sink_password, tables, filters, start_ts, end_ts, no_full, additional_config FROM mo_catalog.mo_cdc_task WHERE account_id = 3 AND task_id = \"019111fd-aed1-70c0-8760-9abadd8f0f4a\""
	assert.Equal(t, wantSql2, sql2)

	sql3 := cdc.CDCSQLBuilder.DeleteWatermarkSQL(13, "task1")
	wantSql3 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 13 AND task_id = 'task1'"
	assert.Equal(t, wantSql3, sql3)
}

func Test_getPatternTuples(t *testing.T) {
	//tables := []string{
	// - table level
	//  "db1.t1:db2.t2,db3.t3:db4.t4",
	//  "db1.t1,db3.t3:db4.t4",
	//  "db1.t1,db3.t3",
	//  "db1.t1:db2.t2,db1.t1:db4.t4", // error
	// - db level
	//  "db1:db2,db3:db4",
	//  "db1,db3:db4",
	// 	"db1,db3",
	// - account level
	//}

	type kase struct {
		tables  string
		level   string
		wantErr bool
		expect  *cdc.PatternTuples
	}

	kases := []kase{
		// table level
		{
			tables:  "db1.t1:db2.t2,db3.t3:db4.t4",
			level:   cdc.CDCPitrGranularity_Table,
			wantErr: false,
			expect: &cdc.PatternTuples{
				Pts: []*cdc.PatternTuple{
					{
						Source: cdc.PatternTable{
							Database: "db1",
							Table:    "t1",
						},
						Sink: cdc.PatternTable{
							Database: "db2",
							Table:    "t2",
						},
					},
					{
						Source: cdc.PatternTable{
							Database: "db3",
							Table:    "t3",
						},
						Sink: cdc.PatternTable{
							Database: "db4",
							Table:    "t4",
						},
					},
				},
			},
		},
		{
			tables:  "db1.t1,db3.t3:db4.t4",
			level:   cdc.CDCPitrGranularity_Table,
			wantErr: false,
			expect: &cdc.PatternTuples{
				Pts: []*cdc.PatternTuple{
					{
						Source: cdc.PatternTable{
							Database: "db1",
							Table:    "t1",
						},
						Sink: cdc.PatternTable{
							Database: "db1",
							Table:    "t1",
						},
					},
					{
						Source: cdc.PatternTable{
							Database: "db3",
							Table:    "t3",
						},
						Sink: cdc.PatternTable{
							Database: "db4",
							Table:    "t4",
						},
					},
				},
			},
		},
		{
			tables:  "db1.t1,db3.t3",
			level:   cdc.CDCPitrGranularity_Table,
			wantErr: false,
			expect: &cdc.PatternTuples{
				Pts: []*cdc.PatternTuple{
					{
						Source: cdc.PatternTable{
							Database: "db1",
							Table:    "t1",
						},
						Sink: cdc.PatternTable{
							Database: "db1",
							Table:    "t1",
						},
					},
					{
						Source: cdc.PatternTable{
							Database: "db3",
							Table:    "t3",
						},
						Sink: cdc.PatternTable{
							Database: "db3",
							Table:    "t3",
						},
					},
				},
			},
		},
		{
			tables:  "db1.t1:db2.t2,db1.t1:db4.t4",
			level:   cdc.CDCPitrGranularity_Table,
			wantErr: true,
		},

		// db level
		{
			tables:  "db1:db2,db3:db4",
			level:   cdc.CDCPitrGranularity_DB,
			wantErr: false,
			expect: &cdc.PatternTuples{
				Pts: []*cdc.PatternTuple{
					{
						Source: cdc.PatternTable{
							Database: "db1",
							Table:    cdc.CDCPitrGranularity_All,
						},
						Sink: cdc.PatternTable{
							Database: "db2",
							Table:    cdc.CDCPitrGranularity_All,
						},
					},
					{
						Source: cdc.PatternTable{
							Database: "db3",
							Table:    cdc.CDCPitrGranularity_All,
						},
						Sink: cdc.PatternTable{
							Database: "db4",
							Table:    cdc.CDCPitrGranularity_All,
						},
					},
				},
			},
		},
		{
			tables:  "db1,db3:db4",
			level:   cdc.CDCPitrGranularity_DB,
			wantErr: false,
			expect: &cdc.PatternTuples{
				Pts: []*cdc.PatternTuple{
					{
						Source: cdc.PatternTable{
							Database: "db1",
							Table:    cdc.CDCPitrGranularity_All,
						},
						Sink: cdc.PatternTable{
							Database: "db1",
							Table:    cdc.CDCPitrGranularity_All,
						},
					},
					{
						Source: cdc.PatternTable{
							Database: "db3",
							Table:    cdc.CDCPitrGranularity_All,
						},
						Sink: cdc.PatternTable{
							Database: "db4",
							Table:    cdc.CDCPitrGranularity_All,
						},
					},
				},
			},
		},
		{
			tables:  "db1,db3",
			level:   cdc.CDCPitrGranularity_DB,
			wantErr: false,
			expect: &cdc.PatternTuples{
				Pts: []*cdc.PatternTuple{
					{
						Source: cdc.PatternTable{
							Database: "db1",
							Table:    cdc.CDCPitrGranularity_All,
						},
						Sink: cdc.PatternTable{
							Database: "db1",
							Table:    cdc.CDCPitrGranularity_All,
						},
					},
					{
						Source: cdc.PatternTable{
							Database: "db3",
							Table:    cdc.CDCPitrGranularity_All,
						},
						Sink: cdc.PatternTable{
							Database: "db3",
							Table:    cdc.CDCPitrGranularity_All,
						},
					},
				},
			},
		},

		// account level
		{
			tables:  "",
			level:   cdc.CDCPitrGranularity_Account,
			wantErr: false,
			expect: &cdc.PatternTuples{
				Pts: []*cdc.PatternTuple{
					{
						Source: cdc.PatternTable{
							Database: cdc.CDCPitrGranularity_All,
							Table:    cdc.CDCPitrGranularity_All,
						},
						Sink: cdc.PatternTable{
							Database: cdc.CDCPitrGranularity_All,
							Table:    cdc.CDCPitrGranularity_All,
						},
					},
				},
			},
		},
	}

	isSame := func(pt0, pt1 *cdc.PatternTuples) {
		assert.Equal(t, len(pt0.Pts), len(pt1.Pts))
		for i := 0; i < len(pt0.Pts); i++ {
			assert.Equal(t, pt0.Pts[i].Source.Database, pt1.Pts[i].Source.Database)
			assert.Equal(t, pt0.Pts[i].Source.Table, pt1.Pts[i].Source.Table)
			assert.Equal(t, pt0.Pts[i].Sink.Database, pt1.Pts[i].Sink.Database)
			assert.Equal(t, pt0.Pts[i].Sink.Table, pt1.Pts[i].Sink.Table)
		}
	}

	for _, tkase := range kases {
		pts, err := CDCParsePitrGranularity(context.Background(), tkase.level, tkase.tables)
		if tkase.wantErr {
			assert.Errorf(t, err, tkase.tables)
		} else {
			assert.NoErrorf(t, err, tkase.tables)
			isSame(pts, tkase.expect)
		}
	}
}

func Test_handleCreateCdc(t *testing.T) {
	type args struct {
		ses     *Session
		execCtx *ExecCtx
		create  *tree.CreateCDC
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql4 := "INSERT INTO mo_catalog.mo_cdc_task .*"
	mock.ExpectExec(sql4).WillReturnResult(sqlmock.NewResult(1, 1))

	pu := config.ParameterUnit{}
	pu.TaskService = &testTaskService{
		db: db,
	}
	setPu("", &pu)

	create := &tree.CreateCDC{
		IfNotExists: false,
		TaskName:    "task1",
		SourceUri:   "mysql://root:111@127.0.0.1:6001",
		SinkType:    cdc.CDCSinkType_MySQL,
		SinkUri:     "mysql://root:111@127.0.0.1:3306",
		Tables:      "db1.t1:db1.t1,db1.t2",
		Option: []string{
			cdc.CDCRequestOptions_Level,
			cdc.CDCPitrGranularity_Table,
			"Account",
			sysAccountName,
			cdc.CDCRequestOptions_Exclude,
			"db2.t3,db2.t4",
			cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn,
			"false",
			cdc.CDCTaskExtraOptions_MaxSqlLength,
			fmt.Sprintf("%d", cdc.CDCDefaultTaskExtra_MaxSQLLen),
			cdc.CDCTaskExtraOptions_SendSqlTimeout,
			cdc.CDCDefaultSendSqlTimeout,
			cdc.CDCRequestOptions_StartTs,
			"2025-01-03 15:20:00",
			cdc.CDCRequestOptions_EndTs,
			"2025-01-03 16:20:00",
		},
	}

	ses.GetTxnCompileCtx().execCtx.stmt = create

	cdc.AesKey = "test-aes-key-not-use-it-in-cloud"
	defer func() { cdc.AesKey = "" }()
	stub := gostub.Stub(&initAesKeyBySqlExecutor, func(context.Context, taskservice.SqlExecutor, uint32, string) (err error) {
		return nil
	})
	defer stub.Reset()

	stubOpenDbConn := gostub.Stub(&cdc.OpenDbConn, func(_, _, _ string, _ int, _ string) (*sql.DB, error) {
		return nil, nil
	})
	defer stubOpenDbConn.Reset()

	stubCheckPitr := gostub.Stub(&CDCCheckPitrGranularity, func(ctx context.Context, bh BackgroundExec, accName string, pts *cdc.PatternTuples, minLength ...int64) error {
		return nil
	})
	defer stubCheckPitr.Reset()

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ses:     ses,
				execCtx: ses.GetTxnCompileCtx().execCtx,
				create:  create,
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err = execInFrontend(tt.args.ses, tt.args.execCtx)
			tt.wantErr(t, err, fmt.Sprintf("handleCreateCdc(%v, %v, %v)", tt.args.ses, tt.args.execCtx, tt.args.create))
		})
	}
}

func Test_doCreateCdc_invalidStartTs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	pu := config.ParameterUnit{}
	pu.TaskService = &testTaskService{}
	setPu("", &pu)

	stubCheckPitr := gostub.Stub(&CDCCheckPitrGranularity, func(ctx context.Context, bh BackgroundExec, accName string, pts *cdc.PatternTuples, minLength ...int64) error {
		return nil
	})
	defer stubCheckPitr.Reset()

	create := &tree.CreateCDC{
		IfNotExists: false,
		TaskName:    "task1",
		SourceUri:   "mysql://root:111@127.0.0.1:6001",
		SinkType:    cdc.CDCSinkType_MySQL,
		SinkUri:     "mysql://root:111@127.0.0.1:3306",
		Tables:      "db1.t1:db1.t1,db1.t2",
		Option: []string{
			cdc.CDCRequestOptions_Level,
			cdc.CDCPitrGranularity_Table,
			"Account",
			sysAccountName,
			cdc.CDCRequestOptions_Exclude,
			"db2.t3,db2.t4",
			cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn,
			"false",
			cdc.CDCTaskExtraOptions_MaxSqlLength,
			fmt.Sprintf("%d", cdc.CDCDefaultTaskExtra_MaxSQLLen),
			cdc.CDCTaskExtraOptions_SendSqlTimeout,
			cdc.CDCDefaultSendSqlTimeout,
			cdc.CDCRequestOptions_StartTs,
			"123456",
		},
	}
	err := handleCreateCDCTaskRequest(context.Background(), ses, create)
	assert.Error(t, err)

	create.Option = []string{
		cdc.CDCRequestOptions_Level,
		cdc.CDCPitrGranularity_Account,
		cdc.CDCRequestOptions_StartTs,
		"2025-01-03 15:20:00",
		cdc.CDCRequestOptions_EndTs,
		"2025-01-03 14:20:00",
	}
	err = handleCreateCDCTaskRequest(context.Background(), ses, create)
	assert.Error(t, err)
}

type testTaskData struct {
	metadata task.TaskMetadata
	details  *task.Details
	status   task.TaskStatus
}

var _ taskservice.TaskService = new(testTaskService)

type testTaskService struct {
	data       []testTaskData
	db         *sql.DB
	dTask      []task.DaemonTask
	curDTaskId int
	taskKeyMap map[taskservice.CDCTaskKey]struct{}
	sqlExec    taskservice.SqlExecutor
}

func (ts *testTaskService) TruncateCompletedTasks(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) AddCDCTask(ctx context.Context,
	metadata task.TaskMetadata,
	details *task.Details,
	callback func(context.Context, taskservice.SqlExecutor) (int, error)) (int, error) {
	ts.data = append(ts.data, testTaskData{
		metadata: metadata,
		details:  details,
	})
	return callback(ctx, ts.db)
}

func (ts *testTaskService) UpdateCDCTask(
	ctx context.Context,
	status task.TaskStatus,
	callback func(context.Context, task.TaskStatus, map[taskservice.CDCTaskKey]struct{}, taskservice.SqlExecutor) (int, error),
	condition ...taskservice.Condition) (int, error) {
	ts.data = append(ts.data, testTaskData{
		status: status,
	})
	return callback(ctx, status, ts.taskKeyMap, ts.sqlExec)
}

func (ts *testTaskService) Close() error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) CreateAsyncTask(ctx context.Context, metadata task.TaskMetadata) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) CreateBatch(ctx context.Context, metadata []task.TaskMetadata) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) CreateCronTask(ctx context.Context, task task.TaskMetadata, cronExpr string) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) Allocate(ctx context.Context, value task.AsyncTask, taskRunner string) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) Complete(ctx context.Context, taskRunner string, task task.AsyncTask, result task.ExecuteResult) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) Heartbeat(ctx context.Context, task task.AsyncTask) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) QueryAsyncTask(ctx context.Context, condition ...taskservice.Condition) ([]task.AsyncTask, error) {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) QueryCronTask(ctx context.Context, condition ...taskservice.Condition) ([]task.CronTask, error) {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) CreateDaemonTask(ctx context.Context, value task.TaskMetadata, details *task.Details) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) QueryDaemonTask(ctx context.Context, conds ...taskservice.Condition) ([]task.DaemonTask, error) {
	return []task.DaemonTask{ts.dTask[ts.curDTaskId]}, nil
}

func (ts *testTaskService) UpdateDaemonTask(ctx context.Context, tasks []task.DaemonTask, cond ...taskservice.Condition) (int, error) {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) HeartbeatDaemonTask(ctx context.Context, task task.DaemonTask) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) StartScheduleCronTask() {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) StopScheduleCronTask() {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) GetStorage() taskservice.TaskStorage {
	//TODO implement me
	panic("implement me")
}

var _ taskservice.SqlExecutor = new(testSqlExecutor)

type testSqlExecutor struct {
	db     *sql.DB
	genIdx func(sql string) int
}

func (sExec *testSqlExecutor) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return sExec.db.PrepareContext(ctx, query)
}

func (sExec *testSqlExecutor) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return sExec.db.ExecContext(ctx, query, args...)
}

func (sExec *testSqlExecutor) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return sExec.db.QueryContext(ctx, query, args...)
}

var _ ie.InternalExecutor = new(testIE)

type testIE struct {
	db     *sql.DB
	genIdx func(sql string) int
}

func (tie *testIE) Exec(ctx context.Context, s string, options ie.SessionOverrideOptions) error {
	_, err := tie.db.Exec(s)
	return err
}

func (tie *testIE) Query(ctx context.Context, s string, options ie.SessionOverrideOptions) ie.InternalExecResult {
	rows, err := tie.db.Query(s)
	if err != nil || rows != nil && rows.Err() != nil {
		panic(err)
	}

	idx := tie.genIdx(s)
	rowValues := make([]any, 0)
	if rows.Next() {
		if idx == mSqlIdx1 || idx == mSqlIdx50 {
			sinkUri := ""
			sinkType := ""
			sinkPwd := ""
			tables := ""
			filters := ""
			startTs := ""
			endTs := ""
			noFull := ""
			splitTxn := ""
			err = rows.Scan(
				&sinkUri,
				&sinkType,
				&sinkPwd,
				&tables,
				&filters,
				&startTs,
				&endTs,
				&noFull,
				&splitTxn)
			if err != nil {
				panic(err)
			}
			rowValues = append(rowValues, sinkUri)
			rowValues = append(rowValues, sinkType)
			rowValues = append(rowValues, sinkPwd)
			rowValues = append(rowValues, tables)
			rowValues = append(rowValues, filters)
			rowValues = append(rowValues, startTs)
			rowValues = append(rowValues, endTs)
			rowValues = append(rowValues, noFull)
			rowValues = append(rowValues, splitTxn)
		} else if idx == mSqlIdx2 {
			dbId := uint64(0)
			tableId := uint64(0)
			err = rows.Scan(
				&dbId,
				&tableId,
			)
			if err != nil {
				panic(err)
			}
			rowValues = append(rowValues, dbId)
			rowValues = append(rowValues, tableId)
		} else if idx == mSqlIdx3 {
			tableIdStr := ""
			watermark := ""
			err = rows.Scan(
				&tableIdStr,
				&watermark,
			)
			if err != nil {
				panic(err)
			}
			rowValues = append(rowValues, tableIdStr, watermark)
		} else if idx == mSqlIdx5 {
			watermark := ""
			err = rows.Scan(
				&watermark,
			)
			if err != nil {
				panic(err)
			}
			rowValues = append(rowValues, watermark)
		} else {
			panic(fmt.Sprintf("invalid idx: %d", idx))
		}
	} else {
		panic("must have data")
	}

	return &testIER{
		rows:      rows,
		sqlIdx:    idx,
		rowValues: rowValues,
	}
}

var _ ie.InternalExecResult = new(testIER)

type testIER struct {
	rows      *sql.Rows
	sqlIdx    int
	rowValues []any
}

func (tier *testIER) Error() error {
	return nil
}

func (tier *testIER) ColumnCount() uint64 {
	cols, err := tier.rows.Columns()
	if err != nil {
		panic(err)
	}
	return uint64(len(cols))
}

func (tier *testIER) Column(ctx context.Context, u uint64) (string, uint8, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (tier *testIER) RowCount() uint64 {
	return 1
}

func (tier *testIER) Row(ctx context.Context, u uint64) ([]interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (tier *testIER) Value(ctx context.Context, u uint64, u2 uint64) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (tier *testIER) GetUint64(ctx context.Context, u uint64, col uint64) (uint64, error) {
	s := tier.rowValues[col].(uint64)
	return s, nil
}

func (tier *testIER) GetFloat64(ctx context.Context, u uint64, u2 uint64) (float64, error) {
	//TODO implement me
	panic("implement me")
}

func (tier *testIER) GetString(ctx context.Context, row uint64, col uint64) (string, error) {
	s := tier.rowValues[col].(string)
	return s, nil
}

func (tie *testIE) ApplySessionOverride(options ie.SessionOverrideOptions) {
	//TODO implement me
	panic("implement me")
}

const (
	mSqlIdx1 int = iota
	mSqlIdx2
	mSqlIdx3
	mSqlIdx4
	mSqlIdx5
	mSqlIdx6
	mSqlIdx7
	mSqlIdx8
	mSqlIdx9
	mSqlIdx10
	mSqlIdx11
	mSqlIdx12
	mSqlIdx13
	mSqlIdx14
	mSqlIdx15
	mSqlIdx16
	mSqlIdx17
	mSqlIdx18
	mSqlIdx19
	mSqlIdx20
	mSqlIdx21
	mSqlIdx22
	mSqlIdx23
	mSqlIdx24
	mSqlIdx25
	mSqlIdx26
	mSqlIdx27
	mSqlIdx28
	mSqlIdx29
	mSqlIdx30
	mSqlIdx31
	mSqlIdx32
	mSqlIdx33
	mSqlIdx34
	mSqlIdx35
	mSqlIdx36
	mSqlIdx37
	mSqlIdx38
	mSqlIdx39
	mSqlIdx40
	mSqlIdx41
	mSqlIdx42
	mSqlIdx43
	mSqlIdx44
	mSqlIdx45
	mSqlIdx46
	mSqlIdx47
	mSqlIdx48
	mSqlIdx49
	mSqlIdx50
)

func TestRegisterCdcExecutor(t *testing.T) {
	type args struct {
		logger       *zap.Logger
		ts           *testTaskService
		ieFactory    func() ie.InternalExecutor
		attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error
		cnUUID       string
		fileService  fileservice.FileService
		cnTxnClient  client.TxnClient
		cnEngine     engine.Engine
		cnEngMp      *mpool.MPool
		dTask        *task.DaemonTask
		curDTaskId   int
	}

	cdc.AesKey = "test-aes-key-not-use-it-in-cloud"
	defer func() { cdc.AesKey = "" }()

	cdc.EnableConsoleSink = true
	defer func() {
		cdc.EnableConsoleSink = false
	}()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	sinkUri, err := cdc.JsonEncode(&cdc.UriInfo{
		User: "root",
		Ip:   "127.0.0.1",
		Port: 3306,
	})
	assert.NoError(t, err)
	pwd, err := cdc.AesCFBEncode([]byte("111"))
	assert.NoError(t, err)
	tables, err := cdc.JsonEncode(cdc.PatternTuples{ //tables
		Pts: []*cdc.PatternTuple{
			{
				Source: cdc.PatternTable{
					Database: "db1",
					Table:    "t1",
				},
			},
		},
	})
	assert.NoError(t, err)
	filters, err := cdc.JsonEncode(cdc.PatternTuples{})
	assert.NoError(t, err)

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	/////////mock sql result
	sql1 := `SELECT sink_uri, sink_type, sink_password, tables, filters, start_ts, end_ts, no_full, additional_config FROM mo_catalog.mo_cdc_task WHERE account_id = 0 AND task_id = "00000000-0000-0000-0000-000000000000"`
	mock.ExpectQuery(sql1).WillReturnRows(sqlmock.NewRows(
		[]string{
			"sink_uri",
			"sink_type",
			"sink_password",
			"tables",
			"filters",
			"start_ts",
			"end_ts",
			"no_full",
			"additional_config",
		},
	).AddRow(
		sinkUri,
		cdc.CDCSinkType_Console,
		pwd,
		tables,
		filters,
		"",
		"",
		true,
		fmt.Sprintf("{\"%s\":%v,\"%s\":\"%s\",\"%s\":%v}",
			cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn, cdc.CDCDefaultTaskExtra_InitSnapshotSplitTxn,
			cdc.CDCTaskExtraOptions_SendSqlTimeout, cdc.CDCDefaultSendSqlTimeout,
			cdc.CDCTaskExtraOptions_MaxSqlLength, cdc.CDCDefaultTaskExtra_MaxSQLLen,
		),
	))

	sql7 := "update `mo_catalog`.`mo_cdc_task` set state = 'running', err_msg = '' where account_id = 0 and task_id = '00000000-0000-0000-0000-000000000000'"
	mock.ExpectExec(sql7).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql1, err := regexp.MatchString(sql1, sql)
		assert.NoError(t, err)

		if mSql1 {
			return mSqlIdx1
		}
		return -1
	}

	///////////mock engine
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	eng.EXPECT().LatestLogtailAppliedTime().Return(timestamp.Timestamp{}).AnyTimes()

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	eng.EXPECT().Database(ctx, gomock.Any(), txnOperator).Return(nil, nil).AnyTimes()

	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
	txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
	txnOperator.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	tIEFactory1 := func() ie.InternalExecutor {
		return &testIE{
			db:     db,
			genIdx: genSqlIdx,
		}
	}

	attacher := func(ctx context.Context, tid uint64, ar taskservice.ActiveRoutine) error {
		return nil
	}

	ts := &testTaskService{
		db: db,
		dTask: []task.DaemonTask{
			{
				Details: &task.Details{
					Details: &task.Details_CreateCdc{
						CreateCdc: &task.CreateCdcDetails{
							TaskId:   "00000000-0000-0000-0000-000000000000",
							TaskName: "task1",
							Accounts: []*task.Account{
								{
									Id:   uint64(sysAccountID),
									Name: sysAccountName,
								},
							},
						},
					},
				},
			},
		},
	}

	mp, err := mpool.NewMPool("cdc", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	gostub.Stub(&cdc.GetTableDetector, func(cnUUID string) *cdc.TableDetector {
		return &cdc.TableDetector{
			Mp:                   make(map[uint32]cdc.TblMap),
			Callbacks:            map[string]cdc.TableCallback{"id": func(mp map[uint32]cdc.TblMap) error { return nil }},
			CallBackAccountId:    map[string]uint32{"id": 0},
			SubscribedAccountIds: map[uint32][]string{0: {"id"}},
			CallBackDbName:       make(map[string][]string),
			SubscribedDbNames:    make(map[string][]string),
			CallBackTableName:    make(map[string][]string),
			SubscribedTableNames: make(map[string][]string),
		}
	})

	tests := []struct {
		name string
		args args
		want func(ctx context.Context, task task.Task) error
	}{
		{
			name: "t1",
			args: args{
				ts:           ts,
				dTask:        &task.DaemonTask{},
				ieFactory:    tIEFactory1,
				attachToTask: attacher,
				cnEngine:     eng,
				cnEngMp:      mp,
				cnTxnClient:  txnClient,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.args.ts.curDTaskId = tt.args.curDTaskId
			fun := CDCTaskExecutorFactory(
				tt.args.logger,
				tt.args.ieFactory,
				tt.args.attachToTask,
				tt.args.cnUUID,
				tt.args.ts,
				tt.args.fileService,
				tt.args.cnTxnClient,
				tt.args.cnEngine,
			)
			ctx2, cancel := context.WithTimeout(ctx, time.Second*3)
			defer cancel()
			err = fun(ctx2, tt.args.dTask)
			assert.NoErrorf(t, err, "RegisterCdcExecutor(%v, %v, %v, %v, %v, %v, %v, %v, %v)",
				tt.args.logger,
				tt.args.ts,
				tt.args.ieFactory,
				tt.args.attachToTask,
				tt.args.cnUUID,
				tt.args.fileService,
				tt.args.cnTxnClient,
				tt.args.cnEngine,
				tt.args.cnEngMp)
		})
	}
}

func Test_updateCdcTask_cancel(t *testing.T) {
	type args struct {
		ctx          context.Context
		targetStatus task.TaskStatus
		taskKeyMap   map[taskservice.CDCTaskKey]struct{}
		tx           taskservice.SqlExecutor
		accountId    uint64
		taskName     string
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql7 := "SELECT task_id FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"

	mock.ExpectQuery(sql7).WillReturnRows(
		sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql8 := "DELETE FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql8).WillReturnResult(sqlmock.NewResult(1, 1))

	sql9 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 0 AND task_id = 'taskID-1'"
	mock.ExpectExec(sql9).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql7, err := regexp.MatchString(sql7, sql)
		assert.NoError(t, err)

		mSql8, err := regexp.MatchString(sql8, sql)
		assert.NoError(t, err)

		if mSql7 {
			return mSqlIdx7
		} else if mSql8 {
			return mSqlIdx8
		}
		return -1
	}

	tx := &testSqlExecutor{
		db:     db,
		genIdx: genSqlIdx,
	}

	tests := []struct {
		name    string
		args    args
		want    int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:          context.Background(),
				targetStatus: task.TaskStatus_CancelRequested,
				taskKeyMap:   map[taskservice.CDCTaskKey]struct{}{},
				tx:           tx,
				accountId:    sysAccountID,
				taskName:     "task1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := onPreUpdateCDCTasks(tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
			assert.NoError(t, err, "updateCdcTask(%v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
		})
	}
}

func Test_updateCdcTask_pause(t *testing.T) {
	type args struct {
		ctx          context.Context
		targetStatus task.TaskStatus
		taskKeyMap   map[taskservice.CDCTaskKey]struct{}
		tx           taskservice.SqlExecutor
		accountId    uint64
		taskName     string
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql10 := "SELECT task_id FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectQuery(sql10).WillReturnRows(
		sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql11 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectPrepare(sql11)

	sql12 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql12).WillReturnResult(sqlmock.NewResult(1, 1))

	sql13 := "DELETE FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql13).WillReturnResult(sqlmock.NewResult(1, 1))

	sql14 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 0 AND task_id = 'taskID-1'"
	mock.ExpectExec(sql14).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql10, err := regexp.MatchString(sql10, sql)
		assert.NoError(t, err)
		mSql11, err := regexp.MatchString(sql11, sql)
		assert.NoError(t, err)
		mSql12, err := regexp.MatchString(sql12, sql)
		assert.NoError(t, err)
		mSql13, err := regexp.MatchString(sql13, sql)
		assert.NoError(t, err)

		if mSql10 {
			return mSqlIdx10
		} else if mSql11 {
			return mSqlIdx11
		} else if mSql12 {
			return mSqlIdx12
		} else if mSql13 {
			return mSqlIdx13
		}

		return -1
	}

	tx := &testSqlExecutor{
		db:     db,
		genIdx: genSqlIdx,
	}

	tests := []struct {
		name    string
		args    args
		want    int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:          context.Background(),
				targetStatus: task.TaskStatus_PauseRequested,
				taskKeyMap:   map[taskservice.CDCTaskKey]struct{}{},
				tx:           tx,
				accountId:    sysAccountID,
				taskName:     "task1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := onPreUpdateCDCTasks(tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
			assert.NoError(t, err, "updateCdcTask(%v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
		})
	}
}

func Test_updateCdcTask_restart(t *testing.T) {
	type args struct {
		ctx          context.Context
		targetStatus task.TaskStatus
		taskKeyMap   map[taskservice.CDCTaskKey]struct{}
		tx           taskservice.SqlExecutor
		accountId    uint64
		taskName     string
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql15 := "SELECT task_id FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectQuery(sql15).WillReturnRows(
		sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql16 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectPrepare(sql16)

	sql17 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql17).WillReturnResult(sqlmock.NewResult(1, 1))

	sql18 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 0 AND task_id = 'taskID-1'"
	mock.ExpectExec(sql18).WillReturnResult(sqlmock.NewResult(1, 1))

	sql19 := "DELETE FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql19).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql15, err := regexp.MatchString(sql15, sql)
		assert.NoError(t, err)
		mSql16, err := regexp.MatchString(sql16, sql)
		assert.NoError(t, err)
		mSql17, err := regexp.MatchString(sql17, sql)
		assert.NoError(t, err)
		mSql19, err := regexp.MatchString(sql19, sql)
		assert.NoError(t, err)

		if mSql15 {
			return mSqlIdx15
		} else if mSql16 {
			return mSqlIdx16
		} else if mSql17 {
			return mSqlIdx17
		} else if mSql19 {
			return mSqlIdx19
		}

		return -1
	}

	tx := &testSqlExecutor{
		db:     db,
		genIdx: genSqlIdx,
	}

	tests := []struct {
		name    string
		args    args
		want    int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:          context.Background(),
				targetStatus: task.TaskStatus_RestartRequested,
				taskKeyMap:   map[taskservice.CDCTaskKey]struct{}{},
				tx:           tx,
				accountId:    sysAccountID,
				taskName:     "task1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := onPreUpdateCDCTasks(tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
			assert.NoError(t, err, "updateCdcTask(%v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
		})
	}
}

func Test_updateCdcTask_resume(t *testing.T) {
	type args struct {
		ctx          context.Context
		targetStatus task.TaskStatus
		taskKeyMap   map[taskservice.CDCTaskKey]struct{}
		tx           taskservice.SqlExecutor
		accountId    uint64
		taskName     string
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql20 := "SELECT task_id FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectQuery(sql20).WillReturnRows(
		sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql21 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectPrepare(sql21)

	sql22 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql22).WillReturnResult(sqlmock.NewResult(1, 1))

	sql23 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 0 AND task_id = 'taskID-1'"
	mock.ExpectExec(sql23).WillReturnResult(sqlmock.NewResult(1, 1))

	sql24 := "DELETE FROM `mo_catalog`.`mo_cdc_task` WHERE account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql24).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql20, err := regexp.MatchString(sql20, sql)
		assert.NoError(t, err)
		mSql21, err := regexp.MatchString(sql21, sql)
		assert.NoError(t, err)
		mSql22, err := regexp.MatchString(sql22, sql)
		assert.NoError(t, err)
		mSql24, err := regexp.MatchString(sql24, sql)
		assert.NoError(t, err)

		if mSql20 {
			return mSqlIdx20
		} else if mSql21 {
			return mSqlIdx21
		} else if mSql22 {
			return mSqlIdx22
		} else if mSql24 {
			return mSqlIdx24
		}

		return -1
	}

	tx := &testSqlExecutor{
		db:     db,
		genIdx: genSqlIdx,
	}

	tests := []struct {
		name    string
		args    args
		want    int
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:          context.Background(),
				targetStatus: task.TaskStatus_ResumeRequested,
				taskKeyMap:   map[taskservice.CDCTaskKey]struct{}{},
				tx:           tx,
				accountId:    sysAccountID,
				taskName:     "task1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := onPreUpdateCDCTasks(tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
			assert.NoError(t, err, "updateCdcTask(%v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
		})
	}
}

func Test_updateCdc_cancel(t *testing.T) {
	type args struct {
		ctx context.Context
		ses *Session
		st  tree.Statement
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	drop := &tree.DropCDC{
		Option: &tree.AllOrNotCDC{
			TaskName: "task1",
		},
	}

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
		stmt:   drop,
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	//////////////////mock result
	sql25 := "SELECT task_id FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectQuery(sql25).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql26 := "DELETE FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql26).WillReturnResult(sqlmock.NewResult(1, 1))

	sql27 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 0 AND task_id = 'taskID-1'"
	mock.ExpectExec(sql27).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql25, err := regexp.MatchString(sql25, sql)
		assert.NoError(t, err)

		mSql26, err := regexp.MatchString(sql26, sql)
		assert.NoError(t, err)

		if mSql25 {
			return mSqlIdx25
		} else if mSql26 {
			return mSqlIdx26
		}
		return -1
	}
	//////////////////

	pu := config.ParameterUnit{}

	taskKeyMap := make(map[taskservice.CDCTaskKey]struct{})
	taskKeyMap[taskservice.CDCTaskKey{
		AccountId: sysAccountID,
		TaskId:    "taskID-1",
	}] = struct{}{}

	pu.TaskService = &testTaskService{
		db:         db,
		taskKeyMap: taskKeyMap,
		sqlExec: &testSqlExecutor{
			db:     db,
			genIdx: genSqlIdx,
		},
	}

	setPu("", &pu)

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: ses.GetTxnCompileCtx().execCtx.reqCtx,
				ses: ses,
				st:  drop,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err = execInFrontend(tt.args.ses, tt.args.ses.GetTxnCompileCtx().execCtx)
			assert.NoError(t, err, fmt.Sprintf("updateCdc(%v, %v, %v)", tt.args.ctx, tt.args.ses, tt.args.st))
		})
	}
}

func Test_updateCdc_cancel_all(t *testing.T) {
	type args struct {
		ctx context.Context
		ses *Session
		st  tree.Statement
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	//////////////////mock result
	sql28 := "SELECT task_id FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0"
	mock.ExpectQuery(sql28).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql29 := "DELETE FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0"
	mock.ExpectExec(sql29).WillReturnResult(sqlmock.NewResult(1, 1))

	sql30 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 0 AND task_id = 'taskID-1'"
	mock.ExpectExec(sql30).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql28, err := regexp.MatchString(sql28, sql)
		assert.NoError(t, err)

		mSql29, err := regexp.MatchString(sql29, sql)
		assert.NoError(t, err)

		if mSql28 {
			return mSqlIdx28
		} else if mSql29 {
			return mSqlIdx29
		}
		return -1
	}
	//////////////////

	pu := config.ParameterUnit{}

	taskKeyMap := make(map[taskservice.CDCTaskKey]struct{})
	taskKeyMap[taskservice.CDCTaskKey{
		AccountId: sysAccountID,
		TaskId:    "taskID-1",
	}] = struct{}{}

	pu.TaskService = &testTaskService{
		db:         db,
		taskKeyMap: taskKeyMap,
		sqlExec: &testSqlExecutor{
			db:     db,
			genIdx: genSqlIdx,
		},
	}

	setPu("", &pu)

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: ses.GetTxnCompileCtx().execCtx.reqCtx,
				ses: ses,
				st: &tree.DropCDC{
					Option: &tree.AllOrNotCDC{
						All: true,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = handleUpdateCDCTaskRequest(tt.args.ctx, tt.args.ses, tt.args.st)
			assert.NoError(t, err, fmt.Sprintf("updateCdc(%v, %v, %v)", tt.args.ctx, tt.args.ses, tt.args.st))
		})
	}
}

func Test_updateCdc_pause(t *testing.T) {
	type args struct {
		ctx context.Context
		ses *Session
		st  tree.Statement
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	pause := &tree.PauseCDC{
		Option: &tree.AllOrNotCDC{
			TaskName: "task1",
		},
	}

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
		stmt:   pause,
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	//////////////////mock result
	sql30 := "SELECT task_id FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectQuery(sql30).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql33 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectPrepare(sql33)

	sql34 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql34).WillReturnResult(sqlmock.NewResult(1, 1))

	sql31 := "DELETE FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql31).WillReturnResult(sqlmock.NewResult(1, 1))

	sql32 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 0 AND task_id = 'taskID-1'"
	mock.ExpectExec(sql32).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql30, err := regexp.MatchString(sql30, sql)
		assert.NoError(t, err)

		mSql31, err := regexp.MatchString(sql31, sql)
		assert.NoError(t, err)

		if mSql30 {
			return mSqlIdx30
		} else if mSql31 {
			return mSqlIdx31
		}
		return -1
	}
	//////////////////

	pu := config.ParameterUnit{}

	taskKeyMap := make(map[taskservice.CDCTaskKey]struct{})
	taskKeyMap[taskservice.CDCTaskKey{
		AccountId: sysAccountID,
		TaskId:    "taskID-1",
	}] = struct{}{}

	pu.TaskService = &testTaskService{
		db:         db,
		taskKeyMap: taskKeyMap,
		sqlExec: &testSqlExecutor{
			db:     db,
			genIdx: genSqlIdx,
		},
	}

	setPu("", &pu)

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: ses.GetTxnCompileCtx().execCtx.reqCtx,
				ses: ses,
				st:  pause,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err = execInFrontend(tt.args.ses, tt.args.ses.GetTxnCompileCtx().execCtx)
			assert.NoError(t, err, fmt.Sprintf("updateCdc(%v, %v, %v)", tt.args.ctx, tt.args.ses, tt.args.st))
		})
	}
}

func Test_updateCdc_pause_all(t *testing.T) {
	type args struct {
		ctx context.Context
		ses *Session
		st  tree.Statement
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	//////////////////mock result
	sql35 := "SELECT task_id FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0"
	mock.ExpectQuery(sql35).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql36 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0"
	mock.ExpectPrepare(sql36)

	sql37 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0"
	mock.ExpectExec(sql37).WillReturnResult(sqlmock.NewResult(1, 1))

	sql38 := "DELETE FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0"
	mock.ExpectExec(sql38).WillReturnResult(sqlmock.NewResult(1, 1))

	sql39 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 0 AND task_id = 'taskID-1'"
	mock.ExpectExec(sql39).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql35, err := regexp.MatchString(sql35, sql)
		assert.NoError(t, err)

		mSql38, err := regexp.MatchString(sql38, sql)
		assert.NoError(t, err)

		if mSql35 {
			return mSqlIdx35
		} else if mSql38 {
			return mSqlIdx38
		}
		return -1
	}
	//////////////////

	pu := config.ParameterUnit{}

	taskKeyMap := make(map[taskservice.CDCTaskKey]struct{})
	taskKeyMap[taskservice.CDCTaskKey{
		AccountId: sysAccountID,
		TaskId:    "taskID-1",
	}] = struct{}{}

	pu.TaskService = &testTaskService{
		db:         db,
		taskKeyMap: taskKeyMap,
		sqlExec: &testSqlExecutor{
			db:     db,
			genIdx: genSqlIdx,
		},
	}

	setPu("", &pu)

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: ses.GetTxnCompileCtx().execCtx.reqCtx,
				ses: ses,
				st: &tree.PauseCDC{
					Option: &tree.AllOrNotCDC{
						All: true,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err = handleUpdateCDCTaskRequest(tt.args.ctx, tt.args.ses, tt.args.st)
			assert.NoError(t, err, fmt.Sprintf("updateCdc(%v, %v, %v)", tt.args.ctx, tt.args.ses, tt.args.st))
		})
	}
}

func Test_updateCdc_restart(t *testing.T) {
	type args struct {
		ctx context.Context
		ses *Session
		st  tree.Statement
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	restart := &tree.RestartCDC{
		TaskName: "task1",
	}

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
		stmt:   restart,
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	//////////////////mock result
	sql40 := "SELECT task_id FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectQuery(sql40).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql41 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectPrepare(sql41)

	sql42 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql42).WillReturnResult(sqlmock.NewResult(1, 1))

	sql44 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 0 AND task_id = 'taskID-1'"
	mock.ExpectExec(sql44).WillReturnResult(sqlmock.NewResult(1, 1))

	sql43 := "DELETE FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql43).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql40, err := regexp.MatchString(sql40, sql)
		assert.NoError(t, err)

		mSql43, err := regexp.MatchString(sql43, sql)
		assert.NoError(t, err)

		if mSql40 {
			return mSqlIdx30
		} else if mSql43 {
			return mSqlIdx31
		}
		return -1
	}
	//////////////////

	pu := config.ParameterUnit{}

	taskKeyMap := make(map[taskservice.CDCTaskKey]struct{})
	taskKeyMap[taskservice.CDCTaskKey{
		AccountId: sysAccountID,
		TaskId:    "taskID-1",
	}] = struct{}{}

	pu.TaskService = &testTaskService{
		db:         db,
		taskKeyMap: taskKeyMap,
		sqlExec: &testSqlExecutor{
			db:     db,
			genIdx: genSqlIdx,
		},
	}

	setPu("", &pu)

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: ses.GetTxnCompileCtx().execCtx.reqCtx,
				ses: ses,
				st:  restart,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err = execInFrontend(tt.args.ses, tt.args.ses.GetTxnCompileCtx().execCtx)
			assert.NoError(t, err, fmt.Sprintf("updateCdc(%v, %v, %v)", tt.args.ctx, tt.args.ses, tt.args.st))
		})
	}
}

func Test_updateCdc_resume(t *testing.T) {
	type args struct {
		ctx context.Context
		ses *Session
		st  tree.Statement
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	resume := &tree.ResumeCDC{
		TaskName: "task1",
	}

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
		stmt:   resume,
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	//////////////////mock result
	sql45 := "SELECT task_id FROM `mo_catalog`.`mo_cdc_task` WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectQuery(sql45).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql46 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectPrepare(sql46)

	sql47 := "UPDATE `mo_catalog`.`mo_cdc_task` SET state = .* WHERE 1=1 AND account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql47).WillReturnResult(sqlmock.NewResult(1, 1))

	sql48 := "DELETE FROM `mo_catalog`.`mo_cdc_watermark` WHERE account_id = 0 AND task_id = 'taskID-1'"
	mock.ExpectExec(sql48).WillReturnResult(sqlmock.NewResult(1, 1))

	sql49 := "DELETE FROM `mo_catalog`.`mo_cdc_task` WHERE account_id = 0 AND task_name = 'task1'"
	mock.ExpectExec(sql49).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql45, err := regexp.MatchString(sql45, sql)
		assert.NoError(t, err)

		mSql49, err := regexp.MatchString(sql49, sql)
		assert.NoError(t, err)

		if mSql45 {
			return mSqlIdx30
		} else if mSql49 {
			return mSqlIdx31
		}
		return -1
	}
	//////////////////

	pu := config.ParameterUnit{}

	taskKeyMap := make(map[taskservice.CDCTaskKey]struct{})
	taskKeyMap[taskservice.CDCTaskKey{
		AccountId: sysAccountID,
		TaskId:    "taskID-1",
	}] = struct{}{}

	pu.TaskService = &testTaskService{
		db:         db,
		taskKeyMap: taskKeyMap,
		sqlExec: &testSqlExecutor{
			db:     db,
			genIdx: genSqlIdx,
		},
	}

	setPu("", &pu)

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx: ses.GetTxnCompileCtx().execCtx.reqCtx,
				ses: ses,
				st:  resume,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err = execInFrontend(tt.args.ses, tt.args.ses.GetTxnCompileCtx().execCtx)
			assert.NoError(t, err, fmt.Sprintf("updateCdc(%v, %v, %v)", tt.args.ctx, tt.args.ses, tt.args.st))
		})
	}
}

func newMrsForGetWatermark(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col2 := &MysqlColumn{}
	col2.SetName("reldatabase")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col3 := &MysqlColumn{}
	col3.SetName("relname")
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col5 := &MysqlColumn{}
	col5.SetName("watermark")
	col5.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col6 := &MysqlColumn{}
	col6.SetName("err_msg")
	col6.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col5)
	mrs.AddColumn(col6)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func Test_GetWatermark(t *testing.T) {
	type args struct {
		ctx       context.Context
		dao       CDCDao
		accountId uint64
		taskId    string
	}

	bh := &backgroundExecTest{}
	bh.init()
	dao := NewCDCDao(nil, WithBGExecutor(bh))

	sql := cdc.CDCSQLBuilder.GetWatermarkSQL(sysAccountID, "taskID-1")
	mrs := newMrsForGetWatermark([][]interface{}{
		{"db1", "tb1", "0-0", ""},
	})
	bh.sql2result[sql] = mrs

	tests := []struct {
		name    string
		args    args
		wantS   string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ctx:       context.Background(),
				dao:       dao,
				accountId: uint64(sysAccountID),
				taskId:    "taskID-1",
			},
			wantS: "{\n  \"db1.tb1\": " + timestamp.Timestamp{}.ToStdTime().In(time.Local).String() + ",\n}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := dao.GetTaskWatermark(
				tt.args.ctx,
				tt.args.accountId,
				tt.args.taskId,
			)
			assert.NoErrorf(
				t, err,
				"GetTaskWatermark(%v, %v, %v)",
				tt.args.ctx,
				tt.args.accountId,
				tt.args.taskId,
			)
			assert.Equal(t, tt.wantS, res)
		})
	}
}

func newMrsForGetTask(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("reldatabase")
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col2 := &MysqlColumn{}
	col2.SetName("reldatabase")
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col3 := &MysqlColumn{}
	col3.SetName("relname")
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col4 := &MysqlColumn{}
	col4.SetName("reldatabase")
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col5 := &MysqlColumn{}
	col5.SetName("watermark")
	col5.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	col6 := &MysqlColumn{}
	col6.SetName("err_msg")
	col6.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)
	mrs.AddColumn(col6)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func Test_handleShowCdc(t *testing.T) {
	type args struct {
		ses     *Session
		execCtx *ExecCtx
		st      *tree.ShowCDC
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	show := &tree.ShowCDC{
		Option: &tree.AllOrNotCDC{All: true},
	}

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
		stmt:   show,
	}

	bh := &backgroundExecTest{}
	bh.init()

	sourceUri, err := cdc.JsonEncode(&cdc.UriInfo{
		User: "root",
		Ip:   "127.0.0.1",
		Port: 6001,
	})
	assert.NoError(t, err)

	sinkUri, err := cdc.JsonEncode(&cdc.UriInfo{
		User: "root",
		Ip:   "127.0.0.1",
		Port: 6001,
	})
	assert.NoError(t, err)

	sql := cdc.CDCSQLBuilder.ShowTaskSQL(sysAccountID, true, "")
	mrs := newMrsForGetTask([][]interface{}{
		{"taskID-1", "task1", sourceUri, sinkUri, cdc.CDCState_Running, ""},
	})
	bh.sql2result[sql] = mrs

	sql = cdc.CDCSQLBuilder.GetWatermarkSQL(sysAccountID, "taskID-1")
	mrs = newMrsForGetWatermark([][]interface{}{
		{"db1", "tb1", "0-0", ""},
	})
	bh.sql2result[sql] = mrs

	bhStub := gostub.StubFunc(&NewBackgroundExec, bh)
	defer bhStub.Reset()

	///////////mock engine
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{
		CommitOrRollbackTimeout: time.Second,
	}).AnyTimes()
	eng.EXPECT().LatestLogtailAppliedTime().Return(timestamp.Timestamp{}).AnyTimes()

	table := mock_frontend.NewMockRelation(ctrl)

	tableDef := &plan.TableDef{
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{
				"a",
			},
		},
	}

	table.EXPECT().CopyTableDef(gomock.Any()).Return(tableDef).AnyTimes()

	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), gomock.Any()).Return("", "", table, nil).AnyTimes()

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	eng.EXPECT().Database(gomock.Any(), gomock.Any(), txnOperator).Return(nil, nil).AnyTimes()

	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
	txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
	txnOperator.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	pu := config.ParameterUnit{
		StorageEngine: eng,
		TxnClient:     txnClient,
	}
	setPu("", &pu)

	//////////

	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			args: args{
				ses:     ses,
				execCtx: ses.GetTxnCompileCtx().execCtx,
				st:      show,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err = execInFrontend(tt.args.ses, tt.args.ses.GetTxnCompileCtx().execCtx)
			assert.NoError(t, err)
			rs := tt.args.ses.GetMysqlResultSet()
			taskId, err := rs.GetString(tt.args.execCtx.reqCtx, 0, 0)
			assert.NoError(t, err)
			assert.Equal(t, "taskID-1", taskId)
		})
	}
}

func TestCdcTask_Resume(t *testing.T) {
	cdc := &CDCTaskExecutor{
		activeRoutine: cdc.NewCdcActiveRoutine(),
		spec: &task.CreateCdcDetails{
			TaskName: "task1",
		},
		holdCh: make(chan int, 1),
		startFunc: func(_ context.Context) error {
			return nil
		},
	}

	err := cdc.Resume()
	assert.NoErrorf(t, err, "Resume()")
}

func TestCdcTask_Restart(t *testing.T) {
	u, _ := cdc.InitCDCWatermarkUpdaterForTest(t)
	u.Start()
	defer u.Stop()

	// Stub GetTableDetector to avoid initialization issues
	// Simply don't call UnRegister when isRunning is false
	stubDetector := gostub.Stub(&cdc.GetTableDetector, func(cnUUID string) *cdc.TableDetector {
		// Return a mock detector that won't panic on UnRegister
		return &cdc.TableDetector{
			Mp:                   make(map[uint32]cdc.TblMap),
			Callbacks:            make(map[string]cdc.TableCallback),
			CallBackAccountId:    make(map[string]uint32),
			SubscribedAccountIds: make(map[uint32][]string),
			CallBackDbName:       make(map[string][]string),
			SubscribedDbNames:    make(map[string][]string),
			CallBackTableName:    make(map[string][]string),
			SubscribedTableNames: make(map[string][]string),
		}
	})
	defer stubDetector.Reset()

	cdcTask := &CDCTaskExecutor{
		activeRoutine:    cdc.NewCdcActiveRoutine(),
		watermarkUpdater: u,
		cnUUID:           "test-uuid", // Add cnUUID to prevent nil pointer
		spec: &task.CreateCdcDetails{
			TaskId:   "task1",
			TaskName: "task1",
		},
		holdCh: make(chan int, 1),
		startFunc: func(_ context.Context) error {
			return nil
		},
		isRunning: false, // Set to false so Restart() won't try to UnRegister
	}

	err := cdcTask.Restart()
	assert.NoErrorf(t, err, "Restart()")

	// Wait a bit for the goroutine to start
	time.Sleep(10 * time.Millisecond)
}

func TestCdcTask_Pause(t *testing.T) {
	holdCh := make(chan int, 1)
	go func() {
		<-holdCh
	}()

	cdc := &CDCTaskExecutor{
		activeRoutine: cdc.NewCdcActiveRoutine(),
		spec: &task.CreateCdcDetails{
			TaskName: "task1",
		},
		isRunning: true,
		holdCh:    holdCh,
	}
	err := cdc.Pause()
	assert.NoErrorf(t, err, "Pause()")
}

func TestCdcTask_Cancel(t *testing.T) {
	ch := make(chan int, 1)
	go func() {
		<-ch
	}()

	u, _ := cdc.InitCDCWatermarkUpdaterForTest(t)
	u.Start()
	defer u.Stop()
	cdc := &CDCTaskExecutor{
		activeRoutine:    cdc.NewCdcActiveRoutine(),
		watermarkUpdater: u,
		spec: &task.CreateCdcDetails{
			TaskName: "task1",
		},
		holdCh:    ch,
		isRunning: true,
	}
	err := cdc.Cancel()
	assert.NoErrorf(t, err, "Cancel()")
}

func TestCdcTask_retrieveCdcTask(t *testing.T) {
	fault.EnableDomain(fault.DomainFrontend)
	type fields struct {
		logger               *zap.Logger
		ie                   ie.InternalExecutor
		cnUUID               string
		cnTxnClient          client.TxnClient
		cnEngine             engine.Engine
		fileService          fileservice.FileService
		cdcTask              *task.CreateCdcDetails
		mp                   *mpool.MPool
		packerPool           *fileservice.Pool[*types.Packer]
		sinkUri              cdc.UriInfo
		tables               cdc.PatternTuples
		exclude              *regexp.Regexp
		startTs              types.TS
		noFull               bool
		activeRoutine        *cdc.ActiveRoutine
		sunkWatermarkUpdater *cdc.CDCWatermarkUpdater
	}
	type args struct {
		ctx context.Context
	}

	cdc.AesKey = "test-aes-key-not-use-it-in-cloud"
	defer func() { cdc.AesKey = "" }()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sqlx := "SELECT sink_uri, sink_type, sink_password, tables, filters, start_ts, end_ts, no_full, additional_config FROM mo_catalog.mo_cdc_task WHERE account_id = .* AND task_id =.*"
	sinkUri, err := cdc.JsonEncode(&cdc.UriInfo{
		User: "root",
		Ip:   "127.0.0.1",
		Port: 3306,
	})
	assert.NoError(t, err)
	pwd, err := cdc.AesCFBEncode([]byte("111"))
	assert.NoError(t, err)
	tables, err := cdc.JsonEncode(cdc.PatternTuples{ //tables
		Pts: []*cdc.PatternTuple{
			{
				Source: cdc.PatternTable{
					Database: "db1",
					Table:    "t1",
				},
			},
		},
	},
	)
	assert.NoError(t, err)
	filters, err := cdc.JsonEncode(cdc.PatternTuples{})
	assert.NoError(t, err)

	mock.ExpectQuery(sqlx).WillReturnRows(sqlmock.NewRows(
		[]string{
			"sink_uri",
			"sink_type",
			"sink_password",
			"tables",
			"filters",
			"start_ts",
			"end_ts",
			"no_full",
			"additional_config",
		},
	).AddRow(
		sinkUri,
		cdc.CDCSinkType_MySQL,
		pwd,
		tables,
		filters,
		"2006-01-02T15:04:05-07:00",
		"2006-01-02T15:04:05-07:00",
		true,
		"{\"InitSnapshotSplitTxn\": false}",
	),
	)

	genIdx := func(s string) int {
		mSqlx, err := regexp.MatchString(sqlx, s)
		assert.NoError(t, err)

		if mSqlx {
			return mSqlIdx50
		}
		return -1
	}

	tie := &testIE{
		db:     db,
		genIdx: genIdx,
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			fields: fields{
				cdcTask: &task.CreateCdcDetails{
					TaskId: "taskID_1",
					Accounts: []*task.Account{
						{
							Id:   0,
							Name: "sys",
						},
					},
				},
				ie: tie,
			},
			args: args{
				ctx: context.Background(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cdc := &CDCTaskExecutor{
				logger:           tt.fields.logger,
				ie:               tt.fields.ie,
				cnUUID:           tt.fields.cnUUID,
				cnTxnClient:      tt.fields.cnTxnClient,
				cnEngine:         tt.fields.cnEngine,
				fileService:      tt.fields.fileService,
				spec:             tt.fields.cdcTask,
				mp:               tt.fields.mp,
				packerPool:       tt.fields.packerPool,
				sinkUri:          tt.fields.sinkUri,
				tables:           tt.fields.tables,
				exclude:          tt.fields.exclude,
				startTs:          tt.fields.startTs,
				noFull:           tt.fields.noFull,
				activeRoutine:    tt.fields.activeRoutine,
				watermarkUpdater: tt.fields.sunkWatermarkUpdater,
			}
			err := cdc.retrieveCdcTask(tt.args.ctx)
			assert.NoError(t, err, fmt.Sprintf("retrieveCdcTask(%v)", tt.args.ctx))
		})
	}
}

func Test_execFrontend(t *testing.T) {
	pu := config.ParameterUnit{}
	setPu("", &pu)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	defer ses.Close()

	ses.GetTxnCompileCtx().execCtx = &ExecCtx{
		reqCtx: context.Background(),
	}

	stmts := []tree.Statement{
		&tree.CreateCDC{},
		&tree.PauseCDC{},
		&tree.DropCDC{},
		&tree.RestartCDC{},
		&tree.ResumeCDC{},
		&tree.ShowCDC{},
	}

	txnOpStub := gostub.Stub(&cdc.GetTxnOp,
		func(ctx context.Context, cnEngine engine.Engine, cnTxnClient client.TxnClient, info string) (client.TxnOperator, error) {
			return nil, moerr.NewInternalError(ctx, "error")
		})
	defer txnOpStub.Reset()

	for _, stmt := range stmts {
		ses.GetTxnCompileCtx().execCtx.stmt = stmt
		_, err := execInFrontend(ses, ses.GetTxnCompileCtx().execCtx)
		assert.Error(t, err)
	}

}

func Test_getSqlForGetTask(t *testing.T) {
	type args struct {
		accountId uint64
		all       bool
		taskName  string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "t1",
			args: args{
				accountId: 0,
				all:       true,
				taskName:  "",
			},
			want: "SELECT task_id, task_name, source_uri, sink_uri, state, err_msg FROM mo_catalog.mo_cdc_task WHERE account_id = 0",
		},
		{
			name: "t2",
			args: args{
				accountId: 0,
				all:       false,
				taskName:  "task1",
			},
			want: "SELECT task_id, task_name, source_uri, sink_uri, state, err_msg FROM mo_catalog.mo_cdc_task WHERE account_id = 0 AND task_name = 'task1'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(
				t,
				tt.want,
				cdc.CDCSQLBuilder.ShowTaskSQL(tt.args.accountId, tt.args.all, tt.args.taskName),
				"getSqlForGetTask(%v, %v, %v)",
				tt.args.accountId,
				tt.args.all,
				tt.args.taskName,
			)
		})
	}
}

func Test_initAesKey(t *testing.T) {
	{
		cdc.AesKey = "test-aes-key-not-use-it-in-cloud"
		err := initAesKeyBySqlExecutor(context.Background(), nil, 0, "")
		assert.NoError(t, err)

		cdc.AesKey = ""
	}

	{
		e := moerr.NewInternalErrorNoCtx("error")
		queryTableStub := gostub.Stub(
			&ForeachQueriedRow,
			func(
				context.Context,
				taskservice.SqlExecutor,
				string,
				func(context.Context, *sql.Rows) (bool, error),
			) (int64, error) {
				return 0, e
			},
		)
		defer queryTableStub.Reset()

		err := initAesKeyBySqlExecutor(context.Background(), nil, 0, "")
		assert.Equal(t, e, err)
	}

	{
		queryTableStub := gostub.Stub(
			&ForeachQueriedRow,
			func(
				context.Context,
				taskservice.SqlExecutor,
				string,
				func(ctx context.Context, rows *sql.Rows) (bool, error),
			) (int64, error) {
				return 0, nil
			})
		defer queryTableStub.Reset()

		err := initAesKeyBySqlExecutor(context.Background(), nil, 0, "")
		assert.Error(t, err)
	}

	{
		queryTableStub := gostub.Stub(
			&ForeachQueriedRow,
			func(
				context.Context,
				taskservice.SqlExecutor,
				string,
				func(ctx context.Context, rows *sql.Rows) (bool, error),
			) (int64, error) {
				return 1, nil
			})
		defer queryTableStub.Reset()

		decryptStub := gostub.Stub(
			&cdc.AesCFBDecodeWithKey,
			func(context.Context, string, []byte) (string, error) {
				return "aesKey", nil
			},
		)
		defer decryptStub.Reset()

		getGlobalPuStub := gostub.Stub(
			&getGlobalPuWrapper,
			func(string) *config.ParameterUnit {
				return &config.ParameterUnit{
					SV: &config.FrontendParameters{
						KeyEncryptionKey: "kek",
					},
				}
			},
		)
		defer getGlobalPuStub.Reset()

		err := initAesKeyBySqlExecutor(context.Background(), nil, 0, "")
		assert.NoError(t, err)
		assert.Equal(t, "aesKey", cdc.AesKey)
		cdc.AesKey = ""
	}
}

var _ ie.InternalExecutor = &mockIe{}

type mockIe struct {
	cnt int
}

func (*mockIe) Exec(ctx context.Context, s string, options ie.SessionOverrideOptions) error {
	//TODO implement me
	panic("implement me")
}

func (e *mockIe) Query(ctx context.Context, s string, options ie.SessionOverrideOptions) ie.InternalExecResult {
	e.cnt += 1

	if e.cnt == 1 {
		return &mockIeResult{
			err:      nil,
			rowCount: 1,
		}
	} else if e.cnt == 2 {
		return &mockIeResult{
			err:      nil,
			rowCount: 0,
		}
	} else if e.cnt == 3 {
		return &mockIeResult{
			err: moerr.NewInternalErrorNoCtx(""),
		}
	}
	return &mockIeResult{
		err:       nil,
		rowCount:  1,
		returnErr: true,
	}
}

func (*mockIe) ApplySessionOverride(options ie.SessionOverrideOptions) {
	//TODO implement me
	panic("implement me")
}

var _ ie.InternalExecResult = &mockIeResult{}

type mockIeResult struct {
	err       error
	rowCount  uint64
	returnErr bool
}

func (r *mockIeResult) Error() error {
	return r.err
}

func (*mockIeResult) ColumnCount() uint64 {
	//TODO implement me
	panic("implement me")
}

func (*mockIeResult) Column(ctx context.Context, u uint64) (string, uint8, bool, error) {
	//TODO implement me
	panic("implement me")
}

func (r *mockIeResult) RowCount() uint64 {
	return r.rowCount
}

func (*mockIeResult) Row(ctx context.Context, u uint64) ([]interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (*mockIeResult) Value(ctx context.Context, u uint64, u2 uint64) (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

func (*mockIeResult) GetUint64(ctx context.Context, u uint64, u2 uint64) (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (*mockIeResult) GetFloat64(ctx context.Context, u uint64, u2 uint64) (float64, error) {
	//TODO implement me
	panic("implement me")
}

func (r *mockIeResult) GetString(ctx context.Context, u uint64, u2 uint64) (string, error) {
	if r.returnErr {
		return "", moerr.NewInternalErrorNoCtx("")
	}
	return "", nil
}

func TestTaskExecutor_initAesKeyByInternalExecutor(t *testing.T) {
	mie := &mockIe{}
	taskExecutor := &CDCTaskExecutor{
		ie: mie,
	}

	decryptStub := gostub.Stub(&cdc.AesCFBDecodeWithKey, func(context.Context, string, []byte) (string, error) {
		return "aesKey", nil
	})
	defer decryptStub.Reset()

	getGlobalPuStub := gostub.Stub(&getGlobalPuWrapper, func(string) *config.ParameterUnit {
		return &config.ParameterUnit{
			SV: &config.FrontendParameters{
				KeyEncryptionKey: "kek",
			},
		}
	})
	defer getGlobalPuStub.Reset()

	err := taskExecutor.initAesKeyByInternalExecutor(context.Background(), 0)
	assert.NoError(t, err)
	cdc.AesKey = ""

	err = taskExecutor.initAesKeyByInternalExecutor(context.Background(), 0)
	assert.Error(t, err)

	err = taskExecutor.initAesKeyByInternalExecutor(context.Background(), 0)
	assert.Error(t, err)

	err = taskExecutor.initAesKeyByInternalExecutor(context.Background(), 0)
	assert.Error(t, err)
}

func TestCdcTask_handleNewTables(t *testing.T) {
	stub1 := gostub.Stub(&cdc.GetTxnOp, func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error) {
		return nil, nil
	})
	defer stub1.Reset()

	stub2 := gostub.Stub(&cdc.FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {})
	defer stub2.Reset()

	// Setup CDC test stubs for TableChangeStream
	cdcStubs := setupCDCTestStubs(t)
	defer func() {
		for _, s := range cdcStubs {
			s.Reset()
		}
	}()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	cdcTask := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			Accounts: []*task.Account{{Id: 0}},
		},
		tables: cdc.PatternTuples{
			Pts: []*cdc.PatternTuple{
				{
					Source: cdc.PatternTable{
						Database: "db1",
						Table:    cdc.CDCPitrGranularity_All,
					},
				},
			},
		},
		exclude:        regexp.MustCompile("db1.tb1"),
		cnEngine:       eng,
		runningReaders: &sync.Map{},
	}

	mp := map[uint32]cdc.TblMap{
		0: {
			"db1.tb1": &cdc.DbTableInfo{},
			"db2.tb1": &cdc.DbTableInfo{},
		},
	}
	cdcTask.handleNewTables(mp)
}

func TestCdcTask_handleNewTables_addpipeline(t *testing.T) {
	stub1 := gostub.Stub(&cdc.GetTxnOp, func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error) {
		return nil, nil
	})
	defer stub1.Reset()

	stub2 := gostub.Stub(&cdc.FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {})
	defer stub2.Reset()

	stub3 := gostub.Stub(&GetTableErrMsg, func(context.Context, uint32, ie.InternalExecutor, string, *cdc.DbTableInfo) (bool, error) {
		return false, nil
	})
	defer stub3.Reset()

	// Setup CDC test stubs for TableChangeStream
	cdcStubs := setupCDCTestStubs(t)
	defer func() {
		for _, s := range cdcStubs {
			s.Reset()
		}
	}()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	cdcTask := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			Accounts: []*task.Account{{Id: 0}},
		},
		tables: cdc.PatternTuples{
			Pts: []*cdc.PatternTuple{
				{
					Source: cdc.PatternTable{
						Database: "db1",
						Table:    cdc.CDCPitrGranularity_All,
					},
				},
			},
		},
		exclude:        regexp.MustCompile("db1.tb1"),
		cnEngine:       eng,
		runningReaders: &sync.Map{},
	}

	mp := map[uint32]cdc.TblMap{
		0: {
			"db1.tb1": &cdc.DbTableInfo{},
			"db1.tb2": &cdc.DbTableInfo{IdChanged: true},
		},
	}

	fault.Enable()
	objectio.SimpleInject(objectio.FJ_CDCAddExecErr)
	err := cdcTask.handleNewTables(mp)
	require.Error(t, err)
	fault.Disable()

	fault.Enable()
	objectio.SimpleInject(objectio.FJ_CDCAddExecConsumeTruncate)
	err = cdcTask.handleNewTables(mp)
	require.NoError(t, err)
	require.Equal(t, false, mp[0]["db1.tb2"].IdChanged)
	fault.Disable()
}

func TestCdcTask_handleNewTables_GetTxnOpErr(t *testing.T) {
	stub1 := gostub.Stub(&cdc.GetTxnOp, func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error) {
		return nil, moerr.NewInternalErrorNoCtx("ERR")
	})
	defer stub1.Reset()

	stub2 := gostub.Stub(&cdc.FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {})
	defer stub2.Reset()

	// Setup CDC test stubs for TableChangeStream (though this test should fail before creating reader)
	cdcStubs := setupCDCTestStubs(t)
	defer func() {
		for _, s := range cdcStubs {
			s.Reset()
		}
	}()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	cdcTask := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			Accounts: []*task.Account{{Id: 0}},
		},
		tables: cdc.PatternTuples{
			Pts: []*cdc.PatternTuple{
				{
					Source: cdc.PatternTable{
						Database: "db1",
						Table:    cdc.CDCPitrGranularity_All,
					},
				},
			},
		},
		exclude:        regexp.MustCompile("db1.tb1"),
		cnEngine:       eng,
		runningReaders: &sync.Map{},
	}

	mp := map[uint32]cdc.TblMap{
		0: {
			"db1.tb1": &cdc.DbTableInfo{},
			"db2.tb1": &cdc.DbTableInfo{},
		},
	}
	err := cdcTask.handleNewTables(mp)
	require.Error(t, err)
}

func TestCdcTask_handleNewTables_NewEngineFailed(t *testing.T) {
	stub1 := gostub.Stub(&cdc.GetTxnOp, func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error) {
		return nil, nil
	})
	defer stub1.Reset()

	stub2 := gostub.Stub(&cdc.FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {})
	defer stub2.Reset()

	// Setup CDC test stubs for TableChangeStream
	cdcStubs := setupCDCTestStubs(t)
	defer func() {
		for _, s := range cdcStubs {
			s.Reset()
		}
	}()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	cdcTask := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{
			Accounts: []*task.Account{{Id: 0}},
		},
		tables: cdc.PatternTuples{
			Pts: []*cdc.PatternTuple{
				{
					Source: cdc.PatternTable{
						Database: "db1",
						Table:    cdc.CDCPitrGranularity_All,
					},
				},
			},
		},
		exclude:        regexp.MustCompile("db1.tb1"),
		cnEngine:       eng,
		runningReaders: &sync.Map{},
	}

	mp := map[uint32]cdc.TblMap{
		0: {
			"db1.tb1": &cdc.DbTableInfo{},
			"db2.tb1": &cdc.DbTableInfo{},
		},
	}
	fault.Enable()
	objectio.SimpleInject(objectio.FJ_CDCHandleErr)
	err := cdcTask.handleNewTables(mp)
	require.Error(t, err)
	fault.Disable()
}

func TestCdcTask_handleNewTables_existingReaderWithDifferentTableID(t *testing.T) {
	stub1 := gostub.Stub(&cdc.GetTxnOp, func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error) {
		return nil, nil
	})
	defer stub1.Reset()

	stub2 := gostub.Stub(&cdc.FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {})
	defer stub2.Reset()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	oldReader := &mockChangeReader{
		info: &cdc.DbTableInfo{SourceTblId: 100},
		wg:   wg,
	}

	cdcTask := &CDCTaskExecutor{
		spec: &task.CreateCdcDetails{Accounts: []*task.Account{{Id: 0}}},
		tables: cdc.PatternTuples{
			Pts: []*cdc.PatternTuple{
				{
					Source: cdc.PatternTable{
						Database: "db1",
						Table:    cdc.CDCPitrGranularity_All,
					},
				},
			},
		},
		exclude:        regexp.MustCompile("db1.important_table"),
		cnEngine:       eng,
		runningReaders: &sync.Map{},
	}
	cdcTask.runningReaders.Store("db1.important_table", oldReader)

	go func() {
		time.Sleep(1000 * time.Millisecond)
		wg.Done()
	}()

	newTable := &cdc.DbTableInfo{SourceTblId: 200}
	mp := map[uint32]cdc.TblMap{
		0: {"db1.important_table": newTable},
	}

	cdcTask.handleNewTables(mp)
}

// setupCDCTestStubs sets up all necessary stubs for CDC tests that create TableChangeStream
// This prevents nil pointer panics when TableChangeStream.Run() is called in goroutines
func setupCDCTestStubs(t *testing.T) []*gostub.Stubs {
	var stubs []*gostub.Stubs

	// Stub GetTableDef to return a valid tableDef
	stubs = append(stubs, gostub.Stub(&cdc.GetTableDef, func(context.Context, client.TxnOperator, engine.Engine, uint64) (*plan.TableDef, error) {
		return &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "id"},
				{Name: "ts"},
			},
			Pkey: &plan.PrimaryKeyDef{
				Names: []string{"id"},
			},
			Name2ColIndex: map[string]int32{
				"id": 0,
				"ts": 1,
			},
		}, nil
	}))

	// Stub GetTxn
	stubs = append(stubs, gostub.Stub(&cdc.GetTxn, func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator) error {
		return nil
	}))

	// Stub GetRelationById to prevent nil pointer in processOneRound
	stubs = append(stubs, gostub.Stub(&cdc.GetRelationById, func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator, tableId uint64) (dbName string, tblName string, rel engine.Relation, err error) {
		// Return error to stop the reader loop quickly
		return "", "", nil, moerr.NewInternalError(ctx, "test stub - no relation")
	}))

	// Stub EnterRunSql and ExitRunSql
	stubs = append(stubs, gostub.Stub(&cdc.EnterRunSql, func(client.TxnOperator) {}))
	stubs = append(stubs, gostub.Stub(&cdc.ExitRunSql, func(client.TxnOperator) {}))

	return stubs
}

type mockReader struct{}

func (m mockReader) Run(ctx context.Context, ar *cdc.ActiveRoutine) {}

func (m mockReader) Close() {}

type mockChangeReader struct {
	info *cdc.DbTableInfo
	wg   *sync.WaitGroup
}

func (m mockChangeReader) Run(ctx context.Context, ar *cdc.ActiveRoutine) {}

func (m mockChangeReader) Close() {}

func (m mockChangeReader) Wait() {
	if m.wg != nil {
		m.wg.Wait()
	}
}

func (m mockChangeReader) GetTableInfo() *cdc.DbTableInfo {
	return m.info
}

type mockSinker struct{}

func (m mockSinker) Run(ctx context.Context, ar *cdc.ActiveRoutine) {}

func (m mockSinker) Sink(ctx context.Context, data *cdc.DecoderOutput) {
	//TODO implement me
	panic("implement me")
}

func (m mockSinker) SendBegin() {
	//TODO implement me
	panic("implement me")
}

func (m mockSinker) SendCommit() {
	//TODO implement me
	panic("implement me")
}

func (m mockSinker) SendRollback() {
	//TODO implement me
	panic("implement me")
}

func (m mockSinker) SendDummy() {
	//TODO implement me
	panic("implement me")
}

func (m mockSinker) Error() error {
	//TODO implement me
	panic("implement me")
}

func (m mockSinker) Reset() {
	// No-op for mock
}

func (m mockSinker) Close() {
	// No-op for mock - Close() is called during cleanup
}

func (m mockSinker) ClearError() {}

func TestCdcTask_addExecPipelineForTable(t *testing.T) {
	u, _ := cdc.InitCDCWatermarkUpdaterForTest(t)
	u.Start()
	defer u.Stop()
	cdcTask := &CDCTaskExecutor{
		activeRoutine:    cdc.NewCdcActiveRoutine(), // Required for reader.Run()
		watermarkUpdater: u,
		runningReaders:   &sync.Map{},
		noFull:           true,
		additionalConfig: map[string]interface{}{
			cdc.CDCTaskExtraOptions_MaxSqlLength:         float64(cdc.CDCDefaultTaskExtra_MaxSQLLen),
			cdc.CDCTaskExtraOptions_SendSqlTimeout:       cdc.CDCDefaultSendSqlTimeout,
			cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn: cdc.CDCDefaultTaskExtra_InitSnapshotSplitTxn,
			cdc.CDCTaskExtraOptions_Frequency:            "",
		},
		spec: &task.CreateCdcDetails{
			Accounts: []*task.Account{{Id: 0}},
		},
	}

	info := &cdc.DbTableInfo{
		SourceDbId:      0,
		SourceDbName:    "",
		SourceTblId:     0,
		SourceTblName:   "",
		SourceCreateSql: "",
		SinkDbName:      "",
		SinkTblName:     "",
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()

	// Stub GetTxnOp to prevent nil pointer in TableChangeStream.Run()
	stubGetTxnOp := gostub.Stub(&cdc.GetTxnOp, func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
		return nil, nil
	})
	defer stubGetTxnOp.Reset()

	stubFinishTxnOp := gostub.Stub(&cdc.FinishTxnOp, func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {})
	defer stubFinishTxnOp.Reset()

	stubGetTxn := gostub.Stub(&cdc.GetTxn, func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator) error {
		return nil
	})
	defer stubGetTxn.Reset()

	stubGetTableDef := gostub.Stub(&cdc.GetTableDef, func(context.Context, client.TxnOperator, engine.Engine, uint64) (*plan.TableDef, error) {
		// Return a valid tableDef to prevent panic in NewTableChangeStream
		return &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "id"},
				{Name: "ts"},
			},
			Pkey: &plan.PrimaryKeyDef{
				Names: []string{"id"},
			},
			Name2ColIndex: map[string]int32{
				"id": 0,
				"ts": 1,
			},
		}, nil
	})
	defer stubGetTableDef.Reset()

	stubSinker := gostub.Stub(
		&cdc.NewSinker,
		func(
			cdc.UriInfo,
			uint64,
			string,
			*cdc.DbTableInfo,
			*cdc.CDCWatermarkUpdater,
			*plan.TableDef,
			int,
			time.Duration,
			*cdc.ActiveRoutine,
			uint64,
			string,
		) (cdc.Sinker, error) {
			return &mockSinker{}, nil
		})
	defer stubSinker.Reset()

	// Don't stub NewTableChangeStream - let it create a real reader
	// The real reader needs proper initialization which the mocks above don't provide
	// So the reader will be created but Run() might fail - which is OK for this test
	// The test just checks that addExecPipelineForTable returns without error

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	assert.NoError(t, cdcTask.addExecPipelineForTable(ctx, info, txnOperator))

	// Get the created reader from runningReaders and wait for it to complete
	// This ensures the goroutine finishes before test cleanup
	key := cdc.GenDbTblKey(info.SourceDbName, info.SourceTblName)
	if val, ok := cdcTask.runningReaders.Load(key); ok {
		if reader, ok := val.(cdc.ChangeReader); ok {
			// Cancel the context to stop the reader
			cancel()
			// Wait for the reader goroutine to finish
			reader.Wait()
		}
	}
}

func TestCdcTask_checkPitr(t *testing.T) {
	pts := &cdc.PatternTuples{
		Pts: []*cdc.PatternTuple{
			{
				Source: cdc.PatternTable{
					Database: "db1",
					Table:    "tb1",
				},
			},
			{
				Source: cdc.PatternTable{
					Database: "db2",
					Table:    cdc.CDCPitrGranularity_All,
				},
			},
			{
				Source: cdc.PatternTable{
					Database: cdc.CDCPitrGranularity_All,
					Table:    cdc.CDCPitrGranularity_All,
				},
			},
		},
	}

	stubGetPitrLength := gostub.Stub(&getPitrLengthAndUnit,
		func(_ context.Context, _ BackgroundExec, level, _, _, _ string) (int64, string, bool, error) {
			return 0, "", level == "table", nil
		},
	)
	err := CDCCheckPitrGranularity(context.Background(), nil, "acc1", pts, 0)
	assert.Error(t, err)
	stubGetPitrLength.Reset()

	stubGetPitrLength = gostub.Stub(&getPitrLengthAndUnit,
		func(_ context.Context, _ BackgroundExec, _, _, _, _ string) (int64, string, bool, error) {
			return 0, "", false, moerr.NewInternalErrorNoCtx("")
		},
	)
	err = CDCCheckPitrGranularity(context.Background(), nil, "acc1", pts, 0, 1)
	assert.Error(t, err)
	stubGetPitrLength.Reset()

	stubGetPitrLength = gostub.Stub(&getPitrLengthAndUnit,
		func(_ context.Context, _ BackgroundExec, _, _, _, _ string) (int64, string, bool, error) {
			return 0, "", true, nil
		},
	)
	err = CDCCheckPitrGranularity(context.Background(), nil, "acc1", pts)
	assert.NoError(t, err)
	stubGetPitrLength.Reset()
}

func Test_parseTimestamp(t *testing.T) {
	_, err := CDCStrToTime("2006-01-02 15:04:05", time.Local)
	assert.NoError(t, err)

	_, err = CDCStrToTime("2006-01-02T15:04:05-07:00", nil)
	assert.NoError(t, err)
}

func TestCDCParseGranularityTuple(t *testing.T) {
	tests := []struct {
		name    string
		level   string
		pattern string
		dup     map[string]struct{}
		want    *cdc.PatternTuple
		wantErr bool
	}{
		{
			name:    "db level - single db",
			level:   cdc.CDCPitrGranularity_DB,
			pattern: "db1",
			dup:     make(map[string]struct{}),
			want: &cdc.PatternTuple{
				OriginString: "db1",
				Source: cdc.PatternTable{
					Database: "db1",
					Table:    cdc.CDCPitrGranularity_All,
				},
				Sink: cdc.PatternTable{
					Database: "db1",
					Table:    cdc.CDCPitrGranularity_All,
				},
			},
			wantErr: false,
		},
		{
			name:    "db level - source and sink",
			level:   cdc.CDCPitrGranularity_DB,
			pattern: "db1:db2",
			dup:     make(map[string]struct{}),
			want: &cdc.PatternTuple{
				OriginString: "db1:db2",
				Source: cdc.PatternTable{
					Database: "db1",
					Table:    cdc.CDCPitrGranularity_All,
				},
				Sink: cdc.PatternTable{
					Database: "db2",
					Table:    cdc.CDCPitrGranularity_All,
				},
			},
			wantErr: false,
		},
		{
			name:    "table level - single table",
			level:   cdc.CDCPitrGranularity_Table,
			pattern: "db1.t1",
			dup:     make(map[string]struct{}),
			want: &cdc.PatternTuple{
				OriginString: "db1.t1",
				Source: cdc.PatternTable{
					Database: "db1",
					Table:    "t1",
				},
				Sink: cdc.PatternTable{
					Database: "db1",
					Table:    "t1",
				},
			},
			wantErr: false,
		},
		{
			name:    "table level - source and sink",
			level:   cdc.CDCPitrGranularity_Table,
			pattern: "db1.t1:db2.t2",
			dup:     make(map[string]struct{}),
			want: &cdc.PatternTuple{
				OriginString: "db1.t1:db2.t2",
				Source: cdc.PatternTable{
					Database: "db1",
					Table:    "t1",
				},
				Sink: cdc.PatternTable{
					Database: "db2",
					Table:    "t2",
				},
			},
			wantErr: false,
		},
		{
			name:    "invalid pattern - multiple colons",
			level:   cdc.CDCPitrGranularity_DB,
			pattern: "db1:db2:db3",
			dup:     make(map[string]struct{}),
			want:    nil,
			wantErr: true,
		},
		{
			name:    "no duplicate source",
			level:   cdc.CDCPitrGranularity_DB,
			pattern: "db1",
			dup:     map[string]struct{}{"db1": {}},
			want: &cdc.PatternTuple{
				OriginString: "db1",
				Source: cdc.PatternTable{
					Database: "db1",
					Table:    cdc.CDCPitrGranularity_All,
				},
				Sink: cdc.PatternTable{
					Database: "db1",
					Table:    cdc.CDCPitrGranularity_All,
				},
			},
			wantErr: false,
		},
		{
			name:    "duplicate source",
			level:   cdc.CDCPitrGranularity_DB,
			pattern: "db1",
			dup:     map[string]struct{}{"db1.*": {}},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CDCParseGranularityTuple(context.Background(), tt.level, tt.pattern, tt.dup)
			t.Logf("got: %v, err: %v", got, err)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCDCCreateTaskOptions_handleLevel(t *testing.T) {
	opts := &CDCCreateTaskOptions{}
	err := opts.handleLevel(context.Background(), nil, nil, "db")
	assert.Error(t, err)

	req := &CDCCreateTaskRequest{
		Tables: "db1.t1:db2.t2,db1.t1:db4.t4",
	}
	level := cdc.CDCPitrGranularity_Table
	err = opts.handleLevel(context.Background(), nil, req, level)
	assert.Error(t, err)
}

func TestCDCCreateTaskOptions_handleFrequency(t *testing.T) {
	opts := &CDCCreateTaskOptions{}
	err := opts.handleFrequency(context.Background(), nil, nil, "db", "1m")
	assert.Error(t, err)

	req := &CDCCreateTaskRequest{
		Tables: "db1.t1:db2.t2,db1.t1:db4.t4",
	}
	level := cdc.CDCPitrGranularity_Table
	err = opts.handleFrequency(context.Background(), nil, req, level, "abc")
	assert.Error(t, err)
	err = opts.handleFrequency(context.Background(), nil, req, level, "1h")
	assert.Error(t, err)
}

func TestIsValidFrequency(t *testing.T) {
	tests := []struct {
		freq  string
		valid bool
	}{
		{"15m", true},
		{"1h", true},
		{"60h", true},

		{"", false},
		{"01m", false},
		{"001m", false},
		{"0m", false},
		{"00m", false},
		{"-1h", false},
		{"2.5h", false},
		{"1s", false},
		{"1", false},
		{"m", false},
		{"h", false},
		{"abc", false},
		{"10min", false},
		{" 1h", false},
		{"1h ", false},
		{"1H", false},
		{"1M", false},
		{"10000001m", false},
	}

	for _, tt := range tests {
		t.Run(tt.freq, func(t *testing.T) {
			if got := isValidFrequency(tt.freq); got != tt.valid {
				t.Errorf("isValidFrequency(%q) = %v, want %v", tt.freq, got, tt.valid)
			}
		})
	}
}

func TestTransformIntoHours(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"15m", 1},
		{"30m", 1},
		{"59m", 1},
		{"60m", 1},
		{"61m", 2},
		{"120m", 2},
		{"121m", 3},

		{"1h", 1},
		{"24h", 24},

		{"", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := transformIntoHours(tt.input); got != tt.want {
				t.Errorf("transformIntoHours(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
