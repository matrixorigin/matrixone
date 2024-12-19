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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	cdc2 "github.com/matrixorigin/matrixone/pkg/cdc"
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
	sql := getSqlForNewCdcTask(
		3,
		id,
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
	wantSql := "insert into mo_catalog.mo_cdc_task values(3,\"019111fd-aed1-70c0-8760-9abadd8f0f4a\",\"task1\",\"src uri\",\"123\",\"dst uri\",\"mysql\",\"456\",\"ca path\",\"cert path\",\"key path\",\"db1:t1\",\"xfilter\",\"op filters\",\"error\",\"common\",\"\",\"\",\"conf path\",\"2024-08-02 15:20:00\",\"running\",125,\"125\",\"true\",\"yyy\",'{}',\"\",\"\",\"\",\"\")"
	assert.Equal(t, wantSql, sql)

	sql2 := getSqlForRetrievingCdcTask(3, id)
	wantSql2 := "select sink_uri, sink_type, sink_password, tables, filters, no_full, additional_config from mo_catalog.mo_cdc_task where account_id = 3 and task_id = \"019111fd-aed1-70c0-8760-9abadd8f0f4a\""
	assert.Equal(t, wantSql2, sql2)

	sql3 := getSqlForDbIdAndTableId(10, "db", "t1")
	wantSql3 := "select reldatabase_id,rel_id from mo_catalog.mo_tables where account_id = 10 and reldatabase = 'db' and relname = 't1'"
	assert.Equal(t, wantSql3, sql3)

	sql4 := getSqlForGetTable(10, "db", "t1")
	wantsql4 := "select rel_id from `mo_catalog`.`mo_tables` where account_id = 10 and reldatabase ='db' and relname = 't1'"
	assert.Equal(t, wantsql4, sql4)

	sql5 := getSqlForCheckAccount("acc1")
	wantsql5 := "select account_id from `mo_catalog`.`mo_account` where account_name='acc1'"
	assert.Equal(t, wantsql5, sql5)

	sql6 := getSqlForGetPkCount(13, "db1", "t2")
	wantsql6 := "select count(att_constraint_type) from `mo_catalog`.`mo_columns` where account_id = 13 and att_database = 'db1' and att_relname = 't2' and att_constraint_type = 'p'"
	assert.Equal(t, wantsql6, sql6)

	sql7 := getSqlForDeleteWatermark(13, "task1")
	wantsql7 := "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = 13 and task_id = 'task1'"
	assert.Equal(t, wantsql7, sql7)
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
		expect  *cdc2.PatternTuples
	}

	kases := []kase{
		// table level
		{
			tables:  "db1.t1:db2.t2,db3.t3:db4.t4",
			level:   cdc2.TableLevel,
			wantErr: false,
			expect: &cdc2.PatternTuples{
				Pts: []*cdc2.PatternTuple{
					{
						Source: cdc2.PatternTable{
							Database: "db1",
							Table:    "t1",
						},
						Sink: cdc2.PatternTable{
							Database: "db2",
							Table:    "t2",
						},
					},
					{
						Source: cdc2.PatternTable{
							Database: "db3",
							Table:    "t3",
						},
						Sink: cdc2.PatternTable{
							Database: "db4",
							Table:    "t4",
						},
					},
				},
			},
		},
		{
			tables:  "db1.t1,db3.t3:db4.t4",
			level:   cdc2.TableLevel,
			wantErr: false,
			expect: &cdc2.PatternTuples{
				Pts: []*cdc2.PatternTuple{
					{
						Source: cdc2.PatternTable{
							Database: "db1",
							Table:    "t1",
						},
						Sink: cdc2.PatternTable{
							Database: "db1",
							Table:    "t1",
						},
					},
					{
						Source: cdc2.PatternTable{
							Database: "db3",
							Table:    "t3",
						},
						Sink: cdc2.PatternTable{
							Database: "db4",
							Table:    "t4",
						},
					},
				},
			},
		},
		{
			tables:  "db1.t1,db3.t3",
			level:   cdc2.TableLevel,
			wantErr: false,
			expect: &cdc2.PatternTuples{
				Pts: []*cdc2.PatternTuple{
					{
						Source: cdc2.PatternTable{
							Database: "db1",
							Table:    "t1",
						},
						Sink: cdc2.PatternTable{
							Database: "db1",
							Table:    "t1",
						},
					},
					{
						Source: cdc2.PatternTable{
							Database: "db3",
							Table:    "t3",
						},
						Sink: cdc2.PatternTable{
							Database: "db3",
							Table:    "t3",
						},
					},
				},
			},
		},
		{
			tables:  "db1.t1:db2.t2,db1.t1:db4.t4",
			level:   cdc2.TableLevel,
			wantErr: true,
		},

		// db level
		{
			tables:  "db1:db2,db3:db4",
			level:   cdc2.DbLevel,
			wantErr: false,
			expect: &cdc2.PatternTuples{
				Pts: []*cdc2.PatternTuple{
					{
						Source: cdc2.PatternTable{
							Database: "db1",
							Table:    cdc2.MatchAll,
						},
						Sink: cdc2.PatternTable{
							Database: "db2",
							Table:    cdc2.MatchAll,
						},
					},
					{
						Source: cdc2.PatternTable{
							Database: "db3",
							Table:    cdc2.MatchAll,
						},
						Sink: cdc2.PatternTable{
							Database: "db4",
							Table:    cdc2.MatchAll,
						},
					},
				},
			},
		},
		{
			tables:  "db1,db3:db4",
			level:   cdc2.DbLevel,
			wantErr: false,
			expect: &cdc2.PatternTuples{
				Pts: []*cdc2.PatternTuple{
					{
						Source: cdc2.PatternTable{
							Database: "db1",
							Table:    cdc2.MatchAll,
						},
						Sink: cdc2.PatternTable{
							Database: "db1",
							Table:    cdc2.MatchAll,
						},
					},
					{
						Source: cdc2.PatternTable{
							Database: "db3",
							Table:    cdc2.MatchAll,
						},
						Sink: cdc2.PatternTable{
							Database: "db4",
							Table:    cdc2.MatchAll,
						},
					},
				},
			},
		},
		{
			tables:  "db1,db3",
			level:   cdc2.DbLevel,
			wantErr: false,
			expect: &cdc2.PatternTuples{
				Pts: []*cdc2.PatternTuple{
					{
						Source: cdc2.PatternTable{
							Database: "db1",
							Table:    cdc2.MatchAll,
						},
						Sink: cdc2.PatternTable{
							Database: "db1",
							Table:    cdc2.MatchAll,
						},
					},
					{
						Source: cdc2.PatternTable{
							Database: "db3",
							Table:    cdc2.MatchAll,
						},
						Sink: cdc2.PatternTable{
							Database: "db3",
							Table:    cdc2.MatchAll,
						},
					},
				},
			},
		},

		// account level
		{
			tables:  "",
			level:   cdc2.AccountLevel,
			wantErr: false,
			expect: &cdc2.PatternTuples{
				Pts: []*cdc2.PatternTuple{
					{
						Source: cdc2.PatternTable{
							Database: cdc2.MatchAll,
							Table:    cdc2.MatchAll,
						},
						Sink: cdc2.PatternTable{
							Database: cdc2.MatchAll,
							Table:    cdc2.MatchAll,
						},
					},
				},
			},
		},
	}

	isSame := func(pt0, pt1 *cdc2.PatternTuples) {
		assert.Equal(t, len(pt0.Pts), len(pt1.Pts))
		for i := 0; i < len(pt0.Pts); i++ {
			assert.Equal(t, pt0.Pts[i].Source.Database, pt1.Pts[i].Source.Database)
			assert.Equal(t, pt0.Pts[i].Source.Table, pt1.Pts[i].Source.Table)
			assert.Equal(t, pt0.Pts[i].Sink.Database, pt1.Pts[i].Sink.Database)
			assert.Equal(t, pt0.Pts[i].Sink.Table, pt1.Pts[i].Sink.Table)
		}
	}

	for _, tkase := range kases {
		pts, err := getPatternTuples(context.Background(), tkase.level, tkase.tables)
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

	sql4 := "insert into mo_catalog.mo_cdc_task .*"
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
		SinkType:    cdc2.MysqlSink,
		SinkUri:     "mysql://root:111@127.0.0.1:3306",
		Tables:      "db1.t1:db1.t1,db1.t2",
		Option: []string{
			"Level",
			cdc2.TableLevel,
			"Account",
			sysAccountName,
			"Exclude",
			"db2.t3,db2.t4",
			cdc2.InitSnapshotSplitTxn,
			"false",
			cdc2.MaxSqlLength,
			fmt.Sprintf("%d", cdc2.DefaultMaxSqlLength),
			cdc2.SendSqlTimeout,
			cdc2.DefaultSendSqlTimeout,
		},
	}

	ses.GetTxnCompileCtx().execCtx.stmt = create

	cdc2.AesKey = "test-aes-key-not-use-it-in-cloud"
	defer func() { cdc2.AesKey = "" }()
	stub := gostub.Stub(&initAesKeyWrapper, func(context.Context, taskservice.SqlExecutor, uint32, string) (err error) {
		return nil
	})
	defer stub.Reset()

	stubOpenDbConn := gostub.Stub(&cdc2.OpenDbConn, func(_, _, _ string, _ int, _ string) (*sql.DB, error) {
		return nil, nil
	})
	defer stubOpenDbConn.Reset()

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
	taskKeyMap map[taskservice.CdcTaskKey]struct{}
	sqlExec    taskservice.SqlExecutor
}

func (ts *testTaskService) TruncateCompletedTasks(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (ts *testTaskService) AddCdcTask(ctx context.Context,
	metadata task.TaskMetadata,
	details *task.Details,
	callback func(context.Context, taskservice.SqlExecutor) (int, error)) (int, error) {
	ts.data = append(ts.data, testTaskData{
		metadata: metadata,
		details:  details,
	})
	return callback(ctx, ts.db)
}

func (ts *testTaskService) UpdateCdcTask(
	ctx context.Context,
	status task.TaskStatus,
	callback func(context.Context, task.TaskStatus, map[taskservice.CdcTaskKey]struct{}, taskservice.SqlExecutor) (int, error),
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
			noFull := ""
			splitTxn := ""
			err = rows.Scan(
				&sinkUri,
				&sinkType,
				&sinkPwd,
				&tables,
				&filters,
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

	cdc2.AesKey = "test-aes-key-not-use-it-in-cloud"
	defer func() { cdc2.AesKey = "" }()

	cdc2.EnableConsoleSink = true
	defer func() {
		cdc2.EnableConsoleSink = false
	}()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	sinkUri, err := cdc2.JsonEncode(&cdc2.UriInfo{
		User: "root",
		Ip:   "127.0.0.1",
		Port: 3306,
	})
	assert.NoError(t, err)
	pwd, err := cdc2.AesCFBEncode([]byte("111"))
	assert.NoError(t, err)
	tables, err := cdc2.JsonEncode(cdc2.PatternTuples{ //tables
		Pts: []*cdc2.PatternTuple{
			{
				Source: cdc2.PatternTable{
					Database: "db1",
					Table:    "t1",
				},
			},
		},
	})
	assert.NoError(t, err)
	filters, err := cdc2.JsonEncode(cdc2.PatternTuples{})
	assert.NoError(t, err)

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	/////////mock sql result
	sql1 := `select sink_uri, sink_type, sink_password, tables, filters, no_full, additional_config from mo_catalog.mo_cdc_task where account_id = 0 and task_id = "00000000-0000-0000-0000-000000000000"`
	mock.ExpectQuery(sql1).WillReturnRows(sqlmock.NewRows(
		[]string{
			"sink_uri",
			"sink_type",
			"sink_password",
			"tables",
			"filters",
			"no_full",
			"additional_config",
		},
	).AddRow(
		sinkUri,
		cdc2.ConsoleSink,
		pwd,
		tables,
		filters,
		true,
		fmt.Sprintf("{\"%s\":%v,\"%s\":\"%s\",\"%s\":%v}",
			cdc2.InitSnapshotSplitTxn, cdc2.DefaultInitSnapshotSplitTxn,
			cdc2.SendSqlTimeout, cdc2.DefaultSendSqlTimeout,
			cdc2.MaxSqlLength, cdc2.DefaultMaxSqlLength,
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

	gostub.Stub(&cdc2.GetTableScanner, func(cnUUID string) *cdc2.TableScanner {
		return &cdc2.TableScanner{
			Mutex:     sync.Mutex{},
			Mp:        make(map[uint32]cdc2.TblMap),
			Callbacks: map[string]func(map[uint32]cdc2.TblMap){"id": func(mp map[uint32]cdc2.TblMap) {}},
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
			fun := RegisterCdcExecutor(
				tt.args.logger,
				tt.args.ts,
				tt.args.ieFactory,
				tt.args.attachToTask,
				tt.args.cnUUID,
				tt.args.fileService,
				tt.args.cnTxnClient,
				tt.args.cnEngine,
				tt.args.cnEngMp)
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
		taskKeyMap   map[taskservice.CdcTaskKey]struct{}
		tx           taskservice.SqlExecutor
		accountId    uint64
		taskName     string
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql7 := "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"

	mock.ExpectQuery(sql7).WillReturnRows(
		sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql8 := "delete from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql8).WillReturnResult(sqlmock.NewResult(1, 1))

	sql9 := "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = 0 and task_id = 'taskID-1'"
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
				taskKeyMap:   map[taskservice.CdcTaskKey]struct{}{},
				tx:           tx,
				accountId:    sysAccountID,
				taskName:     "task1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := updateCdcTask(tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
			assert.NoError(t, err, "updateCdcTask(%v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
		})
	}
}

func Test_updateCdcTask_pause(t *testing.T) {
	type args struct {
		ctx          context.Context
		targetStatus task.TaskStatus
		taskKeyMap   map[taskservice.CdcTaskKey]struct{}
		tx           taskservice.SqlExecutor
		accountId    uint64
		taskName     string
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql10 := "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectQuery(sql10).WillReturnRows(
		sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql11 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectPrepare(sql11)

	sql12 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql12).WillReturnResult(sqlmock.NewResult(1, 1))

	sql13 := "delete from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql13).WillReturnResult(sqlmock.NewResult(1, 1))

	sql14 := "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = 0 and task_id = 'taskID-1'"
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
				taskKeyMap:   map[taskservice.CdcTaskKey]struct{}{},
				tx:           tx,
				accountId:    sysAccountID,
				taskName:     "task1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := updateCdcTask(tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
			assert.NoError(t, err, "updateCdcTask(%v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
		})
	}
}

func Test_updateCdcTask_restart(t *testing.T) {
	type args struct {
		ctx          context.Context
		targetStatus task.TaskStatus
		taskKeyMap   map[taskservice.CdcTaskKey]struct{}
		tx           taskservice.SqlExecutor
		accountId    uint64
		taskName     string
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql15 := "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectQuery(sql15).WillReturnRows(
		sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql16 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectPrepare(sql16)

	sql17 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql17).WillReturnResult(sqlmock.NewResult(1, 1))

	sql18 := "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = 0 and task_id = 'taskID-1'"
	mock.ExpectExec(sql18).WillReturnResult(sqlmock.NewResult(1, 1))

	sql19 := "delete from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
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
				taskKeyMap:   map[taskservice.CdcTaskKey]struct{}{},
				tx:           tx,
				accountId:    sysAccountID,
				taskName:     "task1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := updateCdcTask(tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
			assert.NoError(t, err, "updateCdcTask(%v, %v, %v, %v, %v, %v)", tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
		})
	}
}

func Test_updateCdcTask_resume(t *testing.T) {
	type args struct {
		ctx          context.Context
		targetStatus task.TaskStatus
		taskKeyMap   map[taskservice.CdcTaskKey]struct{}
		tx           taskservice.SqlExecutor
		accountId    uint64
		taskName     string
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sql20 := "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectQuery(sql20).WillReturnRows(
		sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql21 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectPrepare(sql21)

	sql22 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql22).WillReturnResult(sqlmock.NewResult(1, 1))

	sql23 := "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = 0 and task_id = 'taskID-1'"
	mock.ExpectExec(sql23).WillReturnResult(sqlmock.NewResult(1, 1))

	sql24 := "delete from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
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
				taskKeyMap:   map[taskservice.CdcTaskKey]struct{}{},
				tx:           tx,
				accountId:    sysAccountID,
				taskName:     "task1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := updateCdcTask(tt.args.ctx, tt.args.targetStatus, tt.args.taskKeyMap, tt.args.tx, tt.args.accountId, tt.args.taskName)
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
	sql25 := "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectQuery(sql25).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql26 := "delete from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql26).WillReturnResult(sqlmock.NewResult(1, 1))

	sql27 := "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = 0 and task_id = 'taskID-1'"
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

	taskKeyMap := make(map[taskservice.CdcTaskKey]struct{})
	taskKeyMap[taskservice.CdcTaskKey{
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
	sql28 := "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0"
	mock.ExpectQuery(sql28).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql29 := "delete from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0"
	mock.ExpectExec(sql29).WillReturnResult(sqlmock.NewResult(1, 1))

	sql30 := "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = 0 and task_id = 'taskID-1'"
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

	taskKeyMap := make(map[taskservice.CdcTaskKey]struct{})
	taskKeyMap[taskservice.CdcTaskKey{
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
			err = updateCdc(tt.args.ctx, tt.args.ses, tt.args.st)
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
	sql30 := "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectQuery(sql30).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql33 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectPrepare(sql33)

	sql34 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql34).WillReturnResult(sqlmock.NewResult(1, 1))

	sql31 := "delete from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql31).WillReturnResult(sqlmock.NewResult(1, 1))

	sql32 := "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = 0 and task_id = 'taskID-1'"
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

	taskKeyMap := make(map[taskservice.CdcTaskKey]struct{})
	taskKeyMap[taskservice.CdcTaskKey{
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
	sql35 := "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0"
	mock.ExpectQuery(sql35).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql36 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0"
	mock.ExpectPrepare(sql36)

	sql37 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0"
	mock.ExpectExec(sql37).WillReturnResult(sqlmock.NewResult(1, 1))

	sql38 := "delete from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql38).WillReturnResult(sqlmock.NewResult(1, 1))

	sql39 := "delete from `mo_catalog`.`mo_cdc_watermark` where account_id = 0 and task_id = 'taskID-1'"
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

	taskKeyMap := make(map[taskservice.CdcTaskKey]struct{})
	taskKeyMap[taskservice.CdcTaskKey{
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
			err = updateCdc(tt.args.ctx, tt.args.ses, tt.args.st)
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
	sql40 := "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectQuery(sql40).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql41 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectPrepare(sql41)

	sql42 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql42).WillReturnResult(sqlmock.NewResult(1, 1))

	sql44 := "delete from `mo_catalog`.`mo_cdc_watermark` where.*account_id = 0 and task_id = 'taskID-1'"
	mock.ExpectExec(sql44).WillReturnResult(sqlmock.NewResult(1, 1))

	sql43 := "delete from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
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

	taskKeyMap := make(map[taskservice.CdcTaskKey]struct{})
	taskKeyMap[taskservice.CdcTaskKey{
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
	sql45 := "select task_id from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectQuery(sql45).WillReturnRows(sqlmock.NewRows([]string{"task_id"}).AddRow("taskID-1"))

	sql46 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectPrepare(sql46)

	sql47 := "update `mo_catalog`.`mo_cdc_task` set state = .* where 1=1 and account_id = 0 and task_name = 'task1'"
	mock.ExpectExec(sql47).WillReturnResult(sqlmock.NewResult(1, 1))

	sql48 := "delete from `mo_catalog`.`mo_cdc_watermark` where.*account_id = 0 and task_id = 'taskID-1'"
	mock.ExpectExec(sql48).WillReturnResult(sqlmock.NewResult(1, 1))

	sql49 := "delete from `mo_catalog`.`mo_cdc_task` where 1=1 and account_id = 0 and task_name = 'task1'"
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

	taskKeyMap := make(map[taskservice.CdcTaskKey]struct{})
	taskKeyMap[taskservice.CdcTaskKey{
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

func Test_getTaskCkp(t *testing.T) {
	type args struct {
		ctx       context.Context
		bh        BackgroundExec
		accountId uint32
		taskId    string
	}

	bh := &backgroundExecTest{}
	bh.init()

	sql := getSqlForGetWatermark(sysAccountID, "taskID-1")
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
				bh:        bh,
				accountId: sysAccountID,
				taskId:    "taskID-1",
			},
			wantS: "{\n  \"db1.tb1\": " + timestamp.Timestamp{}.ToStdTime().In(time.Local).String() + ",\n}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := getTaskCkp(tt.args.ctx, tt.args.bh, tt.args.accountId, tt.args.taskId)
			assert.NoErrorf(t, err, "getTaskCkp(%v, %v, %v, %v)", tt.args.ctx, tt.args.bh, tt.args.accountId, tt.args.taskId)
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

	sourceUri, err := cdc2.JsonEncode(&cdc2.UriInfo{
		User: "root",
		Ip:   "127.0.0.1",
		Port: 6001,
	})
	assert.NoError(t, err)

	sinkUri, err := cdc2.JsonEncode(&cdc2.UriInfo{
		User: "root",
		Ip:   "127.0.0.1",
		Port: 6001,
	})
	assert.NoError(t, err)

	sql := getSqlForGetTask(sysAccountID, true, "")
	mrs := newMrsForGetTask([][]interface{}{
		{"taskID-1", "task1", sourceUri, sinkUri, CdcRunning, ""},
	})
	bh.sql2result[sql] = mrs

	sql = getSqlForGetWatermark(sysAccountID, "taskID-1")
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

func TestCdcTask_ResetWatermarkForTable(t *testing.T) {
	cdc := &CdcTask{
		watermarkUpdater: &mockWatermarkUpdater{},
	}

	info := &cdc2.DbTableInfo{
		SourceDbId:      0,
		SourceDbName:    "",
		SourceTblId:     0,
		SourceTblName:   "",
		SourceCreateSql: "",
		SinkDbName:      "",
		SinkTblName:     "",
	}

	assert.NoError(t, cdc.resetWatermarkForTable(info))
}

func TestCdcTask_Resume(t *testing.T) {
	cdc := &CdcTask{
		activeRoutine: cdc2.NewCdcActiveRoutine(),
		cdcTask: &task.CreateCdcDetails{
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
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sqlx := "delete from mo_catalog.mo_cdc_watermark where account_id = .* and task_id = .*"
	mock.ExpectExec(sqlx).WillReturnResult(sqlmock.NewResult(1, 1))
	tie := &testIE{
		db: db,
	}

	cdc := &CdcTask{
		activeRoutine: cdc2.NewCdcActiveRoutine(),
		watermarkUpdater: cdc2.NewWatermarkUpdater(
			sysAccountID,
			"taskID-0",
			tie,
		),
		cdcTask: &task.CreateCdcDetails{
			TaskName: "task1",
		},
		holdCh: make(chan int, 1),
		startFunc: func(_ context.Context) error {
			return nil
		},
	}

	err = cdc.Restart()
	assert.NoErrorf(t, err, "Restart()")
}

func TestCdcTask_Pause(t *testing.T) {
	holdCh := make(chan int, 1)
	go func() {
		<-holdCh
	}()

	cdc := &CdcTask{
		activeRoutine: cdc2.NewCdcActiveRoutine(),
		cdcTask: &task.CreateCdcDetails{
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

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sqlx := "delete from mo_catalog.mo_cdc_watermark where account_id = .* and task_id = .*"
	mock.ExpectExec(sqlx).WillReturnResult(sqlmock.NewResult(1, 1))
	tie := &testIE{
		db: db,
	}
	cdc := &CdcTask{
		activeRoutine: cdc2.NewCdcActiveRoutine(),
		watermarkUpdater: cdc2.NewWatermarkUpdater(
			sysAccountID,
			"taskID-1",
			tie,
		),
		cdcTask: &task.CreateCdcDetails{
			TaskName: "task1",
		},
		holdCh:    ch,
		isRunning: true,
	}
	err = cdc.Cancel()
	assert.NoErrorf(t, err, "Pause()")
}

func TestCdcTask_retrieveCdcTask(t *testing.T) {
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
		sinkUri              cdc2.UriInfo
		tables               cdc2.PatternTuples
		exclude              *regexp.Regexp
		startTs              types.TS
		noFull               bool
		activeRoutine        *cdc2.ActiveRoutine
		sunkWatermarkUpdater *cdc2.WatermarkUpdater
	}
	type args struct {
		ctx context.Context
	}

	cdc2.AesKey = "test-aes-key-not-use-it-in-cloud"
	defer func() { cdc2.AesKey = "" }()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sqlx := "select sink_uri, sink_type, sink_password, tables, filters, no_full, additional_config from mo_catalog.mo_cdc_task where account_id = .* and task_id =.*"
	sinkUri, err := cdc2.JsonEncode(&cdc2.UriInfo{
		User: "root",
		Ip:   "127.0.0.1",
		Port: 3306,
	})
	assert.NoError(t, err)
	pwd, err := cdc2.AesCFBEncode([]byte("111"))
	assert.NoError(t, err)
	tables, err := cdc2.JsonEncode(cdc2.PatternTuples{ //tables
		Pts: []*cdc2.PatternTuple{
			{
				Source: cdc2.PatternTable{
					Database: "db1",
					Table:    "t1",
				},
			},
		},
	},
	)
	assert.NoError(t, err)
	filters, err := cdc2.JsonEncode(cdc2.PatternTuples{})
	assert.NoError(t, err)

	mock.ExpectQuery(sqlx).WillReturnRows(sqlmock.NewRows(
		[]string{
			"sink_uri",
			"sink_type",
			"sink_password",
			"tables",
			"filters",
			"no_full",
			"additional_config",
		},
	).AddRow(
		sinkUri,
		cdc2.MysqlSink,
		pwd,
		tables,
		filters,
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
			cdc := &CdcTask{
				logger:           tt.fields.logger,
				ie:               tt.fields.ie,
				cnUUID:           tt.fields.cnUUID,
				cnTxnClient:      tt.fields.cnTxnClient,
				cnEngine:         tt.fields.cnEngine,
				fileService:      tt.fields.fileService,
				cdcTask:          tt.fields.cdcTask,
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

	txnOpStub := gostub.Stub(&cdc2.GetTxnOp,
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
			want: "select task_id, task_name, source_uri, sink_uri, state, err_msg from mo_catalog.mo_cdc_task where account_id = 0",
		},
		{
			name: "t2",
			args: args{
				accountId: 0,
				all:       false,
				taskName:  "task1",
			},
			want: "select task_id, task_name, source_uri, sink_uri, state, err_msg from mo_catalog.mo_cdc_task where account_id = 0 and task_name = 'task1'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getSqlForGetTask(tt.args.accountId, tt.args.all, tt.args.taskName), "getSqlForGetTask(%v, %v, %v)", tt.args.accountId, tt.args.all, tt.args.taskName)
		})
	}
}

func Test_initAesKey(t *testing.T) {
	{
		cdc2.AesKey = "test-aes-key-not-use-it-in-cloud"
		err := initAesKeyBySqlExecutor(context.Background(), nil, 0, "")
		assert.NoError(t, err)

		cdc2.AesKey = ""
	}

	{
		e := moerr.NewInternalErrorNoCtx("error")
		queryTableStub := gostub.Stub(&queryTableWrapper, func(context.Context, taskservice.SqlExecutor, string, func(ctx context.Context, rows *sql.Rows) (bool, error)) (bool, error) {
			return true, e
		})
		defer queryTableStub.Reset()

		err := initAesKeyBySqlExecutor(context.Background(), nil, 0, "")
		assert.Equal(t, e, err)
	}

	{
		queryTableStub := gostub.Stub(&queryTableWrapper, func(context.Context, taskservice.SqlExecutor, string, func(ctx context.Context, rows *sql.Rows) (bool, error)) (bool, error) {
			return false, nil
		})
		defer queryTableStub.Reset()

		err := initAesKeyBySqlExecutor(context.Background(), nil, 0, "")
		assert.Error(t, err)
	}

	{
		queryTableStub := gostub.Stub(&queryTableWrapper, func(context.Context, taskservice.SqlExecutor, string, func(ctx context.Context, rows *sql.Rows) (bool, error)) (bool, error) {
			return true, nil
		})
		defer queryTableStub.Reset()

		decryptStub := gostub.Stub(&decrypt, func(context.Context, string, []byte) (string, error) {
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

		err := initAesKeyBySqlExecutor(context.Background(), nil, 0, "")
		assert.NoError(t, err)
		assert.Equal(t, "aesKey", cdc2.AesKey)
		cdc2.AesKey = ""
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

func TestCdcTask_initAesKeyByInternalExecutor(t *testing.T) {
	mie := &mockIe{}
	cdcTask := &CdcTask{
		ie: mie,
	}

	decryptStub := gostub.Stub(&decrypt, func(context.Context, string, []byte) (string, error) {
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

	err := initAesKeyByInternalExecutor(context.Background(), cdcTask, 0)
	assert.NoError(t, err)
	cdc2.AesKey = ""

	err = initAesKeyByInternalExecutor(context.Background(), cdcTask, 0)
	assert.Error(t, err)

	err = initAesKeyByInternalExecutor(context.Background(), cdcTask, 0)
	assert.Error(t, err)

	err = initAesKeyByInternalExecutor(context.Background(), cdcTask, 0)
	assert.Error(t, err)
}

func TestCdcTask_handleNewTables(t *testing.T) {
	stub1 := gostub.Stub(&cdc2.GetTxnOp, func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error) {
		return nil, nil
	})
	defer stub1.Reset()

	stub2 := gostub.Stub(&cdc2.FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {})
	defer stub2.Reset()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	cdc := &CdcTask{
		cdcTask: &task.CreateCdcDetails{
			Accounts: []*task.Account{{Id: 0}},
		},
		tables: cdc2.PatternTuples{
			Pts: []*cdc2.PatternTuple{
				{
					Source: cdc2.PatternTable{
						Database: "db1",
						Table:    cdc2.MatchAll,
					},
				},
			},
		},
		exclude:        regexp.MustCompile("db1.tb1"),
		cnEngine:       eng,
		runningReaders: &sync.Map{},
	}

	mp := map[uint32]cdc2.TblMap{
		0: {
			"db1.tb1": &cdc2.DbTableInfo{},
			"db2.tb1": &cdc2.DbTableInfo{},
		},
	}
	cdc.handleNewTables(mp)
}

type mockWatermarkUpdater struct{}

func (m mockWatermarkUpdater) Run(context.Context, *cdc2.ActiveRoutine) {
	//TODO implement me
	panic("implement me")
}

func (m mockWatermarkUpdater) InsertIntoDb(*cdc2.DbTableInfo, types.TS) error {
	return nil
}

func (m mockWatermarkUpdater) GetFromMem(string, string) types.TS {
	//TODO implement me
	panic("implement me")
}

func (m mockWatermarkUpdater) GetFromDb(dbName, tblName string) (watermark types.TS, err error) {
	err = moerr.NewErrNoWatermarkFoundNoCtx(dbName, tblName)
	return
}

func (m mockWatermarkUpdater) UpdateMem(string, string, types.TS) {}

func (m mockWatermarkUpdater) DeleteFromMem(string, string) {}

func (m mockWatermarkUpdater) DeleteFromDb(string, string) error {
	return nil
}

func (m mockWatermarkUpdater) DeleteAllFromDb() error {
	//TODO implement me
	panic("implement me")
}

func (m mockWatermarkUpdater) SaveErrMsg(string, string, string) error {
	return nil
}

type mockReader struct{}

func (m mockReader) Run(ctx context.Context, ar *cdc2.ActiveRoutine) {}

func (m mockReader) Close() {}

type mockSinker struct{}

func (m mockSinker) Run(ctx context.Context, ar *cdc2.ActiveRoutine) {}

func (m mockSinker) Sink(ctx context.Context, data *cdc2.DecoderOutput) {
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
	//TODO implement me
	panic("implement me")
}

func (m mockSinker) Close() {
	//TODO implement me
	panic("implement me")
}

func TestCdcTask_addExecPipelineForTable(t *testing.T) {
	cdc := &CdcTask{
		watermarkUpdater: &mockWatermarkUpdater{},
		runningReaders:   &sync.Map{},
		noFull:           true,
		additionalConfig: map[string]interface{}{
			cdc2.MaxSqlLength:         float64(cdc2.DefaultMaxSqlLength),
			cdc2.SendSqlTimeout:       cdc2.DefaultSendSqlTimeout,
			cdc2.InitSnapshotSplitTxn: cdc2.DefaultInitSnapshotSplitTxn,
		},
	}

	info := &cdc2.DbTableInfo{
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

	stubGetTableDef := gostub.Stub(&cdc2.GetTableDef, func(context.Context, client.TxnOperator, engine.Engine, uint64) (*plan.TableDef, error) {
		return nil, nil
	})
	defer stubGetTableDef.Reset()

	stubSinker := gostub.Stub(&cdc2.NewSinker, func(cdc2.UriInfo, *cdc2.DbTableInfo, cdc2.IWatermarkUpdater,
		*plan.TableDef, int, time.Duration, *cdc2.ActiveRoutine, uint64, string) (cdc2.Sinker, error) {
		return &mockSinker{}, nil
	})
	defer stubSinker.Reset()

	stubReader := gostub.Stub(&cdc2.NewTableReader, func(client.TxnClient, engine.Engine, *mpool.MPool, *fileservice.Pool[*types.Packer],
		*cdc2.DbTableInfo, cdc2.Sinker, cdc2.IWatermarkUpdater, *plan.TableDef, func(*cdc2.DbTableInfo) error, bool, *sync.Map) cdc2.Reader {
		return &mockReader{}
	})
	defer stubReader.Reset()

	assert.NoError(t, cdc.addExecPipelineForTable(context.Background(), info, txnOperator))
}
