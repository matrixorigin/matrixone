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
	)
	wantSql := "insert into mo_catalog.mo_cdc_task values(3,\"019111fd-aed1-70c0-8760-9abadd8f0f4a\",\"task1\",\"src uri\",\"123\",\"dst uri\",\"mysql\",\"456\",\"ca path\",\"cert path\",\"key path\",\"db1:t1\",\"xfilter\",\"op filters\",\"error\",\"common\",\"\",\"\",\"conf path\",\"2024-08-02 15:20:00\",\"running\",125,\"125\",\"true\",\"yyy\",\"\",\"\",\"\",\"\",\"\")"
	assert.Equal(t, wantSql, sql)

	sql2 := getSqlForRetrievingCdcTask(3, id)
	wantSql2 := "select sink_uri, sink_type, sink_password, tables, filters, no_full from mo_catalog.mo_cdc_task where account_id = 3 and task_id = \"019111fd-aed1-70c0-8760-9abadd8f0f4a\""
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

func Test_parseTables(t *testing.T) {
	//tables := []string{
	//	"acc1.users.t1:acc1.users.t1",
	//	"acc1.users.t*:acc1.users.t*",
	//	"acc*.users.t?:acc*.users.t?",
	//	"acc*.users|items.*[12]/:acc*.users|items.*[12]/",
	//	"acc*.*./table./",
	//	//"acc*.*.table*",
	//	//"/sys|acc.*/.*.t*",
	//	//"/sys|acc.*/.*./t.$/",
	//	//"/sys|acc.*/.test*./t1{1,3}$/,/acc[23]/.items./.*/",
	//}

	type tableInfo struct {
		account, db, table string
		tableIsRegexp      bool
	}

	isSame := func(info tableInfo, account, db, table string, isRegexp bool, tip string) {
		assert.Equalf(t, info.account, account, tip)
		assert.Equalf(t, info.db, db, tip)
		assert.Equalf(t, info.table, table, tip)
		assert.Equalf(t, info.tableIsRegexp, isRegexp, tip)
	}

	type kase struct {
		input   string
		wantErr bool
		src     tableInfo
		dst     tableInfo
	}

	kases := []kase{
		{
			input:   "acc1.users.t1:acc1.users.t1",
			wantErr: false,
			src: tableInfo{
				account: "acc1",
				db:      "users",
				table:   "t1",
			},
			dst: tableInfo{
				account: "acc1",
				db:      "users",
				table:   "t1",
			},
		},
		{
			input:   "acc1.users.t*:acc1.users.t*",
			wantErr: false,
			src: tableInfo{
				account: "acc1",
				db:      "users",
				table:   "t*",
			},
			dst: tableInfo{
				account: "acc1",
				db:      "users",
				table:   "t*",
			},
		},
		{
			input:   "acc*.users.t?:acc*.users.t?",
			wantErr: false,
			src: tableInfo{
				account: "acc*",
				db:      "users",
				table:   "t?",
			},
			dst: tableInfo{
				account: "acc*",
				db:      "users",
				table:   "t?",
			},
		},
		{
			input:   "acc*.users|items.*[12]/:acc*.users|items.*[12]/",
			wantErr: false,
			src: tableInfo{
				account: "acc*",
				db:      "users|items",
				table:   "*[12]/",
			},
			dst: tableInfo{
				account: "acc*",
				db:      "users|items",
				table:   "*[12]/",
			},
		},
		{
			input:   "acc*.*./table./",
			wantErr: true,
			src:     tableInfo{},
			dst:     tableInfo{},
		},
		{
			input:   "acc*.*.table*:acc*.*.table*",
			wantErr: false,
			src: tableInfo{
				account: "acc*",
				db:      "*",
				table:   "table*",
			},
			dst: tableInfo{
				account: "acc*",
				db:      "*",
				table:   "table*",
			},
		},
	}

	for _, tkase := range kases {
		pirs, err := extractTablePairs(context.Background(), tkase.input, "")
		if tkase.wantErr {
			assert.Errorf(t, err, tkase.input)
		} else {
			assert.NoErrorf(t, err, tkase.input)
			assert.Equal(t, len(pirs.Pts), 1, tkase.input)
			pir := pirs.Pts[0]
			isSame(tkase.src, pir.Source.Account, pir.Source.Database, pir.Source.Table, pir.Source.TableIsRegexp, tkase.input)
			isSame(tkase.dst, pir.Sink.Account, pir.Sink.Database, pir.Sink.Table, pir.Sink.TableIsRegexp, tkase.input)
		}
	}
}

func Test_privilegeCheck(t *testing.T) {
	var tenantInfo *TenantInfo
	var err error
	var pts []*cdc2.PatternTuple
	ctx := context.Background()
	ses := &Session{}

	gen := func(pts []*cdc2.PatternTuple) *cdc2.PatternTuples {
		return &cdc2.PatternTuples{Pts: pts}
	}

	tenantInfo = &TenantInfo{
		Tenant:      sysAccountName,
		DefaultRole: moAdminRoleName,
	}
	ses.tenant = tenantInfo
	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc1"}},
		{Source: cdc2.PatternTable{Account: sysAccountName}},
	}
	err = canCreateCdcTask(ctx, ses, "Cluster", "", gen(pts))
	assert.Nil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: sysAccountName, Database: moCatalog}},
	}
	err = canCreateCdcTask(ctx, ses, "Cluster", "", gen(pts))
	assert.NotNil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: sysAccountName}},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.NotNil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc2"}},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.NotNil(t, err)

	pts = []*cdc2.PatternTuple{
		{},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.Error(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc1"}},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.Nil(t, err)

	tenantInfo = &TenantInfo{
		Tenant:      "acc1",
		DefaultRole: accountAdminRoleName,
	}
	ses.tenant = tenantInfo

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc1"}},
		{},
	}
	err = canCreateCdcTask(ctx, ses, "Cluster", "", gen(pts))
	assert.NotNil(t, err)

	err = canCreateCdcTask(ctx, ses, "Account", "acc2", gen(pts))
	assert.NotNil(t, err)

	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.Error(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc2"}},
		{Source: cdc2.PatternTable{Account: sysAccountName}},
	}
	err = canCreateCdcTask(ctx, ses, "Account", "acc1", gen(pts))
	assert.NotNil(t, err)

}

func Test_attachAccoutForFilters(t *testing.T) {
	var tenantInfo *TenantInfo
	var err error
	var pts []*cdc2.PatternTuple
	ctx := context.Background()
	ses := &Session{}

	gen := func(pts []*cdc2.PatternTuple) *cdc2.PatternTuples {
		return &cdc2.PatternTuples{Pts: pts}
	}

	tenantInfo = &TenantInfo{
		Tenant:      sysAccountName,
		DefaultRole: moAdminRoleName,
	}
	ses.tenant = tenantInfo
	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc1"}},
		{Source: cdc2.PatternTable{Account: sysAccountName}},
	}
	err = attachAccountToFilters(ctx, ses, "Cluster", "", gen(pts))
	assert.Nil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: sysAccountName, Database: moCatalog}},
	}
	err = attachAccountToFilters(ctx, ses, "Cluster", "", gen(pts))
	assert.Nil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: sysAccountName}},
	}
	err = attachAccountToFilters(ctx, ses, "Account", "acc1", gen(pts))
	assert.NotNil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc2"}},
	}
	err = attachAccountToFilters(ctx, ses, "Account", "acc1", gen(pts))
	assert.NotNil(t, err)

	pts = []*cdc2.PatternTuple{
		{},
	}
	err = attachAccountToFilters(ctx, ses, "Account", "acc1", gen(pts))
	assert.Nil(t, err)
	assert.Equalf(t, "acc1", pts[0].Source.Account, "different account")

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc1"}},
	}
	err = attachAccountToFilters(ctx, ses, "Account", "acc1", gen(pts))
	assert.Nil(t, err)

	tenantInfo = &TenantInfo{
		Tenant:      "acc1",
		DefaultRole: accountAdminRoleName,
	}
	ses.tenant = tenantInfo

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc1"}},
		{},
	}
	err = attachAccountToFilters(ctx, ses, "Cluster", "", gen(pts))
	assert.NotNil(t, err)

	err = attachAccountToFilters(ctx, ses, "Account", "acc2", gen(pts))
	assert.NotNil(t, err)

	err = attachAccountToFilters(ctx, ses, "Account", "acc1", gen(pts))
	assert.Nil(t, err)

	pts = []*cdc2.PatternTuple{
		{Source: cdc2.PatternTable{Account: "acc2"}},
		{Source: cdc2.PatternTable{Account: sysAccountName}},
	}
	err = attachAccountToFilters(ctx, ses, "Account", "acc1", gen(pts))
	assert.NotNil(t, err)

}

func Test_extractTableInfo(t *testing.T) {
	type args struct {
		ctx                 context.Context
		input               string
		mustBeConcreteTable bool
	}
	tests := []struct {
		name        string
		args        args
		wantAccount string
		wantDb      string
		wantTable   string
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "t1-1",
			args: args{
				input:               "acc1.db.t1",
				mustBeConcreteTable: true,
			},
			wantAccount: "acc1",
			wantDb:      "db",
			wantTable:   "t1",
			wantErr:     nil,
		},
		{
			name: "t1-2",
			args: args{
				input:               "acc1.db.t*",
				mustBeConcreteTable: true,
			},
			wantAccount: "acc1",
			wantDb:      "db",
			wantTable:   "t*",
			wantErr:     nil,
		},
		{
			name: "t1-3-table pattern needs //",
			args: args{
				input:               "acc1.db.t*",
				mustBeConcreteTable: false,
			},
			wantAccount: "acc1",
			wantDb:      "db",
			wantTable:   "t*",
			wantErr:     nil,
		},
		{
			name: "t1-4",
			args: args{
				input:               "acc1.db./t*/",
				mustBeConcreteTable: false,
			},
			wantAccount: "acc1",
			wantDb:      "db",
			wantTable:   "/t*/",
			wantErr:     nil,
		},
		{
			name: "t2-1",
			args: args{
				input:               "db.t1",
				mustBeConcreteTable: true,
			},
			wantAccount: "",
			wantDb:      "db",
			wantTable:   "t1",
			wantErr:     nil,
		},
		{
			name: "t2-2-table pattern needs //",
			args: args{
				input:               "db.t*",
				mustBeConcreteTable: true,
			},
			wantAccount: "",
			wantDb:      "db",
			wantTable:   "t*",
			wantErr:     nil,
		},
		{
			name: "t2-3-table name can be 't*'",
			args: args{
				input:               "db.t*",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "db",
			wantTable:   "t*",
			wantErr:     nil,
		},
		{
			name: "t2-4",
			args: args{
				input:               "db./t*/",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "db",
			wantTable:   "/t*/",
			wantErr:     nil,
		},
		{
			name: "t2-5",
			args: args{
				input:               "db./t*/",
				mustBeConcreteTable: true,
			},
			wantAccount: "",
			wantDb:      "db",
			wantTable:   "/t*/",
			wantErr:     nil,
		},
		{
			name: "t3--invalid format",
			args: args{
				input:               "nodot",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "",
			wantTable:   "",
			wantErr:     assert.Error,
		},
		{
			name: "t3--invalid account name",
			args: args{
				input:               "1234*90.db.t1",
				mustBeConcreteTable: false,
			},
			wantAccount: "1234*90",
			wantDb:      "db",
			wantTable:   "t1",
			wantErr:     nil,
		},
		{
			name: "t3--invalid database name",
			args: args{
				input:               "acc.12ddg.t1",
				mustBeConcreteTable: false,
			},
			wantAccount: "acc",
			wantDb:      "12ddg",
			wantTable:   "t1",
			wantErr:     nil,
		},
		{
			name: "t4--invalid database name",
			args: args{
				input:               "acc*./users|items/./t.*[12]/",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "",
			wantTable:   "",
			wantErr:     assert.Error,
		},
		{
			name: "t4-- X ",
			args: args{
				input:               "/sys|acc.*/.*.t*",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "",
			wantTable:   "",
			wantErr:     assert.Error,
		},
		{
			name: "t4-- XX",
			args: args{
				input:               "/sys|acc.*/.*./t.$/",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "",
			wantTable:   "",
			wantErr:     assert.Error,
		},
		{
			name: "t4-- XXX",
			args: args{
				input:               "/sys|acc.*/.test*./t1{1,3}$/,/acc[23]/.items./.*/",
				mustBeConcreteTable: false,
			},
			wantAccount: "",
			wantDb:      "",
			wantTable:   "",
			wantErr:     assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAccount, gotDb, gotTable, _, err := extractTableInfo(tt.args.ctx, tt.args.input, tt.args.mustBeConcreteTable)
			if tt.wantErr != nil && tt.wantErr(t, err, fmt.Sprintf("extractTableInfo(%v, %v, %v)", tt.args.ctx, tt.args.input, tt.args.mustBeConcreteTable)) {
				return
			} else {
				assert.Equalf(t, tt.wantAccount, gotAccount, "extractTableInfo(%v, %v, %v)", tt.args.ctx, tt.args.input, tt.args.mustBeConcreteTable)
				assert.Equalf(t, tt.wantDb, gotDb, "extractTableInfo(%v, %v, %v)", tt.args.ctx, tt.args.input, tt.args.mustBeConcreteTable)
				assert.Equalf(t, tt.wantTable, gotTable, "extractTableInfo(%v, %v, %v)", tt.args.ctx, tt.args.input, tt.args.mustBeConcreteTable)
			}
		})
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

	///////mock result
	sql1 := "select account_id from `mo_catalog`.`mo_account` where account_name='sys'"
	mock.ExpectQuery(sql1).WillReturnRows(sqlmock.NewRows([]string{"account_id"}).AddRow(uint64(sysAccountID)))

	sql2 := "select rel_id from `mo_catalog`.`mo_tables` where account_id = 0 and reldatabase ='db1' and relname = 't1'"
	mock.ExpectQuery(sql2).WillReturnRows(sqlmock.NewRows([]string{"table_id"}).AddRow(uint64(11)))

	sql3 := "select count.*att_constraint_type.* from `mo_catalog`.`mo_columns` where account_id = 0 and att_database = 'db1' and att_relname = 't1' and att_constraint_type = 'p'"
	mock.ExpectQuery(sql3).WillReturnRows(sqlmock.NewRows([]string{"count(att_constraint_type)"}).AddRow(uint64(1)))

	sql2_1 := "select rel_id from `mo_catalog`.`mo_tables` where account_id = 0 and reldatabase ='db1' and relname = 't2'"
	mock.ExpectQuery(sql2_1).WillReturnRows(sqlmock.NewRows([]string{"table_id"}).AddRow(uint64(12)))

	sql3_1 := "select count.*att_constraint_type.* from `mo_catalog`.`mo_columns` where account_id = 0 and att_database = 'db1' and att_relname = 't2' and att_constraint_type = 'p'"
	mock.ExpectQuery(sql3_1).WillReturnRows(sqlmock.NewRows([]string{"count(att_constraint_type)"}).AddRow(uint64(1)))

	sql4 := "insert into mo_catalog.mo_cdc_task values.*0,\".*\",\"task1\",\".*\",\"\",\".*\",\"mysql\",\".*\",\"\",\"\",\"\",\".*\",\".*\",\"\",\"common\",\"common\",\"\",\"\",\"\",\".*\",\"running\",0,\"0\",\"false\",\"\",\"\",\"\",\"\",\"\",\"\".*"
	mock.ExpectExec(sql4).WillReturnResult(sqlmock.NewResult(1, 1))
	////

	pu := config.ParameterUnit{}
	pu.TaskService = &testTaskService{
		db: db,
	}
	setGlobalPu(&pu)

	create := &tree.CreateCDC{
		IfNotExists: false,
		TaskName:    "task1",
		SourceUri:   "mysql://root:111@127.0.0.1:6001",
		SinkType:    cdc2.MysqlSink,
		SinkUri:     "mysql://root:111@127.0.0.1:3306",
		Tables:      "db1.t1:db1.t1,db1.t2",
		Option: []string{
			"Level",
			cdc2.AccountLevel,
			"Account",
			sysAccountName,
			"Rules",
			"db2.t3,db2.t4",
		},
	}

	ses.GetTxnCompileCtx().execCtx.stmt = create

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
			tt.wantErr(t,
				execInFrontend(tt.args.ses, tt.args.execCtx),
				fmt.Sprintf("handleCreateCdc(%v, %v, %v)", tt.args.ses, tt.args.execCtx, tt.args.create))
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
			err = rows.Scan(
				&sinkUri,
				&sinkType,
				&sinkPwd,
				&tables,
				&filters,
				&noFull)
			if err != nil {
				panic(err)
			}
			rowValues = append(rowValues, sinkUri)
			rowValues = append(rowValues, sinkType)
			rowValues = append(rowValues, sinkPwd)
			rowValues = append(rowValues, tables)
			rowValues = append(rowValues, filters)
			rowValues = append(rowValues, noFull)
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
			count := uint64(0)
			err = rows.Scan(
				&count,
			)
			if err != nil {
				panic(err)
			}
			rowValues = append(rowValues, count)
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

	cdc2.EnableConsoleSink = true
	defer func() {
		cdc2.EnableConsoleSink = false
	}()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	/////////mock sql result
	sql1 := `select sink_uri, sink_type, sink_password, tables, filters, no_full from mo_catalog.mo_cdc_task where account_id = 0 and task_id = "00000000-0000-0000-0000-000000000000"`
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
					AccountId: uint64(sysAccountID),
					Account:   sysAccountName,
					Database:  "db1",
					Table:     "t1",
				},
			},
		},
	},
	)
	assert.NoError(t, err)
	filters, err := cdc2.JsonEncode(cdc2.PatternTuples{})
	assert.NoError(t, err)

	mock.ExpectQuery(sql1).WillReturnRows(sqlmock.NewRows(
		[]string{
			"sink_uri",
			"sink_type",
			"sink_password",
			"tables",
			"filters",
			"no_full",
		},
	).AddRow(
		sinkUri,
		cdc2.ConsoleSink,
		pwd,
		tables,
		filters,
		true,
	),
	)

	sql2 := "select reldatabase_id,rel_id from mo_catalog.mo_tables where account_id = 0 and reldatabase = 'db1' and relname = 't1'"
	mock.ExpectQuery(sql2).WillReturnRows(sqlmock.NewRows(
		[]string{
			"reldatabase_id",
			"rel_id",
		}).AddRow(
		uint64(10),
		uint64(1001),
	),
	)

	sql3 := "select count.*1.* from mo_catalog.mo_cdc_watermark where account_id = 0 and task_id = '00000000-0000-0000-0000-000000000000'"
	mock.ExpectQuery(sql3).WillReturnRows(sqlmock.NewRows(
		[]string{
			"count",
		},
	).AddRow(
		uint64(0),
	))

	sql4 := "insert into mo_catalog.mo_cdc_watermark values .*0, '00000000-0000-0000-0000-000000000000', 1001, '0-0'.*"
	mock.ExpectExec(sql4).WillReturnResult(sqlmock.NewResult(1, 1))

	sql5 := "select watermark from mo_catalog.mo_cdc_watermark where account_id = 0 and task_id = '00000000-0000-0000-0000-000000000000' and table_id = 1001"
	mock.ExpectQuery(sql5).WillReturnRows(sqlmock.NewRows(
		[]string{
			"watermark",
		},
	).AddRow(
		"0-0",
	))

	sql6 := "update mo_catalog.mo_cdc_watermark set watermark='0-0' where account_id = 0 and task_id = '00000000-0000-0000-0000-000000000000' and table_id = 1001"
	mock.ExpectExec(sql6).WillReturnResult(sqlmock.NewResult(1, 1))

	genSqlIdx := func(sql string) int {
		mSql1, err := regexp.MatchString(sql1, sql)
		assert.NoError(t, err)
		mSql2, err := regexp.MatchString(sql2, sql)
		assert.NoError(t, err)
		mSql3, err := regexp.MatchString(sql3, sql)
		assert.NoError(t, err)
		mSql4, err := regexp.MatchString(sql4, sql)
		assert.NoError(t, err)
		mSql5, err := regexp.MatchString(sql5, sql)
		assert.NoError(t, err)
		mSql6, err := regexp.MatchString(sql6, sql)
		assert.NoError(t, err)

		if mSql1 {
			return mSqlIdx1
		} else if mSql2 {
			return mSqlIdx2
		} else if mSql3 {
			return mSqlIdx3
		} else if mSql4 {
			return mSqlIdx4
		} else if mSql5 {
			return mSqlIdx5
		} else if mSql6 {
			return mSqlIdx6
		}
		return -1
	}

	////////

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
	table.EXPECT().CollectChanges(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, moerr.NewInternalErrorNoCtx("invalid handle"))

	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), gomock.Any()).Return("", "", table, nil).AnyTimes()

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	eng.EXPECT().Database(ctx, gomock.Any(), txnOperator).Return(nil, nil).AnyTimes()

	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnOperator.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	//////////

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

	setGlobalPu(&pu)

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
			err = execInFrontend(tt.args.ses, tt.args.ses.GetTxnCompileCtx().execCtx)
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

	setGlobalPu(&pu)

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

	setGlobalPu(&pu)

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
			err = execInFrontend(tt.args.ses, tt.args.ses.GetTxnCompileCtx().execCtx)
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

	setGlobalPu(&pu)

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

	setGlobalPu(&pu)

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
			err = execInFrontend(tt.args.ses, tt.args.ses.GetTxnCompileCtx().execCtx)
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

	setGlobalPu(&pu)

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
			err = execInFrontend(tt.args.ses, tt.args.ses.GetTxnCompileCtx().execCtx)
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

	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col5)

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
		{"db1", "tb1", "0-0"},
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
			wantS: "{\n  \"db1.tb1\": 0-0,\n}",
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

	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)

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
		{"taskID-1", "task1", sourceUri, sinkUri, CdcRunning},
	})
	bh.sql2result[sql] = mrs

	sql = getSqlForGetWatermark(sysAccountID, "taskID-1")
	mrs = newMrsForGetWatermark([][]interface{}{
		{"db1", "tb1", "0-0"},
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
	txnOperator.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()

	pu := config.ParameterUnit{
		StorageEngine: eng,
		TxnClient:     txnClient,
	}
	setGlobalPu(&pu)

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
			err = execInFrontend(tt.args.ses, tt.args.ses.GetTxnCompileCtx().execCtx)
			assert.NoError(t, err)
			rs := tt.args.ses.GetMysqlResultSet()
			taskId, err := rs.GetString(tt.args.execCtx.reqCtx, 0, 0)
			assert.NoError(t, err)
			assert.Equal(t, "taskID-1", taskId)
		})
	}
}

func TestCdcTask_ResetWatermarkForTable(t *testing.T) {
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
		filters              cdc2.PatternTuples
		startTs              types.TS
		noFull               string
		activeRoutine        *cdc2.ActiveRoutine
		sunkWatermarkUpdater *cdc2.WatermarkUpdater
	}
	type args struct {
		info *cdc2.DbTableInfo
	}
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	tie := &testIE{
		db: db,
	}

	sqlx := "delete from mo_catalog.mo_cdc_watermark where account_id = .* and task_id = .* and table_id = .*"
	mock.ExpectExec(sqlx).WillReturnResult(sqlmock.NewResult(1, 1))

	sqlx1 := "insert into mo_catalog.mo_cdc_watermark values .*"
	mock.ExpectExec(sqlx1).WillReturnResult(sqlmock.NewResult(1, 1))

	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			fields: fields{
				sunkWatermarkUpdater: cdc2.NewWatermarkUpdater(
					sysAccountID, "taskID-1", tie,
				),
				ie: tie,
			},
			args: args{
				info: &cdc2.DbTableInfo{
					SourceTblId: 10,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cdc := &CdcTask{
				logger:               tt.fields.logger,
				ie:                   tt.fields.ie,
				cnUUID:               tt.fields.cnUUID,
				cnTxnClient:          tt.fields.cnTxnClient,
				cnEngine:             tt.fields.cnEngine,
				fileService:          tt.fields.fileService,
				cdcTask:              tt.fields.cdcTask,
				mp:                   tt.fields.mp,
				packerPool:           tt.fields.packerPool,
				sinkUri:              tt.fields.sinkUri,
				tables:               tt.fields.tables,
				filters:              tt.fields.filters,
				startTs:              tt.fields.startTs,
				noFull:               tt.fields.noFull,
				activeRoutine:        tt.fields.activeRoutine,
				sunkWatermarkUpdater: tt.fields.sunkWatermarkUpdater,
			}
			err := cdc.ResetWatermarkForTable(tt.args.info)
			assert.NoErrorf(t, err, fmt.Sprintf("ResetWatermarkForTable(%v)", tt.args.info))
		})
	}
}

func TestCdcTask_Resume(t *testing.T) {
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
		filters              cdc2.PatternTuples
		startTs              types.TS
		noFull               string
		activeRoutine        *cdc2.ActiveRoutine
		sunkWatermarkUpdater *cdc2.WatermarkUpdater
	}

	stub1 := gostub.Stub(&Start,
		func(_ context.Context, _ *CdcTask, _ bool) error {
			return nil
		})
	defer stub1.Reset()

	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			fields: fields{
				activeRoutine: cdc2.NewCdcActiveRoutine(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cdc := &CdcTask{
				logger:               tt.fields.logger,
				ie:                   tt.fields.ie,
				cnUUID:               tt.fields.cnUUID,
				cnTxnClient:          tt.fields.cnTxnClient,
				cnEngine:             tt.fields.cnEngine,
				fileService:          tt.fields.fileService,
				cdcTask:              tt.fields.cdcTask,
				mp:                   tt.fields.mp,
				packerPool:           tt.fields.packerPool,
				sinkUri:              tt.fields.sinkUri,
				tables:               tt.fields.tables,
				filters:              tt.fields.filters,
				startTs:              tt.fields.startTs,
				noFull:               tt.fields.noFull,
				activeRoutine:        tt.fields.activeRoutine,
				sunkWatermarkUpdater: tt.fields.sunkWatermarkUpdater,
			}

			err := cdc.Resume()
			assert.NoErrorf(t, err, "Resume()")
		})
	}
}

func TestCdcTask_Restart(t *testing.T) {
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
		filters              cdc2.PatternTuples
		startTs              types.TS
		noFull               string
		activeRoutine        *cdc2.ActiveRoutine
		sunkWatermarkUpdater *cdc2.WatermarkUpdater
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sqlx := "delete from mo_catalog.mo_cdc_watermark where account_id = .* and task_id = .*"
	mock.ExpectExec(sqlx).WillReturnResult(sqlmock.NewResult(1, 1))
	tie := &testIE{
		db: db,
	}

	stub1 := gostub.Stub(&Start,
		func(_ context.Context, _ *CdcTask, _ bool) error {
			return nil
		})
	defer stub1.Reset()

	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "t1",
			fields: fields{
				activeRoutine: cdc2.NewCdcActiveRoutine(),
				sunkWatermarkUpdater: cdc2.NewWatermarkUpdater(
					sysAccountID,
					"taskID-0",
					tie,
				),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cdc := &CdcTask{
				logger:               tt.fields.logger,
				ie:                   tt.fields.ie,
				cnUUID:               tt.fields.cnUUID,
				cnTxnClient:          tt.fields.cnTxnClient,
				cnEngine:             tt.fields.cnEngine,
				fileService:          tt.fields.fileService,
				cdcTask:              tt.fields.cdcTask,
				mp:                   tt.fields.mp,
				packerPool:           tt.fields.packerPool,
				sinkUri:              tt.fields.sinkUri,
				tables:               tt.fields.tables,
				filters:              tt.fields.filters,
				startTs:              tt.fields.startTs,
				noFull:               tt.fields.noFull,
				activeRoutine:        tt.fields.activeRoutine,
				sunkWatermarkUpdater: tt.fields.sunkWatermarkUpdater,
			}

			err = cdc.Restart()
			assert.NoErrorf(t, err, "Restart()")
		})
	}
}

func TestCdcTask_Pause(t *testing.T) {
	cdc := &CdcTask{
		activeRoutine: cdc2.NewCdcActiveRoutine(),
	}
	err := cdc.Pause()
	assert.NoErrorf(t, err, "Pause()")
}

func TestCdcTask_Cancel(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sqlx := "delete from mo_catalog.mo_cdc_watermark where account_id = .* and task_id = .*"
	mock.ExpectExec(sqlx).WillReturnResult(sqlmock.NewResult(1, 1))
	tie := &testIE{
		db: db,
	}
	cdc := &CdcTask{
		activeRoutine: cdc2.NewCdcActiveRoutine(),
		sunkWatermarkUpdater: cdc2.NewWatermarkUpdater(
			sysAccountID,
			"taskID-1",
			tie,
		),
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
		filters              cdc2.PatternTuples
		startTs              types.TS
		noFull               string
		activeRoutine        *cdc2.ActiveRoutine
		sunkWatermarkUpdater *cdc2.WatermarkUpdater
	}
	type args struct {
		ctx context.Context
	}

	db, mock, err := sqlmock.New()
	assert.NoError(t, err)

	sqlx := "select sink_uri, sink_type, sink_password, tables, filters, no_full from mo_catalog.mo_cdc_task where account_id = .* and task_id =.*"
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
					AccountId: uint64(sysAccountID),
					Account:   sysAccountName,
					Database:  "db1",
					Table:     "t1",
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
		},
	).AddRow(
		sinkUri,
		cdc2.MysqlSink,
		pwd,
		tables,
		filters,
		true,
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
				logger:               tt.fields.logger,
				ie:                   tt.fields.ie,
				cnUUID:               tt.fields.cnUUID,
				cnTxnClient:          tt.fields.cnTxnClient,
				cnEngine:             tt.fields.cnEngine,
				fileService:          tt.fields.fileService,
				cdcTask:              tt.fields.cdcTask,
				mp:                   tt.fields.mp,
				packerPool:           tt.fields.packerPool,
				sinkUri:              tt.fields.sinkUri,
				tables:               tt.fields.tables,
				filters:              tt.fields.filters,
				startTs:              tt.fields.startTs,
				noFull:               tt.fields.noFull,
				activeRoutine:        tt.fields.activeRoutine,
				sunkWatermarkUpdater: tt.fields.sunkWatermarkUpdater,
			}
			err := cdc.retrieveCdcTask(tt.args.ctx)
			assert.NoError(t, err, fmt.Sprintf("retrieveCdcTask(%v)", tt.args.ctx))
		})
	}
}

func Test_execFrontend(t *testing.T) {
	pu := config.ParameterUnit{}
	setGlobalPu(&pu)

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
		err := execInFrontend(ses, ses.GetTxnCompileCtx().execCtx)
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
			want: "select task_id, task_name, source_uri, sink_uri, state from mo_catalog.mo_cdc_task where account_id = 0",
		},
		{
			name: "t2",
			args: args{
				accountId: 0,
				all:       false,
				taskName:  "task1",
			},
			want: "select task_id, task_name, source_uri, sink_uri, state from mo_catalog.mo_cdc_task where account_id = 0 and task_name = 'task1'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getSqlForGetTask(tt.args.accountId, tt.args.all, tt.args.taskName), "getSqlForGetTask(%v, %v, %v)", tt.args.accountId, tt.args.all, tt.args.taskName)
		})
	}
}
