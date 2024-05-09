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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/prashantv/gostub"
	pcg "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	util "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Test_inc_dec(t *testing.T) {
	rt := &Routine{}
	counter := int32(0)
	eg := errgroup.Group{}

	eg.Go(func() error {
		rt.increaseCount(func() {
			atomic.AddInt32(&counter, 1)
		})
		return nil
	})
	time.Sleep(100 * time.Millisecond)

	eg.Go(func() error {
		rt.decreaseCount(func() {
			atomic.AddInt32(&counter, -1)
		})
		return nil
	})
	time.Sleep(100 * time.Millisecond)

	err := eg.Wait()
	assert.NoError(t, err)
	assert.Equal(t, counter, int32(0))
	assert.False(t, rt.connectionBeCounted.Load())
}

const (
	contextCancel int32 = -2
	timeout       int32 = -1
)

type genMrs func(ses *Session) *MysqlResultSet

type result struct {
	gen        genMrs
	isSleepSql bool
	seconds    int
	resultX    atomic.Int32
}

var newMockWrapper = func(ctrl *gomock.Controller, ses *Session,
	sql2result map[string]*result,
	sql2NoResultSet map[string]bool, sql string, stmt tree.Statement, proc *process.Process) ComputationWrapper {
	var mrs *MysqlResultSet
	var columns []interface{}
	var ok, ok2 bool
	var err error
	var res *result
	if res, ok = sql2result[sql]; ok {
		mrs = res.gen(ses)
		for _, col := range mrs.Columns {
			columns = append(columns, col)
		}
	} else if _, ok2 = sql2NoResultSet[sql]; ok2 {
		//no result set
	} else {
		panic(fmt.Sprintf("there is no mysqlResultset for the sql %s", sql))
	}
	uuid, _ := uuid.NewV7()
	runner := mock_frontend.NewMockComputationRunner(ctrl)
	runner.EXPECT().Run(gomock.Any()).DoAndReturn(func(uint64) (*util.RunResult, error) {
		proto := ses.GetMysqlProtocol()
		if mrs != nil {
			if res.isSleepSql {
				select {
				case <-time.After(time.Duration(res.seconds) * time.Second):
					res.resultX.Store(timeout)
				case <-proc.Ctx.Done():
					res.resultX.Store(contextCancel)
				}
			}
			err = proto.SendResultSetTextBatchRowSpeedup(mrs, mrs.GetRowCount())
			if err != nil {
				logutil.Errorf("flush error %v", err)
				return nil, err
			}
		}
		return &util.RunResult{AffectRows: 0}, nil
	}).AnyTimes()
	mcw := mock_frontend.NewMockComputationWrapper(ctrl)
	mcw.EXPECT().GetAst().Return(stmt).AnyTimes()
	mcw.EXPECT().GetProcess().Return(proc).AnyTimes()
	mcw.EXPECT().GetColumns(gomock.Any()).Return(columns, nil).AnyTimes()
	mcw.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
	mcw.EXPECT().GetUUID().Return(uuid[:]).AnyTimes()
	mcw.EXPECT().RecordExecPlan(gomock.Any()).Return(nil).AnyTimes()
	mcw.EXPECT().GetLoadTag().Return(false).AnyTimes()
	mcw.EXPECT().Clear().AnyTimes()
	mcw.EXPECT().Free().AnyTimes()
	mcw.EXPECT().Plan().Return(nil).AnyTimes()
	return mcw
}

func Test_ConnectionCount(t *testing.T) {
	//client connection method: mysql -h 127.0.0.1 -P 6001 --default-auth=mysql_native_password -uroot -p
	//client connect
	//ion method: mysql -h 127.0.0.1 -P 6001 -udump -p
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var conn1, conn2 *sql.DB
	var err error

	//before anything using the configuration
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
	require.NoError(t, err)
	pu.SV.SkipCheckUser = true
	setGlobalPu(pu)

	noResultSet := make(map[string]bool)
	resultSet := make(map[string]*result)

	var wrapperStubFunc = func(execCtx *ExecCtx, db string, user string, eng engine.Engine, proc *process.Process, ses *Session) ([]ComputationWrapper, error) {
		var cw []ComputationWrapper = nil
		var stmts []tree.Statement = nil
		var cmdFieldStmt *InternalCmdFieldList
		var err error
		if isCmdFieldListSql(execCtx.input.getSql()) {
			cmdFieldStmt, err = parseCmdFieldList(execCtx.reqCtx, execCtx.input.getSql())
			if err != nil {
				return nil, err
			}
			stmts = append(stmts, cmdFieldStmt)
		} else {
			stmts, err = parsers.Parse(execCtx.reqCtx, dialect.MYSQL, execCtx.input.getSql(), 1, 0)
			if err != nil {
				return nil, err
			}
		}

		for _, stmt := range stmts {
			cw = append(cw, newMockWrapper(ctrl, ses, resultSet, noResultSet, execCtx.input.getSql(), stmt, proc))
		}
		return cw, nil
	}

	bhStub := gostub.Stub(&GetComputationWrapper, wrapperStubFunc)
	defer bhStub.Reset()

	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

	// A mock autoincrcache manager.
	setGlobalAicm(&defines.AutoIncrCacheManager{})
	rm, _ := NewRoutineManager(ctx)
	setGlobalRtMgr(rm)

	wg := sync.WaitGroup{}
	wg.Add(1)

	//running server
	go func() {
		defer wg.Done()
		echoServer(getGlobalRtMgr().Handler, getGlobalRtMgr(), NewSqlCodec())
	}()

	cCounter := metric.ConnectionCounter(sysAccountName)
	cCounter.Set(0)

	time.Sleep(time.Second * 2)
	conn1, err = openDbConn(t, 6001)
	require.NoError(t, err)

	time.Sleep(time.Second * 2)
	conn2, err = openDbConn(t, 6001)
	require.NoError(t, err)

	time.Sleep(time.Second * 2)

	cc := rm.clientCount()
	assert.GreaterOrEqual(t, cc, 2)

	x := &pcg.Metric{}
	err = cCounter.Write(x)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, x.Gauge.GetValue(), float64(2))

	time.Sleep(time.Second * 2)

	cc = rm.clientCount()
	assert.GreaterOrEqual(t, cc, 0)

	err = cCounter.Write(x)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, x.Gauge.GetValue(), float64(0))

	time.Sleep(time.Millisecond * 10)
	//close server
	setServer(1)
	wg.Wait()

	//close the connection
	closeDbConn(t, conn1)
	closeDbConn(t, conn2)
}
