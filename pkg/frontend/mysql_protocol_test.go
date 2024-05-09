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
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	// mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/BurntSushi/toml"
	"github.com/fagongzi/goetty/v2"
	goetty_buf "github.com/fagongzi/goetty/v2/buf"
	"github.com/golang/mock/gomock"
	fuzz "github.com/google/gofuzz"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type TestRoutineManager struct {
	rwlock  sync.Mutex
	clients map[goetty.IOSession]*Routine

	pu *config.ParameterUnit
}

func (tRM *TestRoutineManager) Created(rs goetty.IOSession) {
	pro := NewMysqlClientProtocol(nextConnectionID(), rs, 1024, tRM.pu.SV)
	routine := NewRoutine(context.TODO(), pro, tRM.pu.SV, rs)

	hsV10pkt := pro.makeHandshakeV10Payload()
	err := pro.writePackets(hsV10pkt, true)
	if err != nil {
		panic(err)
	}

	tRM.rwlock.Lock()
	defer tRM.rwlock.Unlock()
	tRM.clients[rs] = routine
}

func (tRM *TestRoutineManager) Closed(rs goetty.IOSession) {
	tRM.rwlock.Lock()
	defer tRM.rwlock.Unlock()
	delete(tRM.clients, rs)
}

func NewTestRoutineManager(pu *config.ParameterUnit) *TestRoutineManager {
	rm := &TestRoutineManager{
		clients: make(map[goetty.IOSession]*Routine),
		pu:      pu,
	}
	return rm
}

func TestMysqlClientProtocol_Handshake(t *testing.T) {
	//client connection method: mysql -h 127.0.0.1 -P 6001 --default-auth=mysql_native_password -uroot -p
	//client connect
	//ion method: mysql -h 127.0.0.1 -P 6001 -udump -p

	var db *sql.DB
	var err error
	//before anything using the configuration
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	_, err = toml.DecodeFile("test/system_vars_config.toml", pu.SV)
	require.NoError(t, err)
	pu.SV.SkipCheckUser = true
	setGlobalPu(pu)

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

	time.Sleep(time.Second * 2)
	db, err = openDbConn(t, 6001)
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 10)
	//close server
	setServer(1)
	wg.Wait()

	closeDbConn(t, db)
}

func newMrsForConnectionId(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("connection_id")
	col1.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

	mrs.AddColumn(col1)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func newMrsForSleep(rows [][]interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}

	col1 := &MysqlColumn{}
	col1.SetName("sleep")
	col1.SetColumnType(defines.MYSQL_TYPE_TINY)

	mrs.AddColumn(col1)

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return mrs
}

func TestKill(t *testing.T) {
	//client connection method: mysql -h 127.0.0.1 -P 6001 --default-auth=mysql_native_password -uroot -p
	//client connect
	//ion method: mysql -h 127.0.0.1 -P 6001 -udump -p
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var conn1, conn2 *sql.DB
	var err error
	var connIdRow *sql.Row

	//before anything using the configuration
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second * 10}).AnyTimes()
	wp := newTestWorkspace()

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().GetWorkspace().Return(wp).AnyTimes()
	txnOp.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOp.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOp, nil).AnyTimes()
	pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
	require.NoError(t, err)
	pu.SV.SkipCheckUser = true
	setGlobalPu(pu)
	sql1 := "select connection_id();"
	var sql2, sql3, sql4 string

	noResultSet := make(map[string]bool)
	resultSet := make(map[string]*result)
	resultSet[sql1] = &result{
		gen: func(ses *Session) *MysqlResultSet {
			mrs := newMrsForConnectionId([][]interface{}{
				{ses.GetConnectionID()},
			})
			return mrs
		},
		isSleepSql: false,
	}

	sql5 := "select sleep(30);"
	resultSet[sql5] = &result{
		gen: func(ses *Session) *MysqlResultSet {
			return newMrsForSleep([][]interface{}{
				{uint8(0)},
			})
		},
		isSleepSql: true,
		seconds:    30,
	}

	sql6 := "select sleep(30);"
	resultSet[sql6] = &result{
		gen: func(ses *Session) *MysqlResultSet {
			return newMrsForSleep([][]interface{}{
				{uint8(0)},
			})
		},
		isSleepSql: true,
		seconds:    30,
	}

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
	temp, _ := NewRoutineManager(ctx)
	setGlobalRtMgr(temp)

	wg := sync.WaitGroup{}
	wg.Add(1)

	//running server
	go func() {
		defer wg.Done()
		echoServer(getGlobalRtMgr().Handler, getGlobalRtMgr(), NewSqlCodec())
	}()

	logutil.Infof("open conn1")
	time.Sleep(time.Second * 2)
	conn1, err = openDbConn(t, 6001)
	require.NoError(t, err)
	logutil.Infof("open conn1 done")

	logutil.Infof("open conn2")
	time.Sleep(time.Second * 2)
	conn2, err = openDbConn(t, 6001)
	require.NoError(t, err)
	logutil.Infof("open conn2 done")

	logutil.Infof("get the connection id of conn1")
	//get the connection id of conn1
	var conn1Id uint64
	connIdRow = conn1.QueryRow(sql1)
	err = connIdRow.Scan(&conn1Id)
	require.NoError(t, err)
	logutil.Infof("get the connection id of conn1 done")

	logutil.Infof("get the connection id of conn2")
	//get the connection id of conn2
	var conn2Id uint64
	connIdRow = conn2.QueryRow(sql1)
	err = connIdRow.Scan(&conn2Id)
	require.NoError(t, err)
	logutil.Infof("get the connection id of conn2 done")
	logutil.Infof("conn==>%v %v", conn1Id, conn2Id)

	wgSleep := sync.WaitGroup{}
	wgSleep.Add(1)

	//===================================
	//connection 1 exec : select sleep(30);
	go func() {
		defer wgSleep.Done()
		var resultId int
		logutil.Infof("conn1 sleep(30)")
		connIdRow = conn1.QueryRow(sql5)
		//find race on err here
		err := connIdRow.Scan(&resultId)
		require.NoError(t, err)
		logutil.Infof("conn1 sleep(30) done")
	}()

	//sleep before cancel
	time.Sleep(time.Second * 2)

	logutil.Infof("conn2 kill query on conn1")
	//conn2 kills the query
	sql3 = fmt.Sprintf("kill query %d;", conn1Id)
	noResultSet[sql3] = true
	_, err = conn2.Exec(sql3)
	require.NoError(t, err)
	logutil.Infof("conn2 kill query on conn1: KILL query done")

	//check killed result
	wgSleep.Wait()
	res := resultSet[sql5]
	require.Equal(t, res.resultX.Load(), contextCancel)
	logutil.Infof("conn2 kill query on conn1 done")

	//================================

	//connection 1 exec : select sleep(30);
	wgSleep2 := sync.WaitGroup{}
	wgSleep2.Add(1)
	go func() {
		defer wgSleep2.Done()
		var resultId int
		logutil.Infof("conn1 sleep(30) 2")
		connIdRow = conn1.QueryRow(sql6)
		err = connIdRow.Scan(&resultId)
		require.NoError(t, err)
		logutil.Infof("conn1 sleep(30) 2 done")
	}()

	//sleep before cancel
	time.Sleep(time.Second * 2)

	logutil.Infof("conn2 kill conn1")
	//conn2 kills the connection 1
	sql2 = fmt.Sprintf("kill %d;", conn1Id)
	noResultSet[sql2] = true
	_, err = conn2.Exec(sql2)
	require.NoError(t, err)
	logutil.Infof("conn2 kill conn1 : KILL connection done")

	//check killed result
	wgSleep2.Wait()
	res = resultSet[sql6]
	require.Equal(t, res.resultX.Load(), contextCancel)
	logutil.Infof("conn2 kill conn1 done")

	logutil.Infof("conn1 test itself after being killed")
	//==============================
	//conn 1 is killed by conn2
	//check conn1 is disconnected or not
	err = conn1.Ping()
	require.Error(t, err)
	logutil.Infof("conn1 test itself after being killed done")

	//==============================

	logutil.Infof("conn2 kill itself")
	//conn2 kills itself
	sql4 = fmt.Sprintf("kill %d;", conn2Id)
	noResultSet[sql4] = true
	_, err = conn2.Exec(sql4)
	require.NoError(t, err)
	logutil.Infof("conn2 kill itself done")

	time.Sleep(time.Millisecond * 10)
	//close server
	setServer(1)
	wg.Wait()

	logutil.Infof("close conn1,conn2")
	//close the connection
	closeDbConn(t, conn1)
	closeDbConn(t, conn2)
	logutil.Infof("close conn1,conn2 done")
}

func TestReadIntLenEnc(t *testing.T) {
	var intEnc MysqlProtocolImpl
	var data = make([]byte, 24)
	var cases = [][]uint64{
		{0, 123, 250},
		{251, 10000, 1<<16 - 1},
		{1 << 16, 1<<16 + 10000, 1<<24 - 1},
		{1 << 24, 1<<24 + 10000, 1<<64 - 1},
	}
	var caseLens = []int{1, 3, 4, 9}
	for j := 0; j < len(cases); j++ {
		for i := 0; i < len(cases[j]); i++ {
			value := cases[j][i]
			p1 := intEnc.writeIntLenEnc(data, 0, value)
			val, p2, ok := intEnc.readIntLenEnc(data, 0)
			if !ok || p1 != caseLens[j] || p1 != p2 || val != value {
				t.Errorf("IntLenEnc %d failed.", value)
				break
			}
			_, _, ok = intEnc.readIntLenEnc(data[0:caseLens[j]-1], 0)
			if ok {
				t.Errorf("read IntLenEnc failed.")
				break
			}
		}
	}
}

func TestReadCountOfBytes(t *testing.T) {
	var client MysqlProtocolImpl
	var data = make([]byte, 24)
	var length = 10
	for i := 0; i < length; i++ {
		data[i] = byte(length - i)
	}

	r, pos, ok := client.readCountOfBytes(data, 0, length)
	if !ok || pos != length {
		t.Error("read bytes failed.")
		return
	}

	for i := 0; i < length; i++ {
		if r[i] != data[i] {
			t.Error("read != write")
			break
		}
	}

	_, _, ok = client.readCountOfBytes(data, 0, 100)
	if ok {
		t.Error("read bytes failed.")
		return
	}

	_, pos, ok = client.readCountOfBytes(data, 0, 0)
	if !ok || pos != 0 {
		t.Error("read bytes failed.")
		return
	}
}

func TestReadStringFix(t *testing.T) {
	var client MysqlProtocolImpl
	var data = make([]byte, 24)
	var length = 10
	var s = "haha, test read string fix function"
	pos := client.writeStringFix(data, 0, s, length)
	if pos != length {
		t.Error("write string fix failed.")
		return
	}
	var x string
	var ok bool

	x, pos, ok = client.readStringFix(data, 0, length)
	if !ok || pos != length || x != s[0:length] {
		t.Error("read string fix failed.")
		return
	}
	var sLen = []int{
		length + 10,
		length + 20,
		length + 30,
	}
	for i := 0; i < len(sLen); i++ {
		x, pos, ok = client.readStringFix(data, 0, sLen[i])
		if ok && pos == sLen[i] && x == s[0:sLen[i]] {
			t.Error("read string fix failed.")
			return
		}
	}

	//empty string
	pos = client.writeStringFix(data, 0, s, 0)
	if pos != 0 {
		t.Error("write string fix failed.")
		return
	}

	x, pos, ok = client.readStringFix(data, 0, 0)
	if !ok || pos != 0 || x != "" {
		t.Error("read string fix failed.")
		return
	}
}

func TestReadStringNUL(t *testing.T) {
	var client MysqlProtocolImpl
	var data = make([]byte, 24)
	var length = 10
	var s = "haha, test read string fix function"
	pos := client.writeStringNUL(data, 0, s[0:length])
	if pos != length+1 {
		t.Error("write string NUL failed.")
		return
	}
	var x string
	var ok bool

	x, pos, ok = client.readStringNUL(data, 0)
	if !ok || pos != length+1 || x != s[0:length] {
		t.Error("read string NUL failed.")
		return
	}
	var sLen = []int{
		length + 10,
		length + 20,
		length + 30,
	}
	for i := 0; i < len(sLen); i++ {
		x, pos, ok = client.readStringNUL(data, 0)
		if ok && pos == sLen[i]+1 && x == s[0:sLen[i]] {
			t.Error("read string NUL failed.")
			return
		}
	}
}

func TestReadStringLenEnc(t *testing.T) {
	var client MysqlProtocolImpl
	var data = make([]byte, 24)
	var length = 10
	var s = "haha, test read string fix function"
	pos := client.writeStringLenEnc(data, 0, s[0:length])
	if pos != length+1 {
		t.Error("write string lenenc failed.")
		return
	}
	var x string
	var ok bool

	x, pos, ok = client.readStringLenEnc(data, 0)
	if !ok || pos != length+1 || x != s[0:length] {
		t.Error("read string lenenc failed.")
		return
	}

	//empty string
	pos = client.writeStringLenEnc(data, 0, s[0:0])
	if pos != 1 {
		t.Error("write string lenenc failed.")
		return
	}

	x, pos, ok = client.readStringLenEnc(data, 0)
	if !ok || pos != 1 || x != s[0:0] {
		t.Error("read string lenenc failed.")
		return
	}
}

// can not run this test case in ubuntu+golang1.9， let's add an issue(#4656) for that, I will fixed in someday.
// func TestMysqlClientProtocol_TlsHandshake(t *testing.T) {
// 	//before anything using the configuration
// 	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
// 	_, err := toml.DecodeFile("test/system_vars_config.toml", pu.SV)
// 	if err != nil {
// 		panic(err)
// 	}
// 	pu.SV.EnableTls = true
// 	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
// 	rm, _ := NewRoutineManager(ctx, pu)
// 	rm.SetSkipCheckUser(true)

// 	wg := sync.WaitGroup{}
// 	wg.Add(1)

// 	// //running server
// 	go func() {
// 		defer wg.Done()
// 		echoServer(rm.Handler, rm, NewSqlCodec())
// 	}()

// 	// to := NewTimeout(1*time.Minute, false)
// 	// for isClosed() && !to.isTimeout() {
// 	// }

// 	time.Sleep(time.Second * 2)
// 	db := open_tls_db(t, 6001)
// 	closeDbConn(t, db)

// 	time.Sleep(time.Millisecond * 10)
// 	//close server
// 	setServer(1)
// 	wg.Wait()
// }

//	func makeMysqlTinyIntResultSet(unsigned bool) *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Tiny"
//		if unsigned {
//			name = name + "Uint"
//		} else {
//			name = name + "Int"
//		}
//
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_TINY)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//		mysqlCol.SetSigned(!unsigned)
//
//		rs.AddColumn(mysqlCol)
//		if unsigned {
//			var cases = []uint8{0, 1, 254, 255}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		} else {
//			var cases = []int8{-128, -127, 127}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		}
//
//		return rs
//	}
//
//	func makeMysqlTinyResult(unsigned bool) *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlTinyIntResultSet(unsigned))
//	}
//
//	func makeMysqlShortResultSet(unsigned bool) *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Short"
//		if unsigned {
//			name = name + "Uint"
//		} else {
//			name = name + "Int"
//		}
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_SHORT)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//		mysqlCol.SetSigned(!unsigned)
//
//		rs.AddColumn(mysqlCol)
//		if unsigned {
//			var cases = []uint16{0, 1, 254, 255, 65535}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		} else {
//			var cases = []int16{-32768, 0, 32767}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		}
//
//		return rs
//	}
//
//	func makeMysqlShortResult(unsigned bool) *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlShortResultSet(unsigned))
//	}
//
//	func makeMysqlLongResultSet(unsigned bool) *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Long"
//		if unsigned {
//			name = name + "Uint"
//		} else {
//			name = name + "Int"
//		}
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_LONG)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//		mysqlCol.SetSigned(!unsigned)
//
//		rs.AddColumn(mysqlCol)
//		if unsigned {
//			var cases = []uint32{0, 4294967295}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		} else {
//			var cases = []int32{-2147483648, 0, 2147483647}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		}
//
//		return rs
//	}
//
//	func makeMysqlLongResult(unsigned bool) *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlLongResultSet(unsigned))
//	}
//
//	func makeMysqlLongLongResultSet(unsigned bool) *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "LongLong"
//		if unsigned {
//			name = name + "Uint"
//		} else {
//			name = name + "Int"
//		}
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//		mysqlCol.SetSigned(!unsigned)
//
//		rs.AddColumn(mysqlCol)
//		if unsigned {
//			var cases = []uint64{0, 4294967295, 18446744073709551615}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		} else {
//			var cases = []int64{-9223372036854775808, 0, 9223372036854775807}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		}
//
//		return rs
//	}
//
//	func makeMysqlLongLongResult(unsigned bool) *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlLongLongResultSet(unsigned))
//	}
//
//	func makeMysqlInt24ResultSet(unsigned bool) *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Int24"
//		if unsigned {
//			name = name + "Uint"
//		} else {
//			name = name + "Int"
//		}
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_INT24)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//		mysqlCol.SetSigned(!unsigned)
//
//		rs.AddColumn(mysqlCol)
//		if unsigned {
//			//[0,16777215]
//			var cases = []uint32{0, 16777215, 4294967295}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		} else {
//			//[-8388608,8388607]
//			var cases = []int32{-2147483648, -8388608, 0, 8388607, 2147483647}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		}
//
//		return rs
//	}
//
//	func makeMysqlInt24Result(unsigned bool) *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlInt24ResultSet(unsigned))
//	}
//
//	func makeMysqlYearResultSet(unsigned bool) *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Year"
//		if unsigned {
//			name = name + "Uint"
//		} else {
//			name = name + "Int"
//		}
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_YEAR)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//		mysqlCol.SetSigned(!unsigned)
//
//		rs.AddColumn(mysqlCol)
//		if unsigned {
//			var cases = []uint16{0, 1, 254, 255, 65535}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		} else {
//			var cases = []int16{-32768, 0, 32767}
//			for _, v := range cases {
//				var data = make([]interface{}, 1)
//				data[0] = v
//				rs.AddRow(data)
//			}
//		}
//
//		return rs
//	}
//
//	func makeMysqlYearResult(unsigned bool) *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlYearResultSet(unsigned))
//	}
//
//	func makeMysqlVarcharResultSet() *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Varchar"
//
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//		rs.AddColumn(mysqlCol)
//
//		var cases = []string{"abc", "abcde", "", "x-", "xx"}
//		for _, v := range cases {
//			var data = make([]interface{}, 1)
//			data[0] = v
//			rs.AddRow(data)
//		}
//
//		return rs
//	}
//
//	func makeMysqlVarcharResult() *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlVarcharResultSet())
//	}
//
//	func makeMysqlVarStringResultSet() *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Varstring"
//
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//		rs.AddColumn(mysqlCol)
//
//		var cases = []string{"abc", "abcde", "", "x-", "xx"}
//		for _, v := range cases {
//			var data = make([]interface{}, 1)
//			data[0] = v
//			rs.AddRow(data)
//		}
//
//		return rs
//	}
//
//	func makeMysqlVarStringResult() *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlVarStringResultSet())
//	}
//
//	func makeMysqlStringResultSet() *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "String"
//
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_STRING)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//		rs.AddColumn(mysqlCol)
//
//		var cases = []string{"abc", "abcde", "", "x-", "xx"}
//		for _, v := range cases {
//			var data = make([]interface{}, 1)
//			data[0] = v
//			rs.AddRow(data)
//		}
//
//		return rs
//	}
//
//	func makeMysqlStringResult() *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlStringResultSet())
//	}
//
//	func makeMysqlFloatResultSet() *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Float"
//
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_FLOAT)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//		rs.AddColumn(mysqlCol)
//
//		var cases = []float32{math.MaxFloat32, math.SmallestNonzeroFloat32, -math.MaxFloat32, -math.SmallestNonzeroFloat32}
//		for _, v := range cases {
//			var data = make([]interface{}, 1)
//			data[0] = v
//			rs.AddRow(data)
//		}
//
//		return rs
//	}
//
//	func makeMysqlFloatResult() *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlFloatResultSet())
//	}
//
//	func makeMysqlDoubleResultSet() *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Double"
//
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_DOUBLE)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//		rs.AddColumn(mysqlCol)
//
//		var cases = []float64{math.MaxFloat64, math.SmallestNonzeroFloat64, -math.MaxFloat64, -math.SmallestNonzeroFloat64}
//		for _, v := range cases {
//			var data = make([]interface{}, 1)
//			data[0] = v
//			rs.AddRow(data)
//		}
//
//		return rs
//	}
//
//	func makeMysqlDoubleResult() *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlDoubleResultSet())
//	}
//
//	func makeMysqlDateResultSet() *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Date"
//
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_DATE)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//		rs.AddColumn(mysqlCol)
//
//		d1, _ := types.ParseDateCast("1997-01-01")
//		d2, _ := types.ParseDateCast("2008-02-02")
//		var cases = []types.Date{
//			d1,
//			d2,
//		}
//		for _, v := range cases {
//			var data = make([]interface{}, 1)
//			data[0] = v
//			rs.AddRow(data)
//		}
//
//		return rs
//	}
//
//	func makeMysqlDateResult() *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlDateResultSet())
//	}
//
//	func makeMysqlTimeResultSet() *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Time"
//
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_TIME)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//		rs.AddColumn(mysqlCol)
//
//		t1, _ := types.ParseTime("110:21:15", 0)
//		t2, _ := types.ParseTime("2018-04-28 10:21:15.123", 0)
//		t3, _ := types.ParseTime("-112:12:12", 0)
//		var cases = []types.Time{
//			t1,
//			t2,
//			t3,
//		}
//		for _, v := range cases {
//			var data = make([]interface{}, 1)
//			data[0] = v
//			rs.AddRow(data)
//		}
//
//		return rs
//	}
//
//	func makeMysqlTimeResult() *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlTimeResultSet())
//	}
//
//	func makeMysqlDatetimeResultSet() *MysqlResultSet {
//		var rs = &MysqlResultSet{}
//
//		name := "Date"
//
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(defines.MYSQL_TYPE_DATETIME)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//		rs.AddColumn(mysqlCol)
//
//		d1, _ := types.ParseDatetime("2018-04-28 10:21:15", 0)
//		d2, _ := types.ParseDatetime("2018-04-28 10:21:15.123", 0)
//		d3, _ := types.ParseDatetime("2015-03-03 12:12:12", 0)
//		var cases = []types.Datetime{
//			d1,
//			d2,
//			d3,
//		}
//		for _, v := range cases {
//			var data = make([]interface{}, 1)
//			data[0] = v
//			rs.AddRow(data)
//		}
//
//		return rs
//	}
//
//	func makeMysqlDatetimeResult() *MysqlExecutionResult {
//		return NewMysqlExecutionResult(0, 0, 0, 0, makeMysqlDatetimeResultSet())
//	}
func make9ColumnsResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	var columnTypes = []defines.MysqlType{
		defines.MYSQL_TYPE_TINY,
		defines.MYSQL_TYPE_SHORT,
		defines.MYSQL_TYPE_LONG,
		defines.MYSQL_TYPE_LONGLONG,
		defines.MYSQL_TYPE_VARCHAR,
		defines.MYSQL_TYPE_FLOAT,
		defines.MYSQL_TYPE_DATE,
		defines.MYSQL_TYPE_TIME,
		defines.MYSQL_TYPE_DATETIME,
		defines.MYSQL_TYPE_DOUBLE,
	}

	var names = []string{
		"Tiny",
		"Short",
		"Long",
		"Longlong",
		"Varchar",
		"Float",
		"Date",
		"Time",
		"Datetime",
		"Double",
	}

	d1, _ := types.ParseDateCast("1997-01-01")
	d2, _ := types.ParseDateCast("2008-02-02")

	dt1, _ := types.ParseDatetime("2018-04-28 10:21:15", 0)
	dt2, _ := types.ParseDatetime("2018-04-28 10:21:15.123", 0)
	dt3, _ := types.ParseDatetime("2015-03-03 12:12:12", 0)

	t1, _ := types.ParseTime("2018-04-28 10:21:15", 0)
	t2, _ := types.ParseTime("2018-04-28 10:21:15.123", 0)
	t3, _ := types.ParseTime("2015-03-03 12:12:12", 0)

	var cases = [][]interface{}{
		{int8(-128), int16(-32768), int32(-2147483648), int64(-9223372036854775808), "abc", float32(math.MaxFloat32), d1, t1, dt1, float64(0.01)},
		{int8(-127), int16(0), int32(0), int64(0), "abcde", float32(math.SmallestNonzeroFloat32), d2, t2, dt2, float64(0.01)},
		{int8(127), int16(32767), int32(2147483647), int64(9223372036854775807), "", float32(-math.MaxFloat32), d1, t3, dt3, float64(0.01)},
		{int8(126), int16(32766), int32(2147483646), int64(9223372036854775806), "x-", float32(-math.SmallestNonzeroFloat32), d2, t1, dt1, float64(0.01)},
	}

	for i, ct := range columnTypes {
		name := names[i]
		mysqlCol := new(MysqlColumn)
		mysqlCol.SetName(name)
		mysqlCol.SetOrgName(name + "OrgName")
		mysqlCol.SetColumnType(ct)
		mysqlCol.SetSchema(name + "Schema")
		mysqlCol.SetTable(name + "Table")
		mysqlCol.SetOrgTable(name + "Table")
		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

		rs.AddColumn(mysqlCol)
	}

	for _, v := range cases {
		rs.AddRow(v)
	}

	return rs
}

//func makeMysql9ColumnsResult() *MysqlExecutionResult {
//	return NewMysqlExecutionResult(0, 0, 0, 0, make9ColumnsResultSet())
//}

//func makeMoreThan16MBResultSet() *MysqlResultSet {
//	var rs = &MysqlResultSet{}
//
//	var columnTypes = []defines.MysqlType{
//		defines.MYSQL_TYPE_LONGLONG,
//		defines.MYSQL_TYPE_DOUBLE,
//		defines.MYSQL_TYPE_VARCHAR,
//	}
//
//	var names = []string{
//		"Longlong",
//		"Double",
//		"Varchar",
//	}
//
//	var rowCase = []interface{}{int64(9223372036854775807), math.MaxFloat64, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
//
//	for i, ct := range columnTypes {
//		name := names[i]
//		mysqlCol := new(MysqlColumn)
//		mysqlCol.SetName(name)
//		mysqlCol.SetOrgName(name + "OrgName")
//		mysqlCol.SetColumnType(ct)
//		mysqlCol.SetSchema(name + "Schema")
//		mysqlCol.SetTable(name + "Table")
//		mysqlCol.SetOrgTable(name + "Table")
//		mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//		rs.AddColumn(mysqlCol)
//	}
//
//	//the size of the total result set will be more than 16MB
//	for i := 0; i < 40000; i++ {
//		rs.AddRow(rowCase)
//	}
//
//	return rs
//}
//
//// the size of resultset will be morethan 16MB
//func makeMoreThan16MBResult() *MysqlExecutionResult {
//	return NewMysqlExecutionResult(0, 0, 0, 0, makeMoreThan16MBResultSet())
//}
//
//func make16MBRowResultSet() *MysqlResultSet {
//	var rs = &MysqlResultSet{}
//
//	name := "Varstring"
//
//	mysqlCol := new(MysqlColumn)
//	mysqlCol.SetName(name)
//	mysqlCol.SetOrgName(name + "OrgName")
//	mysqlCol.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
//	mysqlCol.SetSchema(name + "Schema")
//	mysqlCol.SetTable(name + "Table")
//	mysqlCol.SetOrgTable(name + "Table")
//	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//	rs.AddColumn(mysqlCol)
//
//	/*
//		How to test the max size of the data in one packet that the client can received ?
//		Environment: Mysql Version 8.0.23
//		1. shell: mysql --help | grep allowed-packet
//			something like:
//			"
//			  --max-allowed-packet=#
//			max-allowed-packet                16777216
//			"
//			so, we get:
//				max-allowed-packet means : The maximum packet length to send to or receive from server.
//				default value : 16777216 (16MB)
//		2. shell execution: mysql -uroot -e "select repeat('a',16*1024*1024-4);" > 16MB-mysql.txt
//			we get: ERROR 2020 (HY000) at line 1: Got packet bigger than 'max_allowed_packet' bytes
//		3. shell execution: mysql -uroot -e "select repeat('a',16*1024*1024-5);" > 16MB-mysql.txt
//			execution succeeded
//		4. so, the max size of the data in one packet is (max-allowed-packet - 5).
//		5. To change max-allowed-packet.
//			shell execution: mysql max-allowed-packet=xxxxx ....
//	*/
//
//	//test in shell : mysql -h 127.0.0.1 -P 6001 -udump -p111 -e "16mbrow" > 16mbrow.txt
//	//max data size : 16 * 1024 * 1024 - 5
//	var stuff = make([]byte, 16*1024*1024-5)
//	for i := range stuff {
//		stuff[i] = 'a'
//	}
//
//	var rowCase = []interface{}{string(stuff)}
//	for i := 0; i < 1; i++ {
//		rs.AddRow(rowCase)
//	}
//
//	return rs
//}
//
//// the size of resultset row will be more than 16MB
//func make16MBRowResult() *MysqlExecutionResult {
//	return NewMysqlExecutionResult(0, 0, 0, 0, make16MBRowResultSet())
//}
//
//func (tRM *TestRoutineManager) resultsetHandler(rs goetty.IOSession, msg interface{}, _ uint64) error {
//	tRM.rwlock.Lock()
//	routine := tRM.clients[rs]
//	tRM.rwlock.Unlock()
//	ctx := context.TODO()
//
//	pu, err := getParameterUnit("test/system_vars_config.toml", nil, nil)
//	if err != nil {
//		return err
//	}
//	pu.SV.SkipCheckUser = true
//	pro := routine.getProtocol().(*MysqlProtocolImpl)
//	packet, ok := msg.(*Packet)
//	pro.SetSequenceID(uint8(packet.SequenceID + 1))
//	if !ok {
//		return moerr.NewInternalError(ctx, "message is not Packet")
//	}
//	setGlobalPu(pu)
//	ses := NewSession(pro, nil, nil, false, nil)
//	ses.SetRequestContext(ctx)
//	pro.SetSession(ses)
//
//	length := packet.Length
//	payload := packet.Payload
//	for uint32(length) == MaxPayloadSize {
//		var err error
//		msg, err = pro.GetTcpConnection().Read(goetty.ReadOptions{})
//		if err != nil {
//			return moerr.NewInternalError(ctx, "read msg error")
//		}
//
//		packet, ok = msg.(*Packet)
//		if !ok {
//			return moerr.NewInternalError(ctx, "message is not Packet")
//		}
//
//		pro.SetSequenceID(uint8(packet.SequenceID + 1))
//		payload = append(payload, packet.Payload...)
//		length = packet.Length
//	}
//
//	// finish handshake process
//	if !pro.IsEstablished() {
//		_, err := pro.HandleHandshake(ctx, payload)
//		if err != nil {
//			return err
//		}
//		if err = pro.Authenticate(ctx); err != nil {
//			return err
//		}
//		pro.SetEstablished()
//		return nil
//	}
//
//	var req *Request
//	var resp *Response
//	req = pro.GetRequest(payload)
//	switch req.GetCmd() {
//	case COM_QUIT:
//		resp = &Response{
//			category: OkResponse,
//			status:   0,
//			data:     nil,
//		}
//		if err := pro.SendResponse(ctx, resp); err != nil {
//			fmt.Printf("send response failed. error:%v", err)
//			break
//		}
//	case COM_QUERY:
//		var query = string(req.GetData().([]byte))
//
//		switch query {
//		case "tiny":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				cmd:      0,
//				data:     makeMysqlTinyResult(false),
//			}
//		case "tinyu":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlTinyResult(true),
//			}
//		case "short":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlShortResult(false),
//			}
//		case "shortu":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlShortResult(true),
//			}
//		case "long":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlLongResult(false),
//			}
//		case "longu":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlLongResult(true),
//			}
//		case "longlong":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlLongLongResult(false),
//			}
//		case "longlongu":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlLongLongResult(true),
//			}
//		case "int24":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlInt24Result(false),
//			}
//		case "int24u":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlInt24Result(true),
//			}
//		case "year":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlYearResult(false),
//			}
//		case "yearu":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlYearResult(true),
//			}
//		case "varchar":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlVarcharResult(),
//			}
//		case "varstring":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlVarStringResult(),
//			}
//		case "string":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlStringResult(),
//			}
//		case "float":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlFloatResult(),
//			}
//		case "double":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlDoubleResult(),
//			}
//		case "date":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlDateResult(),
//			}
//		case "time":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlTimeResult(),
//			}
//		case "datetime":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysqlDatetimeResult(),
//			}
//		case "9columns":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMysql9ColumnsResult(),
//			}
//		case "16mb":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     makeMoreThan16MBResult(),
//			}
//		case "16mbrow":
//			resp = &Response{
//				category: ResultResponse,
//				status:   0,
//				data:     make16MBRowResult(),
//			}
//		default:
//			resp = &Response{
//				category: OkResponse,
//				status:   0,
//				data:     nil,
//			}
//		}
//
//		if err := pro.SendResponse(ctx, resp); err != nil {
//			fmt.Printf("send response failed. error:%v", err)
//			break
//		}
//	case COM_PING:
//		resp = NewResponse(
//			OkResponse,
//			0, 0, 0,
//			0,
//			int(COM_PING),
//			nil,
//		)
//		if err := pro.SendResponse(ctx, resp); err != nil {
//			fmt.Printf("send response failed. error:%v", err)
//			break
//		}
//
//	default:
//		fmt.Printf("unsupported command. 0x%x \n", req.cmd)
//	}
//	if req.cmd == COM_QUIT {
//		return nil
//	}
//	return nil
//}

//TODO:replace by the dedicated table functions.
//func TestMysqlResultSet(t *testing.T) {
//	//client connection method: mysql -h 127.0.0.1 -P 6001 -udump -p
//	//pwd: mysql-server-mysql-8.0.23/mysql-test
//	//with mysqltest: mysqltest --test-file=t/1st.test --result-file=r/1st.result --user=dump -p111 -P 6001 --host=127.0.0.1
//
//	//test:
//	//./mysql-test-run 1st --extern user=root --extern port=3306 --extern host=127.0.0.1
//	//  mysql5.7 failed
//	//	mysql-8.0.23 success
//	//./mysql-test-run 1st --extern user=root --extern port=6001 --extern host=127.0.0.1
//	//	matrixone failed: mysql-test-run: *** ERROR: Could not connect to extern server using command: '/Users/pengzhen/Documents/mysql-server-mysql-8.0.23/bld/runtime_output_directory//mysql --no-defaults --user=root --user=root --port=6001 --host=127.0.0.1 --silent --database=mysql --execute="SHOW GLOBAL VARIABLES"'
//	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
//	_, err := toml.DecodeFile("test/system_vars_config.toml", pu.SV)
//	if err != nil {
//		panic(err)
//	}
//	pu.SV.SkipCheckUser = true
//
//	trm := NewTestRoutineManager(pu)
//	var atomTrm atomic.Value
//	atomTrm.Store(trm)
//
//	wg := sync.WaitGroup{}
//	wg.Add(1)
//
//	go func() {
//		defer wg.Done()
//
//		temTrm := atomTrm.Load().(*TestRoutineManager)
//		echoServer(temTrm.resultsetHandler, temTrm, NewSqlCodec())
//	}()
//
//	// to := NewTimeout(1*time.Minute, false)
//	// for isClosed() && !to.isTimeout() {
//	// }
//
//	time.Sleep(time.Second * 2)
//	db, err := openDbConn(t, 6001)
//	require.NoError(t, err)
//
//	do_query_resp_resultset(t, db, false, false, "tiny", makeMysqlTinyIntResultSet(false))
//	do_query_resp_resultset(t, db, false, false, "tinyu", makeMysqlTinyIntResultSet(true))
//	do_query_resp_resultset(t, db, false, false, "short", makeMysqlShortResultSet(false))
//	do_query_resp_resultset(t, db, false, false, "shortu", makeMysqlShortResultSet(true))
//	do_query_resp_resultset(t, db, false, false, "long", makeMysqlLongResultSet(false))
//	do_query_resp_resultset(t, db, false, false, "longu", makeMysqlLongResultSet(true))
//	do_query_resp_resultset(t, db, false, false, "longlong", makeMysqlLongLongResultSet(false))
//	do_query_resp_resultset(t, db, false, false, "longlongu", makeMysqlLongLongResultSet(true))
//	do_query_resp_resultset(t, db, false, false, "int24", makeMysqlInt24ResultSet(false))
//	do_query_resp_resultset(t, db, false, false, "int24u", makeMysqlInt24ResultSet(true))
//	do_query_resp_resultset(t, db, false, false, "year", makeMysqlYearResultSet(false))
//	do_query_resp_resultset(t, db, false, false, "yearu", makeMysqlYearResultSet(true))
//	do_query_resp_resultset(t, db, false, false, "varchar", makeMysqlVarcharResultSet())
//	do_query_resp_resultset(t, db, false, false, "varstring", makeMysqlVarStringResultSet())
//	do_query_resp_resultset(t, db, false, false, "string", makeMysqlStringResultSet())
//	do_query_resp_resultset(t, db, false, false, "float", makeMysqlFloatResultSet())
//	do_query_resp_resultset(t, db, false, false, "double", makeMysqlDoubleResultSet())
//	do_query_resp_resultset(t, db, false, false, "date", makeMysqlDateResultSet())
//	do_query_resp_resultset(t, db, false, false, "time", makeMysqlTimeResultSet())
//	do_query_resp_resultset(t, db, false, false, "datetime", makeMysqlDatetimeResultSet())
//	do_query_resp_resultset(t, db, false, false, "9columns", make9ColumnsResultSet())
//	do_query_resp_resultset(t, db, false, false, "16mbrow", make16MBRowResultSet())
//	do_query_resp_resultset(t, db, false, false, "16mb", makeMoreThan16MBResultSet())
//
//	time.Sleep(time.Millisecond * 10)
//	//close server
//	setServer(1)
//	wg.Wait()
//
//	closeDbConn(t, db)
//}

// func open_tls_db(t *testing.T, port int) *sql.DB {
// 	tlsName := "custom"
// 	rootCertPool := x509.NewCertPool()
// 	pem, err := os.ReadFile("test/ca.pem")
// 	if err != nil {
// 		setServer(1)
// 		require.NoError(t, err)
// 	}
// 	if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
// 		log.Fatal("Failed to append PEM.")
// 	}
// 	clientCert := make([]tls.Certificate, 0, 1)
// 	certs, err := tls.LoadX509KeyPair("test/client-cert2.pem", "test/client-key2.pem")
// 	if err != nil {
// 		setServer(1)
// 		require.NoError(t, err)
// 	}
// 	clientCert = append(clientCert, certs)
// 	err = mysqlDriver.RegisterTLSConfig(tlsName, &tls.Config{
// 		RootCAs:            rootCertPool,
// 		Certificates:       clientCert,
// 		MinVersion:         tls.VersionTLS12,
// 		InsecureSkipVerify: true,
// 	})
// 	if err != nil {
// 		setServer(1)
// 		require.NoError(t, err)
// 	}

// 	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=5s&timeout=5s&writeTimeout=5s&tls=%s", port, tlsName)
// 	db, err := sql.Open("mysql", dsn)
// 	if err != nil {
// 		require.NoError(t, err)
// 	} else {
// 		db.SetConnMaxLifetime(time.Minute * 3)
// 		db.SetMaxOpenConns(1)
// 		db.SetMaxIdleConns(1)
// 		time.Sleep(time.Millisecond * 100)

// 		// ping opens the connection
// 		logutil.Info("start ping")
// 		err = db.Ping()
// 		if err != nil {
// 			setServer(1)
// 			require.NoError(t, err)
// 		}
// 	}
// 	return db
// }

func openDbConn(t *testing.T, port int) (db *sql.DB, err error) {
	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=30s&timeout=30s&writeTimeout=30s", port)
	for i := 0; i < 3; i++ {
		db, err = tryConn(dsn)
		if err != nil {
			logger.Error("open conn failed.", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}
		break
	}
	return
}

func tryConn(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	} else {
		db.SetConnMaxLifetime(time.Minute * 3)
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		time.Sleep(time.Millisecond * 100)

		//ping opens the connection
		err = db.Ping()
		if err != nil {
			return nil, err
		}
	}
	return db, err
}

func closeDbConn(t *testing.T, db *sql.DB) {
	err := db.Close()
	assert.NoError(t, err)
}

//func do_query_resp_resultset(t *testing.T, db *sql.DB, wantErr bool, skipResultsetCheck bool, query string, mrs *MysqlResultSet) {
//	logutil.Infof("query: %v", query)
//	rows, err := db.Query(query)
//	if wantErr {
//		require.Error(t, err)
//		require.True(t, rows == nil)
//		return
//	}
//	require.NoError(t, err)
//	defer func() {
//		err = rows.Close()
//		require.NoError(t, err)
//		err = rows.Err()
//		require.NoError(t, err)
//	}()
//
//	//column check
//	columns, err := rows.Columns()
//	require.NoError(t, err)
//	require.True(t, len(columns) == len(mrs.Columns))
//
//	//colType, err := rows.ColumnTypes()
//	//require.NoError(t, err)
//	//for i, ct := range colType {
//	//	fmt.Printf("column %d\n",i)
//	//	fmt.Printf("name %v \n",ct.Name())
//	//	l,o := ct.RowCount()
//	//	fmt.Printf("length %v %v \n",l,o)
//	//	p,s,o := ct.DecimalSize()
//	//	fmt.Printf("decimalsize %v %v %v \n",p,s,o)
//	//	fmt.Printf("scantype %v \n",ct.ScanType())
//	//	n,o := ct.Nullable()
//	//	fmt.Printf("nullable %v %v \n",n,o)
//	//	fmt.Printf("databaseTypeName %s \n",ct.DatabaseTypeName())
//	//}
//
//	values := make([][]byte, len(columns))
//
//	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
//	// references into such a slice
//	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
//	scanArgs := make([]interface{}, len(columns))
//	for i := uint64(0); i < mrs.GetColumnCount(); i++ {
//		scanArgs[i] = &values[i]
//	}
//
//	rowIdx := uint64(0)
//	for rows.Next() {
//		err = rows.Scan(scanArgs...)
//		require.NoError(t, err)
//
//		//fmt.Println(rowIdx)
//		//fmt.Println(mrs.GetRow(rowIdx))
//		//
//		//for i := uint64(0); i < mrs.GetColumnCount(); i++ {
//		//	arg := scanArgs[i]
//		//	val := *(arg.(*[]byte))
//		//	fmt.Printf("%v ",val)
//		//}
//		//fmt.Println()
//
//		if !skipResultsetCheck {
//			for i := uint64(0); i < mrs.GetColumnCount(); i++ {
//				arg := scanArgs[i]
//				val := *(arg.(*[]byte))
//
//				column, err := mrs.GetColumn(context.TODO(), i)
//				require.NoError(t, err)
//
//				col, ok := column.(*MysqlColumn)
//				require.True(t, ok)
//
//				isNUll, err := mrs.ColumnIsNull(context.TODO(), rowIdx, i)
//				require.NoError(t, err)
//
//				if isNUll {
//					require.True(t, val == nil)
//				} else {
//					var data []byte = nil
//					switch col.ColumnType() {
//					case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_YEAR:
//						value, err := mrs.GetInt64(context.TODO(), rowIdx, i)
//						require.NoError(t, err)
//						if col.ColumnType() == defines.MYSQL_TYPE_YEAR {
//							if value == 0 {
//								data = append(data, []byte("0000")...)
//							} else {
//								data = strconv.AppendInt(data, value, 10)
//							}
//						} else {
//							data = strconv.AppendInt(data, value, 10)
//						}
//
//					case defines.MYSQL_TYPE_LONGLONG:
//						if uint32(col.Flag())&defines.UNSIGNED_FLAG != 0 {
//							value, err := mrs.GetUint64(context.TODO(), rowIdx, i)
//							require.NoError(t, err)
//							data = strconv.AppendUint(data, value, 10)
//						} else {
//							value, err := mrs.GetInt64(context.TODO(), rowIdx, i)
//							require.NoError(t, err)
//							data = strconv.AppendInt(data, value, 10)
//						}
//					case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING:
//						value, err := mrs.GetString(context.TODO(), rowIdx, i)
//						require.NoError(t, err)
//						data = []byte(value)
//					case defines.MYSQL_TYPE_FLOAT:
//						value, err := mrs.GetFloat64(context.TODO(), rowIdx, i)
//						require.NoError(t, err)
//						data = strconv.AppendFloat(data, value, 'f', -1, 32)
//					case defines.MYSQL_TYPE_DOUBLE:
//						value, err := mrs.GetFloat64(context.TODO(), rowIdx, i)
//						require.NoError(t, err)
//						data = strconv.AppendFloat(data, value, 'f', -1, 64)
//					case defines.MYSQL_TYPE_DATE:
//						value, err := mrs.GetValue(context.TODO(), rowIdx, i)
//						require.NoError(t, err)
//						x := value.(types.Date).String()
//						data = []byte(x)
//					case defines.MYSQL_TYPE_TIME:
//						value, err := mrs.GetValue(context.TODO(), rowIdx, i)
//						require.NoError(t, err)
//						x := value.(types.Time).String()
//						data = []byte(x)
//					case defines.MYSQL_TYPE_DATETIME:
//						value, err := mrs.GetValue(context.TODO(), rowIdx, i)
//						require.NoError(t, err)
//						x := value.(types.Datetime).String()
//						data = []byte(x)
//					default:
//						require.NoError(t, moerr.NewInternalError(context.TODO(), "unsupported type %v", col.ColumnType()))
//					}
//					//check
//					ret := reflect.DeepEqual(data, val)
//					//fmt.Println(i)
//					//fmt.Println(data)
//					//fmt.Println(val)
//					require.True(t, ret)
//				}
//			}
//		}
//
//		rowIdx++
//	}
//
//	require.True(t, rowIdx == mrs.GetRowCount())
//
//}

func Test_writePackets(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("writepackets 16MB succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		err = proto.writePackets(make([]byte, MaxPayloadSize), true)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("writepackets 16MB failed", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		cnt := 0
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(msg interface{}, opts goetty.WriteOptions) error {
			if cnt == 0 {
				cnt++
				return nil
			} else {
				cnt++
				return moerr.NewInternalError(ctx, "write and flush failed.")
			}
		}).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		err = proto.writePackets(make([]byte, MaxPayloadSize), true)
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("writepackets 16MB failed 2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(func(msg interface{}, opts goetty.WriteOptions) error {
			return moerr.NewInternalError(ctx, "write and flush failed.")
		}).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		err = proto.writePackets(make([]byte, MaxPayloadSize), true)
		convey.So(err, convey.ShouldBeError)
	})
}

func Test_openpacket(t *testing.T) {
	convey.Convey("openpacket succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)

		err = proto.openPacket()
		convey.So(err, convey.ShouldBeNil)
		outBuf := proto.tcpConn.OutBuf()
		headLen := outBuf.GetWriteIndex() - beginWriteIndex(outBuf, proto.beginOffset)
		convey.So(headLen, convey.ShouldEqual, HeaderLengthOfTheProtocol)
	})

	convey.Convey("fillpacket succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		pu, err := getParameterUnit("test/system_vars_config.toml", nil, nil)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		// fill proto.ses
		ses := NewSession(context.TODO(), proto, nil, nil, false, nil)
		proto.ses = ses

		err = proto.fillPacket(make([]byte, MaxPayloadSize)...)
		convey.So(err, convey.ShouldBeNil)

		err = proto.closePacket(true)
		convey.So(err, convey.ShouldBeNil)

		proto.append(nil, make([]byte, 1024)...)
	})

	convey.Convey("closepacket falied.", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		pu, err := getParameterUnit("test/system_vars_config.toml", nil, nil)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		// fill proto.ses
		ses := NewSession(context.TODO(), proto, nil, nil, false, nil)
		proto.ses = ses

		err = proto.openPacket()
		convey.So(err, convey.ShouldBeNil)

		proto.beginOffset = proto.tcpConn.OutBuf().GetWriteIndex() - proto.tcpConn.OutBuf().GetReadIndex()
		err = proto.closePacket(true)
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("append -- data checks", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)

		mysqlPack := func(payload []byte) []byte {
			n := len(payload)
			var curLen int
			var header [4]byte
			var data []byte = nil
			var sequenceId byte = 0
			for i := 0; i < n; i += curLen {
				curLen = Min(int(MaxPayloadSize), n-i)
				binary.LittleEndian.PutUint32(header[:], uint32(curLen))
				header[3] = sequenceId
				sequenceId++
				data = append(data, header[:]...)
				data = append(data, payload[i:i+curLen]...)
				if i+curLen == n && curLen == int(MaxPayloadSize) {
					binary.LittleEndian.PutUint32(header[:], uint32(0))
					header[3] = sequenceId
					sequenceId++
					data = append(data, header[:]...)
				}
			}
			return data
		}

		data16MB := func(cnt int) []byte {
			data := make([]byte, cnt*int(MaxPayloadSize))
			return data
		}

		type kase struct {
			data []byte
			len  int
		}

		kases := []kase{
			{
				data: []byte{1, 2, 3, 4},
				len:  HeaderLengthOfTheProtocol + 4,
			},
			{
				data: data16MB(1),
				len:  HeaderLengthOfTheProtocol + int(MaxPayloadSize) + HeaderLengthOfTheProtocol,
			},
			{
				data: data16MB(2),
				len:  HeaderLengthOfTheProtocol + int(MaxPayloadSize) + HeaderLengthOfTheProtocol + int(MaxPayloadSize) + HeaderLengthOfTheProtocol,
			},
			{
				data: data16MB(3),
				len: HeaderLengthOfTheProtocol + int(MaxPayloadSize) +
					HeaderLengthOfTheProtocol + int(MaxPayloadSize) +
					HeaderLengthOfTheProtocol + int(MaxPayloadSize) +
					HeaderLengthOfTheProtocol,
			},
			{
				data: data16MB(4),
				len: HeaderLengthOfTheProtocol + int(MaxPayloadSize) +
					HeaderLengthOfTheProtocol + int(MaxPayloadSize) +
					HeaderLengthOfTheProtocol + int(MaxPayloadSize) +
					HeaderLengthOfTheProtocol + int(MaxPayloadSize) +
					HeaderLengthOfTheProtocol,
			},
		}

		for _, c := range kases {
			proto.SetSequenceID(0)

			err = proto.openRow(nil)
			convey.So(err, convey.ShouldBeNil)

			outBuf := proto.tcpConn.OutBuf()
			beginOffset := proto.beginOffset

			rawBuf := proto.append(nil, c.data...)

			err = proto.closeRow(nil)
			convey.So(err, convey.ShouldBeNil)

			want := mysqlPack(c.data)

			convey.So(c.len, convey.ShouldEqual, len(want))

			widx := outBuf.GetWriteIndex()
			beginIdx := beginWriteIndex(outBuf, beginOffset)
			res := rawBuf[beginIdx:widx]

			convey.So(bytes.Equal(res, want), convey.ShouldBeTrue)
		}
	})

}

func TestSendPrepareResponse(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("send Prepare response succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		proto.SetSession(&Session{
			feSessionImpl: feSessionImpl{
				txnHandler: &TxnHandler{},
			},
		})

		st := tree.NewPrepareString(tree.Identifier(getPrepareStmtName(1)), "select ?, 1")
		stmts, err := mysql.Parse(ctx, st.Sql, 1, 0)
		if err != nil {
			t.Error(err)
		}
		compCtx := plan.NewEmptyCompilerContext()
		preparePlan, err := buildPlan(context.TODO(), nil, compCtx, st)
		if err != nil {
			t.Error(err)
		}
		prepareStmt := &PrepareStmt{
			Name:        preparePlan.GetDcl().GetPrepare().GetName(),
			PreparePlan: preparePlan,
			PrepareStmt: stmts[0],
		}
		err = proto.SendPrepareResponse(ctx, prepareStmt)

		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("send Prepare response error", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)

		st := tree.NewPrepareString("stmt1", "select ?, 1")
		stmts, err := mysql.Parse(ctx, st.Sql, 1, 0)
		if err != nil {
			t.Error(err)
		}
		compCtx := plan.NewEmptyCompilerContext()
		preparePlan, err := buildPlan(context.TODO(), nil, compCtx, st)
		if err != nil {
			t.Error(err)
		}
		prepareStmt := &PrepareStmt{
			Name:        preparePlan.GetDcl().GetPrepare().GetName(),
			PreparePlan: preparePlan,
			PrepareStmt: stmts[0],
		}
		err = proto.SendPrepareResponse(ctx, prepareStmt)

		convey.So(err, convey.ShouldBeError)
	})
}

func FuzzParseExecuteData(f *testing.F) {
	ctx := context.TODO()

	ctrl := gomock.NewController(f)
	defer ctrl.Finish()
	ioses := mock_frontend.NewMockIOSession(ctrl)
	proc := testutil.NewProcess()

	ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
	ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
	ioses.EXPECT().Ref().AnyTimes()
	ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		f.Error(err)
	}

	proto := NewMysqlClientProtocol(0, ioses, 1024, sv)

	st := tree.NewPrepareString(tree.Identifier(getPrepareStmtName(1)), "select ?, 1")
	stmts, err := mysql.Parse(ctx, st.Sql, 1, 0)
	if err != nil {
		f.Error(err)
	}
	compCtx := plan.NewEmptyCompilerContext()
	preparePlan, err := buildPlan(context.TODO(), nil, compCtx, st)
	if err != nil {
		f.Error(err)
	}
	prepareStmt := &PrepareStmt{
		Name:        preparePlan.GetDcl().GetPrepare().GetName(),
		PreparePlan: preparePlan,
		PrepareStmt: stmts[0],
		params:      vector.NewVec(types.T_varchar.ToType()),
	}

	var testData []byte
	testData = append(testData, 0)          //flag
	testData = append(testData, 0, 0, 0, 0) // skip iteration-count
	nullBitmapLen := (1 + 7) >> 3
	//nullBitmapLen
	for i := 0; i < nullBitmapLen; i++ {
		testData = append(testData, 0)
	}
	testData = append(testData, 1)                              // new param bound flag
	testData = append(testData, uint8(defines.MYSQL_TYPE_TINY)) // type
	testData = append(testData, 0)                              //is unsigned
	testData = append(testData, 10)                             //tiny value

	f.Add(testData)

	testData = []byte{}
	testData = append(testData, 0)          //flag
	testData = append(testData, 0, 0, 0, 0) // skip iteration-count
	nullBitmapLen = (1 + 7) >> 3
	//nullBitmapLen
	for i := 0; i < nullBitmapLen; i++ {
		testData = append(testData, 0)
	}
	testData = append(testData, 1)                              // new param bound flag
	testData = append(testData, uint8(defines.MYSQL_TYPE_TINY)) // type
	testData = append(testData, 0)                              //is unsigned
	testData = append(testData, 4)                              //tiny value
	f.Add(testData)

	f.Fuzz(func(t *testing.T, data []byte) {
		proto.ParseExecuteData(ctx, proc, prepareStmt, data, 0)
	})
}

/* FIXME The prepare process has undergone some modifications,
  	so the unit tests for prepare need to be refactored, and the subsequent pr I will resubmit a reasonable ut
func TestParseExecuteData(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("parseExecuteData succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		proc := testutil.NewProcess()

		st := tree.NewPrepareString(tree.Identifier(getPrepareStmtName(1)), "select ?, 1")
		stmts, err := mysql.Parse(ctx, st.Sql, 1)
		if err != nil {
			t.Error(err)
		}
		compCtx := plan.NewEmptyCompilerContext()
		preparePlan, err := buildPlan(context.TODO(), nil, compCtx, st)
		if err != nil {
			t.Error(err)
		}
		prepareStmt := &PrepareStmt{
			Name:        preparePlan.GetDcl().GetPrepare().GetName(),
			PreparePlan: preparePlan,
			PrepareStmt: stmts[0],
			params:      vector.NewVec(types.T_varchar.ToType()),
		}

		var testData []byte
		testData = append(testData, 0)          //flag
		testData = append(testData, 0, 0, 0, 0) // skip iteration-count
		nullBitmapLen := (1 + 7) >> 3
		//nullBitmapLen
		for i := 0; i < nullBitmapLen; i++ {
			testData = append(testData, 0)
		}
		testData = append(testData, 1)                              // new param bound flag
		testData = append(testData, uint8(defines.MYSQL_TYPE_TINY)) // type
		testData = append(testData, 0)                              //is unsigned
		testData = append(testData, 10)                             //tiny value

		err = proto.ParseExecuteData(ctx, proc, prepareStmt, testData, 0)
		convey.So(err, convey.ShouldBeNil)
	})

}
*/

func Test_resultset(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("send result set batch row succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		eng := mock_frontend.NewMockEngine(ctrl)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(ctx, proto, nil, &gSys, true, nil)
		proto.ses = ses

		res := make9ColumnsResultSet()

		err = proto.SendResultSetTextBatchRow(res, uint64(len(res.Data)))
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("send result set batch row speedup succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		eng := mock_frontend.NewMockEngine(ctrl)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(ctx, proto, nil, &gSys, true, nil)
		proto.ses = ses

		res := make9ColumnsResultSet()

		err = proto.SendResultSetTextBatchRowSpeedup(res, uint64(len(res.Data)))
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("send result set succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		eng := mock_frontend.NewMockEngine(ctrl)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(ctx, proto, nil, &gSys, true, nil)
		proto.ses = ses

		res := make9ColumnsResultSet()

		err = proto.sendResultSet(ctx, res, int(COM_QUERY), 0, 0)
		convey.So(err, convey.ShouldBeNil)

		err = proto.SendResultSetTextRow(res, 0)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("send binary result set succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
		eng := mock_frontend.NewMockEngine(ctrl)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
		if err != nil {
			t.Error(err)
		}
		setGlobalPu(pu)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(ctx, proto, nil, &gSys, true, nil)
		ses.cmd = COM_STMT_EXECUTE
		proto.ses = ses

		res := make9ColumnsResultSet()

		err = proto.SendResultSetTextBatchRowSpeedup(res, 0)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_send_packet(t *testing.T) {
	convey.Convey("send err packet", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)

		err = proto.sendErrPacket(1, "fake state", "fake error")
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("send eof packet", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(goetty_buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)

		err = proto.sendEOFPacket(1, 0)
		convey.So(err, convey.ShouldBeNil)

		err = proto.SendEOFPacketIf(1, 0)
		convey.So(err, convey.ShouldBeNil)

		err = proto.sendEOFOrOkPacket(1, 0)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_analyse320resp(t *testing.T) {
	convey.Convey("analyse 320 resp succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)

		var data []byte = nil
		var cap uint16 = 0
		cap |= uint16(CLIENT_CONNECT_WITH_DB)
		var header [2]byte
		proto.io.WriteUint16(header[:], 0, cap)
		//int<2>             capabilities flags, CLIENT_PROTOCOL_41 never set
		data = append(data, header[:]...)
		//int<3>             max-packet size
		data = append(data, 0xff, 0xff, 0xff)
		//string[NUL]        username
		username := "abc"
		data = append(data, []byte(username)...)
		data = append(data, 0x0)
		//auth response
		authResp := []byte{0x1, 0x2, 0x3, 0x4}
		data = append(data, authResp...)
		data = append(data, 0x0)
		//database
		dbName := "T"
		data = append(data, []byte(dbName)...)
		data = append(data, 0x0)

		ok, resp320, err := proto.analyseHandshakeResponse320(context.TODO(), data)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)

		convey.So(resp320.username, convey.ShouldEqual, username)
		convey.So(bytes.Equal(resp320.authResponse, authResp), convey.ShouldBeTrue)
		convey.So(resp320.database, convey.ShouldEqual, dbName)
	})

	convey.Convey("analyse 320 resp failed", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)

		type kase struct {
			data []byte
			res  bool
		}

		kases := []kase{
			{data: []byte{0}, res: false},
			{data: []byte{0, 0, 0, 0}, res: false},
			{data: []byte{0, 0, 1, 2, 3}, res: false},
			{data: []byte{0, 0, 1, 2, 3, 'a', 0}, res: true},
			{data: []byte{0, 0, 1, 2, 3, 'a', 0, 1, 2, 3}, res: true},
			{data: []byte{uint8(CLIENT_CONNECT_WITH_DB), 0, 1, 2, 3, 'a', 0}, res: false},
			{data: []byte{uint8(CLIENT_CONNECT_WITH_DB), 0, 1, 2, 3, 'a', 0, 'b', 'c'}, res: false},
			{data: []byte{uint8(CLIENT_CONNECT_WITH_DB), 0, 1, 2, 3, 'a', 0, 'b', 'c', 0}, res: false},
			{data: []byte{uint8(CLIENT_CONNECT_WITH_DB), 0, 1, 2, 3, 'a', 0, 'b', 'c', 0, 'd', 'e'}, res: false},
			{data: []byte{uint8(CLIENT_CONNECT_WITH_DB), 0, 1, 2, 3, 'a', 0, 'b', 'c', 0, 'd', 'e', 0}, res: true},
		}

		for _, c := range kases {
			ok, _, _ := proto.analyseHandshakeResponse320(context.TODO(), c.data)
			convey.So(ok, convey.ShouldEqual, c.res)
		}
	})
}

func Test_analyse41resp(t *testing.T) {
	convey.Convey("analyse 41 resp succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)

		var data []byte = nil
		var cap uint32 = 0
		cap |= CLIENT_PROTOCOL_41 | CLIENT_CONNECT_WITH_DB
		var header [4]byte
		proto.io.WriteUint32(header[:], 0, cap)
		//int<4>             capabilities flags of the client, CLIENT_PROTOCOL_41 always set
		data = append(data, header[:]...)
		//int<4>             max-packet size
		data = append(data, 0xff, 0xff, 0xff, 0xff)
		//int<1>             character set
		data = append(data, 0x1)
		//string[23]         reserved (all [0])
		data = append(data, make([]byte, 23)...)
		//string[NUL]        username
		username := "abc"
		data = append(data, []byte(username)...)
		data = append(data, 0x0)
		//auth response
		authResp := []byte{0x1, 0x2, 0x3, 0x4}
		data = append(data, authResp...)
		data = append(data, 0x0)
		//database
		dbName := "T"
		data = append(data, []byte(dbName)...)
		data = append(data, 0x0)

		ok, resp41, err := proto.analyseHandshakeResponse41(context.TODO(), data)
		convey.So(err, convey.ShouldBeNil)
		convey.So(ok, convey.ShouldBeTrue)

		convey.So(resp41.username, convey.ShouldEqual, username)
		convey.So(bytes.Equal(resp41.authResponse, authResp), convey.ShouldBeTrue)
		convey.So(resp41.database, convey.ShouldEqual, dbName)
	})

	convey.Convey("analyse 41 resp failed", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().Read(gomock.Any()).Return(new(Packet), nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, sv)

		type kase struct {
			data []byte
			res  bool
		}

		var cap uint32 = 0
		cap |= CLIENT_PROTOCOL_41 | CLIENT_CONNECT_WITH_DB | CLIENT_PLUGIN_AUTH
		var header [4]byte
		proto.io.WriteUint32(header[:], 0, cap)

		kases := []kase{
			{data: []byte{0}, res: false},
			{data: []byte{0, 0, 0, 0}, res: false},
			{data: append(header[:], []byte{
				0, 0, 0,
			}...), res: false},
			{data: append(header[:], []byte{
				0, 0, 0, 0,
			}...), res: false},
			{data: append(header[:], []byte{
				0, 0, 0, 0,
				0,
				0, 0, 0,
			}...), res: false},
			{data: append(header[:], []byte{
				0, 0, 0, 0,
				0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			}...), res: false},
			{data: append(header[:], []byte{
				0, 0, 0, 0,
				0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				'a', 'b', 'c',
			}...), res: false},
			{data: append(header[:], []byte{
				0, 0, 0, 0,
				0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				'a', 'b', 'c', 0,
				'd', 'e', 'f',
			}...), res: false},
			{data: append(header[:], []byte{
				0, 0, 0, 0,
				0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				'a', 'b', 'c', 0,
				'd', 'e', 'f', 0,
				'T',
			}...), res: false},
			{data: append(header[:], []byte{
				0, 0, 0, 0,
				0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				'a', 'b', 'c', 0,
				'd', 'e', 'f', 0,
				'T', 0,
				'm', 'y', 's',
			}...), res: false},
			{data: append(header[:], []byte{
				0, 0, 0, 0,
				0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				'a', 'b', 'c', 0,
				'd', 'e', 'f', 0,
				'T', 0,
				'm', 'y', 's', 'q', 'l', '_', 'n', 'a', 't', 'i', 'v', 'e', '_', 'p', 'a', 's', 's', 'w', 'o', 'r', 'x', 0,
			}...), res: true},
			{data: append(header[:], []byte{
				0, 0, 0, 0,
				0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
				'a', 'b', 'c', 0,
				'd', 'e', 'f', 0,
				'T', 0,
				'm', 'y', 's', 'q', 'l', '_', 'n', 'a', 't', 'i', 'v', 'e', '_', 'p', 'a', 's', 's', 'w', 'o', 'r', 'd', 0,
			}...), res: true},
		}

		for _, c := range kases {
			ok, _, _ := proto.analyseHandshakeResponse41(context.TODO(), c.data)
			convey.So(ok, convey.ShouldEqual, c.res)
		}
	})
}

func Test_handleHandshake(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("handleHandshake succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
		ioses.EXPECT().Ref().AnyTimes()
		ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
		var IO IOPackageImpl
		var SV = &config.FrontendParameters{}
		SV.SkipCheckUser = true
		mp := &MysqlProtocolImpl{SV: SV}
		mp.io = &IO
		mp.tcpConn = ioses
		payload := []byte{'a'}
		_, err := mp.HandleHandshake(ctx, payload)
		convey.So(err, convey.ShouldNotBeNil)

		payload = append(payload, []byte{'b', 'c'}...)
		_, err = mp.HandleHandshake(ctx, payload)
		convey.So(err, convey.ShouldNotBeNil)

		payload = append(payload, []byte{'c', 'd', 0}...)
		_, err = mp.HandleHandshake(ctx, payload)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_handleHandshake_Recover(t *testing.T) {
	f := fuzz.New()
	count := 10000
	maxLen := 0

	ctx := context.TODO()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ioses := mock_frontend.NewMockIOSession(ctrl)
	ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
	ioses.EXPECT().Ref().AnyTimes()
	ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
	convey.Convey("handleHandshake succ", t, func() {
		var IO IOPackageImpl
		var SV = &config.FrontendParameters{}
		SV.SkipCheckUser = true
		mp := &MysqlProtocolImpl{SV: SV}
		mp.io = &IO
		mp.tcpConn = ioses
		var payload []byte
		for i := 0; i < count; i++ {
			f.Fuzz(&payload)
			_, _ = mp.HandleHandshake(ctx, payload)
			maxLen = Max(maxLen, len(payload))
		}
		maxLen = 0
		var payload2 string
		for i := 0; i < count; i++ {
			f.Fuzz(&payload2)
			_, _ = mp.HandleHandshake(ctx, []byte(payload2))
			maxLen = Max(maxLen, len(payload2))
		}
	})
}

func TestMysqlProtocolImpl_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ioses := mock_frontend.NewMockIOSession(ctrl)
	ioses.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	ioses.EXPECT().Read(gomock.Any()).Return(new(Packet), nil).AnyTimes()
	ioses.EXPECT().RemoteAddress().Return("").AnyTimes()
	ioses.EXPECT().Ref().AnyTimes()
	ioses.EXPECT().Flush(gomock.Any()).AnyTimes()
	ioses.EXPECT().Disconnect().AnyTimes()
	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		t.Error(err)
	}

	proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
	proto.Quit()
	assert.Nil(t, proto.GetSalt())
	assert.Nil(t, proto.strconvBuffer)
	assert.Nil(t, proto.lenEncBuffer)
	assert.Nil(t, proto.binaryNullBuffer)
}
