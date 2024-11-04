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
	"net"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/golang/mock/gomock"
	fuzz "github.com/google/gofuzz"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	planPb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func registerConn(clientConn net.Conn) {
	mysqlDriver.RegisterDialContext("custom", func(ctx context.Context, addr string) (net.Conn, error) {
		return clientConn, nil
	})
}
func createInnerServer() *MOServer {

	rm := getRtMgr("")
	pu := getPu("")
	mo := &MOServer{
		addr:        "",
		uaddr:       pu.SV.UnixSocketAddress,
		rm:          rm,
		readTimeout: pu.SV.SessionTimeout.Duration,
		pu:          pu,
		handler:     rm.Handler,
	}
	mo.running = true
	return mo
}
func startInnerServer(conn net.Conn) {

	mo := createInnerServer()
	mo.handleConn(context.Background(), conn)
}

func TestMysqlClientProtocol_Handshake(t *testing.T) {
	//client connection method: mysql -h 127.0.0.1 -P 6001 --default-auth=mysql_native_password -uroot -p
	//client connect
	//ion method: mysql -h 127.0.0.1 -P 6001 -udump -p
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	registerConn(clientConn)
	var db *sql.DB
	var err error
	//before anything using the configuration
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	_, err = toml.DecodeFile("test/system_vars_config.toml", pu.SV)
	require.NoError(t, err)
	pu.SV.SkipCheckUser = true
	setSessionAlloc("", NewLeakCheckAllocator())
	setPu("", pu)

	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)

	// A mock autoincrcache manager.
	acim := &defines.AutoIncrCacheManager{}
	setAicm("", acim)
	rm, _ := NewRoutineManager(ctx, "")
	setRtMgr("", rm)

	wg := sync.WaitGroup{}
	wg.Add(1)

	//running server
	go func() {
		defer wg.Done()
		startInnerServer(serverConn)
	}()

	time.Sleep(time.Second * 2)
	db, err = openDbConn(t, 6001)
	require.NoError(t, err)

	time.Sleep(time.Millisecond * 10)
	closeDbConn(t, db)
	clientConn.Close()
	serverConn.Close()
	wg.Wait()
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

	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	registerConn(clientConn)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var dbConnPool, dbConnPool2 *sql.DB
	var conn1, conn2 *sql.Conn
	var err error
	var connIdRow *sql.Row

	//before anything using the configuration
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second * 10}).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, commitTS any, options ...any) (client.TxnOperator, error) {
		wp := newTestWorkspace()
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOp.EXPECT().GetWorkspace().Return(wp).AnyTimes()
		txnOp.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOp.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOp.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOp.EXPECT().EnterRunSql().Return().AnyTimes()
		txnOp.EXPECT().ExitRunSql().Return().AnyTimes()
		return txnOp, nil
	}).AnyTimes()
	pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
	require.NoError(t, err)
	pu.SV.SkipCheckUser = true
	setPu("", pu)
	setSessionAlloc("", NewLeakCheckAllocator())
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
			stmts, err = parsers.Parse(execCtx.reqCtx, dialect.MYSQL, execCtx.input.getSql(), 1)
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
	acim := &defines.AutoIncrCacheManager{}
	setAicm("", acim)
	temp, _ := NewRoutineManager(ctx, "")
	setRtMgr("", temp)

	wg := sync.WaitGroup{}
	wg.Add(1)
	mo := createInnerServer()

	//running server
	go func() {
		defer wg.Done()
		mo.handleConn(ctx, serverConn)
	}()

	dbConnPool, err = openDbConn(t, 6001)
	require.NoError(t, err)
	dbConnPool.SetConnMaxLifetime(time.Minute * 3)
	dbConnPool.SetMaxIdleConns(2)
	dbConnPool.SetMaxOpenConns(2)
	logutil.Infof("open conn1")
	time.Sleep(time.Second * 2)
	conn1, err = dbConnPool.Conn(ctx)
	require.NoError(t, err)
	logutil.Infof("open conn1 done")

	logutil.Infof("open conn2")
	time.Sleep(time.Second * 2)

	clientConn2, serverConn2 := net.Pipe()
	defer serverConn2.Close()
	defer clientConn2.Close()
	wg.Add(1)
	go func() {
		defer wg.Done()
		mo.handleConn(ctx, serverConn2)
	}()

	registerConn(clientConn2)
	dbConnPool2, err = openDbConn(t, 6001)
	require.NoError(t, err)
	dbConnPool2.SetConnMaxLifetime(time.Minute * 3)
	dbConnPool2.SetMaxIdleConns(2)
	dbConnPool2.SetMaxOpenConns(2)

	conn2, err = dbConnPool2.Conn(ctx)
	require.NoError(t, err)
	logutil.Infof("open conn2 done")

	logutil.Infof("get the connection id of conn1")
	//get the connection id of conn1
	var conn1Id uint64
	connIdRow = conn1.QueryRowContext(ctx, sql1)
	err = connIdRow.Scan(&conn1Id)
	require.NoError(t, err)
	logutil.Infof("get the connection id of conn1 done")

	logutil.Infof("get the connection id of conn2")
	//get the connection id of conn2
	var conn2Id uint64
	connIdRow = conn2.QueryRowContext(ctx, sql1)
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
		connIdRow = conn1.QueryRowContext(ctx, sql5)
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
	_, err = conn2.ExecContext(ctx, sql3)
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
		connIdRow = conn1.QueryRowContext(ctx, sql6)
		err := connIdRow.Scan(&resultId)
		require.NoError(t, err)
		logutil.Infof("conn1 sleep(30) 2 done")
	}()

	//sleep before cancel
	time.Sleep(time.Second * 2)

	logutil.Infof("conn2 kill conn1")
	//conn2 kills the connection 1
	sql2 = fmt.Sprintf("kill %d;", conn1Id)
	noResultSet[sql2] = true
	_, err = conn2.ExecContext(ctx, sql2)
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
	err = conn1.PingContext(ctx)
	require.Error(t, err)
	logutil.Infof("conn1 test itself after being killed done")

	//==============================

	logutil.Infof("conn2 kill itself")
	//conn2 kills itself
	sql4 = fmt.Sprintf("kill %d;", conn2Id)
	noResultSet[sql4] = true
	_, err = conn2.ExecContext(ctx, sql4)
	require.NoError(t, err)
	logutil.Infof("conn2 kill itself done")

	time.Sleep(time.Millisecond * 10)
	//close server

	logutil.Infof("close conn1,conn2")
	//close the connection
	//require.NoError(t, conn1.Close())
	//require.NoError(t, conn2.Close())
	require.NoError(t, dbConnPool.Close())
	require.NoError(t, dbConnPool2.Close())
	logutil.Infof("close conn1,conn2 done")

	serverConn.Close()
	clientConn.Close()
	serverConn2.Close()
	clientConn2.Close()
	wg.Wait()
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
//func TestMysqlClientProtocol_TlsHandshake(t *testing.T) {
//	//before anything using the configuration
//	clientConn, serverConn := net.Pipe()
//	defer serverConn.Close()
//	defer clientConn.Close()
//	registerConn(clientConn)
//	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
//	_, err := toml.DecodeFile("test/system_vars_config.toml", pu.SV)
//	if err != nil {
//		panic(err)
//	}
//	pu.SV.EnableTls = true
//	setGlobalPu(pu)
//	ctx := context.WithValue(context.TODO(), config.ParameterUnitKey, pu)
//	rm, err := NewRoutineManager(ctx)
//	assert.NoError(t, err)
//	setGlobalRtMgr(rm)
//
//	wg := sync.WaitGroup{}
//	wg.Add(1)
//
//	// //running server
//	go func() {
//		defer wg.Done()
//		startInnerServer(serverConn)
//	}()
//
//	// to := NewTimeout(1*time.Minute, false)
//	// for isClosed() && !to.isTimeout() {
//	// }
//
//	time.Sleep(time.Second * 2)
//	db := open_tls_db(t, 6001)
//	closeDbConn(t, db)
//
//	time.Sleep(time.Millisecond * 10)
//	clientConn.Close()
//	serverConn.Close()
//	wg.Wait()
//}

func makeMysqlTinyIntResultSet(unsigned bool) *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Tiny"
	if unsigned {
		name = name + "Uint"
	} else {
		name = name + "Int"
	}

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_TINY)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned {
		var cases = []uint8{0, 1, 254, 255}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	} else {
		var cases = []int8{-128, -127, 127}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlShortResultSet(unsigned bool) *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Short"
	if unsigned {
		name = name + "Uint"
	} else {
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_SHORT)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned {
		var cases = []uint16{0, 1, 254, 255, 65535}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	} else {
		var cases = []int16{-32768, 0, 32767}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlLongResultSet(unsigned bool) *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Long"
	if unsigned {
		name = name + "Uint"
	} else {
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_LONG)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned {
		var cases = []uint32{0, 4294967295}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	} else {
		var cases = []int32{-2147483648, 0, 2147483647}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlLongLongResultSet(unsigned bool) *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "LongLong"
	if unsigned {
		name = name + "Uint"
	} else {
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned {
		var cases = []uint64{0, 4294967295, 18446744073709551615}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	} else {
		var cases = []int64{-9223372036854775808, 0, 9223372036854775807}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlInt24ResultSet(unsigned bool) *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Int24"
	if unsigned {
		name = name + "Uint"
	} else {
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_INT24)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned {
		//[0,16777215]
		var cases = []uint32{0, 16777215, 4294967295}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	} else {
		//[-8388608,8388607]
		var cases = []int32{-2147483648, -8388608, 0, 8388607, 2147483647}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlYearResultSet(unsigned bool) *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Year"
	if unsigned {
		name = name + "Uint"
	} else {
		name = name + "Int"
	}
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_YEAR)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)

	rs.AddColumn(mysqlCol)
	if unsigned {
		var cases = []uint16{0, 1, 254, 255, 65535}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	} else {
		var cases = []int16{-32768, 0, 32767}
		for _, v := range cases {
			var data = make([]interface{}, 1)
			data[0] = v
			rs.AddRow(data)
		}
	}

	return rs
}

func makeMysqlVarcharResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Varchar"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases = []string{"abc", "abcde", "", "x-", "xx"}
	for _, v := range cases {
		var data = make([]interface{}, 1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlVarStringResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Varstring"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases = []string{"abc", "abcde", "", "x-", "xx"}
	for _, v := range cases {
		var data = make([]interface{}, 1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlStringResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "String"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_STRING)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases = []string{"abc", "abcde", "", "x-", "xx"}
	for _, v := range cases {
		var data = make([]interface{}, 1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlFloatResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Float"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_FLOAT)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases = []float32{math.MaxFloat32, math.SmallestNonzeroFloat32, -math.MaxFloat32, -math.SmallestNonzeroFloat32}
	for _, v := range cases {
		var data = make([]interface{}, 1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlDoubleResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Double"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_DOUBLE)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	var cases = []float64{math.MaxFloat64, math.SmallestNonzeroFloat64, -math.MaxFloat64, -math.SmallestNonzeroFloat64}
	for _, v := range cases {
		var data = make([]interface{}, 1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlDateResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Date"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_DATE)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	d1, _ := types.ParseDateCast("1997-01-01")
	d2, _ := types.ParseDateCast("2008-02-02")
	var cases = []types.Date{
		d1,
		d2,
	}
	for _, v := range cases {
		var data = make([]interface{}, 1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlTimeResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Time"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_TIME)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	t1, _ := types.ParseTime("110:21:15", 0)
	t2, _ := types.ParseTime("2018-04-28 10:21:15.123", 0)
	t3, _ := types.ParseTime("-112:12:12", 0)
	var cases = []types.Time{
		t1,
		t2,
		t3,
	}
	for _, v := range cases {
		var data = make([]interface{}, 1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

func makeMysqlDatetimeResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Date"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_DATETIME)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	d1, _ := types.ParseDatetime("2018-04-28 10:21:15", 0)
	d2, _ := types.ParseDatetime("2018-04-28 10:21:15.123", 0)
	d3, _ := types.ParseDatetime("2015-03-03 12:12:12", 0)
	var cases = []types.Datetime{
		d1,
		d2,
		d3,
	}
	for _, v := range cases {
		var data = make([]interface{}, 1)
		data[0] = v
		rs.AddRow(data)
	}

	return rs
}

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

func makeMoreThan16MBResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	var columnTypes = []defines.MysqlType{
		defines.MYSQL_TYPE_LONGLONG,
		defines.MYSQL_TYPE_DOUBLE,
		defines.MYSQL_TYPE_VARCHAR,
	}

	var names = []string{
		"Longlong",
		"Double",
		"Varchar",
	}

	var rowCase = []interface{}{int64(9223372036854775807), math.MaxFloat64, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}

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

	//the size of the total result set will be more than 16MB
	for i := 0; i < 40000; i++ {
		rs.AddRow(rowCase)
	}

	return rs
}

func make16MBRowResultSet() *MysqlResultSet {
	var rs = &MysqlResultSet{}

	name := "Varstring"

	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	rs.AddColumn(mysqlCol)

	/*
		How to test the max size of the data in one packet that the client can received ?
		Environment: Mysql Version 8.0.23
		1. shell: mysql --help | grep allowed-packet
			something like:
			"
			  --max-allowed-packet=#
			max-allowed-packet                16777216
			"
			so, we get:
				max-allowed-packet means : The maximum packet length to send to or receive from server.
				default value : 16777216 (16MB)
		2. shell execution: mysql -uroot -e "select repeat('a',16*1024*1024-4);" > 16MB-mysql.txt
			we get: ERROR 2020 (HY000) at line 1: Got packet bigger than 'max_allowed_packet' bytes
		3. shell execution: mysql -uroot -e "select repeat('a',16*1024*1024-5);" > 16MB-mysql.txt
			execution succeeded
		4. so, the max size of the data in one packet is (max-allowed-packet - 5).
		5. To change max-allowed-packet.
			shell execution: mysql max-allowed-packet=xxxxx ....
	*/

	//test in shell : mysql -h 127.0.0.1 -P 6001 -udump -p111 -e "16mbrow" > 16mbrow.txt
	//max data size : 16 * 1024 * 1024 - 5
	var stuff = make([]byte, 16*1024*1024-5)
	for i := range stuff {
		stuff[i] = 'a'
	}

	var rowCase = []interface{}{string(stuff)}
	for i := 0; i < 1; i++ {
		rs.AddRow(rowCase)
	}

	return rs
}

func TestMysqlResultSet(t *testing.T) {
	//client connection method: mysql -h 127.0.0.1 -P 6001 --default-auth=mysql_native_password -uroot -p
	//client connect
	//ion method: mysql -h 127.0.0.1 -P 6001 -udump -p

	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	registerConn(clientConn)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	var db *sql.DB

	var err error

	//before anything using the configuration
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	eng.EXPECT().Hints().Return(engine.Hints{CommitOrRollbackTimeout: time.Second * 10}).AnyTimes()

	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, commitTS any, options ...any) (client.TxnOperator, error) {
		wp := newTestWorkspace()
		txnOp := mock_frontend.NewMockTxnOperator(ctrl)
		txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
		txnOp.EXPECT().GetWorkspace().Return(wp).AnyTimes()
		txnOp.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
		txnOp.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
		txnOp.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
		txnOp.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
		txnOp.EXPECT().EnterRunSql().Return().AnyTimes()
		txnOp.EXPECT().ExitRunSql().Return().AnyTimes()
		return txnOp, nil
	}).AnyTimes()
	pu, err := getParameterUnit("test/system_vars_config.toml", eng, txnClient)
	require.NoError(t, err)
	pu.SV.SkipCheckUser = true
	setPu("", pu)

	noResultSet := make(map[string]bool)
	resultSet := make(map[string]*result)

	type kase struct {
		sql string
		mrs *MysqlResultSet
	}

	var kases []kase

	kases1 := []kase{
		{
			sql: "select tiny",
			mrs: makeMysqlTinyIntResultSet(false),
		},
		{
			sql: "select tinyu",
			mrs: makeMysqlTinyIntResultSet(true),
		},
		{
			sql: "select short",
			mrs: makeMysqlShortResultSet(false),
		},
		{
			sql: "select shortu",
			mrs: makeMysqlShortResultSet(true),
		},
		{
			sql: "select long",
			mrs: makeMysqlLongResultSet(false),
		},
		{
			sql: "select longu",
			mrs: makeMysqlLongResultSet(true),
		},
		{
			sql: "select longlong",
			mrs: makeMysqlLongLongResultSet(false),
		},
		{
			sql: "select longlongu",
			mrs: makeMysqlLongLongResultSet(true),
		},
		{
			sql: "select int24",
			mrs: makeMysqlInt24ResultSet(false),
		},
		{
			sql: "select int24u",
			mrs: makeMysqlInt24ResultSet(true),
		},

		{
			sql: "select varchar",
			mrs: makeMysqlVarcharResultSet(),
		},
		{
			sql: "select varstring",
			mrs: makeMysqlVarStringResultSet(),
		},
		{
			sql: "select string",
			mrs: makeMysqlStringResultSet(),
		},
		{
			sql: "select date",
			mrs: makeMysqlDateResultSet(),
		},
		{
			sql: "select time",
			mrs: makeMysqlTimeResultSet(),
		},
		{
			sql: "select datetime",
			mrs: makeMysqlDatetimeResultSet(),
		},
		{
			sql: "select 16mbrow",
			mrs: make16MBRowResultSet(),
		},
	}

	appendKases := func(kess []kase) {
		for _, ks := range kess {
			resultSet[ks.sql] = &result{
				gen: func(ses *Session) *MysqlResultSet {
					return ks.mrs
				},
			}
		}
	}

	kases2 := []kase{
		{
			sql: "select year",
			mrs: makeMysqlYearResultSet(false),
		},
		{
			sql: "select yearu",
			mrs: makeMysqlYearResultSet(true),
		},
		{
			sql: "select float",
			mrs: makeMysqlFloatResultSet(),
		},
		{
			sql: "select double",
			mrs: makeMysqlDoubleResultSet(),
		},
		{
			sql: "select 9columns",
			mrs: make9ColumnsResultSet(),
		},
		{
			sql: "select 16mb",
			mrs: makeMoreThan16MBResultSet(),
		},
	}

	appendKases(kases1)
	appendKases(kases2)

	kases = append(kases, kases1...)
	kases = append(kases, kases2...)

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
			stmts, err = parsers.Parse(execCtx.reqCtx, dialect.MYSQL, execCtx.input.getSql(), 1)
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
	acim := &defines.AutoIncrCacheManager{}
	setAicm("", acim)
	temp, _ := NewRoutineManager(ctx, "")
	setRtMgr("", temp)

	wg := sync.WaitGroup{}
	wg.Add(1)
	mo := createInnerServer()

	//running server
	go func() {
		defer wg.Done()
		mo.handleConn(ctx, serverConn)
	}()

	time.Sleep(time.Second * 2)
	db, err = openDbConn(t, 6001)
	require.NoError(t, err)

	for _, ks := range kases {
		do_query_resp_resultset(t, db, false, false, ks.sql, ks.mrs)
	}

	time.Sleep(time.Millisecond * 10)

	closeDbConn(t, db)
	serverConn.Close()
	clientConn.Close()
	wg.Wait()
}

//func open_tls_db(t *testing.T, port int) *sql.DB {
//	tlsName := "custom"
//	rootCertPool := x509.NewCertPool()
//	pem, err := os.ReadFile("test/ca.pem")
//	if err != nil {
//		require.NoError(t, err)
//	}
//	if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
//		log.Fatal("Failed to append PEM.")
//	}
//	clientCert := make([]tls.Certificate, 0, 1)
//	certs, err := tls.LoadX509KeyPair("test/client-cert2.pem", "test/client-key2.pem")
//	if err != nil {
//		require.NoError(t, err)
//	}
//	clientCert = append(clientCert, certs)
//	err = mysqlDriver.RegisterTLSConfig(tlsName, &tls.Config{
//		RootCAs:            rootCertPool,
//		Certificates:       clientCert,
//		MinVersion:         tls.VersionTLS12,
//		InsecureSkipVerify: true,
//	})
//	if err != nil {
//		require.NoError(t, err)
//	}
//
//	dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=5s&timeout=5s&writeTimeout=5s&tls=%s", port, tlsName)
//	db, err := sql.Open("mysql", dsn)
//	if err != nil {
//		require.NoError(t, err)
//	} else {
//		db.SetConnMaxLifetime(time.Minute * 3)
//		db.SetMaxOpenConns(1)
//		db.SetMaxIdleConns(1)
//		time.Sleep(time.Millisecond * 100)
//
//		// ping opens the connection
//		logutil.Info("start ping")
//		err = db.Ping()
//		if err != nil {
//			require.NoError(t, err)
//		}
//	}
//	return db
//}

func openDbConn(t *testing.T, port int) (db *sql.DB, err error) {
	dsn := fmt.Sprintf("dump:111@custom(127.0.0.1:%d)/?readTimeout=30s&timeout=30s&writeTimeout=30s", port)
	for i := 0; i < 3; i++ {
		db, err = tryConn(dsn)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if err != nil {
		panic(err)
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

func do_query_resp_resultset(t *testing.T, db *sql.DB, wantErr bool, skipResultsetCheck bool, query string, mrs *MysqlResultSet) {
	logutil.Infof("query: %v", query)
	rows, err := db.Query(query)
	if wantErr {
		require.Error(t, err)
		require.True(t, rows == nil)
		return
	}
	require.NoError(t, err)
	defer func() {
		err = rows.Close()
		require.NoError(t, err)
		err = rows.Err()
		require.NoError(t, err)
	}()

	//column check
	columns, err := rows.Columns()
	require.NoError(t, err)
	require.True(t, len(columns) == len(mrs.Columns))

	//colType, err := rows.ColumnTypes()
	//require.NoError(t, err)
	//for i, ct := range colType {
	//	fmt.Printf("column %d\n",i)
	//	fmt.Printf("name %v \n",ct.Name())
	//	l,o := ct.RowCount()
	//	fmt.Printf("length %v %v \n",l,o)
	//	p,s,o := ct.DecimalSize()
	//	fmt.Printf("decimalsize %v %v %v \n",p,s,o)
	//	fmt.Printf("scantype %v \n",ct.ScanType())
	//	n,o := ct.Nullable()
	//	fmt.Printf("nullable %v %v \n",n,o)
	//	fmt.Printf("databaseTypeName %s \n",ct.DatabaseTypeName())
	//}

	values := make([][]byte, len(columns))

	// rows.Scan wants '[]interface{}' as an argument, so we must copy the
	// references into such a slice
	// See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	scanArgs := make([]interface{}, len(columns))
	for i := uint64(0); i < mrs.GetColumnCount(); i++ {
		scanArgs[i] = &values[i]
	}

	rowIdx := uint64(0)
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		require.NoError(t, err)

		//fmt.Println(rowIdx)
		//fmt.Println(mrs.GetRow(rowIdx))
		//
		//for i := uint64(0); i < mrs.GetColumnCount(); i++ {
		//	arg := scanArgs[i]
		//	val := *(arg.(*[]byte))
		//	fmt.Printf("%v ",val)
		//}
		//fmt.Println()

		if !skipResultsetCheck {
			for i := uint64(0); i < mrs.GetColumnCount(); i++ {
				arg := scanArgs[i]
				val := *(arg.(*[]byte))

				column, err := mrs.GetColumn(context.TODO(), i)
				require.NoError(t, err)

				col, ok := column.(*MysqlColumn)
				require.True(t, ok)

				isNUll, err := mrs.ColumnIsNull(context.TODO(), rowIdx, i)
				require.NoError(t, err)

				if isNUll {
					require.True(t, val == nil)
				} else {
					var data []byte = nil
					switch col.ColumnType() {
					case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_YEAR:
						value, err := mrs.GetInt64(context.TODO(), rowIdx, i)
						require.NoError(t, err)
						if col.ColumnType() == defines.MYSQL_TYPE_YEAR {
							if value == 0 {
								data = append(data, []byte("0")...)
							} else {
								data = strconv.AppendInt(data, value, 10)
							}
						} else {
							data = strconv.AppendInt(data, value, 10)
						}

					case defines.MYSQL_TYPE_LONGLONG:
						if uint32(col.Flag())&defines.UNSIGNED_FLAG != 0 {
							value, err := mrs.GetUint64(context.TODO(), rowIdx, i)
							require.NoError(t, err)
							data = strconv.AppendUint(data, value, 10)
						} else {
							value, err := mrs.GetInt64(context.TODO(), rowIdx, i)
							require.NoError(t, err)
							data = strconv.AppendInt(data, value, 10)
						}
					case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING:
						value, err := mrs.GetString(context.TODO(), rowIdx, i)
						require.NoError(t, err)
						data = []byte(value)
					case defines.MYSQL_TYPE_FLOAT:
						value, err := mrs.GetFloat64(context.TODO(), rowIdx, i)
						require.NoError(t, err)
						data = strconv.AppendFloat(data, value, 'g', -1, 32)
					case defines.MYSQL_TYPE_DOUBLE:
						value, err := mrs.GetFloat64(context.TODO(), rowIdx, i)
						require.NoError(t, err)
						data = strconv.AppendFloat(data, value, 'g', -1, 64)
					case defines.MYSQL_TYPE_DATE:
						value, err := mrs.GetValue(context.TODO(), rowIdx, i)
						require.NoError(t, err)
						x := value.(types.Date).String()
						data = []byte(x)
					case defines.MYSQL_TYPE_TIME:
						value, err := mrs.GetValue(context.TODO(), rowIdx, i)
						require.NoError(t, err)
						x := value.(types.Time).String()
						data = []byte(x)
					case defines.MYSQL_TYPE_DATETIME:
						value, err := mrs.GetValue(context.TODO(), rowIdx, i)
						require.NoError(t, err)
						x := value.(types.Datetime).String()
						data = []byte(x)
					default:
						require.NoError(t, moerr.NewInternalErrorf(context.TODO(), "unsupported type %v", col.ColumnType()))
					}
					//check
					ret := reflect.DeepEqual(data, val)
					if !ret {
						fmt.Println(i)
						fmt.Println("want", data)
						fmt.Println("get", val)
					}
					require.True(t, ret)
				}
			}
		}

		rowIdx++
	}

	require.True(t, rowIdx == mrs.GetRowCount())

}

func writeExceptResult(clientConn net.Conn, packets []*Packet) {
	for _, packet := range packets {
		header := make([]byte, HeaderLengthOfTheProtocol)
		binary.LittleEndian.PutUint32(header, uint32(len(packet.Payload)))
		header[3] = uint8(packet.SequenceID)
		_, err := clientConn.Write(header)
		if err != nil {
			return
		}
		_, err = clientConn.Write(packet.Payload)
		if err != nil {
			return
		}
	}
}

func Test_writePackets(t *testing.T) {
	convey.Convey("writepackets 16MB succ", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}

		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		if err != nil {
			panic(err)
		}
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
		err = proto.writePackets(make([]byte, MaxPayloadSize))
		convey.So(err, convey.ShouldBeNil)
	})
	//convey.Convey("writepackets 16MB failed", t, func() {
	//
	//	sv, err := getSystemVariables("test/system_vars_config.toml")
	//	if err != nil {
	//		t.Error(err)
	//	}
	//
	//	pu := config.NewParameterUnit(sv, nil, nil, nil)
	//	pu.SV.SkipCheckUser = true
	//	setPu("",pu)
	//	ioses, err := NewIOSession(serverConn, pu)
	//	if err != nil {
	//		panic(err)
	//	}
	//	proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
	//	err = proto.writePackets(make([]byte, MaxPayloadSize))
	//	convey.So(err, convey.ShouldBeError)
	//})
	//
	//convey.Convey("writepackets 16MB failed 2", t, func() {
	//	sv, err := getSystemVariables("test/system_vars_config.toml")
	//	if err != nil {
	//		t.Error(err)
	//	}
	//
	//	pu := config.NewParameterUnit(sv, nil, nil, nil)
	//	pu.SV.SkipCheckUser = true
	//	setPu("",pu)
	//	ioses, err := NewIOSession(serverConn, pu)
	//	if err != nil {
	//		panic(err)
	//	}
	//	proto := NewMysqlClientProtocol(0, ioses, 1024, sv)
	//	err = proto.writePackets(make([]byte, MaxPayloadSize))
	//	convey.So(err, convey.ShouldBeError)
	//})
}

func Test_beginPacket(t *testing.T) {
	convey.Convey("openpacket succ", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

		proto.beginPacket()
		convey.So(err, convey.ShouldBeNil)
		headLen := proto.tcpConn.bufferLength
		convey.So(headLen, convey.ShouldEqual, HeaderLengthOfTheProtocol)
	})

	convey.Convey("fillpacket succ", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)
		// fill proto.ses
		ses := NewSession(context.TODO(), "", proto, nil)
		proto.ses = ses

		err = proto.append(make([]byte, MaxPayloadSize)...)
		convey.So(err, convey.ShouldBeNil)

		err = proto.finishedPacket()
		convey.So(err, convey.ShouldBeNil)

		proto.append(make([]byte, 1024)...)
	})

	convey.Convey("closepacket falied.", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, pu.SV)
		// fill proto.ses
		ses := NewSession(context.TODO(), "", proto, nil)
		proto.ses = ses

		proto.beginPacket()
		convey.So(err, convey.ShouldBeNil)

		proto.tcpConn.bufferLength = -1
		err = proto.finishedPacket()
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("append -- data checks", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

		proto.tcpConn.maxBytesToFlush = 1024 * 1024 * 1024
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

			proto.beginPacket()
			convey.So(err, convey.ShouldBeNil)

			proto.append(c.data...)

			err = proto.finishedPacket()
			convey.So(err, convey.ShouldBeNil)

			want := mysqlPack(c.data)

			convey.So(c.len, convey.ShouldEqual, len(want))

			res := proto.tcpConn.bufferLength
			convey.So(res, convey.ShouldEqual, len(want))

			proto.flush()
		}
	})

}

func TestSendPrepareResponse(t *testing.T) {
	ctx := context.TODO()
	convey.Convey("send Prepare response succ", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
		proto.SetSession(&Session{
			feSessionImpl: feSessionImpl{
				txnHandler: &TxnHandler{},
			},
		})

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
		}
		err = proto.SendPrepareResponse(ctx, prepareStmt)

		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("send Prepare response error", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

		st := tree.NewPrepareString("stmt1", "select ?, 1")
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
		}
		err = proto.SendPrepareResponse(ctx, prepareStmt)

		convey.So(err, convey.ShouldBeError)
	})
}

func FuzzParseExecuteData(f *testing.F) {
	ctx := context.TODO()
	proc := testutil.NewProcess()
	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		f.Error(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	pu.SV.SkipCheckUser = true
	setPu("", pu)
	ioses, err := NewIOSession(&testConn{}, pu, "")
	if err != nil {
		f.Error(err)
	}
	proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

	st := tree.NewPrepareString(tree.Identifier(getPrepareStmtName(1)), "select ?, 1")
	stmts, err := mysql.Parse(ctx, st.Sql, 1)
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
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

		ses := NewSession(ctx, "", proto, nil)
		proto.ses = ses

		res := make9ColumnsResultSet()

		err = proto.SendResultSetTextBatchRow(res, uint64(len(res.Data)))
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("send result set batch row speedup succ", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

		ses := NewSession(ctx, "", proto, nil)
		proto.ses = ses

		res := make9ColumnsResultSet()

		err = proto.WriteResultSetRow(res, uint64(len(res.Data)))
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("send result set succ", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

		ses := NewSession(ctx, "", proto, nil)
		proto.ses = ses

		res := make9ColumnsResultSet()

		err = proto.sendResultSet(ctx, res, int(COM_QUERY), 0, 0)
		convey.So(err, convey.ShouldBeNil)

		err = proto.SendResultSetTextRow(res, 0)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("send binary result set succ", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

		ses := NewSession(ctx, "", proto, nil)
		ses.cmd = COM_STMT_EXECUTE
		proto.ses = ses

		res := make9ColumnsResultSet()

		err = proto.WriteResultSetRow(res, 0)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_send_packet(t *testing.T) {
	convey.Convey("send err packet", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

		err = proto.sendErrPacket(1, "fake state", "fake error")
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("send eof packet", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

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
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

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
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

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
	tConn := &testConn{}
	pkts := []*Packet{{Length: 5, Payload: []byte("hello"), SequenceID: 1},
		{Length: 5, Payload: []byte("world"), SequenceID: 2},
		{Length: 0, Payload: []byte(""), SequenceID: 3}}
	writeExceptResult(tConn, pkts)
	convey.Convey("analyse 41 resp succ", t, func() {
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(tConn, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

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
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(tConn, pu, "")
		convey.ShouldBeNil(err)
		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)

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
		sv, err := getSystemVariables("test/system_vars_config.toml")
		if err != nil {
			t.Error(err)
		}
		pu := config.NewParameterUnit(sv, nil, nil, nil)
		pu.SV.SkipCheckUser = true
		setPu("", pu)
		ioses, err := NewIOSession(&testConn{}, pu, "")
		convey.ShouldBeNil(err)
		var IO IOPackageImpl
		var SV = &config.FrontendParameters{}
		SV.SkipCheckUser = true
		mp := &MysqlProtocolImpl{SV: SV}
		mp.io = &IO
		mp.tcpConn = ioses
		payload := []byte{'a'}
		_, err = mp.HandleHandshake(ctx, payload)
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
	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		t.Error(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	pu.SV.SkipCheckUser = true
	setPu("", pu)
	ioses, err := NewIOSession(&testConn{}, pu, "")
	if err != nil {
		t.Error(err)
	}
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
	sv, err := getSystemVariables("test/system_vars_config.toml")
	if err != nil {
		t.Error(err)
	}
	pu := config.NewParameterUnit(sv, nil, nil, nil)
	pu.SV.SkipCheckUser = true
	setPu("", pu)
	ioses, err := NewIOSession(&testConn{}, pu, "")
	convey.ShouldBeNil(err)
	proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
	proto.Close()
	assert.Nil(t, proto.GetSalt())
	assert.Nil(t, proto.strconvBuffer)
	assert.Nil(t, proto.lenEncBuffer)
	assert.Nil(t, proto.binaryNullBuffer)
}

var _ MysqlRrWr = &testMysqlWriter{}

// testMysqlWriter works for the background transaction that does not use the network protocol.
type testMysqlWriter struct {
	username string
	database string
	ioses    *Conn
	mod      int
}

func (fp *testMysqlWriter) FreeLoadLocal() {

}

func (fp *testMysqlWriter) GetStr(PropertyID) string {
	return ""
}
func (fp *testMysqlWriter) SetStr(PropertyID, string) {}
func (fp *testMysqlWriter) SetU32(PropertyID, uint32) {}
func (fp *testMysqlWriter) GetU32(PropertyID) uint32 {
	return 0
}
func (fp *testMysqlWriter) SetU8(PropertyID, uint8) {}
func (fp *testMysqlWriter) GetU8(PropertyID) uint8 {
	return 0
}
func (fp *testMysqlWriter) SetBool(PropertyID, bool) {}
func (fp *testMysqlWriter) GetBool(PropertyID) bool {
	return false
}

func (fp *testMysqlWriter) Write(ctx *ExecCtx, crs *perfcounter.CounterSet, batch *batch.Batch) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteHandshake() error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteOK(affectedRows, lastInsertId uint64, status, warnings uint16, message string) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteOKtWithEOF(affectedRows, lastInsertId uint64, status, warnings uint16, message string) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteEOF(warnings, status uint16) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteEOFIF(warnings uint16, status uint16) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteEOFIFAndNoFlush(warnings uint16, status uint16) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteEOFOrOK(warnings uint16, status uint16) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteERR(errorCode uint16, sqlState, errorMessage string) error {
	if fp.mod == 1 {
		return moerr.NewInternalErrorNoCtx("writeErr returns err")
	}
	return nil
}

func (fp *testMysqlWriter) WriteLengthEncodedNumber(u uint64) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteColumnDef(ctx context.Context, column Column, i int) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteColumnDefBytes(payload []byte) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteRow() error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteTextRow() error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteBinaryRow() error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WriteResponse(ctx context.Context, response *Response) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) WritePrepareResponse(ctx context.Context, stmt *PrepareStmt) error {
	//TODO implement me
	panic("implement me")
}

func (fp *testMysqlWriter) Read() ([]byte, error) {
	return fp.ioses.Read()
}

func (fp *testMysqlWriter) ReadLoadLocalPacket() ([]byte, error) {
	return fp.ioses.ReadLoadLocalPacket()
}

func (fp *testMysqlWriter) Free(buf []byte) {
	fp.ioses.allocator.Free(buf)
}

func (fp *testMysqlWriter) UpdateCtx(ctx context.Context) {

}

func (fp *testMysqlWriter) GetCapability() uint32 {
	return DefaultCapability
}

func (fp *testMysqlWriter) SetCapability(uint32) {

}

func (fp *testMysqlWriter) IsTlsEstablished() bool {
	return true
}

func (fp *testMysqlWriter) SetTlsEstablished() {

}

func (fp *testMysqlWriter) HandleHandshake(ctx context.Context, payload []byte) (bool, error) {
	return false, nil
}

func (fp *testMysqlWriter) Authenticate(ctx context.Context) error {
	return nil
}

func (fp *testMysqlWriter) GetSequenceId() uint8 {
	return 0
}

func (fp *testMysqlWriter) SetSequenceID(value uint8) {
}

func (fp *testMysqlWriter) ParseSendLongData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	return nil
}

func (fp *testMysqlWriter) ParseExecuteData(ctx context.Context, proc *process.Process, stmt *PrepareStmt, data []byte, pos int) error {
	return nil
}

func (fp *testMysqlWriter) WriteResultSetRow(mrs *MysqlResultSet, cnt uint64) error {
	return nil
}

func (fp *testMysqlWriter) ResetStatistics() {}
func (fp *testMysqlWriter) Reset(_ *Session) {}

func (fp *testMysqlWriter) CalculateOutTrafficBytes(reset bool) (int64, int64) { return 0, 0 }

func (fp *testMysqlWriter) IsEstablished() bool {
	return true
}

func (fp *testMysqlWriter) SetEstablished() {}

func (fp *testMysqlWriter) ConnectionID() uint32 {
	return fakeConnectionID
}

func (fp *testMysqlWriter) Peer() string {
	return "0.0.0.0:0"
}

func (fp *testMysqlWriter) GetDatabaseName() string {
	return fp.database
}

func (fp *testMysqlWriter) SetDatabaseName(s string) {
	fp.database = s
}

func (fp *testMysqlWriter) GetUserName() string {
	return fp.username
}

func (fp *testMysqlWriter) SetUserName(s string) {
	fp.username = s
}

func (fp *testMysqlWriter) Close() {}

func (fp *testMysqlWriter) WriteLocalInfileRequest(filename string) error {
	return nil
}

func (fp *testMysqlWriter) Flush() error {
	return nil
}

func (fp *testMysqlWriter) MakeColumnDefData(ctx context.Context, columns []*planPb.ColDef) ([][]byte, error) {
	return nil, nil
}
