package frontend

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe"
	"testing"

	"github.com/fagongzi/goetty/buf"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
)

//func TestMysqlCmdExecutor(t *testing.T) {
//	fs, err := NewFrontendStub()
//	require.NoError(t, err)
//
//	err = StartFrontendStub(fs)
//	require.NoError(t, err)
//
//	defer func(fs *FrontendStub) {
//		err := CloseFrontendStub(fs)
//		if err != nil {
//			require.NoError(t,err)
//		}
//	}(fs)
//
//	db := open_db(t,6002)
//	time.Sleep(2 * time.Second)
//
//	do_query_resp_resultset(t, db, false, false, "SELECT @@max_allowed_packet", MakeResultSet_select_max_allowed_packet())
//	do_query_resp_resultset(t, db, false, false, "SELECT DATABASE()", MakeResultSet_SELECT_DATABASE("DATABASE()", "NULL"))
//
//	do_query_resp_resultset(t, db, false, false, "show databases", MakeResultSet_ShowDatabases("test", "Database", "test"))
//	do_query_resp_resultset(t, db, true, false, "show databasess", nil)
//
//	do_query_resp_states(t, db, false, "use test")
//	do_query_resp_states(t, db, true, "use test2")
//
//	//create database T
//	do_query_resp_states(t,db,false,"create database T")
//	do_query_resp_states(t,db,true,"create database T")
//
//	//use T
//	do_query_resp_states(t, db, false, "use T")
//
//	tableFormat := "create table %s (a tinyint,b tinyint unsigned," +
//		"c smallint, d smallint unsigned, " +
//		"e int, f int unsigned," +
//		"g bigint,h bigint unsigned," +
//		"i float, j double ," +
//		"k char(100),l varchar(100)" +
//		")"
//
//	//create table A
//	tableA := fmt.Sprintf(tableFormat,"A")
//	do_query_resp_states(t,db,false,tableA)
//	do_query_resp_resultset(t, db, false, false, "select * from A", MakeResultSet_SELECT_1())
//
//	//insert into A values ...
//	values1,mrs1 := MakeValues_insert_1(100)
//	insertFormat := "insert into %s values"
//	insertA := fmt.Sprintf(insertFormat,"A") + values1
//	do_query_resp_states(t,db,false,insertA)
//	do_query_resp_resultset(t, db, false, false, "select * from A", mrs1)
//
//	//create table B
//	tableB := fmt.Sprintf(tableFormat,"B")
//	do_query_resp_states(t,db,false,tableB)
//	do_query_resp_resultset(t, db, false, false, "select * from B", MakeResultSet_SELECT_1())
//
//	//insert into B values ...
//	values2,mrs2 := MakeValues_insert_1_all_NULL(100)
//	insertB := fmt.Sprintf(insertFormat,"B") + values2
//	do_query_resp_states(t,db,false,insertB)
//	do_query_resp_resultset(t, db, false, false, "select * from B", mrs2)
//
//	//create table C
//	tableC := fmt.Sprintf(tableFormat,"C")
//	do_query_resp_states(t,db,false,tableC)
//
//	//insert into C values ...
//	values3,mrs3,_ := MakeValues_insert_1_partial_null(0,100)
//	insertC := fmt.Sprintf(insertFormat,"C") + values3
//	do_query_resp_states(t,db,false,insertC)
//	do_query_resp_resultset(t, db, false, false, "select * from C", mrs3)
//
//	_,mrs4,_ := MakeValues_insert_1_partial_null(0,100)
//	do_query_resp_resultset(t, db, false, false, "select * from C order by f", mrs4)
//
//	//create table D
//	tableD := fmt.Sprintf(tableFormat,"D")
//	do_query_resp_states(t,db,false,tableD)
//
//	//insert into D values ...
//	_,mrs5,loaddata5 := MakeValues_insert_1_partial_null(0,109)
//	loadfile1 := "test/loadcase2"
//	err = ioutil.WriteFile(loadfile1, []byte(loaddata5),0777)
//	require.NoError(t, err)
//
//	loadFormat := "load data " +
//		"infile '%s' " +
//		"ignore " +
//		"INTO TABLE T.%s " +
//		"FIELDS TERMINATED BY '%c' "
//	loadD := fmt.Sprintf(loadFormat,loadfile1,"D",',')
//	fmt.Println(loadD)
//	do_query_resp_states(t,db,false,loadD)
//
//	//memEngine does ensure sort
//	do_query_resp_resultset(t, db, false, true, "select * from D", mrs5)
//
//	//loadFormat2 := "load data " +
//	//	"infile '%s' " +
//	//	"ignore " +
//	//	"INTO TABLE T.%s " +
//	//	"FIELDS TERMINATED BY '%c' " +
//	//	"(a,b,c,d,e,f,g,h,i,j,k,l)"
//	//do_query_resp_states(t,db,false,"drop table D")
//	//do_query_resp_states(t,db,false,tableD)
//	//
//	//loadD2 := fmt.Sprintf(loadFormat2,loadfile1,"D",',')
//	//fmt.Println(loadD2)
//	//do_query_resp_states(t,db,false,loadD2)
//	//
//	//do_query_resp_resultset(t, db, false, true, "select * from D", mrs5)
//
//	//time.Sleep(100 * time.Millisecond)
//	//close_db(t,db)
//}

//func NewColumnDef_string(name string, colType uint8) *MysqlColumn {
//	mysqlCol := new(MysqlColumn)
//	mysqlCol.SetName(name)
//	mysqlCol.SetOrgName(name + "OrgName")
//	mysqlCol.SetColumnType(colType)
//	mysqlCol.SetSchema(name + "Schema")
//	mysqlCol.SetTable(name + "Table")
//	mysqlCol.SetOrgTable(name + "Table")
//	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//
//	return mysqlCol
//}
//
//func NewColumnDef_digital(name string,colType uint8,unsigned bool) *MysqlColumn{
//	mysqlCol := new(MysqlColumn)
//	mysqlCol.SetName(name)
//	mysqlCol.SetOrgName(name + "OrgName")
//	mysqlCol.SetColumnType(colType)
//	mysqlCol.SetSchema(name + "Schema")
//	mysqlCol.SetTable(name + "Table")
//	mysqlCol.SetOrgTable(name + "Table")
//	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
//	mysqlCol.SetSigned(!unsigned)
//	return mysqlCol
//}
//
//func MakeResultSet_ShowDatabases(db string,column string,data ...interface{}) *MysqlResultSet {
//	mrs := &MysqlResultSet{}
//	mrs.AddColumn(NewColumnDef_string(column,defines.MYSQL_TYPE_VARCHAR))
//	for _, da := range data {
//		mrs.AddRow([]interface{}{da})
//	}
//	return mrs
//}
//
//func MakeResultSet_UseDatabase(db string) *MysqlResultSet {
//	return &MysqlResultSet{}
//}
//
//func MakeResultSet_select_max_allowed_packet() *MysqlResultSet {
//	mrs := &MysqlResultSet{}
//	mrs.AddColumn(NewColumnDef_digital("@@max_allowed_packet",defines.MYSQL_TYPE_LONG,false))
//	mrs.AddRow([]interface{}{int32(16777216)})
//	return mrs
//}
//
//func MakeResultSet_SELECT_DATABASE(column,value string) *MysqlResultSet{
//	mrs := &MysqlResultSet{}
//	mrs.AddColumn(NewColumnDef_string(column,defines.MYSQL_TYPE_VARCHAR))
//	mrs.AddRow([]interface{}{value})
//	return mrs
//}
//
//func MakeResultSet_SELECT_1() *MysqlResultSet{
//	mrs := &MysqlResultSet{}
//	mrs.AddColumn(NewColumnDef_digital("a",defines.MYSQL_TYPE_TINY,false))
//	mrs.AddColumn(NewColumnDef_digital("b",defines.MYSQL_TYPE_TINY,true))
//	mrs.AddColumn(NewColumnDef_digital("c",defines.MYSQL_TYPE_SHORT,false))
//	mrs.AddColumn(NewColumnDef_digital("d",defines.MYSQL_TYPE_SHORT,true))
//	mrs.AddColumn(NewColumnDef_digital("e",defines.MYSQL_TYPE_LONG,false))
//	mrs.AddColumn(NewColumnDef_digital("f",defines.MYSQL_TYPE_LONG,true))
//	mrs.AddColumn(NewColumnDef_digital("g",defines.MYSQL_TYPE_LONGLONG,false))
//	mrs.AddColumn(NewColumnDef_digital("h",defines.MYSQL_TYPE_LONGLONG,true))
//	mrs.AddColumn(NewColumnDef_digital("i",defines.MYSQL_TYPE_FLOAT,false))
//	mrs.AddColumn(NewColumnDef_digital("j",defines.MYSQL_TYPE_DOUBLE,false))
//	mrs.AddColumn(NewColumnDef_string("l",defines.MYSQL_TYPE_STRING))
//	mrs.AddColumn(NewColumnDef_string("l",defines.MYSQL_TYPE_VARCHAR))
//	return mrs
//}
//
//func MakeValues_insert_1(cnt int) (string,*MysqlResultSet){
//	mrs := MakeResultSet_SELECT_1()
//	row := []interface{}{
//		int8(-128),uint8(255),
//		int16(-32768),uint16(65535),
//		int32(-2147483648),uint32(4294967295),
//		int64(-9223372036854775808),uint64(18446744073709551615),
//		float32(-3.402823466E+38),float64(1.7976931348623157E+308),
//		"aaaaa","bbbbbbb",
//	}
//	s := ""
//	for i := 0; i < cnt; i++ {
//		if i != 0 {
//			s += ","
//		}
//		s += "("
//
//		s += "-128,255," +
//			"-32768,65535," +
//			"-2147483648,4294967295," +
//			"-9223372036854775808,18446744073709551615," +
//			"-3.402823466E+38,1.7976931348623157E+308," +
//			"\"aaaaa\",\"bbbbbbb\""
//
//		s += ")"
//
//		mrs.AddRow(row)
//	}
//	s += ";"
//
//	return s,mrs
//}
//
//func MakeValues_insert_1_all_NULL(cnt int) (string,*MysqlResultSet){
//	mrs := MakeResultSet_SELECT_1()
//	row := []interface{}{
//		nil,nil,
//		nil,nil,
//		nil,nil,
//		nil,nil,
//		nil,nil,
//		nil,nil,
//	}
//	s := ""
//	for i := 0; i < cnt; i++ {
//		if i != 0 {
//			s += ","
//		}
//		s += "("
//
//		s += "NULL,NULL," +
//			"NULL,NULL," +
//			"NULL,NULL," +
//			"NULL,NULL," +
//			"NULL,NULL," +
//			"NULL,NULL"
//
//		s += ")"
//
//		mrs.AddRow(row)
//	}
//	s += ";"
//
//	return s,mrs
//}
//
//func MakeValues_insert_1_partial_null(start int,cnt int) (string,*MysqlResultSet,string){
//	mrs := MakeResultSet_SELECT_1()
//	//row1 := []interface{}{
//	//	nil,nil,
//	//	nil,nil,
//	//	nil,nil,
//	//	nil,nil,
//	//	nil,nil,
//	//	nil,nil,
//	//}
//	//
//	//row2 := []interface{}{
//	//	int8(-128),uint8(255),
//	//	int16(-32768),uint16(65535),
//	//	int32(-2147483648),uint32(4294967295),
//	//	int64(-9223372036854775808),uint64(18446744073709551615),
//	//	float32(-3.402823466E+38),float64(1.7976931348623157E+308),
//	//	"aaaaa","bbbbbbb",
//	//}
//
//	s1 := "NULL,NULL," +
//		"NULL,NULL," +
//		"NULL,%d," +
//		"NULL,NULL," +
//		"NULL,NULL," +
//		"NULL,NULL"
//
//	s2 := "-128,255," +
//		"-32768,65535," +
//		"-2147483648,%d," +
//		"-9223372036854775808,18446744073709551615," +
//		"-3.402823466E+38,1.7976931348623157E+308," +
//		"\"aaaaa\",\"bbbbbbb\""
//
//	loadDataFormat := ",," +
//		",," +
//		",%d," +
//		",," +
//		",," +
//		","
//
//	s := ""
//	loadData := ""
//
//	for i := 0; i < (start + cnt); i++ {
//		if i < start {
//			continue
//		}
//		if i != 0 {
//			s += ","
//		}
//		s += "("
//
//		if i % 2 == 0 {
//			s += fmt.Sprintf(s1,i)
//		}else{
//			s += fmt.Sprintf(s2,i)
//		}
//
//		s += ")"
//
//		if i % 2 == 0 {
//			row3 := []interface{}{
//				nil,nil,
//				nil,nil,
//				nil,uint32(i),
//				nil,nil,
//				nil,nil,
//				nil,nil,
//			}
//			mrs.AddRow(row3)
//		}else{
//			row4 := []interface{}{
//				int8(-128),uint8(255),
//				int16(-32768),uint16(65535),
//				int32(-2147483648),uint32(i),
//				int64(-9223372036854775808),uint64(18446744073709551615),
//				float32(-3.402823466E+38),float64(1.7976931348623157E+308),
//				"aaaaa","bbbbbbb",
//			}
//			mrs.AddRow(row4)
//		}
//
//		if i % 2 == 0 {
//			loadData += fmt.Sprintln(fmt.Sprintf(loadDataFormat,uint32(i)))
//		} else{
//			loadData += fmt.Sprintln(fmt.Sprintf(s2,uint32(i)))
//		}
//
//	}
//	s += ";"
//
//	return s,mrs,loadData
//}

func Test_mce(t *testing.T) {
	convey.Convey("boot mce succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), nil).Return(nil, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

		use_t := mock_frontend.NewMockComputationWrapper(ctrl)
		stmts, err := parsers.Parse(dialect.MYSQL, "use T")
		if err != nil {
			t.Error(err)
		}
		use_t.EXPECT().GetAst().Return(stmts[0]).AnyTimes()

		runner := mock_frontend.NewMockComputationRunner(ctrl)
		runner.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()

		create_1 := mock_frontend.NewMockComputationWrapper(ctrl)
		stmts, err = parsers.Parse(dialect.MYSQL, "create table A(a varchar(100),b int,c float)")
		if err != nil {
			t.Error(err)
		}
		create_1.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		create_1.EXPECT().SetDatabaseName(gomock.Any()).Return(nil).AnyTimes()
		create_1.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
		create_1.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()
		create_1.EXPECT().GetAffectedRows().Return(uint64(0)).AnyTimes()

		select_1 := mock_frontend.NewMockComputationWrapper(ctrl)
		stmts, err = parsers.Parse(dialect.MYSQL, "select a,b,c from A")
		if err != nil {
			t.Error(err)
		}
		select_1.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
		select_1.EXPECT().SetDatabaseName(gomock.Any()).Return(nil).AnyTimes()
		select_1.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
		select_1.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()

		cola := &MysqlColumn{}
		cola.SetName("a")
		cola.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
		colb := &MysqlColumn{}
		colb.SetName("b")
		colb.SetColumnType(defines.MYSQL_TYPE_LONG)
		colc := &MysqlColumn{}
		colc.SetName("c")
		colc.SetColumnType(defines.MYSQL_TYPE_FLOAT)
		cols := []interface{}{
			cola,
			colb,
			colc,
		}
		select_1.EXPECT().GetColumns().Return(cols, nil).AnyTimes()

		cws := []ComputationWrapper{
			use_t,
			create_1,
			select_1,
		}

		var self_handle_sql = []string{
			"SELECT DATABASE()",
			"SELECT @@max_allowed_packet",
			"SELECT @@version_comment",
			"SELECT @@tx_isolation",
			"set @@tx_isolation=`READ-COMMITTED`",
			//TODO:fix it after parser is ready
			//"set a = b",
			"drop database T",
		}

		sql1Col := &MysqlColumn{}
		sql1Col.SetName("DATABASE()")
		sql1Col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

		sql2Col := &MysqlColumn{}
		sql2Col.SetName("@@max_allowed_packet")
		sql2Col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)

		sql3Col := &MysqlColumn{}
		sql3Col.SetName("@@version_comment")
		sql3Col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

		sql4Col := &MysqlColumn{}
		sql4Col.SetName("@@tx_isolation")
		sql4Col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)

		var self_handle_sql_columns = [][]interface{}{
			{
				sql1Col,
			},
			{
				sql2Col,
			},
			{
				sql3Col,
			},
			{
				sql4Col,
			},
			{},
			{},
		}

		for i := 0; i < len(self_handle_sql); i++ {
			select_2 := mock_frontend.NewMockComputationWrapper(ctrl)
			stmts, err = parsers.Parse(dialect.MYSQL, self_handle_sql[i])
			if err != nil {
				t.Error(err)
			}
			select_2.EXPECT().GetAst().Return(stmts[0]).AnyTimes()
			select_2.EXPECT().SetDatabaseName(gomock.Any()).Return(nil).AnyTimes()
			select_2.EXPECT().Compile(gomock.Any(), gomock.Any()).Return(runner, nil).AnyTimes()
			select_2.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()
			select_2.EXPECT().GetAffectedRows().Return(uint64(0)).AnyTimes()
			select_2.EXPECT().GetColumns().Return(self_handle_sql_columns[i], nil).AnyTimes()
			cws = append(cws, select_2)
		}

		stubs := gostub.StubFunc(&GetComputationWrapper, cws, nil)
		defer stubs.Reset()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		epochgc := getPCI()

		guestMmu := guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu)

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		ses := NewSession(proto, epochgc, guestMmu, pu.Mempool, pu, &gSys)

		mce := NewMysqlCmdExecutor()

		mce.PrepareSessionBeforeExecRequest(ses)

		req := &Request{
			cmd:  int(COM_QUERY),
			data: []byte("test anywhere"),
		}

		mce.ses.Pu.SV.SetRejectWhenHeartbeatFromPDLeaderIsTimeout(true)
		resp, err := mce.ExecRequest(req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldBeNil)

		req = &Request{
			cmd:  int(COM_QUERY),
			data: []byte("kill"),
		}
		resp, err = mce.ExecRequest(req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldNotBeNil)

		req = &Request{
			cmd:  int(COM_QUERY),
			data: []byte("kill 10"),
		}
		mce.SetRoutineManager(&RoutineManager{})
		resp, err = mce.ExecRequest(req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldNotBeNil)

		req = &Request{
			cmd:  int(COM_INIT_DB),
			data: []byte("test anywhere"),
		}

		resp, err = mce.ExecRequest(req)
		convey.So(err, convey.ShouldBeNil)
		//COM_INIT_DB replaced by changeDB()
		//convey.So(resp.category, convey.ShouldEqual, OkResponse)

		req = &Request{
			cmd:  int(COM_PING),
			data: []byte("test anywhere"),
		}

		resp, err = mce.ExecRequest(req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.category, convey.ShouldEqual, OkResponse)

		req = &Request{
			cmd:  int(COM_QUIT),
			data: []byte("test anywhere"),
		}

		resp, err = mce.ExecRequest(req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldBeNil)

	})
}

func Test_mce_selfhandle(t *testing.T) {
	convey.Convey("handleChangeDB", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)

		cnt := 0
		eng.EXPECT().Database(gomock.Any(), nil).DoAndReturn(
			func(db string, dump interface{}) (engine.Database, error) {
				cnt++
				if cnt == 1 {
					return nil, nil
				}
				return nil, fmt.Errorf("fake error")
			},
		).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		epochgc := getPCI()

		guestMmu := guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu)

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		ses := NewSession(proto, epochgc, guestMmu, pu.Mempool, pu, &gSys)

		mce := NewMysqlCmdExecutor()
		mce.PrepareSessionBeforeExecRequest(ses)
		err = mce.handleChangeDB("T")
		convey.So(err, convey.ShouldBeNil)
		convey.So(ses.protocol.GetDatabaseName(), convey.ShouldEqual, "T")

		err = mce.handleChangeDB("T")
		convey.So(err, convey.ShouldBeError)
	})

	convey.Convey("handleSelectDatabase/handleMaxAllowedPacket/handleVersionComment/handleCmdFieldList/handleSetVar", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)

		eng.EXPECT().Database(gomock.Any(), nil).Return(nil, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		epochgc := getPCI()

		guestMmu := guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu)

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		ses := NewSession(proto, epochgc, guestMmu, pu.Mempool, pu, &gSys)
		ses.Mrs = &MysqlResultSet{}

		mce := NewMysqlCmdExecutor()
		mce.PrepareSessionBeforeExecRequest(ses)

		ses.Mrs = &MysqlResultSet{}
		st1, err := parsers.ParseOne(dialect.MYSQL, "select @@max_allowed_packet")
		convey.So(err, convey.ShouldBeNil)
		sv1 := st1.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.VarExpr)
		err = mce.handleSelectVariables(sv1)
		convey.So(err, convey.ShouldBeNil)

		ses.Mrs = &MysqlResultSet{}
		st2, err := parsers.ParseOne(dialect.MYSQL, "select @@version_comment")
		convey.So(err, convey.ShouldBeNil)
		sv2 := st2.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.VarExpr)
		err = mce.handleSelectVariables(sv2)
		convey.So(err, convey.ShouldBeNil)

		ses.Mrs = &MysqlResultSet{}
		err = mce.handleCmdFieldList("A")
		convey.So(err, convey.ShouldBeError)

		ses.Mrs = &MysqlResultSet{}
		ses.protocol.SetDatabaseName("T")
		mce.tableInfos = make(map[string][]ColumnInfo)
		mce.tableInfos["A"] = []ColumnInfo{&aoeColumnInfo{
			info: aoe.ColumnInfo{
				Name: "a",
				Type: types.Type{Oid: types.T_varchar},
			},
		}}

		err = mce.handleCmdFieldList("A")
		convey.So(err, convey.ShouldNotBeNil)

		mce.db = ses.protocol.GetDatabaseName()
		err = mce.handleCmdFieldList("A")
		convey.So(err, convey.ShouldBeNil)

		set := "set @@tx_isolation=`READ-COMMITTED`"
		setVar, err := parsers.ParseOne(dialect.MYSQL, set)
		convey.So(err, convey.ShouldBeNil)

		err = mce.handleSetVar(setVar.(*tree.SetVar))
		convey.So(err, convey.ShouldBeNil)

		req := &Request{
			cmd:  int(COM_FIELD_LIST),
			data: []byte{'A', 0},
		}

		resp, err := mce.ExecRequest(req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp, convey.ShouldBeNil)
	})
}

func Test_getDataFromPipeline(t *testing.T) {
	convey.Convey("getDataFromPipeline", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)

		eng.EXPECT().Database(gomock.Any(), nil).Return(nil, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)

		epochgc := getPCI()

		guestMmu := guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu)

		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		ses := NewSession(proto, epochgc, guestMmu, pu.Mempool, pu, &gSys)
		ses.Mrs = &MysqlResultSet{}

		// mce := NewMysqlCmdExecutor()
		// mce.PrepareSessionBeforeExecRequest(ses)

		genBatch := func() *batch.Batch {
			return allocTestBatch(
				[]string{
					"a", "b", "c", "d", "e", "f",
					"g", "h", "i", "j", "k", "l",
					"m", "n",
				},
				[]types.Type{
					{Oid: types.T_int8},
					{Oid: types.T_uint8},
					{Oid: types.T_int16},
					{Oid: types.T_uint16},
					{Oid: types.T_int32},
					{Oid: types.T_uint32},
					{Oid: types.T_int64},
					{Oid: types.T_uint64},
					{Oid: types.T_float32},
					{Oid: types.T_float64},
					{Oid: types.T_char},
					{Oid: types.T_varchar},
					{Oid: types.T_date},
					{Oid: types.T_datetime},
				},
				3)
		}

		batchCase1 := genBatch()

		err = getDataFromPipeline(ses, batchCase1)
		convey.So(err, convey.ShouldBeNil)

		batchCase2 := func() *batch.Batch {
			bat := genBatch()
			for i := 0; i < len(bat.Attrs); i++ {
				for j := 0; j < vector.Length(bat.Vecs[0]); j++ {
					nulls.Add(bat.Vecs[i].Nsp, uint64(j))
				}
			}
			return bat
		}()

		err = getDataFromPipeline(ses, batchCase2)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("getDataFromPipeline fail", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		ioses := mock_frontend.NewMockIOSession(ctrl)

		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng)
		if err != nil {
			t.Error(err)
		}
		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		epochgc := getPCI()
		guestMmu := guest.New(pu.SV.GetGuestMmuLimitation(), pu.HostMmu)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)

		ses := NewSession(proto, epochgc, guestMmu, pu.Mempool, pu, &gSys)
		ses.Mrs = &MysqlResultSet{}

		convey.So(getDataFromPipeline(ses, nil), convey.ShouldBeNil)

		genBatch := func() *batch.Batch {
			return allocTestBatch(
				[]string{
					"a", "b", "c", "d", "e", "f",
					"g", "h", "i", "j", "k", "l",
					"m", "n",
				},
				[]types.Type{
					{Oid: types.T_int8},
					{Oid: types.T_uint8},
					{Oid: types.T_int16},
					{Oid: types.T_uint16},
					{Oid: types.T_int32},
					{Oid: types.T_uint32},
					{Oid: types.T_int64},
					{Oid: types.T_uint64},
					{Oid: types.T_float32},
					{Oid: types.T_float64},
					{Oid: types.T_char},
					{Oid: types.T_varchar},
					{Oid: types.T_date},
					{Oid: types.T_datetime},
				},
				3)
		}
		batchCase2 := func() *batch.Batch {
			bat := genBatch()

			for i := 0; i < len(bat.Attrs); i++ {
				for j := 0; j < 1; j++ {
					nulls.Add(bat.Vecs[i].Nsp, uint64(j))
				}
			}
			return bat
		}()

		err = getDataFromPipeline(ses, batchCase2)
		convey.So(err, convey.ShouldBeNil)

		batchCase2.Vecs = append(batchCase2.Vecs, &vector.Vector{Typ: types.Type{Oid: 88}})
		err = getDataFromPipeline(ses, batchCase2)
		convey.So(err, convey.ShouldNotBeNil)

	})
}

func Test_typeconvert(t *testing.T) {
	convey.Convey("convertEngineTypeToMysqlType", t, func() {
		input := []types.T{
			types.T_int8,
			types.T_uint8,
			types.T_int16,
			types.T_uint16,
			types.T_int32,
			types.T_uint32,
			types.T_int64,
			types.T_uint64,
			types.T_float32,
			types.T_float64,
			types.T_char,
			types.T_varchar,
			types.T_date,
			types.T_datetime,
		}

		type kase struct {
			tp     uint8
			signed bool
		}
		output := []kase{
			{tp: defines.MYSQL_TYPE_TINY, signed: true},
			{tp: defines.MYSQL_TYPE_TINY},
			{tp: defines.MYSQL_TYPE_SHORT, signed: true},
			{tp: defines.MYSQL_TYPE_SHORT},
			{tp: defines.MYSQL_TYPE_LONG, signed: true},
			{tp: defines.MYSQL_TYPE_LONG},
			{tp: defines.MYSQL_TYPE_LONGLONG, signed: true},
			{tp: defines.MYSQL_TYPE_LONGLONG},
			{tp: defines.MYSQL_TYPE_FLOAT, signed: true},
			{tp: defines.MYSQL_TYPE_DOUBLE, signed: true},
			{tp: defines.MYSQL_TYPE_STRING, signed: true},
			{tp: defines.MYSQL_TYPE_VARCHAR, signed: true},
			{tp: defines.MYSQL_TYPE_DATE, signed: true},
			{tp: defines.MYSQL_TYPE_DATETIME, signed: true},
		}

		convey.So(len(input), convey.ShouldEqual, len(output))

		for i := 0; i < len(input); i++ {
			col := &MysqlColumn{}
			err := convertEngineTypeToMysqlType(input[i], col)
			convey.So(err, convey.ShouldBeNil)
			convey.So(col.columnType, convey.ShouldEqual, output[i].tp)
			convey.So(col.IsSigned() && output[i].signed ||
				!col.IsSigned() && !output[i].signed, convey.ShouldBeTrue)
		}
	})
}

func allocTestBatch(attrName []string, tt []types.Type, batchSize int) *batch.Batch {
	batchData := batch.New(true, attrName)

	//alloc space for vector
	for i := 0; i < len(attrName); i++ {
		vec := vector.New(tt[i])
		switch vec.Typ.Oid {
		case types.T_int8:
			vec.Col = make([]int8, batchSize)
		case types.T_int16:
			vec.Col = make([]int16, batchSize)
		case types.T_int32:
			vec.Col = make([]int32, batchSize)
		case types.T_int64:
			vec.Col = make([]int64, batchSize)
		case types.T_uint8:
			vec.Col = make([]uint8, batchSize)
		case types.T_uint16:
			vec.Col = make([]uint16, batchSize)
		case types.T_uint32:
			vec.Col = make([]uint32, batchSize)
		case types.T_uint64:
			vec.Col = make([]uint64, batchSize)
		case types.T_float32:
			vec.Col = make([]float32, batchSize)
		case types.T_float64:
			vec.Col = make([]float64, batchSize)
		case types.T_char, types.T_varchar:
			vBytes := &types.Bytes{
				Offsets: make([]uint32, batchSize),
				Lengths: make([]uint32, batchSize),
				Data:    nil,
			}
			vec.Col = vBytes
		case types.T_date:
			vec.Col = make([]types.Date, batchSize)
		case types.T_datetime:
			vec.Col = make([]types.Datetime, batchSize)
		default:
			panic("unsupported vector type")
		}
		batchData.Vecs[i] = vec
	}

	batchData.Zs = make([]int64, batchSize)
	for i := 0; i < batchSize; i++ {
		batchData.Zs[i] = 2
	}

	return batchData
}

func Test_mysqlerror(t *testing.T) {
	convey.Convey("mysql error", t, func() {
		err := NewMysqlError(ER_BAD_DB_ERROR, "T")
		convey.So(err.ErrorCode, convey.ShouldEqual, ER_BAD_DB_ERROR)

		err2 := NewMysqlError(65535, "T")
		convey.So(err2.ErrorCode, convey.ShouldEqual, ER_UNKNOWN_ERROR)
	})
}

func Test_handleSelectVariables(t *testing.T) {
	convey.Convey("handleSelectVariables succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), nil).Return(nil, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(proto, nil, nil, nil, nil, &gSys)
		ses.Mrs = &MysqlResultSet{}
		mce := &MysqlCmdExecutor{}
		mce.PrepareSessionBeforeExecRequest(ses)
		st2, err := parsers.ParseOne(dialect.MYSQL, "select @@tx_isolation")
		convey.So(err, convey.ShouldBeNil)
		sv2 := st2.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.VarExpr)
		convey.So(mce.handleSelectVariables(sv2), convey.ShouldBeNil)

		st3, err := parsers.ParseOne(dialect.MYSQL, "select @@XXX")
		convey.So(err, convey.ShouldBeNil)
		sv3 := st3.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.VarExpr)
		convey.So(mce.handleSelectVariables(sv3), convey.ShouldNotBeNil)

	})
}

func Test_handleShowVariables(t *testing.T) {
	convey.Convey("handleShowVariables succ", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), nil).Return(nil, nil).AnyTimes()

		ioses := mock_frontend.NewMockIOSession(ctrl)
		ioses.EXPECT().OutBuf().Return(buf.NewByteBuf(1024)).AnyTimes()
		ioses.EXPECT().WriteAndFlush(gomock.Any()).Return(nil).AnyTimes()

		pu, err := getParameterUnit("test/system_vars_config.toml", eng)
		if err != nil {
			t.Error(err)
		}

		proto := NewMysqlClientProtocol(0, ioses, 1024, pu.SV)
		var gSys GlobalSystemVariables
		InitGlobalSystemVariables(&gSys)
		ses := NewSession(proto, nil, nil, nil, nil, &gSys)
		ses.Mrs = &MysqlResultSet{}
		mce := &MysqlCmdExecutor{}
		mce.PrepareSessionBeforeExecRequest(ses)

		sv := &tree.ShowVariables{Global: true}
		convey.So(mce.handleShowVariables(sv), convey.ShouldBeNil)
	})
}

func Test_GetColumns(t *testing.T) {
	convey.Convey("GetColumns succ", t, func() {
		cw := &ComputationWrapperImpl{exec: &compile.Exec{}}
		mysqlCols, err := cw.GetColumns()
		convey.So(mysqlCols, convey.ShouldBeEmpty)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_GetComputationWrapper(t *testing.T) {
	convey.Convey("GetComputationWrapper succ", t, func() {
		db, sql, user := "T", "SHOW TABLES", "root"
		var eng engine.Engine
		proc := &process.Process{}
		ses := &Session{}
		cw, err := GetComputationWrapper(db, sql, user, eng, proc, ses, false)
		convey.So(cw, convey.ShouldNotBeEmpty)
		convey.So(err, convey.ShouldBeNil)
	})
}
