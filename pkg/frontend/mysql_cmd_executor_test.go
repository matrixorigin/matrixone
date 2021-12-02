package frontend

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memEngine"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"reflect"
	"testing"
	"time"
)

	func TestMysqlCmdExecutor(t *testing.T) {
	fs, err := NewFrontendStub()
	require.NoError(t, err)

	err = StartFrontendStub(fs)
	require.NoError(t, err)

	defer func(fs *FrontendStub) {
		err := CloseFrontendStub(fs)
		if err != nil {
			require.NoError(t,err)
		}
	}(fs)

	db := open_db(t,6002)
	time.Sleep(2 * time.Second)

	do_query_resp_resultset(t, db, false, false, "SELECT @@max_allowed_packet", MakeResultSet_select_max_allowed_packet())
	do_query_resp_resultset(t, db, false, false, "SELECT DATABASE()", MakeResultSet_SELECT_DATABASE("DATABASE()", "NULL"))

	do_query_resp_resultset(t, db, false, false, "show databases", MakeResultSet_ShowDatabases("test", "Database", "test"))
	do_query_resp_resultset(t, db, true, false, "show databasess", nil)

	do_query_resp_states(t, db, false, "use test")
	do_query_resp_states(t, db, true, "use test2")

	//create database T
	do_query_resp_states(t,db,false,"create database T")
	do_query_resp_states(t,db,true,"create database T")

	//use T
	do_query_resp_states(t, db, false, "use T")

	tableFormat := "create table %s (a tinyint,b tinyint unsigned," +
		"c smallint, d smallint unsigned, " +
		"e int, f int unsigned," +
		"g bigint,h bigint unsigned," +
		"i float, j double ," +
		"k char(100),l varchar(100)" +
		")"

	//create table A
	tableA := fmt.Sprintf(tableFormat,"A")
	do_query_resp_states(t,db,false,tableA)
	do_query_resp_resultset(t, db, false, false, "select * from A", MakeResultSet_SELECT_1())

	//insert into A values ...
	values1,mrs1 := MakeValues_insert_1(100)
	insertFormat := "insert into %s values"
	insertA := fmt.Sprintf(insertFormat,"A") + values1
	do_query_resp_states(t,db,false,insertA)
	do_query_resp_resultset(t, db, false, false, "select * from A", mrs1)

	//create table B
	tableB := fmt.Sprintf(tableFormat,"B")
	do_query_resp_states(t,db,false,tableB)
	do_query_resp_resultset(t, db, false, false, "select * from B", MakeResultSet_SELECT_1())

	//insert into B values ...
	values2,mrs2 := MakeValues_insert_1_all_NULL(100)
	insertB := fmt.Sprintf(insertFormat,"B") + values2
	do_query_resp_states(t,db,false,insertB)
	do_query_resp_resultset(t, db, false, false, "select * from B", mrs2)

	//create table C
	tableC := fmt.Sprintf(tableFormat,"C")
	do_query_resp_states(t,db,false,tableC)

	//insert into C values ...
	values3,mrs3,_ := MakeValues_insert_1_partial_null(0,100)
	insertC := fmt.Sprintf(insertFormat,"C") + values3
	do_query_resp_states(t,db,false,insertC)
	do_query_resp_resultset(t, db, false, false, "select * from C", mrs3)

	_,mrs4,_ := MakeValues_insert_1_partial_null(0,100)
	do_query_resp_resultset(t, db, false, false, "select * from C order by f", mrs4)

	//create table D
	tableD := fmt.Sprintf(tableFormat,"D")
	do_query_resp_states(t,db,false,tableD)

	//insert into D values ...
	_,mrs5,loaddata5 := MakeValues_insert_1_partial_null(0,109)
	loadfile1 := "test/loadcase2"
	err = ioutil.WriteFile(loadfile1, []byte(loaddata5),0777)
	require.NoError(t, err)

	loadFormat := "load data " +
		"infile '%s' " +
		"ignore " +
		"INTO TABLE T.%s " +
		"FIELDS TERMINATED BY '%c' "
	loadD := fmt.Sprintf(loadFormat,loadfile1,"D",',')
	fmt.Println(loadD)
	do_query_resp_states(t,db,false,loadD)

	//memEngine does ensure sort
	do_query_resp_resultset(t, db, false, true, "select * from D", mrs5)

	//loadFormat2 := "load data " +
	//	"infile '%s' " +
	//	"ignore " +
	//	"INTO TABLE T.%s " +
	//	"FIELDS TERMINATED BY '%c' " +
	//	"(a,b,c,d,e,f,g,h,i,j,k,l)"
	//do_query_resp_states(t,db,false,"drop table D")
	//do_query_resp_states(t,db,false,tableD)
	//
	//loadD2 := fmt.Sprintf(loadFormat2,loadfile1,"D",',')
	//fmt.Println(loadD2)
	//do_query_resp_states(t,db,false,loadD2)
	//
	//do_query_resp_resultset(t, db, false, true, "select * from D", mrs5)

	time.Sleep(100 * time.Millisecond)
	close_db(t,db)
}

func TestChannelProtocol(t *testing.T) {
	cps, err := NewChannelProtocolStub()
	require.NoError(t, err)

	err = StartChannelProtocolStub(cps)
	require.NoError(t, err)

	defer func(cps *ChannelProtocolStub) {
		err := CloseChannelProtocolStub(cps)
		if err != nil {
			require.NoError(t,err)
		}
	}(cps)

	cpRt := cps.cps.CreateRoutine()
	cpClient := NewChannelProtocolClient(cpRt)

	cpClient.SendQuery("show databases")

	//resultset
	mrs := cpClient.GetResultSet()
	db := string(mrs.Data[0][0].([]byte))

	cpClient.SendQuery(fmt.Sprintf("use %s",db))
	cpClient.GetStateOK()

	cpClient.SendQuery("show tables")
	showtable := cpClient.GetResultSet()

	for _, dr := range showtable.Data {
		tab := string(dr[0].([]byte))
		//result set is wrong ?
		fmt.Println(tab)
	}

	cpClient.SendQuery("create database T")
	cpClient.GetStateOK()

	cpClient.SendQuery("use T")
	cpClient.GetStateOK()

	cpClient.SendQuery("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci'")
	cpClient.GetStateOK()

	cpClient.SendQuery("SET @@session.autocommit = OFF")
	cpClient.GetStateOK()

	cpClient.SendQuery("create table A(a int)")
	cpClient.GetStateOK()

	//duplicate create table A
	cpClient.SendQuery("create table A(a int)")
	cpClient.GetState(CP_ERR)

	cpClient.SendQuery("insert into A values (1),(1),(1),(1),(1)")
	cpClient.GetStateOK()

	cpClient.SendQuery("select * from A")
	tabA := cpClient.GetResultSet()
	require.True(t, tabA.GetColumnCount() == 1)
	require.True(t, tabA.GetRowCount() == 5)
	for _, dr := range tabA.Data {
		//fmt.Println(dr[0])
		require.True(t, reflect.DeepEqual(dr[0],int32(1)))
	}

	tableFormat := "create table %s (a tinyint,b tinyint unsigned," +
		"c smallint, d smallint unsigned, " +
		"e int, f int unsigned," +
		"g bigint,h bigint unsigned," +
		"i float, j double ," +
		"k char(100),l varchar(100)" +
		")"

	//create table B
	tableB := fmt.Sprintf(tableFormat,"B")
	cpClient.SendQuery(tableB)
	cpClient.GetStateOK()

	//insert into B values ...
	values1,mrs1 := MakeValues_insert_1(100)
	insertFormat := "insert into %s values"
	insertB := fmt.Sprintf(insertFormat,"B") + values1

	cpClient.SendQuery(insertB)
	cpClient.GetStateOK()

	check_resultset(t,cpClient,false,"select * from B",mrs1)

	cpClient.SendRequest(COM_QUIT,nil)

	//close client
	cps.cps.CloseRoutine(cpRt.getConnID())
}

func newTestEngine() (engine.Engine, *process.Process) {
	hm := host.New(1 << 30)
	gm := guest.New(1<<30, hm)
	proc := process.New(mheap.New(gm))
	e := memEngine.NewTestEngine()
	return e, proc
}

// a simple select function Todo: need delete when support select * from relation.
func TempSelect(e engine.Engine, schema, name string) string {
	var buff bytes.Buffer

	db, err := e.Database(schema)
	if err != nil {
		return err.Error()
	}
	r, err := db.Relation(name)
	if err != nil {
		return err.Error()
	}
	defs := r.TableDefs()
	attrs := make([]string, 0, len(defs))
	{
		for _, def := range defs {
			if v, ok := def.(*engine.AttributeDef); ok {
				attrs = append(attrs, v.Attr.Name)
			}
		}
	}
	cs := make([]uint64, len(attrs))
	for i := range cs {
		cs[i] = 1
	}
	rd := r.NewReader(1)[0]
	{
		bat, err := rd.Read(cs, attrs)
		if err != nil {
			return err.Error()
		}
		buff.WriteString(fmt.Sprintf("%s\n", bat))
	}
	return buff.String()
}

func TestLoadDate(t *testing.T){
	fs, err := NewFrontendStub()
	require.NoError(t, err)

	err = StartFrontendStub(fs)
	require.NoError(t, err)

	defer func(fs *FrontendStub) {
		err := CloseFrontendStub(fs)
		if err != nil {
			require.NoError(t,err)
		}
	}(fs)

	db := open_db(t,6002)
	time.Sleep(2 * time.Second)

	do_query_resp_states(t, db, false, "use test")

	//create table E
	tableE := "create table E(a varchar,b date,c int)"
	do_query_resp_states(t,db,false,tableE)

	loadFormat := "load data " +
		"infile '%s' " +
		"ignore " +
		"INTO TABLE test.%s " +
		"FIELDS TERMINATED BY '%c' "

	loadfile2 := "test/loadcase3"
	loadE := fmt.Sprintf(loadFormat,loadfile2,"E",',')
	fmt.Println(loadE)
	do_query_resp_states(t,db,false,loadE)

	loadE_cases := [][]string {
		{"tdate", "a\n\t[abc abc abc abc abc abc abc abc abc abc]-&{<nil>}\nb\n\t[2007-02-10 1997-02-10 2001-04-28 2004-11-12 2007-02-10 1997-02-10 2001-04-28 2004-11-12 2007-02-10 1997-02-10]-&{<nil>}\nc\n\t[1 2 3 4 5 6 7 8 9 10]-&{<nil>}\n\n"},
	}

	for _, ec := range loadE_cases {
		//fmt.Println(ec[1])
		require.Equal(t, ec[1], TempSelect(fs.eng,"test","E"))
	}

	time.Sleep(100 * time.Millisecond)
	close_db(t,db)
}

func NewColumnDef_string(name string, colType uint8) *MysqlColumn {
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(colType)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))

	return mysqlCol
}

func NewColumnDef_digital(name string,colType uint8,unsigned bool) *MysqlColumn{
	mysqlCol := new(MysqlColumn)
	mysqlCol.SetName(name)
	mysqlCol.SetOrgName(name + "OrgName")
	mysqlCol.SetColumnType(colType)
	mysqlCol.SetSchema(name + "Schema")
	mysqlCol.SetTable(name + "Table")
	mysqlCol.SetOrgTable(name + "Table")
	mysqlCol.SetCharset(uint16(Utf8mb4CollationID))
	mysqlCol.SetSigned(!unsigned)
	return mysqlCol
}

func MakeResultSet_ShowDatabases(db string,column string,data ...interface{}) *MysqlResultSet {
	mrs := &MysqlResultSet{}
	mrs.AddColumn(NewColumnDef_string(column,defines.MYSQL_TYPE_VARCHAR))
	for _, da := range data {
		mrs.AddRow([]interface{}{da})
	}
	return mrs
}

func MakeResultSet_UseDatabase(db string) *MysqlResultSet {
	return &MysqlResultSet{}
}

func MakeResultSet_select_max_allowed_packet() *MysqlResultSet {
	mrs := &MysqlResultSet{}
	mrs.AddColumn(NewColumnDef_digital("@@max_allowed_packet",defines.MYSQL_TYPE_LONG,false))
	mrs.AddRow([]interface{}{int32(16777216)})
	return mrs
}

func MakeResultSet_SELECT_DATABASE(column,value string) *MysqlResultSet{
	mrs := &MysqlResultSet{}
	mrs.AddColumn(NewColumnDef_string(column,defines.MYSQL_TYPE_VARCHAR))
	mrs.AddRow([]interface{}{value})
	return mrs
}

func MakeResultSet_SELECT_1() *MysqlResultSet{
	mrs := &MysqlResultSet{}
	mrs.AddColumn(NewColumnDef_digital("a",defines.MYSQL_TYPE_TINY,false))
	mrs.AddColumn(NewColumnDef_digital("b",defines.MYSQL_TYPE_TINY,true))
	mrs.AddColumn(NewColumnDef_digital("c",defines.MYSQL_TYPE_SHORT,false))
	mrs.AddColumn(NewColumnDef_digital("d",defines.MYSQL_TYPE_SHORT,true))
	mrs.AddColumn(NewColumnDef_digital("e",defines.MYSQL_TYPE_LONG,false))
	mrs.AddColumn(NewColumnDef_digital("f",defines.MYSQL_TYPE_LONG,true))
	mrs.AddColumn(NewColumnDef_digital("g",defines.MYSQL_TYPE_LONGLONG,false))
	mrs.AddColumn(NewColumnDef_digital("h",defines.MYSQL_TYPE_LONGLONG,true))
	mrs.AddColumn(NewColumnDef_digital("i",defines.MYSQL_TYPE_FLOAT,false))
	mrs.AddColumn(NewColumnDef_digital("j",defines.MYSQL_TYPE_DOUBLE,false))
	mrs.AddColumn(NewColumnDef_string("l",defines.MYSQL_TYPE_STRING))
	mrs.AddColumn(NewColumnDef_string("l",defines.MYSQL_TYPE_VARCHAR))
	return mrs
}

func MakeValues_insert_1(cnt int) (string,*MysqlResultSet){
	mrs := MakeResultSet_SELECT_1()
	row := []interface{}{
		int8(-128),uint8(255),
		int16(-32768),uint16(65535),
		int32(-2147483648),uint32(4294967295),
		int64(-9223372036854775808),uint64(18446744073709551615),
		float32(-3.402823466E+38),float64(1.7976931348623157E+308),
		"aaaaa","bbbbbbb",
	}
	s := ""
	for i := 0; i < cnt; i++ {
		if i != 0 {
			s += ","
		}
		s += "("

		s += "-128,255," +
			"-32768,65535," +
			"-2147483648,4294967295," +
			"-9223372036854775808,18446744073709551615," +
			"-3.402823466E+38,1.7976931348623157E+308," +
			"\"aaaaa\",\"bbbbbbb\""

		s += ")"

		mrs.AddRow(row)
	}
	s += ";"

	return s,mrs
}

func MakeValues_insert_1_all_NULL(cnt int) (string,*MysqlResultSet){
	mrs := MakeResultSet_SELECT_1()
	row := []interface{}{
		nil,nil,
		nil,nil,
		nil,nil,
		nil,nil,
		nil,nil,
		nil,nil,
	}
	s := ""
	for i := 0; i < cnt; i++ {
		if i != 0 {
			s += ","
		}
		s += "("

		s += "NULL,NULL," +
			"NULL,NULL," +
			"NULL,NULL," +
			"NULL,NULL," +
			"NULL,NULL," +
			"NULL,NULL"

		s += ")"

		mrs.AddRow(row)
	}
	s += ";"

	return s,mrs
}

func MakeValues_insert_1_partial_null(start int,cnt int) (string,*MysqlResultSet,string){
	mrs := MakeResultSet_SELECT_1()
	//row1 := []interface{}{
	//	nil,nil,
	//	nil,nil,
	//	nil,nil,
	//	nil,nil,
	//	nil,nil,
	//	nil,nil,
	//}
	//
	//row2 := []interface{}{
	//	int8(-128),uint8(255),
	//	int16(-32768),uint16(65535),
	//	int32(-2147483648),uint32(4294967295),
	//	int64(-9223372036854775808),uint64(18446744073709551615),
	//	float32(-3.402823466E+38),float64(1.7976931348623157E+308),
	//	"aaaaa","bbbbbbb",
	//}

	s1 := "NULL,NULL," +
		"NULL,NULL," +
		"NULL,%d," +
		"NULL,NULL," +
		"NULL,NULL," +
		"NULL,NULL"

	s2 := "-128,255," +
		"-32768,65535," +
		"-2147483648,%d," +
		"-9223372036854775808,18446744073709551615," +
		"-3.402823466E+38,1.7976931348623157E+308," +
		"\"aaaaa\",\"bbbbbbb\""

	loadDataFormat := ",," +
		",," +
		",%d," +
		",," +
		",," +
		","

	s := ""
	loadData := ""

	for i := 0; i < (start + cnt); i++ {
		if i < start {
			continue
		}
		if i != 0 {
			s += ","
		}
		s += "("

		if i % 2 == 0 {
			s += fmt.Sprintf(s1,i)
		}else{
			s += fmt.Sprintf(s2,i)
		}

		s += ")"

		if i % 2 == 0 {
			row3 := []interface{}{
				nil,nil,
				nil,nil,
				nil,uint32(i),
				nil,nil,
				nil,nil,
				nil,nil,
			}
			mrs.AddRow(row3)
		}else{
			row4 := []interface{}{
				int8(-128),uint8(255),
				int16(-32768),uint16(65535),
				int32(-2147483648),uint32(i),
				int64(-9223372036854775808),uint64(18446744073709551615),
				float32(-3.402823466E+38),float64(1.7976931348623157E+308),
				"aaaaa","bbbbbbb",
			}
			mrs.AddRow(row4)
		}

		if i % 2 == 0 {
			loadData += fmt.Sprintln(fmt.Sprintf(loadDataFormat,uint32(i)))
		} else{
			loadData += fmt.Sprintln(fmt.Sprintf(s2,uint32(i)))
		}

	}
	s += ";"

	return s,mrs,loadData
}