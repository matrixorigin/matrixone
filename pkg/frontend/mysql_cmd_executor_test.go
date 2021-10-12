package frontend

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"matrixone/pkg/defines"
	"testing"
	"time"
)

func TestMysqlCmdExecutor(t *testing.T) {
	fs, err := NewFrontendStub()
	require.NoError(t, err)
	defer func(fs *FrontendStub) {
		err := CloseFrontendStub(fs)
		if err != nil {
			require.NoError(t,err)
		}
	}(fs)

	db := open_db(t,6002)
	time.Sleep(100 * time.Millisecond)

	do_query_with_null(t,db,false,"SELECT @@max_allowed_packet",MakeResultSet_select_max_allowed_packet())
	do_query_with_null(t,db,false,"SELECT DATABASE()",MakeResultSet_SELECT_DATABASE("DATABASE()","NULL"))

	do_query_with_null(t, db, false, "show databases", MakeResultSet_ShowDatabases("test", "Database", "test"))
	do_query_with_null(t, db, true, "show databasess", nil)

	do_query_without_result_set(t, db, false, "use test")
	do_query_without_result_set(t, db, true, "use test2")

	//create database T
	do_query_without_result_set(t,db,false,"create database T")
	do_query_without_result_set(t,db,true,"create database T")

	//use T
	do_query_without_result_set(t, db, false, "use T")

	tableFormat := "create table %s (a tinyint,b tinyint unsigned," +
		"c smallint, d smallint unsigned, " +
		"e int, f int unsigned," +
		"g bigint,h bigint unsigned," +
		"i float, j double ," +
		"l char(100),m varchar(100)" +
		")"

	//create table A
	tableA := fmt.Sprintf(tableFormat,"A")
	do_query_without_result_set(t,db,false,tableA)
	do_query_with_null(t,db,false,"select * from A",MakeResultSet_SELECT_1())

	//insert into A values ...
	values1,mrs1 := MakeValues_insert_1(100)
	insertFormat := "insert into %s values"
	insertA := fmt.Sprintf(insertFormat,"A") + values1
	do_query_without_result_set(t,db,false,insertA)
	do_query_with_null(t,db,false,"select * from A",mrs1)

	//create table B
	tableB := fmt.Sprintf(tableFormat,"B")
	do_query_without_result_set(t,db,false,tableB)
	do_query_with_null(t,db,false,"select * from B",MakeResultSet_SELECT_1())

	//insert into B values ...
	values2,mrs2 := MakeValues_insert_1_all_NULL(100)
	insertB := fmt.Sprintf(insertFormat,"B") + values2
	do_query_without_result_set(t,db,false,insertB)
	do_query_with_null(t,db,false,"select * from B",mrs2)

	//create table C
	tableC := fmt.Sprintf(tableFormat,"C")
	do_query_without_result_set(t,db,false,tableC)

	//insert into C values ...
	values3,mrs3 := MakeValues_insert_1_partial_null(0,100)
	insertC := fmt.Sprintf(insertFormat,"C") + values3
	do_query_without_result_set(t,db,false,insertC)
	do_query_with_null(t,db,false,"select * from C",mrs3)

	_,mrs4 := MakeValues_insert_1_partial_null(1,10)
	do_query_with_null(t,db,false,"select * from C limit 1,10",mrs4)

	defer close_db(t,db)
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

func MakeValues_insert_1_partial_null(start int,cnt int) (string,*MysqlResultSet){
	mrs := MakeResultSet_SELECT_1()
	row1 := []interface{}{
		nil,nil,
		nil,nil,
		nil,nil,
		nil,nil,
		nil,nil,
		nil,nil,
	}

	row2 := []interface{}{
		int8(-128),uint8(255),
		int16(-32768),uint16(65535),
		int32(-2147483648),uint32(4294967295),
		int64(-9223372036854775808),uint64(18446744073709551615),
		float32(-3.402823466E+38),float64(1.7976931348623157E+308),
		"aaaaa","bbbbbbb",
	}

	s1 := "NULL,NULL," +
		"NULL,NULL," +
		"NULL,NULL," +
		"NULL,NULL," +
		"NULL,NULL," +
		"NULL,NULL"

	s2 := "-128,255," +
		"-32768,65535," +
		"-2147483648,4294967295," +
		"-9223372036854775808,18446744073709551615," +
		"-3.402823466E+38,1.7976931348623157E+308," +
		"\"aaaaa\",\"bbbbbbb\""

	s := ""

	for i := 0; i < (start + cnt); i++ {
		if i < start {
			continue
		}
		if i != 0 {
			s += ","
		}
		s += "("

		if i % 2 == 0 {
			s += s1
		}else{
			s += s2
		}

		s += ")"

		if i % 2 == 0 {
			mrs.AddRow(row1)
		}else{
			mrs.AddRow(row2)
		}

	}
	s += ";"

	return s,mrs
}