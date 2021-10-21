package unittest

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/compile"
	"matrixone/pkg/sql/testutil"
	"matrixone/pkg/sqlerror"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"testing"
)

func Print(_ interface{}, bat *batch.Batch) error {
	fmt.Printf("%s\n", bat)
	return nil
}

func TestEngine(t *testing.T) {
	e, err := testutil.NewTestEngine()
	require.NoError(t, err)
	db, err := e.Database("T")
	require.NotNil(t, err)
	err = e.Create(0, "T", 0)
	require.NoError(t, err)
	dbs := e.Databases()
	require.Equal(t, 2, len(dbs))
	db, err = e.Database("T")
	require.NoError(t, err)
	require.Equal(t, 0, len(db.Relations()))

}

func TestDDLSql(t *testing.T) {
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(gm)
	{
		proc.Id = "0"
		proc.Lim.Size = 10 << 32
		proc.Lim.BatchRows = 10 << 32
		proc.Lim.PartitionRows = 10 << 32
		proc.Refer = make(map[string]uint64)
	}
	e, err := testutil.NewTestEngine()
	require.NoError(t, err)

	sql := "CREATE DATABASE T1; CREATE DATABASE T2;"
	c := compile.New("", sql, "admin", e, proc)
	srv, err := testutil.NewTestServer(e, proc)
	require.NoError(t, err)
	go srv.Run()
	defer srv.Stop()
	es, err := c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NoError(t, err)
		}
		if err := e.Run(1); err != nil {
			require.NoError(t, err)
		}
	}

	sql = "SHOW DATABASES;"
	c = compile.New("", sql, "admin", e, proc)
	es, err = c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NoError(t, err)
		}
		if err := e.Run(1); err != nil {
			require.NoError(t, err)
		}
	}

	sql = "CREATE TABLE R (orderId varchar(100), uid INT, price FLOAT);"
	c = compile.New("T1", sql, "admin", e, proc)
	es, err = c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NoError(t, err)
		}
		if err := e.Run(1); err != nil {
			require.NoError(t, err)
		}
	}

	sql = "SHOW TABLES;"
	c = compile.New("T1", sql, "admin", e, proc)
	es, err = c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NoError(t, err)
		}
		if err := e.Run(1); err != nil {
			require.NoError(t, err)
		}
	}

	sql = "SHOW TABLES;"
	c = compile.New("T2", sql, "admin", e, proc)
	es, err = c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NoError(t, err)
		}
		if err := e.Run(1); err != nil {
			require.NoError(t, err)
		}
	}
}

func TestInsert(t *testing.T) {
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(gm)
	{
		proc.Id = "0"
		proc.Lim.Size = 10 << 32
		proc.Lim.BatchRows = 10 << 32
		proc.Lim.PartitionRows = 10 << 32
		proc.Refer = make(map[string]uint64)
	}
	e, err := testutil.NewTestEngine()
	require.NoError(t, err)

	srv, err := testutil.NewTestServer(e, proc)
	require.NoError(t, err)
	go srv.Run()
	defer srv.Stop()

	type insertTestCase struct{
		testSql string
		expectErr1 error // compile err expected
		expectErr2 error // run err expected
	}

	testCases := []insertTestCase{
		{"create database testinsert;", nil, nil},
		{"CREATE TABLE TBL(A INT DEFAULT NULL, B VARCHAR(10) DEFAULT 'ABC');", nil, nil},
		{"insert into TBL () values ();", nil, nil},
		{"insert into TBL values (1, '12345678901');", sqlerror.New(errno.DataException, "Data too long for column 'B' at row 1"), nil},
		{"insert into TBL values (1, '1234567890');", nil, nil},
		{"insert into TBL values (2, null);", nil, nil},
		{"insert into TBL values (default, default);", nil, nil},
		{"CREATE TABLE CMS(A INT2, B INT4 DEFAULT 1);", nil, nil},
		{"insert into CMS values (7777777777777777, default);", sqlerror.New(errno.DataException, "Out of range value for column 'A' at row 1"), nil},
		{"insert into CMS () values (), (1, 2);", sqlerror.New(errno.InvalidColumnReference, "Column count doesn't match value count at row 0"), nil},
		{"insert into CMS () values (), ();", nil, nil},
		{"CREATE TABLE TBL3 (A INT NOT NULL DEFAULT NULL);", sqlerror.New(errno.InvalidColumnDefinition, "Invalid default value for 'A'"), nil},
		{"CREATE TABLE TBL4 (A INT NOT NULL);", nil, nil},
		{"insert into TBL4 values ();", sqlerror.New(errno.InvalidColumnDefinition, "Field 'A' doesn't have a default value"), nil},
		{"insert into TBL4 values (default);", sqlerror.New(errno.InvalidColumnDefinition, "Field 'A' doesn't have a default value"), nil},
		{"CREATE TABLE TBL5 (A INT);", nil, nil},
		{"insert into TBL5 values (default);", nil, nil},
		{"CREATE TABLE TBL6 (A INT DEFAULT 1, B INT);", nil, nil},
		{"insert into TBL6 (B) values (1);", nil, nil},
		{"insert into TBL6 (A) values (1);", nil, nil},
		{"CREATE TABLE TBL7 (A INT NOT NULL, B INT DEFAULT 5);", nil, nil},
		{"insert into TBL7 (B) values (10);", sqlerror.New(errno.InvalidColumnDefinition, "Field 'A' doesn't have a default value"), nil},
		{"insert into TBL7 () values ();", sqlerror.New(errno.InvalidColumnDefinition, "Field 'A' doesn't have a default value"), nil},
		{"insert into TBL7 (A) values (1);", nil, nil},
		{"drop database testinsert;", nil, nil},
	}

	type affectRowsCase struct {
		sql string
		err1, err2 error
		affectRows int64 // -1 means there's no need to check this number
	}
	affectRowsCases := []affectRowsCase{
		{"create database testaffect;", nil, nil, -1},
		{"create table cms (a int, b int);", nil, nil, -1},
		{"insert into cms values (1, 2), (3, 4);", nil, nil, 2},
		{"insert into cms values (null, null);", nil, nil, 1},
		{"insert into cms values (null, default);", nil, nil, 1},
		{"insert into cms values (), (), ();", nil, nil, 3},
		{"drop database testaffect", nil, nil, -1},
	}

	for i, tc := range testCases {
		sql := tc.testSql
		expected1 := tc.expectErr1
		expected2 := tc.expectErr2

		c := compile.New("testinsert", sql, "admin", e, proc)
		es, err := c.Build()
		require.NoError(t, err)
		println(i)
		for _, e := range es {
			err := e.Compile(nil, Print)
			if expected1 == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, expected1.Error())
			}
			if expected1 != nil {
				break
			}
			err = e.Run(1)
			if expected2 == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, expected2.Error())
			}
		}
	}

	for i, ac := range affectRowsCases {
		c := compile.New("testaffect", ac.sql, "admin", e, proc)
		es, err := c.Build()
		require.NoError(t, err)
		println(fmt.Sprintf("actest %d", i))
		for _, e := range es {
			err := e.Compile(nil, Print)
			if ac.err1 == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, ac.err1.Error())
			}
			if ac.err1 != nil {
				break
			}
			err = e.Run(1)
			if ac.err2 == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, ac.err2.Error())
			}
			if ac.affectRows == -1 {
				continue
			}
			if e.GetAffectedRows() != uint64(ac.affectRows) {
				err = errors.New("affect rows number error")
				require.NoError(t, err)
			}
		}
	}

}

func TestSql(t *testing.T) {
	sql := "SELECT uid, SUM(price), MIN(price), MAX(price), COUNT(price), AVG(price) FROM R GROUP BY uid ORDER BY uid;" +
		"SELECT SUM(price), MIN(price), MAX(price), COUNT(price), AVG(price) FROM R;" +
		"SELECT uid, SUM(price), MIN(price), MAX(price), COUNT(price), AVG(price) FROM R GROUP BY uid;" +
		"SELECT uid FROM R ORDER BY uid;" +
		"SELECT uid FROM R GROUP BY uid ORDER BY uid;"
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(gm)
	{
		proc.Id = "0"
		proc.Lim.Size = 10 << 32
		proc.Lim.BatchRows = 10 << 32
		proc.Lim.PartitionRows = 10 << 32
		proc.Refer = make(map[string]uint64)
	}
	e, err := testutil.NewTestEngine()
	require.NoError(t, err)

	c := compile.New("test", sql, "tom", e, proc)
	srv, err := testutil.NewTestServer(e, proc)
	require.NoError(t, err)
	go srv.Run()
	defer srv.Stop()
	es, err := c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NoError(t, err)
		}
		if err := e.Run(1); err != nil {
			require.NoError(t, err)
		}
	}

	println(">>>>>>>----------------------------------")

	sql = "SELECT * FROM R; SELECT price FROM R; SELECT uid FROM R; SELECT orderId from R; SELECT uid, orderId from R;"
	c = compile.New("test", sql, "tom", e, proc)
	es, err = c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NoError(t, err)
		}
		if err := e.Run(1); err != nil {
			require.NoError(t, err)
		}
	}

	println(">>>>>>>----------------------------------")

	sql = "select * from R join S on R.uid = S.uid ORDER BY R.uid;"
	c = compile.New("test", sql, "tom", e, proc)
	es, err = c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NoError(t, err)
		}
		if err := e.Run(1); err != nil {
			require.NoError(t, err)
		}
	}

	println(">>>>>>>----------------------------------")

	sql = "SELECT DISTINCT price from R;"
	c = compile.New("test", sql, "tom", e, proc)
	es, err = c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NoError(t, err)
		}
		if err := e.Run(1); err != nil {
			require.NoError(t, err)
		}
	}

	println(">>>>>>>----------------------------------")

	sql = "SELECT unknownCol, price from R where uid = 1;"
	c = compile.New("test", sql, "tom", e, proc)
	es, err = c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NotNil(t, err)
		}
	}

	sql = "SELECT orderId, price from R where uid = '1';"
	c = compile.New("test", sql, "tom", e, proc)
	es, err = c.Build()
	require.NoError(t, err)
	for _, e := range es {
		if err := e.Compile(nil, Print); err != nil {
			require.NoError(t, err)
		}
		if err := e.Run(1); err != nil {
			require.NoError(t, err)
		}
	}

}

func TestCreateTable(t *testing.T) {
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(gm)
	{
		proc.Id = "0"
		proc.Lim.Size = 10 << 32
		proc.Lim.BatchRows = 10 << 32
		proc.Lim.PartitionRows = 10 << 32
		proc.Refer = make(map[string]uint64)
	}
	e, err := testutil.NewTestEngine()
	require.NoError(t, err)
	{
		sql := "CREATE TABLE foo (id BIGINT, c1 INT, c2 TINYINT, c3 VARCHAR(100), c4 CHAR, c5 FLOAT) DEFAULT CHARSET=utf8;"
		c := compile.New("test", sql, "tom", e, proc)
		es, err := c.Build()
		require.NoError(t, err)
		for _, e := range es {
			if err := e.Compile(nil, Print); err != nil {
				require.NoError(t, err)
			}
			if err := e.Run(1); err != nil {
				require.NoError(t, err)
			}
		}
	}

	{
		sql := "CREATE TABLE foo (id BIGINT, c1 INT, c2 TINYINT, c3 VARCHAR(100), c4 CHAR, c5 FLOAT) DEFAULT CHARSET=utf8;"
		c := compile.New("test", sql, "tom", e, proc)
		es, err := c.Build()
		require.NoError(t, err)
		for _, e := range es {
			if err := e.Compile(nil, Print); err != nil {
				require.NoError(t, err)
			}
			if err := e.Run(1); err != nil {
				require.NotNil(t, err)
			}
		}

	}
}

// TestOperators to test operators (binary operator and unary operator) for each type
func TestOperators(t *testing.T) {
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(gm)
	{
		proc.Id = "0"
		proc.Lim.Size = 10 << 32
		proc.Lim.BatchRows = 10 << 32
		proc.Lim.PartitionRows = 10 << 32
		proc.Refer = make(map[string]uint64)
	}
	e, err := testutil.NewTestEngine()
	require.NoError(t, err)

	srv, err := testutil.NewTestServer(e, proc)
	require.NoError(t, err)
	go srv.Run()
	defer srv.Stop()

	type testCase struct {
		id	int
		sql string
		expectedError error
	}

	testCases := []testCase{
		{0, "create database testoperators;", nil},
		{1, "create table iis (i1 tinyint, i2 smallint, i3 int, i4 bigint);", nil},
		{2, "create table ffs (f1 float, f2 double);", nil},
		{3, "create table uus (u1 tinyint unsigned, u2 smallint unsigned, u3 int unsigned, u4 bigint unsigned);", nil},
		{4, "insert into iis values (1, 11, 111, 1111);", nil},
		{5, "insert into ffs values (22.2, 222.222);", nil},
		{6, "insert into uus values (3, 33, 333, 3333);", nil},
		// operator between same types.
		// test int
		{7, "select i1 + i1, i1 - i1, i1 / i1, i1 * i1, i2 + i2, i2 - i2, i2 / i2, i2 * i2 from iis;", nil},
		{8, "select i3 + i3, i3 - i3, i3 / i3, i3 * i3, i4 + i4, i4 - i4, i4 / i4, i4 * i4 from iis;", nil},
		{9, "select -i1, -i2, -i3, -i4 from iis;", nil},
		{10, "select * from iis where i1 = i1 and i2 = i2 and i3 = i3 and i4 = i4;", nil},
		{11, "select CAST(i1 AS FLOAT(1)) ci1f1, CAST(i1 AS DOUBLE) ci1f2, CAST(i1 AS CHAR(2)) ci1c2 from iis;", nil},
		// test float
		{12, "select f1 + f1, f1 - f1, f1 * f1, f1 / f1 from ffs;", nil},
		{13, "select f2 + f2, f2 - f2, f2 * f2, f2 / f2 from ffs;", nil},
		{14, "select -f1, -f2 from ffs;", nil},
		{15, "select * from ffs where f1 = f1 or f2 = f2;", nil},
		{16, "select * from ffs where f1 > f1 and f2 <= f2;", nil},
		// test uint
		{17, "select u1 + u1, u1 - u1, u1 * u1, u1 % u1, u1 / u1 from uus;", nil},
		{18, "select u2 + u2, u2 - u2, u2 * u2, u2 % u2, u2 / u2 from uus;", nil},
		{19, "select u3 + u3, u3 - u3, u3 * u3, u3 % u3, u3 / u3 from uus;", nil},
		{20, "select u4 - u4, u4 + u4, u4 / u4, u4 * u4, u4 % u4 from uus;", nil},
		// test char, varchar // TODO: should add limit for char while cast ?
		{21, "create table ccs (c1 char(10), c2 varchar(20));", nil},
		{22, "select cast(c2 AS char) cc2c1 from ccs;", nil},
	}

	for i, tc := range testCases {
		sql := tc.sql
		expected := tc.expectedError

		c := compile.New("testoperators", sql, "admin", e, proc)
		es, err := c.Build()
		require.NoError(t, err)
		println(i)
		for _, e := range es {
			err := e.Compile(nil, Print)
			if expected == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, expected.Error())
				break
			}
			err = e.Run(1)
			if expected == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, expected.Error())
				break
			}
		}
	}
}
