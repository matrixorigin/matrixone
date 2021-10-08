package unittest

import (
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
		{"insert into TBL values (1, '12345678901');", sqlerror.New(errno.DataException, "Data too long for column 'a' at row 1"), nil},
		{"insert into TBL values (1, '1234567890');", nil, nil},
		{"insert into TBL values (2, null);", nil, nil},
		{"insert into TBL values (default, default);", nil, nil},
		{"CREATE TABLE CMS(A INT2, B INT4 DEFAULT 1);", nil, nil},
		{"insert into CMS values (7777777777777777, default);", sqlerror.New(errno.DataException, "Out of range value for column 'a' at row 1"), nil},
		{"insert into CMS () values (), (1, 2);", sqlerror.New(errno.InvalidColumnReference, "Column count doesn't match value count at row 0"), nil},
		{"insert into CMS () values (), ();", nil, nil},
		{"CREATE TABLE TBL2 (A INT NOT NULL);", nil, nil},
		// {"insert into TBL2 () values ();", errMsg1, errMsg2}, // TODO: notNull constraint does not implement.
		{"drop database testinsert;", nil, nil},
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
				require.Error(t, expected1)
			}
			if err != nil {
				continue
			}
			err = e.Run(1)
			if expected2 == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, expected2)
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
