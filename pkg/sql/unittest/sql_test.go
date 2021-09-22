package unittest

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/compile"
	"matrixone/pkg/sql/testutil"
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

func TestSql(t *testing.T) {
	sql := "SELECT SUM(price) x, MIN(orderId) y, MAX(orderId) z from R group by uid;"
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

	sql = "SELECT SUM(price) x, MIN(orderId) y, MAX(orderId) z from R group by uid;"
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
