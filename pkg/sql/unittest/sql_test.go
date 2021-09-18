package unittest

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/compile"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"testing"
)

func Print(_ interface{}, bat *batch.Batch) error {

	fmt.Printf("%s\n", bat)
	return nil
}

func TestSql(t *testing.T) {
	sql := "select * from R order by uid; select * from S order by uid;"
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
	e, err := NewTestEngine()
	require.NoError(t, err)

	c := compile.New("test", sql, "tom", e, proc)
	srv, err := NewTestServer(e, proc)
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

	sql = "select * from R join S on R.uid = S.uid order by R.uid;"
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
	sql := "CREATE TABLE foo (k INT, v INT)"
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
	e, err := NewTestEngine()
	require.NoError(t, err)
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
