package unittest

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/sql/compile"
	"matrixone/pkg/vm/engine/memEngine"
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

	sql := "select * from R;"
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
	e := memEngine.NewTestEngine()
	c := compile.New("test", sql, "tom", e, proc)
	StartTestServer(40000+100, e, proc)
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
