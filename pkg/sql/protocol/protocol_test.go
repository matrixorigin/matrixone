package protocol

import (
	"bytes"
	"fmt"
	"log"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"testing"
)

func TestBatch(t *testing.T) {
	var buf bytes.Buffer

	bat := batch.New(true, []string{"a", "b", "c"})
	bat.Vecs[0] = NewFloatVector(1.2)
	bat.Vecs[1] = NewFloatVector(2.1)
	bat.Vecs[2] = NewStrVector([]byte("x"))
	fmt.Printf("bat: %v\n", bat)
	if err := EncodeBatch(bat, &buf); err != nil {
		log.Fatal(err)
	}
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	proc := process.New(gm, mempool.New(1<<40, 8))
	{
		proc.Id = "0"
		proc.Lim.Size = 10 << 32
		proc.Lim.BatchRows = 10 << 32
		proc.Lim.PartitionRows = 10 << 32
		proc.Refer = make(map[string]uint64)
	}
	{
		fmt.Printf("proc.Size: %v\n", proc.Size())
	}
	nbat, data, err := DecodeBatchWithProcess(buf.Bytes(), proc)
	fmt.Printf("nbat: %v\n", nbat)
	fmt.Printf("data: %v, err: %v\n", data, err)
	{
		fmt.Printf("proc.Size: %v\n", proc.Size())
	}
	nbat.Clean(proc)
	{
		fmt.Printf("proc.Size: %v\n", proc.Size())
	}
}

func NewStrVector(v []byte) *vector.Vector {
    vec := vector.New(types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0})
	vec.Append([][]byte{v, v, v})
	return vec
}

func NewFloatVector(v float64) *vector.Vector {
    vec := vector.New(types.Type{Oid: types.T(types.T_float64), Size: 8, Width: 0, Precision: 0})
	vec.Append([]float64{v, v, v})
	return vec
}
