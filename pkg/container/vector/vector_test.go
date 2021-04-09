package vector

import (
	"fmt"
	"log"
	"matrixone/pkg/container/types"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/mmu/host"
	"matrixone/pkg/vm/process"
	"testing"
)

func TestVector(t *testing.T) {
	v := New(types.Type{types.T(types.T_varchar), 24, 0, 0})
	w := New(types.Type{types.T(types.T_varchar), 24, 0, 0})
	{
		vs := make([][]byte, 10)
		for i := 0; i < 10; i++ {
			vs[i] = []byte(fmt.Sprintf("%v", i*i))
		}
		vs[9] = []byte("abcd")
		if err := v.Append(vs); err != nil {
			log.Fatal(err)
		}
		v.Data = encoding.EncodeInt64(1)
	}
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	proc := process.New(gm, mempool.New(1<<32, 8))
	for i := 0; i < 5; i++ {
		if err := w.UnionOne(v, int64(i), proc); err != nil {
			log.Fatal(err)
		}
	}
	{
		fmt.Printf("v: %v\n", v)
		fmt.Printf("w: %v\n", w)
	}
	{
		if err := w.Copy(v, 1, 9, proc); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("w[0] = v[6]: %v\n", w)
	}
	w.Free(proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
}
