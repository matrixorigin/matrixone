package varchar

import (
	"fmt"
	"log"
	"math/rand"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"testing"
)

const (
	Num   = 10
	Limit = 100
)

func generate() *vector.Vector {
	vs := make([][]byte, Num)
	{
		for i := 0; i < Num; i++ {
			vs[i] = []byte(fmt.Sprintf("%v", rand.Int63()%Limit))
		}
	}
    vec := vector.New(types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0})
	if err := vec.Append(vs); err != nil {
		log.Fatal(err)
	}
	return vec
}

func TestSort(t *testing.T) {
	vec := generate()
	os := make([]int64, Num)
	vs := vec.Col.(*types.Bytes)
	{
		for i := 0; i < Num; i++ {
			os[i] = int64(i)
		}
	}
	for i, o := range os {
		fmt.Printf("[%v] = %s\n", i, vs.Get(o))
	}
	Sort(vs, os[2:])
	fmt.Printf("\n")
	for i, o := range os {
		fmt.Printf("[%v] = %s\n", i, vs.Get(o))
	}
}
