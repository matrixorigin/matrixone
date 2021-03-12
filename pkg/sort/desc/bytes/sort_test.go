package bytes

import (
	"fmt"
	"log"
	"math/rand"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
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
	vec := vector.New(types.T_bytes)
	if err := vec.Append(vs); err != nil {
		log.Fatal(err)
	}
	return vec
}

func TestSort(t *testing.T) {
	vs := generate()
	fmt.Printf("%s\n", vs)
	{
		col := vs.Col.(*types.Bytes)
		Sort(&types.Bytes{
			Data:    col.Data,
			Lengths: col.Lengths,
			Offsets: col.Offsets[:Num-2],
		})
	}
	fmt.Printf("\n")
	fmt.Printf("%s\n", vs)
}
