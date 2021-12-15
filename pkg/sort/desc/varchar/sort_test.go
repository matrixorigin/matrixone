// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package varchar

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"log"
	"math/rand"
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
	if err := vector.Append(vec, vs); err != nil {
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
