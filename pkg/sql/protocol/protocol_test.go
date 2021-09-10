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

package protocol

import (
	"bytes"
	"fmt"
	"log"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
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
	proc := process.New(gm)
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
	nbat, data, err := DecodeBatch(buf.Bytes())
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
