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

package vector

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"log"
	"testing"
)

func TestVector(t *testing.T) {
	v := New(types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0})
	w := New(types.Type{Oid: types.T(types.T_varchar), Size: 24, Width: 0, Precision: 0})
	{
		vs := make([][]byte, 10)
		for i := 0; i < 10; i++ {
			vs[i] = []byte(fmt.Sprintf("%v", i*i))
		}
		vs[9] = []byte("abcd")
		if err := v.Append(vs); err != nil {
			log.Fatal(err)
		}
	}
	hm := host.New(1 << 20)
	gm := guest.New(1<<20, hm)
	proc := process.New(gm)
	proc.Mp = mempool.New()
	defer proc.Mp.Release()
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
		fmt.Printf("w[1] = v[9]: %v\n", w)
	}
	w.Ref = 1
	w.Free(proc)
	fmt.Printf("guest: %v, host: %v\n", proc.Size(), proc.HostSize())
}
