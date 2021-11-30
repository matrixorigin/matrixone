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

package process

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

// New creates a new Process.
// A process stores the execution context.
func New(m *mheap.Mheap) *Process {
	return &Process{
		Mp: m,
	}
}

func GetSels(proc *Process) []int64 {
	if len(proc.Reg.Ss) == 0 {
		return make([]int64, 0, 16)
	}
	sels := proc.Reg.Ss[0]
	proc.Reg.Ss = proc.Reg.Ss[1:]
	return sels[:0]
}

func PutSels(sels []int64, proc *Process) {
	proc.Reg.Ss = append(proc.Reg.Ss, sels)
}

func Get(proc *Process, size int64, typ types.Type) (*vector.Vector, error) {
	for i, vec := range proc.Reg.Vecs {
		if int64(cap(vec.Data)) >= size {
			vec.Ref = 0
			vec.Or = false
			vec.Typ = typ
			nulls.Reset(vec.Nsp)
			vec.Data = vec.Data[:size]
			proc.Reg.Vecs[i] = proc.Reg.Vecs[len(proc.Reg.Vecs)-1]
			proc.Reg.Vecs = proc.Reg.Vecs[:len(proc.Reg.Vecs)-1]
			return vec, nil
		}
	}
	data, err := mheap.Alloc(proc.Mp, size)
	if err != nil {
		return nil, err
	}
	vec := vector.New(typ)
	vec.Data = data
	return vec, nil
}

func Put(proc *Process, vec *vector.Vector) {
	proc.Reg.Vecs = append(proc.Reg.Vecs, vec)
}

func FreeRegisters(proc *Process) {
	for _, vec := range proc.Reg.Vecs {
		vec.Ref = 0
		vector.Free(vec, proc.Mp)
	}
	proc.Reg.Vecs = proc.Reg.Vecs[:0]
}
