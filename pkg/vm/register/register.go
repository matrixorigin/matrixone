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

package register

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
)

func Get(proc *process.Process, size int64, typ types.Type) (*vector.Vector, error) {
	for i, t := range proc.Reg.Ts {
		v := t.(*vector.Vector)
		if int64(cap(v.Data[mempool.CountSize:])) >= size {
			vec := vector.New(typ)
			vec.Data = v.Data
			proc.Reg.Ts = append(proc.Reg.Ts[:i], proc.Reg.Ts[i+1:])
			return vec, nil
		}
	}
	data, err := proc.Alloc(size)
	if err != nil {
		return nil, err
	}
	vec := vector.New(typ)
	vec.Data = data
	return vec, nil
}

func Put(proc *process.Process, vec *vector.Vector) {
	proc.Reg.Ts = append(proc.Reg.Ts, vec)
}

func FreeRegisters(proc *process.Process) {
	var vec *vector.Vector

	for _, t := range proc.Reg.Ts {
		vec = t.(*vector.Vector)
		vec.Free(proc)
	}
	proc.Reg.Ts = proc.Reg.Ts[:0]
}
