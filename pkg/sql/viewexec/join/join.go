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

package join

import (
	"bytes"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(" ⨝ ")
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.ctr = new(Container)
	n.ctr.zs = make([]int64, UnitLimit)
	n.ctr.views = make([]*view, len(n.Vars))
	{
		n.ctr.mx0, n.ctr.mx1 = make([][]int64, len(n.Vars)+1), make([][]int64, len(n.Vars)+1)
		for i := range n.ctr.mx0 {
			n.ctr.mx0[i] = make([]int64, UnitLimit)
		}
		for i := range n.ctr.mx1 {
			n.ctr.mx1[i] = make([]int64, UnitLimit)
		}
	}
	n.ctr.hstr.keys = make([][]byte, UnitLimit)
	n.ctr.strHashStates = make([][3]uint64, UnitLimit)
	for i := 0; i < len(n.ctr.views); i++ {
		n.ctr.views[i] = new(view)
		n.ctr.views[i].attrs = n.Vars[i]
		n.ctr.views[i].values = make([]uint64, UnitLimit)
		n.ctr.views[i].vecs = make([]*vector.Vector, len(n.Vars[i]))

		n.ctr.views[i].bat = n.Bats[i]
		ht := n.Bats[i].Ht.(*HashTable)
		n.ctr.views[i].sels = ht.Sels
		n.ctr.views[i].strHashMap = ht.StrHashMap
	}
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil {
		return false, nil
	}
	if len(bat.Zs) == 0 {
		proc.Reg.InputBatch = &batch.Batch{}
		return false, nil
	}
	if err := n.ctr.probe(bat, proc); err != nil {
		proc.Reg.InputBatch = nil
		return true, err
	}
	return false, nil
}

func (ctr *Container) probe(bat *batch.Batch, proc *process.Process) error {
	defer batch.Clean(bat, proc.Mp)
	if len(ctr.attrs) == 0 {
		ctr.attrs = append(ctr.attrs, bat.Attrs...)
		for _, v := range ctr.views {
			for _ = range v.attrs {
				v.is = append(v.is, -1)
			}
			for i := len(v.attrs); i < len(v.bat.Attrs); i++ {
				v.is = append(v.is, len(ctr.attrs))
				ctr.attrs = append(ctr.attrs, v.bat.Attrs[i])
			}
		}
	} else {
		batch.Reorder(bat, ctr.attrs[:len(bat.Attrs)])
	}
	rbat := batch.New(true, ctr.attrs)
	{ // construct result batch
		for k, vec := range bat.Vecs {
			rbat.Vecs[k] = vector.New(vec.Typ)
		}
		for i, r := range bat.Rs {
			rbat.Rs = append(rbat.Rs, r.Dup())
			rbat.As = append(rbat.As, bat.As[i])
			rbat.Refs = append(rbat.Refs, bat.Refs[i])
		}
		for _, v := range ctr.views {
			for k := len(v.attrs); k < len(v.bat.Attrs); k++ {
				rbat.Vecs[v.is[k]] = vector.New(v.bat.Vecs[k].Typ)
			}
			if len(v.ris) == 0 {
				for i, r := range v.bat.Rs {
					v.ris = append(v.ris, len(rbat.Rs))
					rbat.Rs = append(rbat.Rs, r.Dup())
					rbat.As = append(rbat.As, v.bat.As[i])
					rbat.Refs = append(rbat.Refs, v.bat.Refs[i])
				}
			} else {
				for i, r := range v.bat.Rs {
					rbat.Rs = append(rbat.Rs, r.Dup())
					rbat.As = append(rbat.As, v.bat.As[i])
					rbat.Refs = append(rbat.Refs, v.bat.Refs[i])
				}
			}
		}
	}
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		for _, v := range ctr.views {
			if err := ctr.probeView(int(i), int(n), bat, v); err != nil {
				return err
			}
		}
		if err := ctr.processJoin(int(n), int(i), bat, rbat, proc); err != nil {
			batch.Clean(rbat, proc.Mp)
			return err
		}
	}
	proc.Reg.InputBatch = rbat
	return nil
}

func (ctr *Container) processJoin(n, start int, bat, rbat *batch.Batch, proc *process.Process) error {
	{
		var flg bool

		for i := 0; i < n; i++ {
			flg = false
			for _, v := range ctr.views {
				if v.values[i] == 0 {
					flg = true
					break
				}
			}
			if flg {
				bat.Zs[i+start] = 0
			}
		}
	}
	for i := 0; i < n; i++ {
		if bat.Zs[i+start] == 0 {
			continue
		}
		mx := ctr.mx0[:1]
		{
			mx[0] = mx[0][:0]
			mx[0] = append(mx[0], int64(i+start))
		}
		for _, v := range ctr.views {
			mx = ctr.dupMatrix(ctr.product(mx, v.sels[v.values[i]-1]))
		}
		if cap(ctr.zs) < len(mx[0]) {
			ctr.zs = make([]int64, len(mx[0]))
		}
		oldlen := len(rbat.Zs)
		ctr.zs = ctr.zs[:len(mx[0])]
		for j, col := range mx { // fill vector
			if j == 0 {
				for k, vec := range bat.Vecs {
					for x, row := range col {
						if err := vector.UnionOne(rbat.Vecs[k], vec, row, proc.Mp); err != nil {
							return err
						}
						ctr.zs[x] = bat.Zs[row]
					}
				}
			} else {
				v := ctr.views[j-1]
				for k := len(v.attrs); k < len(v.bat.Attrs); k++ {
					vec := v.bat.Vecs[k]
					for x, row := range col {
						if err := vector.UnionOne(rbat.Vecs[v.is[k]], vec, row, proc.Mp); err != nil {
							return err
						}
						ctr.zs[x] *= v.bat.Zs[row]
					}
				}
			}
		}
		rbat.Zs = append(rbat.Zs, ctr.zs...)
		for j, col := range mx { // fill ring
			if j == 0 {
				for k, r := range bat.Rs {
					if err := rbat.Rs[k].Grows(len(col), proc.Mp); err != nil {
						return err
					}
					for ri, row := range col {
						rbat.Rs[k].Mul(r, int64(oldlen+ri), row, rbat.Zs[oldlen+ri]/bat.Zs[row])
					}
				}
			} else {
				v := ctr.views[j-1]
				for k, r := range v.bat.Rs {
					if err := rbat.Rs[v.ris[k]].Grows(len(col), proc.Mp); err != nil {
						return err
					}
					for ri, row := range col {
						rbat.Rs[v.ris[k]].Mul(r, int64(oldlen+ri), row, rbat.Zs[oldlen+ri]/v.bat.Zs[row])
					}
				}
			}
		}
	}
	return nil
}

func (ctr *Container) product(xs [][]int64, ys []int64) [][]int64 {
	rs := ctr.mx1[:len(xs)+1]
	{ // reset
		for i := range rs {
			rs[i] = rs[i][:0]
		}
	}
	for _, y := range ys {
		for i := range xs {
			rs[i] = append(rs[i], xs[i]...)
		}
		for i := 0; i < len(xs[0]); i++ {
			rs[len(xs)] = append(rs[len(xs)], y)
		}
	}
	return rs
}

func (ctr *Container) dupMatrix(xs [][]int64) [][]int64 {
	mx := ctr.mx0[:len(xs)]
	for i, x := range xs {
		mx[i] = append(xs[i][:0], x...)
	}
	return mx
}

func (ctr *Container) probeView(i, n int, bat *batch.Batch, v *view) error {
	for i, attr := range v.attrs {
		v.vecs[i] = batch.GetVector(bat, attr)
	}
	for _, vec := range v.vecs {
		switch vec.Typ.Oid {
		case types.T_int8:
			vs := vec.Col.([]int8)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*1:(i+k+1)*1]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*1:(i+k+1)*1]...)
					}
				}
			}
		case types.T_int16:
			vs := vec.Col.([]int16)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*2:(i+k+1)*2]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*2:(i+k+1)*2]...)
					}
				}
			}
		case types.T_int32:
			vs := vec.Col.([]int32)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				}
			}
		case types.T_int64:
			vs := vec.Col.([]int64)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				}
			}
		case types.T_uint8:
			vs := vec.Col.([]uint8)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*1:(i+k+1)*1]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*1:(i+k+1)*1]...)
					}
				}
			}
		case types.T_uint16:
			vs := vec.Col.([]uint16)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*2:(i+k+1)*2]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*2:(i+k+1)*2]...)
					}
				}
			}
		case types.T_uint32:
			vs := vec.Col.([]uint32)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				}
			}
		case types.T_uint64:
			vs := vec.Col.([]uint64)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				}
			}
		case types.T_date:
			vs := vec.Col.([]types.Date)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				}
			}
		case types.T_datetime:
			vs := vec.Col.([]types.Datetime)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				}
			}
		case types.T_float32:
			vs := vec.Col.([]float32)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				}
			}
		case types.T_float64:
			vs := vec.Col.([]float64)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				}
			}
		case types.T_char, types.T_varchar:
			vs := vec.Col.(*types.Bytes)
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(i+k))...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(i+k))...)
					}
				}
			}
		}
	}
	for k := 0; k < n; k++ {
		if l := len(ctr.hstr.keys[k]); l < 16 {
			ctr.hstr.keys[k] = append(ctr.hstr.keys[k], hashtable.StrKeyPadding[l:]...)
		}
	}
	v.strHashMap.FindStringBatch(ctr.strHashStates, ctr.hstr.keys[:n], v.values)
	for k := 0; k < n; k++ {
		ctr.hstr.keys[k] = ctr.hstr.keys[k][:0]
	}
	return nil
}
