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

func init() {
	OneInt64s = make([]int64, UnitLimit)
	for i := range OneInt64s {
		OneInt64s[i] = 1
	}
}

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(" ⨝ ")
}

func Prepare(proc *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.ctr = new(Container)
	{
		n.ctr.result = make(map[string]uint8)
		for _, attr := range n.Result {
			n.ctr.result[attr] = 0
		}
	}
	n.ctr.zs = make([]int64, UnitLimit)
	n.ctr.sels = make([]int64, UnitLimit)
	n.ctr.views = make([]*view, len(n.Vars))
	{
		n.ctr.mx = make([][]int64, len(n.Vars)+1)
		for i := range n.ctr.mx {
			n.ctr.mx[i] = make([]int64, UnitLimit)
		}
		n.ctr.mx0, n.ctr.mx1 = make([][]int64, len(n.Vars)+1), make([][]int64, len(n.Vars)+1)
		for i := range n.ctr.mx0 {
			n.ctr.mx0[i] = make([]int64, UnitLimit)
		}
		for i := range n.ctr.mx1 {
			n.ctr.mx1[i] = make([]int64, UnitLimit)
		}
	}
	n.ctr.zValues = make([]int64, UnitLimit)
	n.ctr.hashes = make([]uint64, UnitLimit)
	n.ctr.h8.keys = make([]uint64, UnitLimit)
	n.ctr.hstr.keys = make([][]byte, UnitLimit)
	n.ctr.strHashStates = make([][3]uint64, UnitLimit)
	n.ctr.isPure = true
	for i := 0; i < len(n.ctr.views); i++ {
		n.ctr.views[i] = new(view)
		n.ctr.views[i].attrs = n.Vars[i]
		n.ctr.views[i].values = make([]uint64, UnitLimit)
		n.ctr.views[i].vecs = make([]*vector.Vector, len(n.Vars[i]))

		n.ctr.views[i].bat = n.Bats[i]
		ht := n.Bats[i].Ht.(*HashTable)
		n.ctr.views[i].sels = ht.Sels
		n.ctr.views[i].isPure = true
		for _, sel := range ht.Sels {
			if len(sel) > 1 {
				n.ctr.isPure = false
				n.ctr.views[i].isPure = false
				break
			}
			if n.Bats[i].Zs[sel[0]] > 1 {
				n.ctr.views[i].isPure = false
			}
		}
		n.ctr.views[i].intHashMap = ht.IntHashMap
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
		ctr.oattrs = append(ctr.oattrs, bat.Attrs...)
		for i, attr := range bat.Attrs {
			if _, ok := ctr.result[attr]; ok {
				ctr.ois = append(ctr.ois, i)
				ctr.is = append(ctr.is, len(ctr.attrs))
				ctr.attrs = append(ctr.attrs, attr)
			}
		}
		for _, v := range ctr.views {
			for i := len(v.attrs); i < len(v.bat.Attrs); i++ {
				if _, ok := ctr.result[v.bat.Attrs[i]]; ok {
					v.ois = append(v.ois, i)
					v.is = append(v.is, len(ctr.attrs))
					ctr.attrs = append(ctr.attrs, v.bat.Attrs[i])
				}
			}
		}
	} else {
		batch.Reorder(bat, ctr.oattrs)
	}
	rbat := batch.New(true, ctr.attrs)
	{ // construct result batch
		for i, j := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			rbat.Vecs[j] = vector.New(vec.Typ)
			vector.PreAlloc(rbat.Vecs[j], vec, len(bat.Zs), proc.Mp)
		}
		for _, v := range ctr.views {
			for i, j := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				rbat.Vecs[j] = vector.New(vec.Typ)
				vector.PreAlloc(rbat.Vecs[j], vec, len(bat.Zs), proc.Mp)
			}
		}
	}
	for _, v := range ctr.views {
		for i, attr := range v.attrs {
			v.vecs[i] = batch.GetVector(bat, attr)
		}
	}
	count := len(bat.Zs)
	if ctr.isPure {
		for i := 0; i < count; i += UnitLimit {
			n := count - i
			if n > UnitLimit {
				n = UnitLimit
			}
			for _, v := range ctr.views {
				if err := ctr.probeView(int(i), int(n), bat, v); err != nil {
					return err
				}
			}
			if err := ctr.processPureJoin(int(n), int(i), bat, rbat, proc); err != nil {
				batch.Clean(rbat, proc.Mp)
				return err
			}
		}
	} else {
		for i := 0; i < count; i += UnitLimit {
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
	}
	proc.Reg.InputBatch = rbat
	return nil
}

func (ctr *Container) processPureJoin(n, start int, bat, rbat *batch.Batch, proc *process.Process) error {
	{
		var flg bool

		for i := 0; i < n; i++ {
			flg = false
			for _, v := range ctr.views {
				val := v.values[i]
				if val == 0 {
					flg = true
					break
				}
				if !v.isPure {
					bat.Zs[i+start] *= v.bat.Zs[v.sels[val-1][0]]
				}
			}
			if flg {
				bat.Zs[i+start] = 0
			}
		}
	}
	if len(ctr.is) > 0 {
		ctr.sels = ctr.sels[:0]
		for i := 0; i < n; i++ {
			if bat.Zs[i+start] == 0 {
				continue
			}
			rbat.Zs = append(rbat.Zs, bat.Zs[i+start])
			ctr.sels = append(ctr.sels, int64(i+start))
		}
		for i, j := range ctr.is {
			for _, sel := range ctr.sels {
				if err := vector.UnionOne(rbat.Vecs[j], bat.Vecs[ctr.ois[i]], sel, proc.Mp); err != nil {
					return err
				}
			}
		}
	} else {
		for i := 0; i < n; i++ {
			if bat.Zs[i+start] == 0 {
				continue
			}
			rbat.Zs = append(rbat.Zs, bat.Zs[i+start])
		}
	}
	for _, v := range ctr.views {
		if len(v.is) > 0 {
			ctr.sels = ctr.sels[:0]
			for i := 0; i < n; i++ {
				if bat.Zs[i+start] == 0 {
					continue
				}
				row := v.sels[v.values[i]-1][0]
				ctr.sels = append(ctr.sels, row)
			}
			for i, j := range v.is {
				for _, sel := range ctr.sels {
					if err := vector.UnionOne(rbat.Vecs[j], v.bat.Vecs[v.ois[i]], sel, proc.Mp); err != nil {
						return err
					}
				}
			}
		}
	}
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
	for j := range ctr.mx {
		ctr.mx[j] = ctr.mx[j][:0]
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
		for j := range ctr.mx {
			ctr.mx[j] = append(ctr.mx[j], mx[j]...)
		}
	}
	if cap(ctr.zs) < len(ctr.mx[0]) {
		ctr.zs = make([]int64, len(ctr.mx[0]))
	}
	ctr.zs = ctr.zs[:len(ctr.mx[0])]
	for j, rows := range ctr.mx {
		if j == 0 {
			for i, k := range ctr.is {
				for _, row := range rows {
					if err := vector.UnionOne(rbat.Vecs[k], bat.Vecs[ctr.ois[i]], row, proc.Mp); err != nil {
						return err
					}
				}
			}
			for x, row := range rows {
				ctr.zs[x] = bat.Zs[row]
			}
		} else {
			v := ctr.views[j-1]
			for i, k := range v.is {
				for _, row := range rows {
					if err := vector.UnionOne(rbat.Vecs[k], v.bat.Vecs[ctr.ois[i]], row, proc.Mp); err != nil {
						return err
					}
				}
			}
			for x, row := range rows {
				ctr.zs[x] *= v.bat.Zs[row]
			}
		}
	}
	rbat.Zs = append(rbat.Zs, ctr.zs...)
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
	if len(v.vecs) == 1 {
		return ctr.probeViewWithOneVar(i, n, bat, v)
	}
	copy(ctr.zValues[:n], OneInt64s[:n])
	for _, vec := range v.vecs {
		switch vec.Typ.Oid {
		case types.T_int8:
			vs := vec.Col.([]int8)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*1:(i+k+1)*1]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*1:(i+k+1)*1]...)
					}
				}
			}
		case types.T_int16:
			vs := vec.Col.([]int16)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*2:(i+k+1)*2]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*2:(i+k+1)*2]...)
					}
				}
			}
		case types.T_int32:
			vs := vec.Col.([]int32)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				}
			}
		case types.T_int64:
			vs := vec.Col.([]int64)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				}
			}
		case types.T_uint8:
			vs := vec.Col.([]uint8)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*1:(i+k+1)*1]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*1:(i+k+1)*1]...)
					}
				}
			}
		case types.T_uint16:
			vs := vec.Col.([]uint16)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*2:(i+k+1)*2]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*2:(i+k+1)*2]...)
					}
				}
			}
		case types.T_uint32:
			vs := vec.Col.([]uint32)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				}
			}
		case types.T_uint64:
			vs := vec.Col.([]uint64)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				}
			}
		case types.T_date:
			vs := vec.Col.([]types.Date)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				}
			}
		case types.T_datetime:
			vs := vec.Col.([]types.Datetime)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				}
			}
		case types.T_float32:
			vs := vec.Col.([]float32)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
					}
				}
			}
		case types.T_float64:
			vs := vec.Col.([]float64)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
					}
				}
			}
		case types.T_char, types.T_varchar:
			vs := vec.Col.(*types.Bytes)
			if !nulls.Any(vec.Nsp) {
				for k := 0; k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(i+k))...)
				}
			} else {
				for k := 0; k < n; k++ {
					if vec.Nsp.Np.Contains(uint64(i + k)) {
						ctr.zValues[i] = 0
					} else {
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

func (ctr *Container) probeViewWithOneVar(i, n int, bat *batch.Batch, v *view) error {
	vec := v.vecs[0]
	switch vec.Typ.Oid {
	case types.T_int8:
		vs := vec.Col.([]int8)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_uint8:
		vs := vec.Col.([]uint8)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_int16:
		vs := vec.Col.([]int16)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_uint16:
		vs := vec.Col.([]uint16)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_int32:
		vs := vec.Col.([]int32)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_uint32:
		vs := vec.Col.([]uint32)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_int64:
		vs := vec.Col.([]int64)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_uint64:
		vs := vec.Col.([]uint64)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_float32:
		vs := vec.Col.([]float32)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_date:
		vs := vec.Col.([]types.Date)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_datetime:
		vs := vec.Col.([]types.Datetime)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.h8.keys[k] = uint64(vs[i+k])
			}
			ctr.hashes[0] = 0
			v.intHashMap.FindBatchWithRing(n, ctr.zValues, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), v.values)
		}
	case types.T_char, types.T_varchar:
		vs := vec.Col.(*types.Bytes)
		if !nulls.Any(vec.Nsp) {
			for k := 0; k < n; k++ {
				ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(i+k))...)
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
		} else {
			copy(ctr.zValues[:n], OneInt64s[:n])
			for k := 0; k < n; k++ {
				if vec.Nsp.Np.Contains(uint64(i + k)) {
					ctr.zValues[i] = 0
				}
				ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(i+k))...)
			}
			for k := 0; k < n; k++ {
				if l := len(ctr.hstr.keys[k]); l < 16 {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], hashtable.StrKeyPadding[l:]...)
				}
			}
			v.strHashMap.FindStringBatchWithRing(ctr.strHashStates, ctr.zValues, ctr.hstr.keys[:n], v.values)
			for k := 0; k < n; k++ {
				ctr.hstr.keys[k] = ctr.hstr.keys[k][:0]
			}
		}

	}
	return nil
}
