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

package times

import (
	"bytes"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/join"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
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
	n.ctr.pctr = new(probeContainer)
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
	n.ctr.keyOffs = make([]uint32, UnitLimit)
	n.ctr.zKeyOffs = make([]uint32, UnitLimit)
	n.ctr.values = make([]uint64, UnitLimit)
	n.ctr.zValues = make([]int64, UnitLimit)
	n.ctr.hashes = make([]uint64, UnitLimit)
	n.ctr.h8.keys = make([]uint64, UnitLimit)
	n.ctr.h8.zKeys = make([]uint64, UnitLimit)
	n.ctr.hstr.keys = make([][]byte, UnitLimit)
	n.ctr.strHashStates = make([][3]uint64, UnitLimit)
	n.ctr.isPure = true
	for i := 0; i < len(n.ctr.views); i++ {
		n.ctr.views[i] = new(view)
		n.ctr.views[i].attrs = n.Vars[i]
		n.ctr.views[i].values = make([]uint64, UnitLimit)
		n.ctr.views[i].vecs = make([]*vector.Vector, len(n.Vars[i]))

		n.ctr.views[i].bat = n.Bats[i]
		ht := n.Bats[i].Ht.(*join.HashTable)
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
		if n.ctr.pctr.bat != nil {
			proc.Reg.InputBatch = n.ctr.pctr.bat
			n.ctr.pctr.bat = nil
		}
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
	if ctr.attrs == nil {
		ctr.attrs = make([]string, 0, 4)
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
		ctr.pctr.bat = batch.New(true, ctr.attrs)
		{ // construct result batch
			for i, j := range ctr.is {
				vec := bat.Vecs[ctr.ois[i]]
				ctr.pctr.bat.Vecs[j] = vector.New(vec.Typ)
				vector.PreAlloc(ctr.pctr.bat.Vecs[j], vec, len(bat.Zs), proc.Mp)
			}
			for _, v := range ctr.views {
				for i, j := range v.is {
					vec := v.bat.Vecs[v.ois[i]]
					ctr.pctr.bat.Vecs[j] = vector.New(vec.Typ)
					vector.PreAlloc(ctr.pctr.bat.Vecs[j], vec, len(bat.Zs), proc.Mp)
				}
				for i, r := range v.bat.Rs {
					ctr.pctr.bat.Rs = append(ctr.pctr.bat.Rs, r.Dup())
					ctr.pctr.bat.As = append(ctr.pctr.bat.As, v.bat.As[i])
					ctr.pctr.bat.Refs = append(ctr.pctr.bat.Refs, v.bat.Refs[i])
				}
			}
			for i, r := range bat.Rs {
				ctr.pctr.bat.Rs = append(ctr.pctr.bat.Rs, r.Dup())
				ctr.pctr.bat.As = append(ctr.pctr.bat.As, bat.As[i])
				ctr.pctr.bat.Refs = append(ctr.pctr.bat.Refs, bat.Refs[i])
			}
		}
		size := 0
		for _, vec := range ctr.pctr.bat.Vecs {
			switch vec.Typ.Oid {
			case types.T_int8, types.T_uint8:
				size += 1 + 1
			case types.T_int16, types.T_uint16:
				size += 2 + 1
			case types.T_int32, types.T_uint32, types.T_float32, types.T_date:
				size += 4 + 1
			case types.T_int64, types.T_uint64, types.T_float64, types.T_datetime:
				size += 8 + 1
			case types.T_char, types.T_varchar:
				if width := vec.Typ.Width; width > 0 {
					size += int(width) + 1
				} else {
					size = 128
				}
			}
		}
		switch {
		case size == 0:
			ctr.pctr.typ = H0
			for _, r := range ctr.pctr.bat.Rs {
				if err := r.Grow(proc.Mp); err != nil {
					return err
				}
			}
		case size <= 8:
			ctr.pctr.typ = H8
			ctr.pctr.intHashMap = &hashtable.Int64HashMap{}
			ctr.pctr.intHashMap.Init()
			ctr.pctr.bat.Ht = ctr.pctr.intHashMap
		case size <= 24:
			ctr.pctr.typ = H24
			ctr.h24.keys = make([][3]uint64, UnitLimit)
			ctr.h24.zKeys = make([][3]uint64, UnitLimit)
			ctr.pctr.strHashMap = &hashtable.StringHashMap{}
			ctr.pctr.strHashMap.Init()
			ctr.pctr.bat.Ht = ctr.pctr.strHashMap
		case size <= 32:
			ctr.pctr.typ = H32
			ctr.h32.keys = make([][4]uint64, UnitLimit)
			ctr.h32.zKeys = make([][4]uint64, UnitLimit)
			ctr.pctr.strHashMap = &hashtable.StringHashMap{}
			ctr.pctr.strHashMap.Init()
			ctr.pctr.bat.Ht = ctr.pctr.strHashMap
		case size <= 40:
			ctr.pctr.typ = H40
			ctr.h40.keys = make([][5]uint64, UnitLimit)
			ctr.h40.zKeys = make([][5]uint64, UnitLimit)
			ctr.pctr.strHashMap = &hashtable.StringHashMap{}
			ctr.pctr.strHashMap.Init()
			ctr.pctr.bat.Ht = ctr.pctr.strHashMap
		default:
			ctr.pctr.typ = HStr
			ctr.pctr.strHashMap = &hashtable.StringHashMap{}
			ctr.pctr.strHashMap.Init()
			ctr.pctr.bat.Ht = ctr.pctr.strHashMap
		}
	} else {
		batch.Reorder(bat, ctr.oattrs[:len(bat.Attrs)])
	}
	for _, v := range ctr.views {
		for i, attr := range v.attrs {
			v.vecs[i] = batch.GetVector(bat, attr)
		}
	}
	switch ctr.pctr.typ {
	case H0:
		if ctr.isPure {
			return ctr.processPureH0(bat, proc)
		}
		return ctr.processH0(bat, proc)
	case H8:
		if ctr.isPure {
			return ctr.processPureH8(bat, proc)
		}
		return ctr.processH8(bat, proc)
	case H24:
		if ctr.isPure {
			return ctr.processPureH24(bat, proc)
		}
		return ctr.processH24(bat, proc)
	case H32:
		if ctr.isPure {
			return ctr.processPureH32(bat, proc)
		}
		return ctr.processH32(bat, proc)
	case H40:
		if ctr.isPure {
			return ctr.processPureH40(bat, proc)
		}
		return ctr.processH40(bat, proc)
	default:
		if ctr.isPure {
			return ctr.processPureHStr(bat, proc)
		}
		return ctr.processHStr(bat, proc)
	}
}

func (ctr *Container) processH0(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processJoinH0(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processPureH0(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processPureJoinH0(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processH8(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processJoinH8(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processPureH8(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processPureJoinH8(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processH24(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processJoinH24(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processPureH24(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processPureJoinH24(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processH32(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processJoinH32(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processPureH32(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processPureJoinH32(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processH40(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processJoinH40(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processPureH40(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processPureJoinH40(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processHStr(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processJoinHStr(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processPureHStr(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
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
		if err := ctr.processPureJoinHStr(n, i, bat, proc); err != nil {
			batch.Clean(ctr.pctr.bat, proc.Mp)
			ctr.pctr.bat = nil
			return err
		}
	}
	proc.Reg.InputBatch = &batch.Batch{}
	return nil
}

func (ctr *Container) processJoinH0(n, start int, bat *batch.Batch, proc *process.Process) error {
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
			for x, row := range rows {
				ctr.zs[x] = bat.Zs[row]
			}
		} else {
			v := ctr.views[j-1]
			for x, row := range rows {
				ctr.zs[x] *= v.bat.Zs[row]
			}
		}
	}
	vecs := bat.Ht.([]*vector.Vector)
	for j, rows := range ctr.mx {
		if j == 0 {
			for k := range bat.Rs {
				for x, row := range rows {
					ctr.pctr.bat.Rs[k].Fill(0, row, ctr.zs[x]/bat.Zs[row], vecs[k])
				}
			}
		} else {
			v := ctr.views[j-1]
			for k, r := range v.bat.Rs {
				for _, row := range rows {
					ctr.pctr.bat.Rs[k].Add(r, 0, row)
				}
			}
		}
	}
	if len(ctr.pctr.bat.Zs) == 0 {
		ctr.pctr.bat.Zs = make([]int64, 1)
	}
	for _, z := range ctr.zs {
		ctr.pctr.bat.Zs[0] += z
	}
	return nil
}

func (ctr *Container) processPureJoinH0(n, start int, bat *batch.Batch, proc *process.Process) error {
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
	if len(ctr.pctr.bat.Zs) == 0 {
		ctr.pctr.bat.Zs = make([]int64, 1)
	}
	ctr.sels = ctr.sels[:0]
	for i := 0; i < n; i++ {
		if bat.Zs[i+start] == 0 {
			continue
		}
		ctr.sels = append(ctr.sels, int64(i+start))
		ctr.pctr.bat.Zs[0] += bat.Zs[i+start]
	}
	vecs := bat.Ht.([]*vector.Vector)
	for k := range bat.Rs {
		for _, sel := range ctr.sels {
			ctr.pctr.bat.Rs[k].Fill(0, sel, bat.Zs[sel], vecs[k])
		}
	}
	for _, v := range ctr.views {
		if len(v.bat.Rs) > 0 {
			ctr.sels = ctr.sels[:0]
			for i := 0; i < n; i++ {
				if bat.Zs[i+start] == 0 {
					continue
				}
				row := v.sels[v.values[i]-1][0]
				ctr.sels = append(ctr.sels, row)
			}
			for k, r := range v.bat.Rs {
				for _, sel := range ctr.sels {
					ctr.pctr.bat.Rs[v.ris[k]].Add(r, 0, sel)
				}
			}
		}
	}
	return nil
}

func (ctr *Container) processJoinH8(n, start int, bat *batch.Batch, proc *process.Process) error {
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
			for x, row := range rows {
				ctr.zs[x] = bat.Zs[row]
			}
		} else {
			v := ctr.views[j-1]
			for x, row := range rows {
				ctr.zs[x] *= v.bat.Zs[row]
			}
		}
	}
	copy(ctr.keyOffs, ctr.zKeyOffs)
	copy(ctr.h8.keys, ctr.h8.zKeys)
	{ // fill group
		rows := ctr.mx[0]
		if cap(ctr.h8.keys) < len(rows) {
			ctr.h8.keys = make([]uint64, len(rows))
		}
		if cap(ctr.keyOffs) < len(rows) {
			ctr.keyOffs = make([]uint32, len(rows))
		}
		ctr.h8.keys = ctr.h8.keys[:len(rows)]
		ctr.keyOffs = ctr.keyOffs[:len(rows)]
		for i, _ := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vec.Col.([]int8)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_uint8:
				vs := vec.Col.([]uint8)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_int16:
				vs := vec.Col.([]int16)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_uint16:
				vs := vec.Col.([]uint16)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_int32:
				vs := vec.Col.([]int32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_uint32:
				vs := vec.Col.([]uint32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_int64:
				vs := vec.Col.([]int64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_uint64:
				vs := vec.Col.([]uint64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_float32:
				vs := vec.Col.([]float32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_float64:
				vs := vec.Col.([]float64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_date:
				vs := vec.Col.([]types.Date)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_datetime:
				vs := vec.Col.([]types.Datetime)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vec.Col.(*types.Bytes)
				vData := vs.Data
				vOff := vs.Offsets
				vLen := vs.Lengths
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h8.keys[k])), 8)[ctr.keyOffs[k]+1:], vData[vOff[row]:vOff[row]+vLen[row]])
						ctr.keyOffs[k] += vLen[row] + 1
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h8.keys[k])), 8)[ctr.keyOffs[k]+1:], vData[vOff[row]:vOff[row]+vLen[row]])
							ctr.keyOffs[k] += vLen[row] + 1
						}
					}
				}
			}
		}
		for vi, v := range ctr.views {
			rows := ctr.mx[vi+1]
			for i, _ := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_date:
					vs := vec.Col.([]types.Date)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_datetime:
					vs := vec.Col.([]types.Datetime)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_char, types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					vData := vs.Data
					vOff := vs.Offsets
					vLen := vs.Lengths
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h8.keys[k])), 8)[ctr.keyOffs[k]+1:], vData[vOff[row]:vOff[row]+vLen[row]])
							ctr.keyOffs[k] += vLen[row] + 1
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h8.keys[k])), 8)[ctr.keyOffs[k]+1:], vData[vOff[row]:vOff[row]+vLen[row]])
								ctr.keyOffs[k] += vLen[row] + 1
							}
						}
					}
				}
			}
		}
	}
	ctr.hashes[0] = 0
	vecs := bat.Ht.([]*vector.Vector)
	if cap(ctr.values) < len(ctr.zs) {
		ctr.values = make([]uint64, len(ctr.zs))
	}
	ctr.values = ctr.values[:len(ctr.zs)]
	ctr.pctr.intHashMap.InsertBatch(len(ctr.zs), ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values)
	{ // batch
		for k, v := range ctr.values[:len(ctr.zs)] {
			if v > ctr.pctr.rows {
				ctr.pctr.rows++
				{ // fill vector
					row := ctr.mx[0][k]
					for i, j := range ctr.is {
						if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], bat.Vecs[ctr.ois[i]], row, proc.Mp); err != nil {
							return err
						}
					}
					for vi, v := range ctr.views {
						row := ctr.mx[vi+1][k]
						for i, j := range v.is {
							if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], v.bat.Vecs[v.ois[i]], row, proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				{ // grow ring
					for i := range bat.Rs {
						if err := ctr.pctr.bat.Rs[i].Grow(proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						for i := range v.bat.Rs {
							if err := ctr.pctr.bat.Rs[v.ris[i]].Grow(proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				ctr.pctr.bat.Zs = append(ctr.pctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			{ // fill ring
				for i := range bat.Rs {
					row := ctr.mx[0][k]
					ctr.pctr.bat.Rs[i].Fill(ai, row, ctr.zs[k]/bat.Zs[row], vecs[i])
				}
				for i, v := range ctr.views {
					row := ctr.mx[i+1][k]
					for j, r := range v.bat.Rs {
						ctr.pctr.bat.Rs[v.ris[j]].Mul(r, ai, row, ctr.zs[k]/v.bat.Zs[row])
					}
				}
			}
			ctr.pctr.bat.Zs[ai] += ctr.zs[k]
		}
	}
	return nil
}

func (ctr *Container) processPureJoinH8(n, start int, bat *batch.Batch, proc *process.Process) error {
	{
		var flg bool

		ctr.zs = ctr.zs[:n]
		for i := 0; i < n; i++ {
			flg = false
			ctr.zs[i] = bat.Zs[i+start]
			for _, v := range ctr.views {
				if v.values[i] == 0 {
					flg = true
					break
				}
				if !v.isPure {
					ctr.zs[i] *= v.bat.Zs[v.sels[v.values[i]-1][0]]
				}
			}
			if flg {
				ctr.zs[i] = 0
				bat.Zs[i+start] = 0
			}
		}
	}
	copy(ctr.keyOffs, ctr.zKeyOffs)
	copy(ctr.h8.keys, ctr.h8.zKeys)
	{ // fill group
		for i, _ := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vec.Col.([]int8)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_uint8:
				vs := vec.Col.([]uint8)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_int16:
				vs := vec.Col.([]int16)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_uint16:
				vs := vec.Col.([]uint16)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_int32:
				vs := vec.Col.([]int32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_uint32:
				vs := vec.Col.([]uint32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_int64:
				vs := vec.Col.([]int64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_uint64:
				vs := vec.Col.([]uint64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_float32:
				vs := vec.Col.([]float32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_float64:
				vs := vec.Col.([]float64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_date:
				vs := vec.Col.([]types.Date)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_datetime:
				vs := vec.Col.([]types.Datetime)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vec.Col.(*types.Bytes)
				vData := vs.Data
				vOff := vs.Offsets
				vLen := vs.Lengths
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
						copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h8.keys[k])), 8)[ctr.keyOffs[k]+1:], vData[vOff[k+start]:vOff[k+start]+vLen[k+start]])
						ctr.keyOffs[k] += vLen[k+start] + 1
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
							copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h8.keys[k])), 8)[ctr.keyOffs[k]+1:], vData[vOff[k+start]:vOff[k+start]+vLen[k+start]])
							ctr.keyOffs[k] += vLen[k+start] + 1
						}
					}
				}
			}
		}
		for _, v := range ctr.views {
			for i, _ := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_date:
					vs := vec.Col.([]types.Date)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_datetime:
					vs := vec.Col.([]types.Datetime)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_char, types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					vData := vs.Data
					vOff := vs.Offsets
					vLen := vs.Lengths
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								row := v.sels[vp-1][0]
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h8.keys[k])), 8)[ctr.keyOffs[k]+1:], vData[vOff[row]:vOff[row]+vLen[row]])
								ctr.keyOffs[k] += vLen[row] + 1
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								row := v.sels[vp-1][0]
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = 0
								copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h8.keys[k])), 8)[ctr.keyOffs[k]+1:], vData[vOff[row]:vOff[row]+vLen[row]])
								ctr.keyOffs[k] += vLen[row] + 1
							}
						}
					}
				}
			}
		}
	}
	ctr.hashes[0] = 0
	vecs := bat.Ht.([]*vector.Vector)
	ctr.pctr.intHashMap.InsertBatchWithRing(n, ctr.zs, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values)
	{ // batch
		for k, v := range ctr.values[:n] {
			if ctr.zs[k] == 0 {
				continue
			}
			if v > ctr.pctr.rows {
				ctr.pctr.rows++
				{ // fill vector
					for i, j := range ctr.is {
						if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], bat.Vecs[ctr.ois[i]], int64(k+start), proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						if len(v.is) > 0 {
							row := v.sels[v.values[k]-1][0]
							for i, j := range v.is {
								if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], v.bat.Vecs[v.ois[i]], row, proc.Mp); err != nil {
									return err
								}
							}
						}
					}
				}
				{ // grow ring
					for i := range bat.Rs {
						if err := ctr.pctr.bat.Rs[i].Grow(proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						for i := range v.bat.Rs {
							if err := ctr.pctr.bat.Rs[v.ris[i]].Grow(proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				ctr.pctr.bat.Zs = append(ctr.pctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			{ // fill ring
				for i := range bat.Rs {
					ctr.pctr.bat.Rs[i].Fill(ai, int64(k+start), ctr.zs[k]/bat.Zs[k+start], vecs[i])
				}
				for _, v := range ctr.views {
					if len(v.bat.Rs) > 0 {
						row := v.sels[v.values[k]-1][0]
						for j, r := range v.bat.Rs {
							ctr.pctr.bat.Rs[v.ris[j]].Mul(r, ai, row, ctr.zs[k]/v.bat.Zs[row])
						}
					}
				}
			}
			ctr.pctr.bat.Zs[ai] += ctr.zs[k]
		}
	}
	return nil
}

func (ctr *Container) processJoinH24(n, start int, bat *batch.Batch, proc *process.Process) error {
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
			for x, row := range rows {
				ctr.zs[x] = bat.Zs[row]
			}
		} else {
			v := ctr.views[j-1]
			for x, row := range rows {
				ctr.zs[x] *= v.bat.Zs[row]
			}
		}
	}
	copy(ctr.keyOffs, ctr.zKeyOffs)
	copy(ctr.h24.keys, ctr.h24.zKeys)
	{ // fill group
		rows := ctr.mx[0]
		if cap(ctr.h24.keys) < len(rows) {
			ctr.h24.keys = make([][3]uint64, len(rows))
		}
		if cap(ctr.keyOffs) < len(rows) {
			ctr.keyOffs = make([]uint32, len(rows))
		}
		ctr.h24.keys = ctr.h24.keys[:len(rows)]
		ctr.keyOffs = ctr.keyOffs[:len(rows)]
		data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h24.keys[0])), cap(ctr.h24.keys)*24)[:len(ctr.h24.keys)*24]
		for i, _ := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vec.Col.([]int8)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_uint8:
				vs := vec.Col.([]uint8)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_int16:
				vs := vec.Col.([]int16)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_uint16:
				vs := vec.Col.([]uint16)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_int32:
				vs := vec.Col.([]int32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_uint32:
				vs := vec.Col.([]uint32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_int64:
				vs := vec.Col.([]int64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_uint64:
				vs := vec.Col.([]uint64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_float32:
				vs := vec.Col.([]float32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_float64:
				vs := vec.Col.([]float64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_date:
				vs := vec.Col.([]types.Date)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_datetime:
				vs := vec.Col.([]types.Datetime)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vec.Col.(*types.Bytes)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						key := vs.Get(int64(row))
						data[k*24+int(ctr.keyOffs[k])] = 0
						copy(data[k*24+int(ctr.keyOffs[k])+1:], key)
						ctr.keyOffs[k] += uint32(len(key)) + 1
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							data[k*24+int(ctr.keyOffs[k])] = 1
							ctr.keyOffs[k]++
						} else {
							key := vs.Get(row)
							data[k*24+int(ctr.keyOffs[k])] = 0
							copy(data[k*24+int(ctr.keyOffs[k])+1:], key)
							ctr.keyOffs[k] += uint32(len(key)) + 1
						}
					}
				}
			}
		}
		for vi, v := range ctr.views {
			rows := ctr.mx[vi+1]
			for i, _ := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_date:
					vs := vec.Col.([]types.Date)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_datetime:
					vs := vec.Col.([]types.Datetime)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_char, types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							key := vs.Get(row)
							data[k*24+int(ctr.keyOffs[k])] = 0
							copy(data[k*24+int(ctr.keyOffs[k])+1:], key)
							ctr.keyOffs[k] += uint32(len(key)) + 1
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								data[k*24+int(ctr.keyOffs[k])] = 1
								ctr.keyOffs[k]++
							} else {
								key := vs.Get(row)
								data[k*24+int(ctr.keyOffs[k])] = 0
								copy(data[k*24+int(ctr.keyOffs[k])+1:], key)
								ctr.keyOffs[k] += uint32(len(key)) + 1
							}
						}
					}
				}
			}
		}
	}
	ctr.hashes[0] = 0
	vecs := bat.Ht.([]*vector.Vector)
	if cap(ctr.values) < len(ctr.zs) {
		ctr.values = make([]uint64, len(ctr.zs))
	}
	if cap(ctr.strHashStates) < len(ctr.zs) {
		ctr.strHashStates = make([][3]uint64, len(ctr.zs))
	}
	ctr.values = ctr.values[:len(ctr.zs)]
	ctr.strHashStates = ctr.strHashStates[:len(ctr.zs)]
	ctr.pctr.strHashMap.InsertString24Batch(ctr.strHashStates, ctr.h24.keys[:len(ctr.zs)], ctr.values)
	{ // batch
		for k, v := range ctr.values[:len(ctr.zs)] {
			if v > ctr.pctr.rows {
				ctr.pctr.rows++
				{ // fill vector
					row := ctr.mx[0][k]
					for i, j := range ctr.is {
						if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], bat.Vecs[ctr.ois[i]], row, proc.Mp); err != nil {
							return err
						}
					}
					for vi, v := range ctr.views {
						row := ctr.mx[vi+1][k]
						for i, j := range v.is {
							if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], v.bat.Vecs[v.ois[i]], row, proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				{ // grow ring
					for i := range bat.Rs {
						if err := ctr.pctr.bat.Rs[i].Grow(proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						for i := range v.bat.Rs {
							if err := ctr.pctr.bat.Rs[v.ris[i]].Grow(proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				ctr.pctr.bat.Zs = append(ctr.pctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			{ // fill ring
				for i := range bat.Rs {
					row := ctr.mx[0][k]
					ctr.pctr.bat.Rs[i].Fill(ai, row, ctr.zs[k]/bat.Zs[row], vecs[i])
				}
				for i, v := range ctr.views {
					row := ctr.mx[i+1][k]
					for j, r := range v.bat.Rs {
						ctr.pctr.bat.Rs[v.ris[j]].Mul(r, ai, row, ctr.zs[k]/v.bat.Zs[row])
					}
				}
			}
			ctr.pctr.bat.Zs[ai] += ctr.zs[k]
		}
	}
	return nil
}

func (ctr *Container) processPureJoinH24(n, start int, bat *batch.Batch, proc *process.Process) error {
	{
		var flg bool

		ctr.zs = ctr.zs[:n]
		for i := 0; i < n; i++ {
			flg = false
			ctr.zs[i] = bat.Zs[i+start]
			for _, v := range ctr.views {
				if v.values[i] == 0 {
					flg = true
					break
				}
				if !v.isPure {
					ctr.zs[i] *= v.bat.Zs[v.sels[v.values[i]-1][0]]
				}
			}
			if flg {
				ctr.zs[i] = 0
				bat.Zs[i+start] = 0
			}
		}
	}
	copy(ctr.keyOffs, ctr.zKeyOffs)
	copy(ctr.h24.keys, ctr.h24.zKeys)
	data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h24.keys[0])), cap(ctr.h24.keys)*24)[:len(ctr.h24.keys)*24]
	{ // fill group
		for i, _ := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vec.Col.([]int8)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_uint8:
				vs := vec.Col.([]uint8)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_int16:
				vs := vec.Col.([]int16)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_uint16:
				vs := vec.Col.([]uint16)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_int32:
				vs := vec.Col.([]int32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_uint32:
				vs := vec.Col.([]uint32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_int64:
				vs := vec.Col.([]int64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_uint64:
				vs := vec.Col.([]uint64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_float32:
				vs := vec.Col.([]float32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_float64:
				vs := vec.Col.([]float64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_date:
				vs := vec.Col.([]types.Date)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_datetime:
				vs := vec.Col.([]types.Datetime)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vec.Col.(*types.Bytes)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						key := vs.Get(int64(k + start))
						data[k*24+int(ctr.keyOffs[k])] = 0
						copy(data[k*24+int(ctr.keyOffs[k])+1:], key)
						ctr.keyOffs[k] += uint32(len(key)) + 1
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							data[k*24+int(ctr.keyOffs[k])] = 1
							ctr.keyOffs[k]++
						} else {
							key := vs.Get(int64(k + start))
							data[k*24+int(ctr.keyOffs[k])] = 0
							copy(data[k*24+int(ctr.keyOffs[k])+1:], key)
							ctr.keyOffs[k] += uint32(len(key)) + 1
						}
					}
				}
			}
		}
		for _, v := range ctr.views {
			for i, _ := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_date:
					vs := vec.Col.([]types.Date)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_datetime:
					vs := vec.Col.([]types.Datetime)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_char, types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								data[k*24+int(ctr.keyOffs[k])] = 1
								ctr.keyOffs[k]++
							} else {
								row := v.sels[vp-1][0]
								key := vs.Get(row)
								data[k*24+int(ctr.keyOffs[k])] = 0
								copy(data[k*24+int(ctr.keyOffs[k])+1:], key)
								ctr.keyOffs[k] += uint32(len(key)) + 1
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								data[k*24+int(ctr.keyOffs[k])] = 1
								ctr.keyOffs[k]++
							} else {
								row := v.sels[vp-1][0]
								key := vs.Get(row)
								data[k*24+int(ctr.keyOffs[k])] = 0
								copy(data[k*24+int(ctr.keyOffs[k])+1:], key)
								ctr.keyOffs[k] += uint32(len(key)) + 1
							}
						}
					}
				}
			}
		}
	}
	ctr.hashes[0] = 0
	vecs := bat.Ht.([]*vector.Vector)
	ctr.pctr.strHashMap.InsertString24BatchWithRing(ctr.zs, ctr.strHashStates, ctr.h24.keys[:n], ctr.values)
	{ // batch
		for k, v := range ctr.values[:n] {
			if ctr.zs[k] == 0 {
				continue
			}
			if v > ctr.pctr.rows {
				ctr.pctr.rows++
				{ // fill vector
					for i, j := range ctr.is {
						if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], bat.Vecs[ctr.ois[i]], int64(k+start), proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						if len(v.is) > 0 {
							row := v.sels[v.values[k]-1][0]
							for i, j := range v.is {
								if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], v.bat.Vecs[v.ois[i]], row, proc.Mp); err != nil {
									return err
								}
							}
						}
					}
				}
				{ // grow ring
					for i := range bat.Rs {
						if err := ctr.pctr.bat.Rs[i].Grow(proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						for i := range v.bat.Rs {
							if err := ctr.pctr.bat.Rs[v.ris[i]].Grow(proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				ctr.pctr.bat.Zs = append(ctr.pctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			{ // fill ring
				for i := range bat.Rs {
					ctr.pctr.bat.Rs[i].Fill(ai, int64(k+start), ctr.zs[k]/bat.Zs[k+start], vecs[i])
				}
				for _, v := range ctr.views {
					if len(v.bat.Rs) > 0 {
						row := v.sels[v.values[k]-1][0]
						for j, r := range v.bat.Rs {
							ctr.pctr.bat.Rs[v.ris[j]].Mul(r, ai, row, ctr.zs[k]/v.bat.Zs[row])
						}
					}
				}
			}
			ctr.pctr.bat.Zs[ai] += ctr.zs[k]
		}
	}
	return nil
}

func (ctr *Container) processJoinH32(n, start int, bat *batch.Batch, proc *process.Process) error {
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
			for x, row := range rows {
				ctr.zs[x] = bat.Zs[row]
			}
		} else {
			v := ctr.views[j-1]
			for x, row := range rows {
				ctr.zs[x] *= v.bat.Zs[row]
			}
		}
	}
	copy(ctr.keyOffs, ctr.zKeyOffs)
	copy(ctr.h32.keys, ctr.h32.zKeys)
	{ // fill group
		rows := ctr.mx[0]
		if cap(ctr.h32.keys) < len(rows) {
			ctr.h32.keys = make([][4]uint64, len(rows))
		}
		if cap(ctr.keyOffs) < len(rows) {
			ctr.keyOffs = make([]uint32, len(rows))
		}
		ctr.h32.keys = ctr.h32.keys[:len(rows)]
		ctr.keyOffs = ctr.keyOffs[:len(rows)]
		data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h32.keys[0])), cap(ctr.h32.keys)*32)[:len(ctr.h32.keys)*32]
		for i, _ := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vec.Col.([]int8)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_uint8:
				vs := vec.Col.([]uint8)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_int16:
				vs := vec.Col.([]int16)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_uint16:
				vs := vec.Col.([]uint16)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_int32:
				vs := vec.Col.([]int32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_uint32:
				vs := vec.Col.([]uint32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_int64:
				vs := vec.Col.([]int64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_uint64:
				vs := vec.Col.([]uint64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_float32:
				vs := vec.Col.([]float32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_float64:
				vs := vec.Col.([]float64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_date:
				vs := vec.Col.([]types.Date)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_datetime:
				vs := vec.Col.([]types.Datetime)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vec.Col.(*types.Bytes)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						key := vs.Get(int64(row))
						data[k*32+int(ctr.keyOffs[k])] = 0
						copy(data[k*32+int(ctr.keyOffs[k])+1:], key)
						ctr.keyOffs[k] += uint32(len(key)) + 1
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							data[k*32+int(ctr.keyOffs[k])] = 1
							ctr.keyOffs[k]++
						} else {
							key := vs.Get(row)
							data[k*32+int(ctr.keyOffs[k])] = 0
							copy(data[k*32+int(ctr.keyOffs[k])+1:], key)
							ctr.keyOffs[k] += uint32(len(key)) + 1
						}
					}
				}
			}
		}
		for vi, v := range ctr.views {
			rows := ctr.mx[vi+1]
			for i, _ := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_date:
					vs := vec.Col.([]types.Date)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_datetime:
					vs := vec.Col.([]types.Datetime)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_char, types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							key := vs.Get(row)
							data[k*32+int(ctr.keyOffs[k])] = 0
							copy(data[k*32+int(ctr.keyOffs[k])+1:], key)
							ctr.keyOffs[k] += uint32(len(key)) + 1
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								data[k*32+int(ctr.keyOffs[k])] = 1
								ctr.keyOffs[k]++
							} else {
								key := vs.Get(row)
								data[k*32+int(ctr.keyOffs[k])] = 0
								copy(data[k*32+int(ctr.keyOffs[k])+1:], key)
								ctr.keyOffs[k] += uint32(len(key)) + 1
							}
						}
					}
				}
			}
		}
	}
	ctr.hashes[0] = 0
	vecs := bat.Ht.([]*vector.Vector)
	if cap(ctr.values) < len(ctr.zs) {
		ctr.values = make([]uint64, len(ctr.zs))
	}
	if cap(ctr.strHashStates) < len(ctr.zs) {
		ctr.strHashStates = make([][3]uint64, len(ctr.zs))
	}
	ctr.values = ctr.values[:len(ctr.zs)]
	ctr.strHashStates = ctr.strHashStates[:len(ctr.zs)]
	ctr.pctr.strHashMap.InsertString32Batch(ctr.strHashStates, ctr.h32.keys[:len(ctr.zs)], ctr.values)
	{ // batch
		for k, v := range ctr.values[:len(ctr.zs)] {
			if v > ctr.pctr.rows {
				ctr.pctr.rows++
				{ // fill vector
					row := ctr.mx[0][k]
					for i, j := range ctr.is {
						if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], bat.Vecs[ctr.ois[i]], row, proc.Mp); err != nil {
							return err
						}
					}
					for vi, v := range ctr.views {
						row := ctr.mx[vi+1][k]
						for i, j := range v.is {
							if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], v.bat.Vecs[v.ois[i]], row, proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				{ // grow ring
					for i := range bat.Rs {
						if err := ctr.pctr.bat.Rs[i].Grow(proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						for i := range v.bat.Rs {
							if err := ctr.pctr.bat.Rs[v.ris[i]].Grow(proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				ctr.pctr.bat.Zs = append(ctr.pctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			{ // fill ring
				for i := range bat.Rs {
					row := ctr.mx[0][k]
					ctr.pctr.bat.Rs[i].Fill(ai, row, ctr.zs[k]/bat.Zs[row], vecs[i])
				}
				for i, v := range ctr.views {
					row := ctr.mx[i+1][k]
					for j, r := range v.bat.Rs {
						ctr.pctr.bat.Rs[v.ris[j]].Mul(r, ai, row, ctr.zs[k]/v.bat.Zs[row])
					}
				}
			}
			ctr.pctr.bat.Zs[ai] += ctr.zs[k]
		}
	}
	return nil
}

func (ctr *Container) processPureJoinH32(n, start int, bat *batch.Batch, proc *process.Process) error {
	{
		var flg bool

		ctr.zs = ctr.zs[:n]
		for i := 0; i < n; i++ {
			flg = false
			ctr.zs[i] = bat.Zs[i+start]
			for _, v := range ctr.views {
				if v.values[i] == 0 {
					flg = true
					break
				}
				if !v.isPure {
					ctr.zs[i] *= v.bat.Zs[v.sels[v.values[i]-1][0]]
				}
			}
			if flg {
				ctr.zs[i] = 0
				bat.Zs[i+start] = 0
			}
		}
	}
	copy(ctr.keyOffs, ctr.zKeyOffs)
	copy(ctr.h32.keys, ctr.h32.zKeys)
	data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h32.keys[0])), cap(ctr.h32.keys)*32)[:len(ctr.h32.keys)*32]
	{ // fill group
		for i, _ := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vec.Col.([]int8)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_uint8:
				vs := vec.Col.([]uint8)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_int16:
				vs := vec.Col.([]int16)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_uint16:
				vs := vec.Col.([]uint16)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_int32:
				vs := vec.Col.([]int32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_uint32:
				vs := vec.Col.([]uint32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_int64:
				vs := vec.Col.([]int64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_uint64:
				vs := vec.Col.([]uint64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_float32:
				vs := vec.Col.([]float32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_float64:
				vs := vec.Col.([]float64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_date:
				vs := vec.Col.([]types.Date)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_datetime:
				vs := vec.Col.([]types.Datetime)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vec.Col.(*types.Bytes)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						key := vs.Get(int64(k + start))
						data[k*32+int(ctr.keyOffs[k])] = 0
						copy(data[k*32+int(ctr.keyOffs[k])+1:], key)
						ctr.keyOffs[k] += uint32(len(key)) + 1
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							data[k*32+int(ctr.keyOffs[k])] = 1
							ctr.keyOffs[k]++
						} else {
							key := vs.Get(int64(k + start))
							data[k*32+int(ctr.keyOffs[k])] = 0
							copy(data[k*32+int(ctr.keyOffs[k])+1:], key)
							ctr.keyOffs[k] += uint32(len(key)) + 1
						}
					}
				}
			}
		}
		for _, v := range ctr.views {
			for i, _ := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_date:
					vs := vec.Col.([]types.Date)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_datetime:
					vs := vec.Col.([]types.Datetime)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_char, types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								data[k*32+int(ctr.keyOffs[k])] = 1
								ctr.keyOffs[k]++
							} else {
								row := v.sels[vp-1][0]
								key := vs.Get(row)
								data[k*32+int(ctr.keyOffs[k])] = 0
								copy(data[k*32+int(ctr.keyOffs[k])+1:], key)
								ctr.keyOffs[k] += uint32(len(key)) + 1
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								data[k*32+int(ctr.keyOffs[k])] = 1
								ctr.keyOffs[k]++
							} else {
								row := v.sels[vp-1][0]
								key := vs.Get(row)
								data[k*32+int(ctr.keyOffs[k])] = 0
								copy(data[k*32+int(ctr.keyOffs[k])+1:], key)
								ctr.keyOffs[k] += uint32(len(key)) + 1
							}
						}
					}
				}
			}
		}
	}
	ctr.hashes[0] = 0
	vecs := bat.Ht.([]*vector.Vector)
	ctr.pctr.strHashMap.InsertString32BatchWithRing(ctr.zs, ctr.strHashStates, ctr.h32.keys[:n], ctr.values)
	{ // batch
		for k, v := range ctr.values[:n] {
			if ctr.zs[k] == 0 {
				continue
			}
			if v > ctr.pctr.rows {
				ctr.pctr.rows++
				{ // fill vector
					for i, j := range ctr.is {
						if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], bat.Vecs[ctr.ois[i]], int64(k+start), proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						if len(v.is) > 0 {
							row := v.sels[v.values[k]-1][0]
							for i, j := range v.is {
								if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], v.bat.Vecs[v.ois[i]], row, proc.Mp); err != nil {
									return err
								}
							}
						}
					}
				}
				{ // grow ring
					for i := range bat.Rs {
						if err := ctr.pctr.bat.Rs[i].Grow(proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						for i := range v.bat.Rs {
							if err := ctr.pctr.bat.Rs[v.ris[i]].Grow(proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				ctr.pctr.bat.Zs = append(ctr.pctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			{ // fill ring
				for i := range bat.Rs {
					ctr.pctr.bat.Rs[i].Fill(ai, int64(k+start), ctr.zs[k]/bat.Zs[k+start], vecs[i])
				}
				for _, v := range ctr.views {
					if len(v.bat.Rs) > 0 {
						row := v.sels[v.values[k]-1][0]
						for j, r := range v.bat.Rs {
							ctr.pctr.bat.Rs[v.ris[j]].Mul(r, ai, row, ctr.zs[k]/v.bat.Zs[row])
						}
					}
				}
			}
			ctr.pctr.bat.Zs[ai] += ctr.zs[k]
		}
	}
	return nil
}

func (ctr *Container) processJoinH40(n, start int, bat *batch.Batch, proc *process.Process) error {
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
			for x, row := range rows {
				ctr.zs[x] = bat.Zs[row]
			}
		} else {
			v := ctr.views[j-1]
			for x, row := range rows {
				ctr.zs[x] *= v.bat.Zs[row]
			}
		}
	}
	copy(ctr.keyOffs, ctr.zKeyOffs)
	copy(ctr.h40.keys, ctr.h40.zKeys)
	{ // fill group
		rows := ctr.mx[0]
		if cap(ctr.h40.keys) < len(rows) {
			ctr.h40.keys = make([][5]uint64, len(rows))
		}
		if cap(ctr.keyOffs) < len(rows) {
			ctr.keyOffs = make([]uint32, len(rows))
		}
		ctr.h40.keys = ctr.h40.keys[:len(rows)]
		ctr.keyOffs = ctr.keyOffs[:len(rows)]
		data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h40.keys[0])), cap(ctr.h40.keys)*40)[:len(ctr.h40.keys)*40]
		for i, _ := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vec.Col.([]int8)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_uint8:
				vs := vec.Col.([]uint8)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_int16:
				vs := vec.Col.([]int16)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_uint16:
				vs := vec.Col.([]uint16)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_int32:
				vs := vec.Col.([]int32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_uint32:
				vs := vec.Col.([]uint32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_int64:
				vs := vec.Col.([]int64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_uint64:
				vs := vec.Col.([]uint64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_float32:
				vs := vec.Col.([]float32)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_float64:
				vs := vec.Col.([]float64)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_date:
				vs := vec.Col.([]types.Date)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_datetime:
				vs := vec.Col.([]types.Datetime)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vec.Col.(*types.Bytes)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						key := vs.Get(int64(row))
						data[k*40+int(ctr.keyOffs[k])] = 0
						copy(data[k*40+int(ctr.keyOffs[k])+1:], key)
						ctr.keyOffs[k] += uint32(len(key)) + 1
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							data[k*40+int(ctr.keyOffs[k])] = 1
							ctr.keyOffs[k]++
						} else {
							key := vs.Get(row)
							data[k*40+int(ctr.keyOffs[k])] = 0
							copy(data[k*40+int(ctr.keyOffs[k])+1:], key)
							ctr.keyOffs[k] += uint32(len(key)) + 1
						}
					}
				}
			}
		}
		for vi, v := range ctr.views {
			rows := ctr.mx[vi+1]
			for i, _ := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(2, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(3, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_date:
					vs := vec.Col.([]types.Date)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(5, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_datetime:
					vs := vec.Col.([]types.Datetime)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
						}
						add.Uint32AddScalar(9, ctr.keyOffs[:len(rows)], ctr.keyOffs[:len(rows)])
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[row]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_char, types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							key := vs.Get(row)
							data[k*40+int(ctr.keyOffs[k])] = 0
							copy(data[k*40+int(ctr.keyOffs[k])+1:], key)
							ctr.keyOffs[k] += uint32(len(key)) + 1
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								data[k*40+int(ctr.keyOffs[k])] = 1
								ctr.keyOffs[k]++
							} else {
								key := vs.Get(row)
								data[k*40+int(ctr.keyOffs[k])] = 0
								copy(data[k*40+int(ctr.keyOffs[k])+1:], key)
								ctr.keyOffs[k] += uint32(len(key)) + 1
							}
						}
					}
				}
			}
		}
	}
	ctr.hashes[0] = 0
	vecs := bat.Ht.([]*vector.Vector)
	if cap(ctr.values) < len(ctr.zs) {
		ctr.values = make([]uint64, len(ctr.zs))
	}
	if cap(ctr.strHashStates) < len(ctr.zs) {
		ctr.strHashStates = make([][3]uint64, len(ctr.zs))
	}
	ctr.values = ctr.values[:len(ctr.zs)]
	ctr.strHashStates = ctr.strHashStates[:len(ctr.zs)]
	ctr.pctr.strHashMap.InsertString40Batch(ctr.strHashStates, ctr.h40.keys[:len(ctr.zs)], ctr.values)
	{ // batch
		for k, v := range ctr.values[:len(ctr.zs)] {
			if v > ctr.pctr.rows {
				ctr.pctr.rows++
				{ // fill vector
					row := ctr.mx[0][k]
					for i, j := range ctr.is {
						if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], bat.Vecs[ctr.ois[i]], row, proc.Mp); err != nil {
							return err
						}
					}
					for vi, v := range ctr.views {
						row := ctr.mx[vi+1][k]
						for i, j := range v.is {
							if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], v.bat.Vecs[v.ois[i]], row, proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				{ // grow ring
					for i := range bat.Rs {
						if err := ctr.pctr.bat.Rs[i].Grow(proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						for i := range v.bat.Rs {
							if err := ctr.pctr.bat.Rs[v.ris[i]].Grow(proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				ctr.pctr.bat.Zs = append(ctr.pctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			{ // fill ring
				for i := range bat.Rs {
					row := ctr.mx[0][k]
					ctr.pctr.bat.Rs[i].Fill(ai, row, ctr.zs[k]/bat.Zs[row], vecs[i])
				}
				for i, v := range ctr.views {
					row := ctr.mx[i+1][k]
					for j, r := range v.bat.Rs {
						ctr.pctr.bat.Rs[v.ris[j]].Mul(r, ai, row, ctr.zs[k]/v.bat.Zs[row])
					}
				}
			}
			ctr.pctr.bat.Zs[ai] += ctr.zs[k]
		}
	}
	return nil
}

func (ctr *Container) processPureJoinH40(n, start int, bat *batch.Batch, proc *process.Process) error {
	{
		var flg bool

		ctr.zs = ctr.zs[:n]
		for i := 0; i < n; i++ {
			flg = false
			ctr.zs[i] = bat.Zs[i+start]
			for _, v := range ctr.views {
				if v.values[i] == 0 {
					flg = true
					break
				}
				if !v.isPure {
					ctr.zs[i] *= v.bat.Zs[v.sels[v.values[i]-1][0]]
				}
			}
			if flg {
				ctr.zs[i] = 0
				bat.Zs[i+start] = 0
			}
		}
	}
	copy(ctr.keyOffs, ctr.zKeyOffs)
	copy(ctr.h40.keys, ctr.h40.zKeys)
	data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h40.keys[0])), cap(ctr.h40.keys)*40)[:len(ctr.h40.keys)*40]
	{ // fill group
		for i, _ := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vec.Col.([]int8)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_uint8:
				vs := vec.Col.([]uint8)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 2
						}
					}
				}
			case types.T_int16:
				vs := vec.Col.([]int16)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_uint16:
				vs := vec.Col.([]uint16)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(3, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 3
						}
					}
				}
			case types.T_int32:
				vs := vec.Col.([]int32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_uint32:
				vs := vec.Col.([]uint32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_int64:
				vs := vec.Col.([]int64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_uint64:
				vs := vec.Col.([]uint64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_float32:
				vs := vec.Col.([]float32)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_float64:
				vs := vec.Col.([]float64)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_date:
				vs := vec.Col.([]types.Date)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(5, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 5
						}
					}
				}
			case types.T_datetime:
				vs := vec.Col.([]types.Datetime)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
						*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
					}
					add.Uint32AddScalar(9, ctr.keyOffs[:n], ctr.keyOffs[:n])
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
							ctr.keyOffs[k]++
						} else {
							*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[k+start]
							ctr.keyOffs[k] += 9
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vec.Col.(*types.Bytes)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						key := vs.Get(int64(k + start))
						data[k*40+int(ctr.keyOffs[k])] = 0
						copy(data[k*40+int(ctr.keyOffs[k])+1:], key)
						ctr.keyOffs[k] += uint32(len(key)) + 1
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							data[k*40+int(ctr.keyOffs[k])] = 1
							ctr.keyOffs[k]++
						} else {
							key := vs.Get(int64(k + start))
							data[k*40+int(ctr.keyOffs[k])] = 0
							copy(data[k*40+int(ctr.keyOffs[k])+1:], key)
							ctr.keyOffs[k] += uint32(len(key)) + 1
						}
					}
				}
			}
		}
		for _, v := range ctr.views {
			for i, _ := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 2
							}
						}
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 3
							}
						}
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_date:
					vs := vec.Col.([]types.Date)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Date)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 5
							}
						}
					}
				case types.T_datetime:
					vs := vec.Col.([]types.Datetime)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 1
								ctr.keyOffs[k]++
							} else {
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
								*(*types.Datetime)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k]+1)) = vs[v.sels[vp-1][0]]
								ctr.keyOffs[k] += 9
							}
						}
					}
				case types.T_char, types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								data[k*40+int(ctr.keyOffs[k])] = 1
								ctr.keyOffs[k]++
							} else {
								row := v.sels[vp-1][0]
								key := vs.Get(row)
								data[k*40+int(ctr.keyOffs[k])] = 0
								copy(data[k*40+int(ctr.keyOffs[k])+1:], key)
								ctr.keyOffs[k] += uint32(len(key)) + 1
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								data[k*40+int(ctr.keyOffs[k])] = 1
								ctr.keyOffs[k]++
							} else {
								row := v.sels[vp-1][0]
								key := vs.Get(row)
								data[k*40+int(ctr.keyOffs[k])] = 0
								copy(data[k*40+int(ctr.keyOffs[k])+1:], key)
								ctr.keyOffs[k] += uint32(len(key)) + 1
							}
						}
					}
				}
			}
		}
	}
	ctr.hashes[0] = 0
	vecs := bat.Ht.([]*vector.Vector)
	ctr.pctr.strHashMap.InsertString40BatchWithRing(ctr.zs, ctr.strHashStates, ctr.h40.keys[:n], ctr.values)
	{ // batch
		for k, v := range ctr.values[:n] {
			if ctr.zs[k] == 0 {
				continue
			}
			if v > ctr.pctr.rows {
				ctr.pctr.rows++
				{ // fill vector
					for i, j := range ctr.is {
						if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], bat.Vecs[ctr.ois[i]], int64(k+start), proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						if len(v.is) > 0 {
							row := v.sels[v.values[k]-1][0]
							for i, j := range v.is {
								if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], v.bat.Vecs[v.ois[i]], row, proc.Mp); err != nil {
									return err
								}
							}
						}
					}
				}
				{ // grow ring
					for i := range bat.Rs {
						if err := ctr.pctr.bat.Rs[i].Grow(proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						for i := range v.bat.Rs {
							if err := ctr.pctr.bat.Rs[v.ris[i]].Grow(proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				ctr.pctr.bat.Zs = append(ctr.pctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			{ // fill ring
				for i := range bat.Rs {
					ctr.pctr.bat.Rs[i].Fill(ai, int64(k+start), ctr.zs[k]/bat.Zs[k+start], vecs[i])
				}
				for _, v := range ctr.views {
					if len(v.bat.Rs) > 0 {
						row := v.sels[v.values[k]-1][0]
						for j, r := range v.bat.Rs {
							ctr.pctr.bat.Rs[v.ris[j]].Mul(r, ai, row, ctr.zs[k]/v.bat.Zs[row])
						}
					}
				}
			}
			ctr.pctr.bat.Zs[ai] += ctr.zs[k]
		}
	}
	return nil
}

func (ctr *Container) processJoinHStr(n, start int, bat *batch.Batch, proc *process.Process) error {
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
			for x, row := range rows {
				ctr.zs[x] = bat.Zs[row]
			}
		} else {
			v := ctr.views[j-1]
			for x, row := range rows {
				ctr.zs[x] *= v.bat.Zs[row]
			}
		}
	}
	{ // fill group
		rows := ctr.mx[0]
		if cap(ctr.hstr.keys) < len(rows) {
			ctr.hstr.keys = make([][]byte, len(rows))
		}
		ctr.hstr.keys = ctr.hstr.keys[:len(rows)]
		for i, _ := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vec.Col.([]int8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row:(row+1)]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row:(row+1)]...)
						}
					}
				}
			case types.T_uint8:
				vs := vec.Col.([]uint8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row:(row+1)]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row:(row+1)]...)
						}
					}
				}
			case types.T_int16:
				vs := vec.Col.([]int16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*2:(row+1)*2]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*2:(row+1)*2]...)
						}
					}
				}
			case types.T_uint16:
				vs := vec.Col.([]uint16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*2:(row+1)*2]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*2:(row+1)*2]...)
						}
					}
				}
			case types.T_int32:
				vs := vec.Col.([]int32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
						}
					}
				}
			case types.T_uint32:
				vs := vec.Col.([]uint32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
						}
					}
				}
			case types.T_int64:
				vs := vec.Col.([]int64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
						}
					}
				}
			case types.T_uint64:
				vs := vec.Col.([]uint64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
						}
					}
				}
			case types.T_float32:
				vs := vec.Col.([]float32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
						}
					}
				}
			case types.T_float64:
				vs := vec.Col.([]float64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
						}
					}
				}
			case types.T_date:
				vs := vec.Col.([]types.Date)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
						}
					}
				}
			case types.T_datetime:
				vs := vec.Col.([]types.Datetime)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vec.Col.(*types.Bytes)
				if !nulls.Any(vec.Nsp) {
					for k, row := range rows {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(row))...)
					}
				} else {
					for k, row := range rows {
						if vec.Nsp.Np.Contains(uint64(row)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(row))...)
						}
					}
				}
			}
		}
		for vi, v := range ctr.views {
			rows := ctr.mx[vi+1]
			for i, _ := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row:(row+1)]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row:(row+1)]...)
							}
						}
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row:(row+1)]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row:(row+1)]...)
							}
						}
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*2:(row+1)*2]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*2:(row+1)*2]...)
							}
						}
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*2:(row+1)*2]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*2:(row+1)*2]...)
							}
						}
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
							}
						}
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
							}
						}
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
							}
						}
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
							}
						}
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
							}
						}
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
							}
						}
					}
				case types.T_date:
					vs := vec.Col.([]types.Date)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*4:(row+1)*4]...)
							}
						}
					}
				case types.T_datetime:
					vs := vec.Col.([]types.Datetime)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[row*8:(row+1)*8]...)
							}
						}
					}
				case types.T_char, types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					if !nulls.Any(vec.Nsp) {
						for k, row := range rows {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(row))...)
						}
					} else {
						for k, row := range rows {
							if vec.Nsp.Np.Contains(uint64(row)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(row))...)
							}
						}
					}
				}
			}
		}
	}
	ctr.hashes[0] = 0
	vecs := bat.Ht.([]*vector.Vector)
	if cap(ctr.values) < len(ctr.zs) {
		ctr.values = make([]uint64, len(ctr.zs))
	}
	if cap(ctr.strHashStates) < len(ctr.zs) {
		ctr.strHashStates = make([][3]uint64, len(ctr.zs))
	}
	for k := 0; k < len(ctr.zs); k++ {
		if l := len(ctr.hstr.keys[k]); l < 16 {
			ctr.hstr.keys[k] = append(ctr.hstr.keys[k], hashtable.StrKeyPadding[l:]...)
		}
	}
	ctr.values = ctr.values[:len(ctr.zs)]
	ctr.strHashStates = ctr.strHashStates[:len(ctr.zs)]
	ctr.pctr.strHashMap.InsertStringBatch(ctr.strHashStates, ctr.hstr.keys[:len(ctr.zs)], ctr.values)
	{ // batch
		for k, v := range ctr.values[:len(ctr.zs)] {
			ctr.hstr.keys[k] = ctr.hstr.keys[k][:0]
			if v > ctr.pctr.rows {
				ctr.pctr.rows++
				{ // fill vector
					row := ctr.mx[0][k]
					for i, j := range ctr.is {
						if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], bat.Vecs[ctr.ois[i]], row, proc.Mp); err != nil {
							return err
						}
					}
					for vi, v := range ctr.views {
						row := ctr.mx[vi+1][k]
						for i, j := range v.is {
							if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], v.bat.Vecs[v.ois[i]], row, proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				{ // grow ring
					for i := range bat.Rs {
						if err := ctr.pctr.bat.Rs[i].Grow(proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						for i := range v.bat.Rs {
							if err := ctr.pctr.bat.Rs[v.ris[i]].Grow(proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				ctr.pctr.bat.Zs = append(ctr.pctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			{ // fill ring
				for i := range bat.Rs {
					row := ctr.mx[0][k]
					ctr.pctr.bat.Rs[i].Fill(ai, row, ctr.zs[k]/bat.Zs[row], vecs[i])
				}
				for i, v := range ctr.views {
					row := ctr.mx[i+1][k]
					for j, r := range v.bat.Rs {
						ctr.pctr.bat.Rs[v.ris[j]].Mul(r, ai, row, ctr.zs[k]/v.bat.Zs[row])
					}
				}
			}
			ctr.pctr.bat.Zs[ai] += ctr.zs[k]
		}
	}
	return nil
}

func (ctr *Container) processPureJoinHStr(n, start int, bat *batch.Batch, proc *process.Process) error {
	{
		var flg bool

		ctr.zs = ctr.zs[:n]
		for i := 0; i < n; i++ {
			flg = false
			ctr.zs[i] = bat.Zs[i+start]
			for _, v := range ctr.views {
				if v.values[i] == 0 {
					flg = true
					break
				}
				if !v.isPure {
					ctr.zs[i] *= v.bat.Zs[v.sels[v.values[i]-1][0]]
				}
			}
			if flg {
				ctr.zs[i] = 0
				bat.Zs[i+start] = 0
			}
		}
	}
	{ // fill group
		for i, _ := range ctr.is {
			vec := bat.Vecs[ctr.ois[i]]
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vec.Col.([]int8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start):(k+start+1)]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start):(k+start+1)]...)
						}
					}
				}
			case types.T_uint8:
				vs := vec.Col.([]uint8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start):(k+start+1)]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start):(k+start+1)]...)
						}
					}
				}
			case types.T_int16:
				vs := vec.Col.([]int16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*2:(k+start+1)*2]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*2:(k+start+1)*2]...)
						}
					}
				}
			case types.T_uint16:
				vs := vec.Col.([]uint16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*2:(k+start+1)*2]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*2:(k+start+1)*2]...)
						}
					}
				}
			case types.T_int32:
				vs := vec.Col.([]int32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*4:(k+start+1)*4]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*4:(k+start+1)*4]...)
						}
					}
				}
			case types.T_uint32:
				vs := vec.Col.([]uint32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*4:(k+start+1)*4]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*4:(k+start+1)*4]...)
						}
					}
				}
			case types.T_int64:
				vs := vec.Col.([]int64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*8:(k+start+1)*8]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*8:(k+start+1)*8]...)
						}
					}
				}
			case types.T_uint64:
				vs := vec.Col.([]uint64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*8:(k+start+1)*8]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*8:(k+start+1)*8]...)
						}
					}
				}
			case types.T_float32:
				vs := vec.Col.([]float32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*4:(k+start+1)*4]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*4:(k+start+1)*4]...)
						}
					}
				}
			case types.T_float64:
				vs := vec.Col.([]float64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*8:(k+start+1)*8]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*8:(k+start+1)*8]...)
						}
					}
				}
			case types.T_date:
				vs := vec.Col.([]types.Date)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*4:(k+start+1)*4]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*4:(k+start+1)*4]...)
						}
					}
				}
			case types.T_datetime:
				vs := vec.Col.([]types.Datetime)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*8:(k+start+1)*8]...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(k+start)*8:(k+start+1)*8]...)
						}
					}
				}
			case types.T_char, types.T_varchar:
				vs := vec.Col.(*types.Bytes)
				if !nulls.Any(vec.Nsp) {
					for k := 0; k < n; k++ {
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
						ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(k+start))...)
					}
				} else {
					for k := 0; k < n; k++ {
						if vec.Nsp.Np.Contains(uint64(k + start)) {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
						} else {
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
							ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(int64(k+start))...)
						}
					}
				}
			}
		}
		for _, v := range ctr.views {
			for i, _ := range v.is {
				vec := v.bat.Vecs[v.ois[i]]
				switch vec.Typ.Oid {
				case types.T_int8:
					vs := vec.Col.([]int8)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row):(row+1)]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row):(row+1)]...)
							}
						}
					}
				case types.T_uint8:
					vs := vec.Col.([]uint8)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row):(row+1)]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row):(row+1)]...)
							}
						}
					}
				case types.T_int16:
					vs := vec.Col.([]int16)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*2:(row+1)*2]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*2:(row+1)*2]...)
							}
						}
					}
				case types.T_uint16:
					vs := vec.Col.([]uint16)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*2:(row+1)*2]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*2:(row+1)*2]...)
							}
						}
					}
				case types.T_int32:
					vs := vec.Col.([]int32)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*4:(row+1)*4]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*4:(row+1)*4]...)
							}
						}
					}
				case types.T_uint32:
					vs := vec.Col.([]uint32)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*4:(row+1)*4]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*4:(row+1)*4]...)
							}
						}
					}
				case types.T_int64:
					vs := vec.Col.([]int64)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*8:(row+1)*8]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*8:(row+1)*8]...)
							}
						}
					}
				case types.T_uint64:
					vs := vec.Col.([]uint64)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*8:(row+1)*8]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*8:(row+1)*8]...)
							}
						}
					}
				case types.T_float32:
					vs := vec.Col.([]float32)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*4:(row+1)*4]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*4:(row+1)*4]...)
							}
						}
					}
				case types.T_float64:
					vs := vec.Col.([]float64)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*8:(row+1)*8]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*8:(row+1)*8]...)
							}
						}
					}
				case types.T_date:
					vs := vec.Col.([]types.Date)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*4:(row+1)*4]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*4:(row+1)*4]...)
							}
						}
					}
				case types.T_datetime:
					vs := vec.Col.([]types.Datetime)
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*8:(row+1)*8]...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(row)*8:(row+1)*8]...)
								*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = 0
							}
						}
					}
				case types.T_char, types.T_varchar:
					vs := vec.Col.(*types.Bytes)
					if !nulls.Any(vec.Nsp) {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(row)...)
							}
						}
					} else {
						for k := 0; k < n; k++ {
							if vp := v.values[k]; vp == 0 || vec.Nsp.Np.Contains(uint64(k+start)) {
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(1))
							} else {
								row := v.sels[vp-1][0]
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], byte(0))
								ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(row)...)
							}
						}
					}
				}
			}
		}
	}
	ctr.hashes[0] = 0
	vecs := bat.Ht.([]*vector.Vector)
	ctr.pctr.strHashMap.InsertString40BatchWithRing(ctr.zs, ctr.strHashStates, ctr.h40.keys[:n], ctr.values)
	{ // batch
		for k, v := range ctr.values[:n] {
			if ctr.zs[k] == 0 {
				continue
			}
			if v > ctr.pctr.rows {
				ctr.pctr.rows++
				{ // fill vector
					for i, j := range ctr.is {
						if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], bat.Vecs[ctr.ois[i]], int64(k+start), proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						if len(v.is) > 0 {
							row := v.sels[v.values[k]-1][0]
							for i, j := range v.is {
								if err := vector.UnionOne(ctr.pctr.bat.Vecs[j], v.bat.Vecs[v.ois[i]], row, proc.Mp); err != nil {
									return err
								}
							}
						}
					}
				}
				{ // grow ring
					for i := range bat.Rs {
						if err := ctr.pctr.bat.Rs[i].Grow(proc.Mp); err != nil {
							return err
						}
					}
					for _, v := range ctr.views {
						for i := range v.bat.Rs {
							if err := ctr.pctr.bat.Rs[v.ris[i]].Grow(proc.Mp); err != nil {
								return err
							}
						}
					}
				}
				ctr.pctr.bat.Zs = append(ctr.pctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			{ // fill ring
				for i := range bat.Rs {
					ctr.pctr.bat.Rs[i].Fill(ai, int64(k+start), ctr.zs[k]/bat.Zs[k+start], vecs[i])
				}
				for _, v := range ctr.views {
					if len(v.bat.Rs) > 0 {
						row := v.sels[v.values[k]-1][0]
						for j, r := range v.bat.Rs {
							ctr.pctr.bat.Rs[v.ris[j]].Mul(r, ai, row, ctr.zs[k]/v.bat.Zs[row])
						}
					}
				}
			}
			ctr.pctr.bat.Zs[ai] += ctr.zs[k]
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
	if len(v.vecs) == 1 {
		return ctr.probeViewWithOneVar(i, n, bat, v)
	}
	ctr.hstr.keys = ctr.hstr.keys[:UnitLimit]
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
	ctr.hashes = ctr.hashes[:UnitLimit]
	ctr.h8.keys = ctr.h8.keys[:UnitLimit]
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
