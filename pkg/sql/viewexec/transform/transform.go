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

package transform

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/hashtable"
	"matrixone/pkg/container/ring"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/viewexec/transformer"
	"matrixone/pkg/vm/process"
	"unsafe"
)

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	if n.Restrict != nil {
		restrict.String(n.Restrict, buf)
		buf.WriteString(" -> ")
	}
	if n.Projection != nil {
		projection.String(n.Projection, buf)
		buf.WriteString(" -> ")
	}
	buf.WriteString("∏([")
	for i, fvar := range n.FreeVars {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fvar)
	}
	buf.WriteString("], [")
	for i, bvar := range n.BoundVars {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fmt.Sprintf("%s <- %s(%s)", bvar.Alias, transformer.TransformerNames[bvar.Op], bvar.Name))
	}
	buf.WriteString("])")
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.Ctr = new(Container)
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	switch {
	case n.Typ == Bare:
		return n.Ctr.processBare(proc, n)
	case n.Typ == BoundVars:
		return n.Ctr.processBoundVars(proc, n)
	case n.Typ == FreeVarsAndBoundVars && !n.IsMerge:
		return n.Ctr.processFreeVars(proc, n)
	default:
		return n.Ctr.processFreeVarsUnit(proc, n)
	}
}

func (ctr *Container) processBare(proc *process.Process, arg *Argument) (bool, error) {
	return false, preprocess(proc, arg)
}

func (ctr *Container) processBoundVars(proc *process.Process, arg *Argument) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil { // begin eval
		if ctr.bat != nil {
			proc.Reg.InputBatch = ctr.bat
			ctr.bat = nil
		}
		return true, nil
	}
	if err := preprocess(proc, arg); err != nil {
		proc.Reg.InputBatch = &batch.Batch{}
		return false, err
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	defer batch.Clean(bat, proc.Mp)
	if len(ctr.vars) == 0 {
		var err error

		ctr.constructContainer(arg, bat)
		ctr.vars = append(ctr.vars, bat.Attrs...)
		ctr.bat = &batch.Batch{}
		ctr.bat.Zs = []int64{1}
		ctr.bat.As = make([]string, len(arg.BoundVars))
		ctr.bat.Refs = make([]uint64, len(arg.BoundVars))
		ctr.bat.Rs = make([]ring.Ring, len(arg.BoundVars))
		for i, bvar := range arg.BoundVars {
			ctr.bat.As[i] = bvar.Alias
			ctr.bat.Refs[i] = uint64(bvar.Ref)
			if ctr.bat.Rs[i], err = transformer.New(bvar.Op, bat.Vecs[ctr.Is[i]].Typ); err != nil {
				ctr.bat.Rs = ctr.bat.Rs[:i]
				batch.Clean(ctr.bat, proc.Mp)
				ctr.bat = nil
				return false, err
			}
		}
		for _, r := range ctr.bat.Rs {
			if err := r.Grow(proc.Mp); err != nil {
				batch.Clean(ctr.bat, proc.Mp)
				ctr.bat = nil
				return false, err
			}
		}
	} else {
		batch.Reorder(bat, ctr.vars)
	}
	for i, r := range ctr.bat.Rs {
		r.BulkFill(0, bat.Zs, bat.Vecs[ctr.Is[i]])
	}
	return false, nil
}

func (ctr *Container) processFreeVars(proc *process.Process, arg *Argument) (bool, error) {
	var err error

	bat := proc.Reg.InputBatch
	if bat == nil { // begin eval
		if ctr.bat != nil {
			switch ctr.typ {
			case H8:
				ctr.bat.Ht = ctr.h8.ht
			case H24:
				ctr.bat.Ht = ctr.h24.ht
			case H32:
				ctr.bat.Ht = ctr.h32.ht
			case H40:
				ctr.bat.Ht = ctr.h40.ht
			default:
				ctr.bat.Ht = ctr.hstr.ht
			}
			proc.Reg.InputBatch = ctr.bat
			ctr.bat = nil
		}
		return true, nil
	}
	if err := preprocess(proc, arg); err != nil {
		proc.Reg.InputBatch = &batch.Batch{}
		return false, err
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	defer batch.Clean(bat, proc.Mp)
	proc.Reg.InputBatch = &batch.Batch{}
	if len(ctr.vars) == 0 {
		var err error

		batch.Reorder(bat, arg.FreeVars)
		ctr.constructContainer(arg, bat)
		ctr.vars = append(ctr.vars, bat.Attrs...)
		ctr.bat = batch.New(true, arg.FreeVars)
		{
			size := 0
			for i := 0; i < ctr.n; i++ {
				ctr.bat.Vecs[i] = vector.New(bat.Vecs[i].Typ)
				ctr.bat.Vecs[i].Ref = bat.Vecs[i].Ref
				switch bat.Vecs[i].Typ.Oid {
				case types.T_int8:
					size += 1
				case types.T_int16:
					size += 2
				case types.T_int32:
					size += 4
				case types.T_int64:
					size += 8
				case types.T_uint8:
					size += 1
				case types.T_uint16:
					size += 2
				case types.T_uint32:
					size += 4
				case types.T_uint64:
					size += 8
				case types.T_float32:
					size += 4
				case types.T_float64:
					size += 8
				case types.T_char:
					if width := bat.Vecs[i].Typ.Width; width > 0 {
						size += int(width)
					} else {
						size = 128
					}
				case types.T_varchar:
					if width := bat.Vecs[i].Typ.Width; width > 0 {
						size += int(width)
					} else {
						size = 128
					}
				}
			}
			switch {
			case size <= 8:
				ctr.typ = H8
				ctr.inserts = make([]uint8, UnitLimit)
				ctr.zinserts = make([]uint8, UnitLimit)
				ctr.hashs = make([]uint64, UnitLimit)
				ctr.values = make([]*uint64, UnitLimit)
				ctr.h8.keys = make([]uint64, UnitLimit)
				ctr.h8.zkeys = make([]uint64, UnitLimit)
				ctr.h8.ht = &hashtable.Int64HashMap{}
				ctr.h8.ht.Init()
			case size <= 24:
				ctr.typ = H24
				ctr.inserts = make([]uint8, UnitLimit)
				ctr.zinserts = make([]uint8, UnitLimit)
				ctr.hashs = make([]uint64, UnitLimit)
				ctr.values = make([]*uint64, UnitLimit)
				ctr.h24.keys = make([][3]uint64, UnitLimit)
				ctr.h24.zkeys = make([][3]uint64, UnitLimit)
				ctr.h24.ht = &hashtable.String24HashMap{}
				ctr.h24.ht.Init()
			case size <= 32:
				ctr.typ = H32
				ctr.inserts = make([]uint8, UnitLimit)
				ctr.zinserts = make([]uint8, UnitLimit)
				ctr.hashs = make([]uint64, UnitLimit)
				ctr.values = make([]*uint64, UnitLimit)
				ctr.h32.keys = make([][4]uint64, UnitLimit)
				ctr.h32.zkeys = make([][4]uint64, UnitLimit)
				ctr.h32.ht = &hashtable.String32HashMap{}
				ctr.h32.ht.Init()
			case size <= 40:
				ctr.typ = H40
				ctr.inserts = make([]uint8, UnitLimit)
				ctr.zinserts = make([]uint8, UnitLimit)
				ctr.hashs = make([]uint64, UnitLimit)
				ctr.values = make([]*uint64, UnitLimit)
				ctr.h40.keys = make([][5]uint64, UnitLimit)
				ctr.h40.zkeys = make([][5]uint64, UnitLimit)
				ctr.h40.ht = &hashtable.String40HashMap{}
				ctr.h40.ht.Init()
			default:
				ctr.typ = HStr
				ctr.hstr.keys = make([]byte, 0, 1<<20)
				ctr.hstr.ht = &hashtable.StringHashMap{}
				ctr.hstr.ht.Init()
			}
		}
		ctr.bat.As = make([]string, len(arg.BoundVars))
		ctr.bat.Refs = make([]uint64, len(arg.BoundVars))
		ctr.bat.Rs = make([]ring.Ring, len(arg.BoundVars))
		for i, bvar := range arg.BoundVars {
			ctr.bat.As[i] = bvar.Alias
			ctr.bat.Refs[i] = uint64(bvar.Ref)
			if ctr.bat.Rs[i], err = transformer.New(bvar.Op, bat.Vecs[ctr.Is[i]].Typ); err != nil {
				ctr.bat.Rs = ctr.bat.Rs[:i]
				batch.Clean(ctr.bat, proc.Mp)
				ctr.bat = nil
				return false, err
			}
		}
	} else {
		batch.Reorder(bat, ctr.vars)
	}
	switch ctr.typ {
	case H8:
		err = ctr.processH8(bat, proc)
	case H24:
		err = ctr.processH24(bat, proc)
	case H32:
		err = ctr.processH32(bat, proc)
	case H40:
		err = ctr.processH40(bat, proc)
	default:
		err = ctr.processHStr(bat, proc)
	}
	if err != nil {
		batch.Clean(ctr.bat, proc.Mp)
		ctr.bat = nil
		return false, err
	}
	return false, err
}

func (ctr *Container) processFreeVarsUnit(proc *process.Process, arg *Argument) (bool, error) {
	var err error

	bat := proc.Reg.InputBatch
	if bat == nil { // begin eval
		return true, nil
	}
	if err := preprocess(proc, arg); err != nil {
		proc.Reg.InputBatch = &batch.Batch{}
		return false, err
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	if len(ctr.vars) == 0 {
		batch.Reorder(bat, arg.FreeVars)
		ctr.constructContainer(arg, bat)
		ctr.vars = append(ctr.vars, bat.Attrs...)
	} else {
		batch.Reorder(bat, ctr.vars)
	}
	{
		bat.As = make([]string, len(arg.BoundVars))
		bat.Refs = make([]uint64, len(arg.BoundVars))
		bat.Rs = make([]ring.Ring, len(arg.BoundVars))
		for i, bvar := range arg.BoundVars {
			bat.As[i] = bvar.Alias
			bat.Refs[i] = uint64(bvar.Ref)
			if bat.Rs[i], err = transformer.New(bvar.Op, bat.Vecs[ctr.Is[i]].Typ); err != nil {
				bat.Rs = bat.Rs[:i]
				batch.Clean(bat, proc.Mp)
				return false, err
			}
		}
	}
	return false, err
}

func (ctr *Container) constructContainer(n *Argument, bat *batch.Batch) {
	ctr.n = len(n.FreeVars)
	mp := make(map[string]int)
	for i, attr := range bat.Attrs {
		mp[attr] = i
	}
	for _, fvar := range n.FreeVars {
		delete(mp, fvar)
	}
	{
		mq := make(map[string]int)
		for _, bvar := range n.BoundVars {
			mq[bvar.Name]++
			ctr.Is = append(ctr.Is, mp[bvar.Name])
		}
		for k, v := range mq {
			vec := batch.GetVector(bat, k)
			if int(vec.Ref) == v {
				delete(mp, k)
			}
		}
	}
	for i, attr := range bat.Attrs {
		if _, ok := mp[attr]; !ok {
			continue
		}
		n.BoundVars = append(n.BoundVars, transformer.Transformer{
			Name:  attr,
			Alias: attr,
			Op:    transformer.Max,
			Ref:   int(bat.Vecs[i].Ref),
		})
		ctr.Is = append(ctr.Is, i)
	}
}

func preprocess(proc *process.Process, arg *Argument) error {
	if arg.Restrict != nil {
		if _, err := restrict.Call(proc, arg.Restrict); err != nil {
			return err
		}
	}
	if arg.Projection != nil {
		if _, err := projection.Call(proc, arg.Projection); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) processH8(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte
	var os, ns [][]uint32

	vecs := bat.Vecs[:ctr.n]
	{
		os = make([][]uint32, len(vecs))
		ns = make([][]uint32, len(vecs))
		keys = make([][]byte, len(vecs))
		for i := range vecs {
			switch vecs[i].Typ.Oid {
			case types.T_int8:
				vs := vecs[i].Col.([]int8)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			case types.T_int16:
				vs := vecs[i].Col.([]int16)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			case types.T_int32:
				vs := vecs[i].Col.([]int32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_int64:
				vs := vecs[i].Col.([]int64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_uint8:
				vs := vecs[i].Col.([]uint8)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			case types.T_uint16:
				vs := vecs[i].Col.([]uint16)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			case types.T_uint32:
				vs := vecs[i].Col.([]uint32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_uint64:
				vs := vecs[i].Col.([]uint64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_float32:
				vs := vecs[i].Col.([]float32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_float64:
				vs := vecs[i].Col.([]float64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_char:
				vs := vecs[i].Col.(*types.Bytes)
				keys[i] = vs.Data
				os[i] = vs.Offsets
				ns[i] = vs.Lengths
			case types.T_varchar:
				vs := vecs[i].Col.(*types.Bytes)
				keys[i] = vs.Data
				os[i] = vs.Offsets
				ns[i] = vs.Lengths
			}
		}
	}
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := int(count - i)
		if n > UnitLimit {
			n = UnitLimit
		}
		{
			copy(ctr.h8.keys, ctr.h8.zkeys)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h8.keys[0])), cap(ctr.h8.keys)*8)[:len(ctr.h8.keys)*8]
			data = data[:0]
			for k := 0; k < n; k++ {
				o := int(i) + k // offset
				for j, vec := range vecs {
					switch vec.Typ.Oid {
					case types.T_int8:
						data = append(data, keys[j][o*1:(o+1)*1]...)
					case types.T_int16:
						data = append(data, keys[j][o*2:(o+1)*2]...)
					case types.T_int32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_int64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_uint8:
						data = append(data, keys[j][o*1:(o+1)*1]...)
					case types.T_uint16:
						data = append(data, keys[j][o*2:(o+1)*2]...)
					case types.T_uint32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_uint64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_float32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_float64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_char:
						data = append(data, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
					case types.T_varchar:
						data = append(data, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
					}
				}
				data = data[:(k+1)*8]
			}
		}
		ctr.hashs[0] = 0
		copy(ctr.inserts[:n], ctr.zinserts[:n])
		ctr.h8.ht.InsertBatch(n, ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values)
		for k, ok := range ctr.inserts[:n] {
			if ok == 1 {
				for j, vec := range ctr.bat.Vecs {
					if err := vector.UnionOne(vec, vecs[j], i+int64(k), proc.Mp); err != nil {
						return err
					}
				}
				*ctr.values[k] = ctr.rows
				ctr.rows++
				for _, r := range ctr.bat.Rs {
					if err := r.Grow(proc.Mp); err != nil {
						return err
					}
				}
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(*ctr.values[k])
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
			for j, r := range ctr.bat.Rs {
				r.Fill(ai, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[ctr.Is[j]])
			}
		}
	}
	return nil
}

func (ctr *Container) processH24(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte
	var os, ns [][]uint32

	vecs := bat.Vecs[:ctr.n]
	{
		os = make([][]uint32, len(vecs))
		ns = make([][]uint32, len(vecs))
		keys = make([][]byte, len(vecs))
		for i := range vecs {
			switch vecs[i].Typ.Oid {
			case types.T_int8:
				vs := vecs[i].Col.([]int8)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			case types.T_int16:
				vs := vecs[i].Col.([]int16)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			case types.T_int32:
				vs := vecs[i].Col.([]int32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_int64:
				vs := vecs[i].Col.([]int64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_uint8:
				vs := vecs[i].Col.([]uint8)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			case types.T_uint16:
				vs := vecs[i].Col.([]uint16)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			case types.T_uint32:
				vs := vecs[i].Col.([]uint32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_uint64:
				vs := vecs[i].Col.([]uint64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_float32:
				vs := vecs[i].Col.([]float32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_float64:
				vs := vecs[i].Col.([]float64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_char:
				vs := vecs[i].Col.(*types.Bytes)
				keys[i] = vs.Data
				os[i] = vs.Offsets
				ns[i] = vs.Lengths
			case types.T_varchar:
				vs := vecs[i].Col.(*types.Bytes)
				keys[i] = vs.Data
				os[i] = vs.Offsets
				ns[i] = vs.Lengths
			}
		}
	}
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := int(count - i)
		if n > UnitLimit {
			n = UnitLimit
		}
		{
			copy(ctr.h24.keys, ctr.h24.zkeys)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h24.keys[0])), cap(ctr.h24.keys)*24)[:len(ctr.h24.keys)*24]
			data = data[:0]
			for k := 0; k < n; k++ {
				o := int(i) + k // offset
				for j, vec := range vecs {
					switch vec.Typ.Oid {
					case types.T_int8:
						data = append(data, keys[j][o*1:(o+1)*1]...)
					case types.T_int16:
						data = append(data, keys[j][o*2:(o+1)*2]...)
					case types.T_int32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_int64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_uint8:
						data = append(data, keys[j][o*1:(o+1)*1]...)
					case types.T_uint16:
						data = append(data, keys[j][o*2:(o+1)*2]...)
					case types.T_uint32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_uint64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_float32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_float64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_char:
						data = append(data, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
					case types.T_varchar:
						data = append(data, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
					}
				}
				data = data[:(k+1)*24]
			}
		}
		ctr.hashs[0] = 0
		copy(ctr.inserts[:n], ctr.zinserts[:n])
		ctr.h24.ht.InsertBatch(ctr.hashs, ctr.h24.keys[:n], ctr.inserts, ctr.values)
		for k, ok := range ctr.inserts[:n] {
			if ok == 1 {
				for j, vec := range ctr.bat.Vecs {
					if err := vector.UnionOne(vec, vecs[j], i+int64(k), proc.Mp); err != nil {
						return err
					}
				}
				*ctr.values[k] = ctr.rows
				ctr.rows++
				for _, r := range ctr.bat.Rs {
					if err := r.Grow(proc.Mp); err != nil {
						return err
					}
				}
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(*ctr.values[k])
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
			for j, r := range ctr.bat.Rs {
				r.Fill(ai, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[ctr.Is[j]])
			}
		}
	}
	return nil
}

func (ctr *Container) processH32(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte
	var os, ns [][]uint32

	vecs := bat.Vecs[:ctr.n]
	{
		os = make([][]uint32, len(vecs))
		ns = make([][]uint32, len(vecs))
		keys = make([][]byte, len(vecs))
		for i := range vecs {
			switch vecs[i].Typ.Oid {
			case types.T_int8:
				vs := vecs[i].Col.([]int8)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			case types.T_int16:
				vs := vecs[i].Col.([]int16)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			case types.T_int32:
				vs := vecs[i].Col.([]int32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_int64:
				vs := vecs[i].Col.([]int64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_uint8:
				vs := vecs[i].Col.([]uint8)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			case types.T_uint16:
				vs := vecs[i].Col.([]uint16)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			case types.T_uint32:
				vs := vecs[i].Col.([]uint32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_uint64:
				vs := vecs[i].Col.([]uint64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_float32:
				vs := vecs[i].Col.([]float32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_float64:
				vs := vecs[i].Col.([]float64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_char:
				vs := vecs[i].Col.(*types.Bytes)
				keys[i] = vs.Data
				os[i] = vs.Offsets
				ns[i] = vs.Lengths
			case types.T_varchar:
				vs := vecs[i].Col.(*types.Bytes)
				keys[i] = vs.Data
				os[i] = vs.Offsets
				ns[i] = vs.Lengths
			}
		}
	}
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := int(count - i)
		if n > UnitLimit {
			n = UnitLimit
		}
		{
			copy(ctr.h32.keys, ctr.h32.zkeys)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h32.keys[0])), cap(ctr.h32.keys)*32)[:len(ctr.h32.keys)*32]
			data = data[:0]
			for k := 0; k < n; k++ {
				o := int(i) + k // offset
				for j, vec := range vecs {
					switch vec.Typ.Oid {
					case types.T_int8:
						data = append(data, keys[j][o*1:(o+1)*1]...)
					case types.T_int16:
						data = append(data, keys[j][o*2:(o+1)*2]...)
					case types.T_int32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_int64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_uint8:
						data = append(data, keys[j][o*1:(o+1)*1]...)
					case types.T_uint16:
						data = append(data, keys[j][o*2:(o+1)*2]...)
					case types.T_uint32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_uint64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_float32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_float64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_char:
						data = append(data, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
					case types.T_varchar:
						data = append(data, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
					}
				}
				data = data[:(k+1)*32]
			}
		}
		ctr.hashs[0] = 0
		copy(ctr.inserts[:n], ctr.zinserts[:n])
		ctr.h32.ht.InsertBatch(ctr.hashs, ctr.h32.keys[:n], ctr.inserts, ctr.values)
		for k, ok := range ctr.inserts[:n] {
			if ok == 1 {
				for j, vec := range ctr.bat.Vecs {
					if err := vector.UnionOne(vec, vecs[j], i+int64(k), proc.Mp); err != nil {
						return err
					}
				}
				*ctr.values[k] = ctr.rows
				ctr.rows++
				for _, r := range ctr.bat.Rs {
					if err := r.Grow(proc.Mp); err != nil {
						return err
					}
				}
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(*ctr.values[k])
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
			for j, r := range ctr.bat.Rs {
				r.Fill(ai, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[ctr.Is[j]])
			}
		}
	}
	return nil
}

func (ctr *Container) processH40(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte
	var os, ns [][]uint32

	vecs := bat.Vecs[:ctr.n]
	{
		os = make([][]uint32, len(vecs))
		ns = make([][]uint32, len(vecs))
		keys = make([][]byte, len(vecs))
		for i := range vecs {
			switch vecs[i].Typ.Oid {
			case types.T_int8:
				vs := vecs[i].Col.([]int8)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			case types.T_int16:
				vs := vecs[i].Col.([]int16)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			case types.T_int32:
				vs := vecs[i].Col.([]int32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_int64:
				vs := vecs[i].Col.([]int64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_uint8:
				vs := vecs[i].Col.([]uint8)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			case types.T_uint16:
				vs := vecs[i].Col.([]uint16)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			case types.T_uint32:
				vs := vecs[i].Col.([]uint32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_uint64:
				vs := vecs[i].Col.([]uint64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_float32:
				vs := vecs[i].Col.([]float32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_float64:
				vs := vecs[i].Col.([]float64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_char:
				vs := vecs[i].Col.(*types.Bytes)
				keys[i] = vs.Data
				os[i] = vs.Offsets
				ns[i] = vs.Lengths
			case types.T_varchar:
				vs := vecs[i].Col.(*types.Bytes)
				keys[i] = vs.Data
				os[i] = vs.Offsets
				ns[i] = vs.Lengths
			}
		}
	}
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := int(count - i)
		if n > UnitLimit {
			n = UnitLimit
		}
		{
			copy(ctr.h40.keys, ctr.h40.zkeys)
			data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h40.keys[0])), cap(ctr.h40.keys)*40)[:len(ctr.h40.keys)*40]
			data = data[:0]
			for k := 0; k < n; k++ {
				o := int(i) + k // offset
				for j, vec := range vecs {
					switch vec.Typ.Oid {
					case types.T_int8:
						data = append(data, keys[j][o*1:(o+1)*1]...)
					case types.T_int16:
						data = append(data, keys[j][o*2:(o+1)*2]...)
					case types.T_int32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_int64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_uint8:
						data = append(data, keys[j][o*1:(o+1)*1]...)
					case types.T_uint16:
						data = append(data, keys[j][o*2:(o+1)*2]...)
					case types.T_uint32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_uint64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_float32:
						data = append(data, keys[j][o*4:(o+1)*4]...)
					case types.T_float64:
						data = append(data, keys[j][o*8:(o+1)*8]...)
					case types.T_char:
						data = append(data, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
					case types.T_varchar:
						data = append(data, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
					}
				}
				data = data[:(k+1)*40]
			}
		}
		ctr.hashs[0] = 0
		copy(ctr.inserts[:n], ctr.zinserts[:n])
		ctr.h40.ht.InsertBatch(ctr.hashs, ctr.h40.keys[:n], ctr.inserts, ctr.values)
		for k, ok := range ctr.inserts[:n] {
			if ok == 1 {
				for j, vec := range ctr.bat.Vecs {
					if err := vector.UnionOne(vec, vecs[j], i+int64(k), proc.Mp); err != nil {
						return err
					}
				}
				*ctr.values[k] = ctr.rows
				ctr.rows++
				for _, r := range ctr.bat.Rs {
					if err := r.Grow(proc.Mp); err != nil {
						return err
					}
				}
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(*ctr.values[k])
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
			for j, r := range ctr.bat.Rs {
				r.Fill(ai, i+int64(k), bat.Zs[i+int64(k)], bat.Vecs[ctr.Is[j]])
			}
		}
	}
	return nil
}

func (ctr *Container) processHStr(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte
	var os, ns [][]uint32

	vecs := bat.Vecs[:ctr.n]
	{
		os = make([][]uint32, len(vecs))
		ns = make([][]uint32, len(vecs))
		keys = make([][]byte, len(vecs))
		for i := range vecs {
			switch vecs[i].Typ.Oid {
			case types.T_int8:
				vs := vecs[i].Col.([]int8)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			case types.T_int16:
				vs := vecs[i].Col.([]int16)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			case types.T_int32:
				vs := vecs[i].Col.([]int32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_int64:
				vs := vecs[i].Col.([]int64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_uint8:
				vs := vecs[i].Col.([]uint8)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
			case types.T_uint16:
				vs := vecs[i].Col.([]uint16)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
			case types.T_uint32:
				vs := vecs[i].Col.([]uint32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_uint64:
				vs := vecs[i].Col.([]uint64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_float32:
				vs := vecs[i].Col.([]float32)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
			case types.T_float64:
				vs := vecs[i].Col.([]float64)
				keys[i] = unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
			case types.T_char:
				vs := vecs[i].Col.(*types.Bytes)
				keys[i] = vs.Data
				os[i] = vs.Offsets
				ns[i] = vs.Lengths
			case types.T_varchar:
				vs := vecs[i].Col.(*types.Bytes)
				keys[i] = vs.Data
				os[i] = vs.Offsets
				ns[i] = vs.Lengths
			}
		}
	}
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i++ {
		data := make([]byte, 0, 8)
		{
			for j, vec := range vecs {
				switch vec.Typ.Oid {
				case types.T_int8:
					data = append(data, keys[j][i*1:(i+1)*1]...)
				case types.T_int16:
					data = append(data, keys[j][i*2:(i+1)*2]...)
				case types.T_int32:
					data = append(data, keys[j][i*4:(i+1)*4]...)
				case types.T_int64:
					data = append(data, keys[j][i*8:(i+1)*8]...)
				case types.T_uint8:
					data = append(data, keys[j][i*1:(i+1)*1]...)
				case types.T_uint16:
					data = append(data, keys[j][i*2:(i+1)*2]...)
				case types.T_uint32:
					data = append(data, keys[j][i*4:(i+1)*4]...)
				case types.T_uint64:
					data = append(data, keys[j][i*8:(i+1)*8]...)
				case types.T_float32:
					data = append(data, keys[j][i*4:(i+1)*4]...)
				case types.T_float64:
					data = append(data, keys[j][i*8:(i+1)*8]...)
				case types.T_char:
					data = append(data, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
				case types.T_varchar:
					data = append(data, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
				}
			}
		}
		ok, vp := ctr.hstr.ht.Insert(hashtable.StringRef{Ptr: &data[0], Len: len(data)})
		if ok {
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionOne(vec, vecs[j], i, proc.Mp); err != nil {
					return err
				}
			}
			*vp = ctr.rows
			ctr.rows++
			for _, r := range ctr.bat.Rs {
				if err := r.Grow(proc.Mp); err != nil {
					return err
				}
			}
			ctr.bat.Zs = append(ctr.bat.Zs, 0)
			ctr.hstr.keys = append(ctr.hstr.keys, data...)
		}
		ai := int64(*vp)
		ctr.bat.Zs[ai] += bat.Zs[i]
		for j, r := range ctr.bat.Rs {
			r.Fill(ai, i, bat.Zs[i], bat.Vecs[ctr.Is[j]])
		}
	}
	return nil
}
