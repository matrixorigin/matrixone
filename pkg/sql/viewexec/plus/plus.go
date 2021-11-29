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

package plus

import (
	"bytes"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/hashtable"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
	"unsafe"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(" + ")
}

func Prepare(_ *process.Process, arg interface{}) error {
	n := arg.(*Argument)
	n.ctr = new(Container)
	n.ctr.state = Fill
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	switch n.Typ {
	case BoundVars:
		return n.ctr.processBoundVars(proc)
	case FreeVarsAndBoundVars:
		return n.ctr.processFreeVars(proc)
	default:
		panic("no possible")
	}
}

func (ctr *Container) processBoundVars(proc *process.Process) (bool, error) {
	for {
		switch ctr.state {
		case Fill:
			for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
				bat := <-proc.Reg.MergeReceivers[i].Ch
				if bat == nil {
					continue
				}
				if len(bat.Zs) == 0 {
					i--
					continue
				}
				if ctr.bat == nil {
					ctr.bat = bat
				} else {
					ctr.bat.Zs[0] += bat.Zs[0]
					for j, r := range ctr.bat.Rs {
						r.Add(bat.Rs[j], 0, 0)
					}
					batch.Clean(bat, proc.Mp)
				}
			}
			ctr.state = Eval
		case Eval:
			if ctr.bat != nil {
				proc.Reg.InputBatch = ctr.bat
				ctr.bat = nil
			}
			return true, nil
		}
	}
}

func (ctr *Container) processFreeVars(proc *process.Process) (bool, error) {
	for {
		switch ctr.state {
		case Fill:
			if err := ctr.fill(proc); err != nil {
				batch.Clean(ctr.bat, proc.Mp)
				proc.Reg.InputBatch = nil
				ctr.state = Eval
				return true, err
			}
			ctr.state = Eval
		case Eval:
			if ctr.bat != nil {
				proc.Reg.InputBatch = ctr.bat
				ctr.bat = nil
			}
			return true, nil
		}
	}
}

func (ctr *Container) fill(proc *process.Process) error {
	for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
		bat := <-proc.Reg.MergeReceivers[i].Ch
		if bat == nil {
			continue
		}
		if len(bat.Zs) == 0 {
			i--
			continue
		}
		if err := ctr.fillBatch(bat, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) fillBatch(bat *batch.Batch, proc *process.Process) error {
	if len(ctr.vars) == 0 {
		ctr.vars = append(ctr.vars, bat.Attrs...)
		size := 0
		for _, vec := range bat.Vecs {
			switch vec.Typ.Oid {
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
				if width := vec.Typ.Width; width > 0 {
					size += int(width)
				} else {
					size = 128
				}
			case types.T_varchar:
				if width := vec.Typ.Width; width > 0 {
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
	} else {
		batch.Reorder(bat, ctr.vars)
	}
	switch ctr.typ {
	case H8:
		return ctr.fillH8(bat, proc)
	case H24:
		return ctr.fillH24(bat, proc)
	case H32:
		return ctr.fillH32(bat, proc)
	case H40:
		return ctr.fillH40(bat, proc)
	default:
		return ctr.fillHStr(bat, proc)
	}
}

func (ctr *Container) fillH8(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte
	var os, ns [][]uint32

	vecs := bat.Vecs
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
	if ctr.bat == nil {
		ctr.bat = bat
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
			for k, _ := range ctr.inserts[:n] {
				*ctr.values[k] = ctr.rows
				ctr.rows++
			}
		}
		return nil
	}
	defer batch.Clean(bat, proc.Mp)
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
				r.Add(bat.Rs[j], ai, i+int64(k))
			}
		}
	}
	return nil
}

func (ctr *Container) fillH24(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte
	var os, ns [][]uint32

	vecs := bat.Vecs
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
	if ctr.bat == nil {
		ctr.bat = bat
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
			for k, _ := range ctr.inserts[:n] {
				*ctr.values[k] = ctr.rows
				ctr.rows++
			}
		}
		return nil
	}
	defer batch.Clean(bat, proc.Mp)
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
				r.Add(bat.Rs[j], ai, i+int64(k))
			}

		}
	}
	return nil
}

func (ctr *Container) fillH32(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte
	var os, ns [][]uint32

	vecs := bat.Vecs
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
	if ctr.bat == nil {
		ctr.bat = bat
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
			for k, _ := range ctr.inserts[:n] {
				*ctr.values[k] = ctr.rows
				ctr.rows++
			}
		}
		return nil
	}
	defer batch.Clean(bat, proc.Mp)
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
				r.Add(bat.Rs[j], ai, i+int64(k))
			}

		}
	}
	return nil
}

func (ctr *Container) fillH40(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte
	var os, ns [][]uint32

	vecs := bat.Vecs
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
	if ctr.bat == nil {
		ctr.bat = bat
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
			for k, _ := range ctr.inserts[:n] {
				*ctr.values[k] = ctr.rows
				ctr.rows++
			}
		}
		return nil
	}
	defer batch.Clean(bat, proc.Mp)
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
				r.Add(bat.Rs[j], ai, i+int64(k))
			}

		}
	}
	return nil
}

func (ctr *Container) fillHStr(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte
	var os, ns [][]uint32

	vecs := bat.Vecs
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
	if ctr.bat == nil {
		ctr.bat = bat
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
				ctr.hstr.keys = append(ctr.hstr.keys, data...)
			}
			*vp = ctr.rows
			ctr.rows++
		}
		return nil
	}
	defer batch.Clean(bat, proc.Mp)
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
			r.Add(bat.Rs[j], ai, i)
		}
	}
	return nil
}
