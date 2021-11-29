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

package dedup

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/hashtable"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
	"unsafe"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString(fmt.Sprintf("δ"))
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error

	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}
	n := arg.(*Argument)
	defer batch.Clean(bat, proc.Mp)
	if n.ctr == nil {
		size := 0
		n.ctr = new(Container)
		n.ctr.bat = batch.New(true, bat.Attrs)
		for i, vec := range bat.Vecs {
			n.ctr.bat.Vecs[i] = vector.New(vec.Typ)
			n.ctr.bat.Vecs[i].Ref = vec.Ref
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
			n.ctr.typ = H8
			n.ctr.inserts = make([]uint8, UnitLimit)
			n.ctr.zinserts = make([]uint8, UnitLimit)
			n.ctr.hashs = make([]uint64, UnitLimit)
			n.ctr.values = make([]*uint64, UnitLimit)
			n.ctr.h8.keys = make([]uint64, UnitLimit)
			n.ctr.h8.zkeys = make([]uint64, UnitLimit)
			n.ctr.h8.ht = &hashtable.Int64HashMap{}
			n.ctr.h8.ht.Init()
		case size <= 24:
			n.ctr.typ = H24
			n.ctr.inserts = make([]uint8, UnitLimit)
			n.ctr.zinserts = make([]uint8, UnitLimit)
			n.ctr.hashs = make([]uint64, UnitLimit)
			n.ctr.values = make([]*uint64, UnitLimit)
			n.ctr.h24.keys = make([][3]uint64, UnitLimit)
			n.ctr.h24.zkeys = make([][3]uint64, UnitLimit)
			n.ctr.h24.ht = &hashtable.String24HashMap{}
			n.ctr.h24.ht.Init()
		case size <= 32:
			n.ctr.typ = H32
			n.ctr.inserts = make([]uint8, UnitLimit)
			n.ctr.zinserts = make([]uint8, UnitLimit)
			n.ctr.hashs = make([]uint64, UnitLimit)
			n.ctr.values = make([]*uint64, UnitLimit)
			n.ctr.h32.keys = make([][4]uint64, UnitLimit)
			n.ctr.h32.zkeys = make([][4]uint64, UnitLimit)
			n.ctr.h32.ht = &hashtable.String32HashMap{}
			n.ctr.h32.ht.Init()
		case size <= 40:
			n.ctr.typ = H40
			n.ctr.inserts = make([]uint8, UnitLimit)
			n.ctr.zinserts = make([]uint8, UnitLimit)
			n.ctr.hashs = make([]uint64, UnitLimit)
			n.ctr.values = make([]*uint64, UnitLimit)
			n.ctr.h40.keys = make([][5]uint64, UnitLimit)
			n.ctr.h40.zkeys = make([][5]uint64, UnitLimit)
			n.ctr.h40.ht = &hashtable.String40HashMap{}
			n.ctr.h40.ht.Init()
		default:
			n.ctr.typ = HStr
			n.ctr.hstr.keys = make([]byte, 0, 1<<20)
			n.ctr.hstr.ht = &hashtable.StringHashMap{}
			n.ctr.hstr.ht.Init()
		}
	}
	switch n.ctr.typ {
	case H8:
		err = n.ctr.processH8(bat, proc)
	case H24:
		err = n.ctr.processH24(bat, proc)
	case H32:
		err = n.ctr.processH32(bat, proc)
	case H40:
		err = n.ctr.processH40(bat, proc)
	default:
		err = n.ctr.processHStr(bat, proc)
	}
	if err != nil {
		batch.Clean(n.ctr.bat, proc.Mp)
		proc.Reg.InputBatch = nil
		return false, err
	}
	for i := range n.ctr.bat.Zs {
		n.ctr.bat.Zs[i] = 1
	}
	proc.Reg.InputBatch = n.ctr.bat
	return false, err
}

func (ctr *Container) processH8(bat *batch.Batch, proc *process.Process) error {
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
						data = append(data, keys[j][os[j][o]:os[j][o]+ns[j][o]]...)
					case types.T_varchar:
						data = append(data, keys[j][os[j][o]:os[j][o]+ns[j][o]]...)
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
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(*ctr.values[k])
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
		}
	}
	return nil
}

func (ctr *Container) processH24(bat *batch.Batch, proc *process.Process) error {
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
						data = append(data, keys[j][os[j][o]:os[j][o]+ns[j][o]]...)
					case types.T_varchar:
						data = append(data, keys[j][os[j][o]:os[j][o]+ns[j][o]]...)
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
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(*ctr.values[k])
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
		}
	}
	return nil
}

func (ctr *Container) processH32(bat *batch.Batch, proc *process.Process) error {
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
						data = append(data, keys[j][os[j][o]:os[j][o]+ns[j][o]]...)
					case types.T_varchar:
						data = append(data, keys[j][os[j][o]:os[j][o]+ns[j][o]]...)
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
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(*ctr.values[k])
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
		}
	}
	return nil
}

func (ctr *Container) processH40(bat *batch.Batch, proc *process.Process) error {
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
						data = append(data, keys[j][os[j][o]:os[j][o]+ns[j][o]]...)
					case types.T_varchar:
						data = append(data, keys[j][os[j][o]:os[j][o]+ns[j][o]]...)
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
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(*ctr.values[k])
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
		}
	}
	return nil
}

func (ctr *Container) processHStr(bat *batch.Batch, proc *process.Process) error {
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
			ctr.bat.Zs = append(ctr.bat.Zs, 0)
			ctr.hstr.keys = append(ctr.hstr.keys, data...)
		}
		ai := int64(*vp)
		ctr.bat.Zs[ai] += bat.Zs[i]
	}
	return nil
}
