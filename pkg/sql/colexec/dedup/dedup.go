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
		n.ctr.bat = batch.New(true, bat.Attrs)
		n.ctr.mp = &hashtable.MockStringHashTable{}
		n.ctr.mp.Init()
		for i, vec := range bat.Vecs {
			n.ctr.bat.Vecs[i] = vector.New(vec.Typ)
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
				size = 128
			case types.T_varchar:
				size = 128
			}
		}
		switch {
		case size <= 8:
			n.ctr = &Container{
				typ:     H8,
				inserts: make([]bool, UnitLimit),
				hashs:   make([]uint64, UnitLimit),
				values:  make([]*uint64, UnitLimit),
			}
			n.ctr.h8.keys = make([]uint64, UnitLimit)
		case size <= 16:
			n.ctr = &Container{
				typ:     H16,
				inserts: make([]bool, UnitLimit),
				hashs:   make([]uint64, UnitLimit),
				values:  make([]*uint64, UnitLimit),
			}
			n.ctr.h16.keys = make([][2]uint64, UnitLimit)
		case size <= 24:
			n.ctr = &Container{
				typ:     H24,
				inserts: make([]bool, UnitLimit),
				hashs:   make([]uint64, UnitLimit),
				values:  make([]*uint64, UnitLimit),
			}
			n.ctr.h24.keys = make([][3]uint64, UnitLimit)
		default:
			n.ctr = &Container{
				typ: HStr,
				key: make([]byte, 0, size),
			}
		}
	}
	switch n.ctr.typ {
	case H8:
		err = n.ctr.processH8(bat, proc)
	case H16:
		err = n.ctr.processH16(bat, proc)
	case H24:
		err = n.ctr.processH24(bat, proc)
	default:
		err = n.ctr.processHStr(bat, proc)
	}
	if err != nil {
		batch.Clean(n.ctr.bat, proc.Mp)
		proc.Reg.InputBatch = nil
		return false, err
	}
	proc.Reg.InputBatch = n.ctr.bat
	return false, err
}

func (ctr *Container) processH8(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte

	vecs := bat.Vecs
	{
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
					}
				}
				for len(data) < (k+1)*8 {
					data = append(data, 0)
				}
			}
		}
		ctr.hashs[0] = 0
		if err := ctr.mp.H1.InsertBatch(ctr.hashs[:n], ctr.h8.keys[:n], ctr.values[:n], ctr.inserts[:n]); err != nil {
			return err
		}
		for k, ok := range ctr.inserts[:n] {
			if ok {
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

func (ctr *Container) processH16(bat *batch.Batch, proc *process.Process) error {
	var keys [][]byte

	vecs := bat.Vecs
	{
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
			data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h16.keys[0])), cap(ctr.h16.keys)*16)[:len(ctr.h16.keys)*16]
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
					}
				}
				for len(data) < (k+1)*16 {
					data = append(data, 0)
				}
			}
		}
		ctr.hashs[0] = 0
		if err := ctr.mp.H2.InsertBatch(ctr.hashs[:n], ctr.h16.keys[:n], ctr.values[:n], ctr.inserts[:n]); err != nil {
			return err
		}
		for k, ok := range ctr.inserts[:n] {
			if ok {
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

	vecs := bat.Vecs
	{
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
					}
				}
				for len(data) < (k+1)*24 {
					data = append(data, 0)
				}
			}
		}
		ctr.hashs[0] = 0
		if err := ctr.mp.H3.InsertBatch(ctr.hashs[:n], ctr.h24.keys[:n], ctr.values[:n], ctr.inserts[:n]); err != nil {
			return err
		}
		for k, ok := range ctr.inserts[:n] {
			if ok {
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
		{
			ctr.key = ctr.key[:0]
			for j, vec := range vecs {
				switch vec.Typ.Oid {
				case types.T_int8:
					ctr.key = append(ctr.key, keys[j][i*1:(i+1)*1]...)
				case types.T_int16:
					ctr.key = append(ctr.key, keys[j][i*2:(i+1)*2]...)
				case types.T_int32:
					ctr.key = append(ctr.key, keys[j][i*4:(i+1)*4]...)
				case types.T_int64:
					ctr.key = append(ctr.key, keys[j][i*8:(i+1)*8]...)
				case types.T_uint8:
					ctr.key = append(ctr.key, keys[j][i*1:(i+1)*1]...)
				case types.T_uint16:
					ctr.key = append(ctr.key, keys[j][i*2:(i+1)*2]...)
				case types.T_uint32:
					ctr.key = append(ctr.key, keys[j][i*4:(i+1)*4]...)
				case types.T_uint64:
					ctr.key = append(ctr.key, keys[j][i*8:(i+1)*8]...)
				case types.T_float32:
					ctr.key = append(ctr.key, keys[j][i*4:(i+1)*4]...)
				case types.T_float64:
					ctr.key = append(ctr.key, keys[j][i*8:(i+1)*8]...)
				case types.T_char:
					ctr.key = append(ctr.key, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
				case types.T_varchar:
					ctr.key = append(ctr.key, keys[j][os[j][i]:os[j][i]+ns[j][i]]...)
				}
			}
		}
		ok, vp, err := ctr.mp.Hs.Insert(0, hashtable.StringRef{Ptr: &ctr.key[0], Length: len(ctr.key)})
		if err != nil {
			return err
		}
		if ok {
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionOne(vec, vecs[j], i, proc.Mp); err != nil {
					return err
				}
			}
			*vp = ctr.rows
			ctr.rows++
			ctr.bat.Zs = append(ctr.bat.Zs, 0)
		}
		ai := int64(*vp)
		ctr.bat.Zs[ai] += bat.Zs[i]
	}
	return nil
}
