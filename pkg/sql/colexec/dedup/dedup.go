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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
		n.ctr.keyOffs = make([]uint32, UnitLimit)
		n.ctr.zKeyOffs = make([]uint32, UnitLimit)
		n.ctr.inserted = make([]uint8, UnitLimit)
		n.ctr.zInserted = make([]uint8, UnitLimit)
		n.ctr.hashes = make([]uint64, UnitLimit)
		n.ctr.values = make([]uint64, UnitLimit)
		switch {
		case size <= 8:
			n.ctr.typ = H8
			n.ctr.h8.keys = make([]uint64, UnitLimit)
			n.ctr.h8.zKeys = make([]uint64, UnitLimit)
			n.ctr.h8.ht = &hashtable.Int64HashMap{}
			n.ctr.h8.ht.Init()
		case size <= 24:
			n.ctr.typ = H24
			n.ctr.h24.keys = make([][3]uint64, UnitLimit)
			n.ctr.h24.zKeys = make([][3]uint64, UnitLimit)
			n.ctr.h24.ht = &hashtable.String24HashMap{}
			n.ctr.h24.ht.Init()
		case size <= 32:
			n.ctr.typ = H32
			n.ctr.h32.keys = make([][4]uint64, UnitLimit)
			n.ctr.h32.zKeys = make([][4]uint64, UnitLimit)
			n.ctr.h32.ht = &hashtable.String32HashMap{}
			n.ctr.h32.ht.Init()
		case size <= 40:
			n.ctr.typ = H40
			n.ctr.h40.keys = make([][5]uint64, UnitLimit)
			n.ctr.h40.zKeys = make([][5]uint64, UnitLimit)
			n.ctr.h40.ht = &hashtable.String40HashMap{}
			n.ctr.h40.ht.Init()
		default:
			n.ctr.typ = HStr
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
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h8.keys, ctr.h8.zKeys)
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vecs[j].Col.([]int8)
				for k := int64(0); k < n; k++ {
					*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint8:
				vs := vecs[j].Col.([]uint8)
				for k := int64(0); k < n; k++ {
					*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int16:
				vs := vecs[j].Col.([]int16)
				for k := int64(0); k < n; k++ {
					*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint16:
				vs := vecs[j].Col.([]uint16)
				for k := int64(0); k < n; k++ {
					*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int32:
				vs := vecs[j].Col.([]int32)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint32:
				vs := vecs[j].Col.([]uint32)
				for k := int64(0); k < n; k++ {
					*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_float32:
				vs := vecs[j].Col.([]float32)
				for k := int64(0); k < n; k++ {
					*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int64:
				vs := vecs[j].Col.([]int64)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint64:
				vs := vecs[j].Col.([]uint64)
				for k := int64(0); k < n; k++ {
					*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_float64:
				vs := vecs[j].Col.([]float64)
				for k := int64(0); k < n; k++ {
					*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				vData := vs.Data
				vOff := vs.Offsets
				vLen := vs.Lengths
				for k := int64(0); k < n; k++ {
					copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h8.keys[k])), 8)[ctr.keyOffs[k]:], vData[vOff[i+k]:vOff[i+k]+vLen[i+k]])
					ctr.keyOffs[k] += vLen[i+k]
				}
			}
		}
		ctr.hashes[0] = 0
		ctr.h8.ht.InsertBatch(int(n), ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values)
		cnt := 0
		copy(ctr.inserted, ctr.zInserted)
		for k, v := range ctr.values[:n] {
			if v > ctr.rows {
				ctr.inserted[k] = 1
				ctr.rows++
				cnt++
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
		}
		if cnt > 0 {
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserted[:n], proc.Mp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ctr *Container) processH24(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h24.keys, ctr.h24.zKeys)
		data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h24.keys[0])), cap(ctr.h24.keys)*24)[:len(ctr.h24.keys)*24]
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vecs[j].Col.([]int8)
				for k := int64(0); k < n; k++ {
					*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint8:
				vs := vecs[j].Col.([]uint8)
				for k := int64(0); k < n; k++ {
					*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int16:
				vs := vecs[j].Col.([]int16)
				for k := int64(0); k < n; k++ {
					*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint16:
				vs := vecs[j].Col.([]uint16)
				for k := int64(0); k < n; k++ {
					*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int32:
				vs := vecs[j].Col.([]int32)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint32:
				vs := vecs[j].Col.([]uint32)
				for k := int64(0); k < n; k++ {
					*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_float32:
				vs := vecs[j].Col.([]float32)
				for k := int64(0); k < n; k++ {
					*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int64:
				vs := vecs[j].Col.([]int64)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint64:
				vs := vecs[j].Col.([]uint64)
				for k := int64(0); k < n; k++ {
					*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_float64:
				vs := vecs[j].Col.([]float64)
				for k := int64(0); k < n; k++ {
					*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				for k := int64(0); k < n; k++ {
					key := vs.Get(i + k)
					copy(data[k*24+int64(ctr.keyOffs[k]):], key)
					ctr.keyOffs[k] += uint32(len(key))
				}
			}
		}
		ctr.hashes[0] = 0
		ctr.h24.ht.InsertBatch(ctr.hashes, ctr.h24.keys[:n], ctr.values)
		cnt := 0
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		for k, v := range ctr.values[:n] {
			if v > ctr.rows {
				ctr.inserted[k] = 1
				ctr.rows++
				cnt++
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
		}
		if cnt > 0 {
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserted[:n], proc.Mp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ctr *Container) processH32(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h32.keys, ctr.h32.zKeys)
		data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h32.keys[0])), cap(ctr.h32.keys)*32)[:len(ctr.h32.keys)*32]
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vecs[j].Col.([]int8)
				for k := int64(0); k < n; k++ {
					*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint8:
				vs := vecs[j].Col.([]uint8)
				for k := int64(0); k < n; k++ {
					*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int16:
				vs := vecs[j].Col.([]int16)
				for k := int64(0); k < n; k++ {
					*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint16:
				vs := vecs[j].Col.([]uint16)
				for k := int64(0); k < n; k++ {
					*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int32:
				vs := vecs[j].Col.([]int32)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint32:
				vs := vecs[j].Col.([]uint32)
				for k := int64(0); k < n; k++ {
					*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_float32:
				vs := vecs[j].Col.([]float32)
				for k := int64(0); k < n; k++ {
					*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int64:
				vs := vecs[j].Col.([]int64)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint64:
				vs := vecs[j].Col.([]uint64)
				for k := int64(0); k < n; k++ {
					*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_float64:
				vs := vecs[j].Col.([]float64)
				for k := int64(0); k < n; k++ {
					*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				for k := int64(0); k < n; k++ {
					key := vs.Get(i + k)
					copy(data[k*32+int64(ctr.keyOffs[k]):], key)
					ctr.keyOffs[k] += uint32(len(key))
				}
			}
		}
		ctr.hashes[0] = 0
		ctr.h32.ht.InsertBatch(ctr.hashes, ctr.h32.keys[:n], ctr.values)
		cnt := 0
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		for k, v := range ctr.values[:n] {
			if v > ctr.rows {
				ctr.inserted[k] = 1
				ctr.rows++
				cnt++
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
		}
		if cnt > 0 {
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserted[:n], proc.Mp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ctr *Container) processH40(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h40.keys, ctr.h40.zKeys)
		data := unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h40.keys[0])), cap(ctr.h40.keys)*40)[:len(ctr.h40.keys)*40]
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vecs[j].Col.([]int8)
				for k := int64(0); k < n; k++ {
					*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint8:
				vs := vecs[j].Col.([]uint8)
				for k := int64(0); k < n; k++ {
					*(*uint8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int16:
				vs := vecs[j].Col.([]int16)
				for k := int64(0); k < n; k++ {
					*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint16:
				vs := vecs[j].Col.([]uint16)
				for k := int64(0); k < n; k++ {
					*(*uint16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int32:
				vs := vecs[j].Col.([]int32)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint32:
				vs := vecs[j].Col.([]uint32)
				for k := int64(0); k < n; k++ {
					*(*uint32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_float32:
				vs := vecs[j].Col.([]float32)
				for k := int64(0); k < n; k++ {
					*(*float32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int64:
				vs := vecs[j].Col.([]int64)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_uint64:
				vs := vecs[j].Col.([]uint64)
				for k := int64(0); k < n; k++ {
					*(*uint64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_float64:
				vs := vecs[j].Col.([]float64)
				for k := int64(0); k < n; k++ {
					*(*float64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				for k := int64(0); k < n; k++ {
					key := vs.Get(i + k)
					copy(data[k*40+int64(ctr.keyOffs[k]):], key)
					ctr.keyOffs[k] += uint32(len(key))
				}
			}
		}
		ctr.hashes[0] = 0
		ctr.h40.ht.InsertBatch(ctr.hashes, ctr.h40.keys[:n], ctr.values)
		cnt := 0
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		for k, v := range ctr.values[:n] {
			if v > ctr.rows {
				ctr.inserted[k] = 1
				ctr.rows++
				cnt++
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(v) - 1
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
		}
		if cnt > 0 {
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserted[:n], proc.Mp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (ctr *Container) processHStr(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	keys := make([][]byte, UnitLimit)
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit { // batch
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8:
				vs := vecs[j].Col.([]int8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
				}
			case types.T_uint8:
				vs := vecs[j].Col.([]uint8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
				}
			case types.T_int16:
				vs := vecs[j].Col.([]int16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
				}
			case types.T_uint16:
				vs := vecs[j].Col.([]uint16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
				}
			case types.T_int32:
				vs := vecs[j].Col.([]int32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			case types.T_uint32:
				vs := vecs[j].Col.([]uint32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			case types.T_float32:
				vs := vecs[j].Col.([]float32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			case types.T_int64:
				vs := vecs[j].Col.([]int64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			case types.T_uint64:
				vs := vecs[j].Col.([]uint64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			case types.T_float64:
				vs := vecs[j].Col.([]float64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], vs.Get(i+k)...)
				}
			}
		}
		cnt := 0
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		for k := int64(0); k < n; k++ {
			v := ctr.hstr.ht.Insert(hashtable.StringRef{Ptr: &keys[k][0], Len: len(keys[k])})
			keys[k] = keys[k][:0]
			if v > ctr.rows {
				ctr.inserted[k] = 1
				ctr.rows++
				cnt++
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ctr.values[k] = v
			ai := int64(v) - 1
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
		}
		if cnt > 0 {
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserted[:n], proc.Mp); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
