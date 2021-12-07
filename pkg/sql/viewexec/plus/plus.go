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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
			ctr.keyOffs = make([]uint32, UnitLimit)
			ctr.zKeyOffs = make([]uint32, UnitLimit)
			ctr.inserts = make([]uint8, UnitLimit)
			ctr.zinserts = make([]uint8, UnitLimit)
			ctr.hashs = make([]uint64, UnitLimit)
			ctr.values = make([]*uint64, UnitLimit)
			ctr.h8.keys = make([]uint64, UnitLimit)
			ctr.h8.zKeys = make([]uint64, UnitLimit)
			ctr.h8.ht = &hashtable.Int64HashMap{}
			ctr.h8.ht.Init()
		case size <= 24:
			ctr.typ = H24
			ctr.keyOffs = make([]uint32, UnitLimit)
			ctr.zKeyOffs = make([]uint32, UnitLimit)
			ctr.inserts = make([]uint8, UnitLimit)
			ctr.zinserts = make([]uint8, UnitLimit)
			ctr.hashs = make([]uint64, UnitLimit)
			ctr.values = make([]*uint64, UnitLimit)
			ctr.h24.keys = make([][3]uint64, UnitLimit)
			ctr.h24.zKeys = make([][3]uint64, UnitLimit)
			ctr.h24.ht = &hashtable.String24HashMap{}
			ctr.h24.ht.Init()
		case size <= 32:
			ctr.typ = H32
			ctr.keyOffs = make([]uint32, UnitLimit)
			ctr.zKeyOffs = make([]uint32, UnitLimit)
			ctr.inserts = make([]uint8, UnitLimit)
			ctr.zinserts = make([]uint8, UnitLimit)
			ctr.hashs = make([]uint64, UnitLimit)
			ctr.values = make([]*uint64, UnitLimit)
			ctr.h32.keys = make([][4]uint64, UnitLimit)
			ctr.h32.zKeys = make([][4]uint64, UnitLimit)
			ctr.h32.ht = &hashtable.String32HashMap{}
			ctr.h32.ht.Init()
		case size <= 40:
			ctr.typ = H40
			ctr.keyOffs = make([]uint32, UnitLimit)
			ctr.zKeyOffs = make([]uint32, UnitLimit)
			ctr.inserts = make([]uint8, UnitLimit)
			ctr.zinserts = make([]uint8, UnitLimit)
			ctr.hashs = make([]uint64, UnitLimit)
			ctr.values = make([]*uint64, UnitLimit)
			ctr.h40.keys = make([][5]uint64, UnitLimit)
			ctr.h40.zKeys = make([][5]uint64, UnitLimit)
			ctr.h40.ht = &hashtable.String40HashMap{}
			ctr.h40.ht.Init()
		default:
			ctr.typ = HStr
			ctr.keyOffs = make([]uint32, UnitLimit)
			ctr.zKeyOffs = make([]uint32, UnitLimit)
			ctr.inserts = make([]uint8, UnitLimit)
			ctr.zinserts = make([]uint8, UnitLimit)
			ctr.hashs = make([]uint64, UnitLimit)
			ctr.values = make([]*uint64, UnitLimit)
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
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	if ctr.bat == nil {
		ctr.bat = bat
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
			ctr.hashs[0] = 0
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			ctr.h8.ht.InsertBatch(int(n), ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values)
			for k, _ := range ctr.inserts[:n] {
				*ctr.values[k] = ctr.rows
				ctr.rows++
			}
		}
		return nil
	}
	defer batch.Clean(bat, proc.Mp)
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
		ctr.hashs[0] = 0
		copy(ctr.inserts[:n], ctr.zinserts[:n])
		ctr.h8.ht.InsertBatch(int(n), ctr.hashs, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserts, ctr.values)
		{ // batch
			cnt := 0
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = ctr.rows
					ctr.rows++
					cnt++
					ctr.bat.Zs = append(ctr.bat.Zs, 0)
				}
				ai := int64(*ctr.values[k])
				ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
			}
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserts[:n], proc.Mp); err != nil {
					return err
				}
			}
			for _, r := range ctr.bat.Rs {
				if err := r.Grows(cnt, proc.Mp); err != nil {
					return err
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchAdd(bat.Rs[j], i, ctr.inserts[:n], ctr.values)
			}
		}
	}
	return nil
}

func (ctr *Container) fillH24(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	if ctr.bat == nil {
		ctr.bat = bat
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
		ctr.hashs[0] = 0
		copy(ctr.inserts[:n], ctr.zinserts[:n])
		ctr.h24.ht.InsertBatch(ctr.hashs, ctr.h24.keys[:n], ctr.inserts, ctr.values)
		{ // batch
			cnt := 0
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = ctr.rows
					ctr.rows++
					cnt++
					ctr.bat.Zs = append(ctr.bat.Zs, 0)
				}
				ai := int64(*ctr.values[k])
				ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
			}
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserts[:n], proc.Mp); err != nil {
					return err
				}
			}
			for _, r := range ctr.bat.Rs {
				if err := r.Grows(cnt, proc.Mp); err != nil {
					return err
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchAdd(bat.Rs[j], i, ctr.inserts[:n], ctr.values)
			}
		}
	}
	return nil
}

func (ctr *Container) fillH32(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	if ctr.bat == nil {
		ctr.bat = bat
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
		ctr.hashs[0] = 0
		copy(ctr.inserts[:n], ctr.zinserts[:n])
		ctr.h32.ht.InsertBatch(ctr.hashs, ctr.h32.keys[:n], ctr.inserts, ctr.values)
		{ // batch
			cnt := 0
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = ctr.rows
					ctr.rows++
					cnt++
					ctr.bat.Zs = append(ctr.bat.Zs, 0)
				}
				ai := int64(*ctr.values[k])
				ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
			}
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserts[:n], proc.Mp); err != nil {
					return err
				}
			}
			for _, r := range ctr.bat.Rs {
				if err := r.Grows(cnt, proc.Mp); err != nil {
					return err
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchAdd(bat.Rs[j], i, ctr.inserts[:n], ctr.values)
			}
		}
	}
	return nil
}

func (ctr *Container) fillH40(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	if ctr.bat == nil {
		ctr.bat = bat
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
		ctr.hashs[0] = 0
		copy(ctr.inserts[:n], ctr.zinserts[:n])
		ctr.h40.ht.InsertBatch(ctr.hashs, ctr.h40.keys[:n], ctr.inserts, ctr.values)
		{ // batch
			cnt := 0
			for k, ok := range ctr.inserts[:n] {
				if ok == 1 {
					*ctr.values[k] = ctr.rows
					ctr.rows++
					cnt++
					ctr.bat.Zs = append(ctr.bat.Zs, 0)
				}
				ai := int64(*ctr.values[k])
				ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
			}
			for j, vec := range ctr.bat.Vecs {
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserts[:n], proc.Mp); err != nil {
					return err
				}
			}
			for _, r := range ctr.bat.Rs {
				if err := r.Grows(cnt, proc.Mp); err != nil {
					return err
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchAdd(bat.Rs[j], i, ctr.inserts[:n], ctr.values)
			}
		}
	}
	return nil
}

func (ctr *Container) fillHStr(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	if ctr.bat == nil {
		ctr.bat = bat
		for i := int64(0); i < count; i += UnitLimit { // batch
			n := count - i
			if n > UnitLimit {
				n = UnitLimit
			}
			copy(ctr.inserts[:n], ctr.zinserts[:n])
			keys := make([][]byte, UnitLimit)
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
					data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*4]
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
						keys[k] = vs.Get(i + k)
					}
				}
			}
			for k := int64(0); k < n; k++ {
				_, vp := ctr.hstr.ht.Insert(hashtable.StringRef{Ptr: &keys[k][0], Len: len(keys[k])})
				*vp = ctr.rows
				ctr.rows++
			}
		}
		return nil
	}
	defer batch.Clean(bat, proc.Mp)
	for i := int64(0); i < count; i += UnitLimit { // batch
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.inserts[:n], ctr.zinserts[:n])
		keys := make([][]byte, UnitLimit)
		cnt := 0
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
					keys[k] = vs.Get(i + k)
				}
			}
		}
		for k := int64(0); k < n; k++ {
			ok, vp := ctr.hstr.ht.Insert(hashtable.StringRef{Ptr: &keys[k][0], Len: len(keys[k])})
			if ok {
				ctr.inserts[k] = 1
				*vp = ctr.rows
				ctr.rows++
				cnt++
				ctr.bat.Zs = append(ctr.bat.Zs, 0)
			}
			ai := int64(*vp)
			ctr.values[k] = vp
			ctr.bat.Zs[ai] += bat.Zs[i+int64(k)]
		}
		for j, vec := range ctr.bat.Vecs {
			if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserts[:n], proc.Mp); err != nil {
				return err
			}
		}
		for _, r := range ctr.bat.Rs {
			if err := r.Grows(cnt, proc.Mp); err != nil {
				return err
			}
		}
		for j, r := range ctr.bat.Rs {
			r.BatchAdd(bat.Rs[j], i, ctr.inserts[:n], ctr.values)
		}
	}
	return nil
}
