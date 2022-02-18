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

package mergededup

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedup"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"unsafe"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("mergeDeduplication")
}

func Prepare(_ *process.Process, arg interface{}) error {
	argument := arg.(*Argument)
	argument.ctr = new(container)
	argument.ctr.state = running
	argument.ctr.initFlag = false
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	var err error
	argument := arg.(*Argument)

	if len(proc.Reg.MergeReceivers) == 1 {
		reg := proc.Reg.MergeReceivers[0]
		bat := <-reg.Ch
		proc.Reg.InputBatch = bat
		return true, nil
	}

	for {
		switch argument.ctr.state {
		case running:
			for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
				reg := proc.Reg.MergeReceivers[i]
				bat := <-reg.Ch

				if bat == nil {
					proc.Reg.MergeReceivers = append(proc.Reg.MergeReceivers[:i], proc.Reg.MergeReceivers[i+1:]...)
					i--
					continue
				}
				if len(bat.Zs) == 0 {
					i--
					continue
				}
				// do init work
				if argument.ctr.initFlag == false {
					initHashTable(argument, bat)
					argument.ctr.initFlag = true
				}
				// do deduplication work
				switch argument.ctr.typ {
				case dedup.H8:
					err = argument.ctr.processH8(bat, proc)
				case dedup.H24:
					err = argument.ctr.processH24(bat, proc)
				case dedup.H32:
					err = argument.ctr.processH32(bat, proc)
				case dedup.H40:
					err = argument.ctr.processH40(bat, proc)
				default:
					err = argument.ctr.processHStr(bat, proc)
				}
				if err != nil {
					batch.Clean(argument.ctr.bat, proc.Mp)
					proc.Reg.InputBatch = nil
					return false, err
				}

				i--
			}
			if argument.ctr.bat != nil {
				for i := range argument.ctr.bat.Zs {
					argument.ctr.bat.Zs[i] = 1
				}
			}
			argument.ctr.state = end
		case end:
			proc.Reg.InputBatch = argument.ctr.bat
			argument.ctr.bat = nil
			return true, nil
		}
	}
}

func initHashTable(n *Argument, bat *batch.Batch) {
	size := 0
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
		case types.T_date:
			size += 4
		case types.T_datetime:
			size += 8
		}
	}
	n.ctr.keyOffs = make([]uint32, dedup.UnitLimit)
	n.ctr.zKeyOffs = make([]uint32, dedup.UnitLimit)
	n.ctr.inserted = make([]uint8, dedup.UnitLimit)
	n.ctr.zInserted = make([]uint8, dedup.UnitLimit)
	n.ctr.hashes = make([]uint64, dedup.UnitLimit)
	n.ctr.strHashStates = make([][3]uint64, dedup.UnitLimit)
	n.ctr.values = make([]uint64, dedup.UnitLimit)
	n.ctr.intHashMap = &hashtable.Int64HashMap{}
	n.ctr.strHashMap = &hashtable.StringHashMap{}
	switch {
	case size <= 8:
		n.ctr.typ = dedup.H8
		n.ctr.h8.keys = make([]uint64, dedup.UnitLimit)
		n.ctr.h8.zKeys = make([]uint64, dedup.UnitLimit)
		n.ctr.intHashMap.Init()
	case size <= 24:
		n.ctr.typ = dedup.H24
		n.ctr.h24.keys = make([][3]uint64, dedup.UnitLimit)
		n.ctr.h24.zKeys = make([][3]uint64, dedup.UnitLimit)
		n.ctr.strHashMap.Init()
	case size <= 32:
		n.ctr.typ = dedup.H32
		n.ctr.h32.keys = make([][4]uint64, dedup.UnitLimit)
		n.ctr.h32.zKeys = make([][4]uint64, dedup.UnitLimit)
		n.ctr.strHashMap.Init()
	case size <= 40:
		n.ctr.typ = dedup.H40
		n.ctr.h40.keys = make([][5]uint64, dedup.UnitLimit)
		n.ctr.h40.zKeys = make([][5]uint64, dedup.UnitLimit)
		n.ctr.strHashMap.Init()
	default:
		n.ctr.typ = dedup.HStr
		n.ctr.strHashMap.Init()
	}
}

func (ctr *container) processH8(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += dedup.UnitLimit {
		n := count - i
		if n > dedup.UnitLimit {
			n = dedup.UnitLimit
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
			case types.T_date:
				vs := vecs[j].Col.([]types.Date)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = int32(vs[i+k])
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
			case types.T_datetime:
				vs := vecs[j].Col.([]types.Datetime)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = int64(vs[i+k])
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
		ctr.intHashMap.InsertBatch(int(n), ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values)
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

func (ctr *container) processH24(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += dedup.UnitLimit {
		n := count - i
		if n > dedup.UnitLimit {
			n = dedup.UnitLimit
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
			case types.T_date:
				vs := vecs[j].Col.([]types.Date)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = int32(vs[i+k])
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
			case types.T_datetime:
				vs := vecs[j].Col.([]types.Datetime)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = int64(vs[i+k])
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
		ctr.strHashMap.InsertString24Batch(ctr.strHashStates, ctr.h24.keys[:n], ctr.values)
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

func (ctr *container) processH32(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += dedup.UnitLimit {
		n := count - i
		if n > dedup.UnitLimit {
			n = dedup.UnitLimit
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
			case types.T_date:
				vs := vecs[j].Col.([]types.Date)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = int32(vs[i+k])
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
			case types.T_datetime:
				vs := vecs[j].Col.([]types.Datetime)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = int64(vs[i+k])
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
		ctr.strHashMap.InsertString32Batch(ctr.strHashStates, ctr.h32.keys[:n], ctr.values)
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

func (ctr *container) processH40(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += dedup.UnitLimit {
		n := count - i
		if n > dedup.UnitLimit {
			n = dedup.UnitLimit
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
			case types.T_date:
				vs := vecs[j].Col.([]types.Date)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = int32(vs[i+k])
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
			case types.T_datetime:
				vs := vecs[j].Col.([]types.Datetime)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = int64(vs[i+k])
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
		ctr.strHashMap.InsertString40Batch(ctr.strHashStates, ctr.h40.keys[:n], ctr.values)
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

func (ctr *container) processHStr(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs
	keys := make([][]byte, dedup.UnitLimit)
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += dedup.UnitLimit { // batch
		n := count - i
		if n > dedup.UnitLimit {
			n = dedup.UnitLimit
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
			case types.T_date:
				vs := vecs[j].Col.([]types.Date)
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
			case types.T_datetime:
				vs := vecs[j].Col.([]types.Datetime)
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
		for k := int64(0); k < n; k++ {
			if l := len(keys[k]); l < 16 {
				keys[k] = append(keys[k], hashtable.StrKeyPadding[l:]...)
			}
		}
		ctr.strHashMap.InsertStringBatch(ctr.strHashStates, keys[:n], ctr.values)
		cnt := 0
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		for k, v := range ctr.values[:n] {
			keys[k] = keys[k][:0]
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
