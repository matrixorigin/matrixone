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

package mergegroup

import (
	"bytes"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ interface{}, buf *bytes.Buffer) {
	buf.WriteString("γ()")
}

func Prepare(_ *process.Process, arg interface{}) error {
	ap := arg.(*Argument)
	ap.ctr = new(Container)
	return nil
}

func Call(_ int, proc *process.Process, arg interface{}) (bool, error) {
	ap := arg.(*Argument)
	ctr := ap.ctr
	for {
		switch ctr.state {
		case Build:
			if err := ctr.build(proc); err != nil {
				ctr.state = End
				return true, err
			}
			ctr.state = Eval
		case Eval:
			ctr.state = End
			if ctr.bat != nil {
				if ap.NeedEval {
					for _, r := range ctr.bat.Rs {
						ctr.bat.Vecs = append(ctr.bat.Vecs, r.Eval(ctr.bat.Zs))
					}
					ctr.bat.Rs = nil
					for i := range ctr.bat.Zs { // reset zs
						ctr.bat.Zs[i] = 1
					}
				}
				ctr.bat.ExpandNulls()
			}
			proc.Reg.InputBatch = ctr.bat
			ctr.bat = nil
			return true, nil
		case End:
			proc.Reg.InputBatch = nil
			return true, nil
		}
	}
}

func (ctr *Container) build(proc *process.Process) error {
	if len(proc.Reg.MergeReceivers) == 1 {
		for {
			bat := <-proc.Reg.MergeReceivers[0].Ch
			if bat == nil {
				return nil
			}
			if len(bat.Zs) == 0 {
				continue
			}
			ctr.bat = bat
			return nil
		}
	}
	for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
		bat := <-proc.Reg.MergeReceivers[i].Ch
		if bat == nil {
			continue
		}
		if len(bat.Zs) == 0 {
			i--
			continue
		}
		if err := ctr.process(bat, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) process(bat *batch.Batch, proc *process.Process) error {
	var err error

	if ctr.bat == nil {
		size := 0
		for _, vec := range bat.Vecs {
			switch vec.Typ.Oid {
			case types.T_int8, types.T_uint8, types.T_bool:
				size += 1 + 1
			case types.T_int16, types.T_uint16:
				size += 2 + 1
			case types.T_int32, types.T_uint32, types.T_float32, types.T_date:
				size += 4 + 1
			case types.T_int64, types.T_uint64, types.T_float64, types.T_datetime, types.T_decimal64:
				size += 8 + 1
			case types.T_decimal128:
				size += 16 + 1
			case types.T_char, types.T_varchar:
				if width := vec.Typ.Width; width > 0 {
					size += int(width) + 1
				} else {
					size = 128
				}
			}
		}
		ctr.keyOffs = make([]uint32, UnitLimit)
		ctr.zKeyOffs = make([]uint32, UnitLimit)
		ctr.inserted = make([]uint8, UnitLimit)
		ctr.zInserted = make([]uint8, UnitLimit)
		ctr.hashes = make([]uint64, UnitLimit)
		ctr.strHashStates = make([][3]uint64, UnitLimit)
		ctr.values = make([]uint64, UnitLimit)
		ctr.intHashMap = &hashtable.Int64HashMap{}
		ctr.strHashMap = &hashtable.StringHashMap{}
		switch {
		case size == 0:
			ctr.typ = H0
		case size <= 8:
			ctr.typ = H8
			ctr.h8.keys = make([]uint64, UnitLimit)
			ctr.h8.zKeys = make([]uint64, UnitLimit)
			ctr.intHashMap.Init()
		case size <= 24:
			ctr.typ = H24
			ctr.h24.keys = make([][3]uint64, UnitLimit)
			ctr.h24.zKeys = make([][3]uint64, UnitLimit)
			ctr.strHashMap.Init()
		case size <= 32:
			ctr.typ = H32
			ctr.h32.keys = make([][4]uint64, UnitLimit)
			ctr.h32.zKeys = make([][4]uint64, UnitLimit)
			ctr.strHashMap.Init()
		case size <= 40:
			ctr.typ = H40
			ctr.h40.keys = make([][5]uint64, UnitLimit)
			ctr.h40.zKeys = make([][5]uint64, UnitLimit)
			ctr.strHashMap.Init()
		default:
			ctr.typ = HStr
			ctr.hstr.keys = make([][]byte, UnitLimit)
			ctr.strHashMap.Init()
		}
	}
	switch ctr.typ {
	case H0:
		err = ctr.processH0(bat, proc)
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
		ctr.bat.Clean(proc.Mp)
		ctr.bat = nil
		return err
	}
	return nil
}

func (ctr *Container) processH0(bat *batch.Batch, proc *process.Process) error {
	if ctr.bat == nil {
		ctr.bat = bat
		return nil
	}
	defer bat.Clean(proc.Mp)
	for _, z := range bat.Zs {
		ctr.bat.Zs[0] += z
	}
	for i, r := range ctr.bat.Rs {
		r.Add(bat.Rs[i], 0, 0)
	}
	return nil
}

func (ctr *Container) processH8(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
	flg := ctr.bat == nil
	if !flg {
		defer bat.Clean(proc.Mp)
	}
	for i := 0; i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h8.keys, ctr.h8.zKeys)
		for _, vec := range bat.Vecs {
			switch typLen := vec.Typ.Oid.FixedLength(); typLen {
			case 1:
				fillGroup[uint8](ctr, vec, ctr.h8.keys, n, 1, i)
			case 2:
				fillGroup[uint16](ctr, vec, ctr.h8.keys, n, 2, i)
			case 4:
				fillGroup[uint32](ctr, vec, ctr.h8.keys, n, 4, i)
			case 8:
				fillGroup[uint64](ctr, vec, ctr.h8.keys, n, 8, i)
			case -8:
				fillGroup[uint64](ctr, vec, ctr.h8.keys, n, 8, i)
			case -16:
				fillGroup[types.Decimal128](ctr, vec, ctr.h8.keys, n, 16, i)
			default:
				fillStringGroup(ctr, vec, ctr.h8.keys, n, 8, i)
			}
		}
		ctr.hashes[0] = 0
		ctr.intHashMap.InsertBatch(n, ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), ctr.values)
		if !flg {
			if err := ctr.batchFill(i, n, bat, proc); err != nil {
				return err
			}
		}
	}
	if flg {
		ctr.bat = bat
		ctr.rows = ctr.intHashMap.Cardinality()
	}
	return nil
}

func (ctr *Container) processH24(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
	flg := ctr.bat == nil
	if !flg {
		defer bat.Clean(proc.Mp)
	}
	for i := 0; i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h24.keys, ctr.h24.zKeys)
		for _, vec := range bat.Vecs {
			switch typLen := vec.Typ.Oid.FixedLength(); typLen {
			case 1:
				fillGroup[uint8](ctr, vec, ctr.h24.keys, n, 1, i)
			case 2:
				fillGroup[uint16](ctr, vec, ctr.h24.keys, n, 2, i)
			case 4:
				fillGroup[uint32](ctr, vec, ctr.h24.keys, n, 4, i)
			case 8:
				fillGroup[uint64](ctr, vec, ctr.h24.keys, n, 8, i)
			case -8:
				fillGroup[types.Decimal64](ctr, vec, ctr.h24.keys, n, 8, i)
			case -16:
				fillGroup[types.Decimal128](ctr, vec, ctr.h24.keys, n, 16, i)
			default:
				fillStringGroup(ctr, vec, ctr.h24.keys, n, 24, i)
			}
		}
		ctr.strHashMap.InsertString24Batch(ctr.strHashStates, ctr.h24.keys[:n], ctr.values)
		if !flg {
			if err := ctr.batchFill(i, n, bat, proc); err != nil {
				return err
			}
		}
	}
	if flg {
		ctr.bat = bat
		ctr.rows = ctr.strHashMap.Cardinality()
	}
	return nil
}

func (ctr *Container) processH32(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
	flg := ctr.bat == nil
	if !flg {
		defer bat.Clean(proc.Mp)
	}
	for i := 0; i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h32.keys, ctr.h32.zKeys)
		for _, vec := range bat.Vecs {
			switch typLen := vec.Typ.Oid.FixedLength(); typLen {
			case 1:
				fillGroup[uint8](ctr, vec, ctr.h32.keys, n, 1, i)
			case 2:
				fillGroup[uint16](ctr, vec, ctr.h32.keys, n, 2, i)
			case 4:
				fillGroup[uint32](ctr, vec, ctr.h32.keys, n, 4, i)
			case 8:
				fillGroup[uint64](ctr, vec, ctr.h32.keys, n, 8, i)
			case -8:
				fillGroup[uint64](ctr, vec, ctr.h32.keys, n, 8, i)
			case -16:
				fillGroup[types.Decimal128](ctr, vec, ctr.h32.keys, n, 16, i)
			default:
				fillStringGroup(ctr, vec, ctr.h32.keys, n, 32, i)
			}
		}
		ctr.strHashMap.InsertString32Batch(ctr.strHashStates, ctr.h32.keys[:n], ctr.values)
		if !flg {
			if err := ctr.batchFill(i, n, bat, proc); err != nil {
				return err
			}
		}
	}
	if flg {
		ctr.bat = bat
		ctr.rows = ctr.strHashMap.Cardinality()
	}
	return nil
}

func (ctr *Container) processH40(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
	flg := ctr.bat == nil
	if !flg {
		defer bat.Clean(proc.Mp)
	}
	for i := 0; i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h40.keys, ctr.h40.zKeys)
		for _, vec := range bat.Vecs {
			switch typLen := vec.Typ.Oid.FixedLength(); typLen {
			case 1:
				fillGroup[uint8](ctr, vec, ctr.h40.keys, n, 1, i)
			case 2:
				fillGroup[uint16](ctr, vec, ctr.h40.keys, n, 2, i)
			case 4:
				fillGroup[uint32](ctr, vec, ctr.h40.keys, n, 4, i)
			case 8:
				fillGroup[uint64](ctr, vec, ctr.h40.keys, n, 8, i)
			case -8:
				fillGroup[uint64](ctr, vec, ctr.h40.keys, n, 8, i)
			case -16:
				fillGroup[types.Decimal128](ctr, vec, ctr.h40.keys, n, 16, i)
			default:
				fillStringGroup(ctr, vec, ctr.h40.keys, n, 40, i)
			}
		}
		ctr.strHashMap.InsertString40Batch(ctr.strHashStates, ctr.h40.keys[:n], ctr.values)
		if !flg {
			if err := ctr.batchFill(i, n, bat, proc); err != nil {
				return err
			}
		}
	}
	if flg {
		ctr.bat = bat
		ctr.rows = ctr.strHashMap.Cardinality()
	}
	return nil
}

func (ctr *Container) processHStr(bat *batch.Batch, proc *process.Process) error {
	count := len(bat.Zs)
	flg := ctr.bat == nil
	if !flg {
		defer bat.Clean(proc.Mp)
	}
	for i := 0; i < count; i += UnitLimit { // batch
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		for _, vec := range bat.Vecs {
			switch typLen := vec.Typ.Oid.FixedLength(); typLen {
			case 1:
				fillGroupStr[uint8](ctr, vec, n, 1, i)
			case 2:
				fillGroupStr[uint16](ctr, vec, n, 2, i)
			case 4:
				fillGroupStr[uint32](ctr, vec, n, 4, i)
			case 8:
				fillGroupStr[uint64](ctr, vec, n, 8, i)
			case -8:
				fillGroupStr[uint64](ctr, vec, n, 8, i)
			case -16:
				fillGroupStr[types.Decimal128](ctr, vec, n, 16, i)
			default:
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
		ctr.strHashMap.InsertStringBatch(ctr.strHashStates, ctr.hstr.keys[:n], ctr.values)
		for k := 0; k < n; k++ {
			ctr.hstr.keys[k] = ctr.hstr.keys[k][:0]
		}
		if !flg {
			if err := ctr.batchFill(i, n, bat, proc); err != nil {
				return err
			}
		}
	}
	if flg {
		ctr.bat = bat
		ctr.rows = ctr.strHashMap.Cardinality()
	}
	return nil
}

func (ctr *Container) batchFill(i int, n int, bat *batch.Batch, proc *process.Process) error {
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
		ctr.bat.Zs[ai] += bat.Zs[i+k]
	}
	if cnt > 0 {
		for j, vec := range ctr.bat.Vecs {
			if err := vector.UnionBatch(vec, bat.Vecs[j], int64(i), cnt, ctr.inserted[:n], proc.Mp); err != nil {
				return err
			}
		}
		for _, r := range ctr.bat.Rs {
			if err := r.Grows(cnt, proc.Mp); err != nil {
				return err
			}
		}
	}
	for j, r := range ctr.bat.Rs {
		r.BatchAdd(bat.Rs[j], int64(i), ctr.inserted[:n], ctr.values)
	}
	return nil
}

func fillGroup[T1, T2 any](ctr *Container, vec *vector.Vector, keys []T2, n int, sz uint32, start int) {
	vs := vector.GetFixedVectorValues[T1](vec, int(sz))
	if !nulls.Any(vec.Nsp) {
		for i := 0; i < n; i++ {
			*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i])) = 0
			*(*T1)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i]+1)) = vs[i+start]
		}
		add.Uint32AddScalar(1+sz, ctr.keyOffs[:n], ctr.keyOffs[:n])
	} else {
		for i := 0; i < n; i++ {
			if vec.Nsp.Np.Contains(uint64(i + start)) {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i])) = 1
				ctr.keyOffs[i]++
			} else {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i])) = 0
				*(*T1)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i]+1)) = vs[i+start]
				ctr.keyOffs[i] += 1 + sz
			}
		}
	}
}

func fillStringGroup[T any](ctr *Container, vec *vector.Vector, keys []T, n int, sz uint32, start int) {
	vData, vOff, vLen := vector.GetStrVectorValues(vec)
	if !nulls.Any(vec.Nsp) {
		for i := 0; i < n; i++ {
			*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i])) = 0
			copy(unsafe.Slice((*byte)(unsafe.Pointer(&keys[i])), sz)[ctr.keyOffs[i]+1:], vData[vOff[i+start]:vOff[i+start]+vLen[i+start]])
			ctr.keyOffs[i] += vLen[i+start] + 1
		}
	} else {
		for i := 0; i < n; i++ {
			if vec.Nsp.Np.Contains(uint64(i + start)) {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i])) = 1
				ctr.keyOffs[i]++
			} else {
				*(*int8)(unsafe.Add(unsafe.Pointer(&keys[i]), ctr.keyOffs[i])) = 0
				copy(unsafe.Slice((*byte)(unsafe.Pointer(&keys[i])), sz)[ctr.keyOffs[i]+1:], vData[vOff[i+start]:vOff[i+start]+vLen[i+start]])
				ctr.keyOffs[i] += vLen[i+start] + 1
			}
		}
	}
}

func fillGroupStr[T any](ctr *Container, vec *vector.Vector, n int, sz int, start int) {
	vs := vector.GetFixedVectorValues[T](vec, int(sz))
	data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*sz)[:len(vs)*sz]
	if !nulls.Any(vec.Nsp) {
		for i := 0; i < n; i++ {
			ctr.hstr.keys[i] = append(ctr.hstr.keys[i], byte(0))
			ctr.hstr.keys[i] = append(ctr.hstr.keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
		}
	} else {
		for i := 0; i < n; i++ {
			if vec.Nsp.Np.Contains(uint64(i + start)) {
				ctr.hstr.keys[i] = append(ctr.hstr.keys[i], byte(1))
			} else {
				ctr.hstr.keys[i] = append(ctr.hstr.keys[i], byte(0))
				ctr.hstr.keys[i] = append(ctr.hstr.keys[i], data[(i+start)*sz:(i+start+1)*sz]...)
			}
		}
	}
}
