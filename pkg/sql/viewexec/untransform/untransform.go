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

package untransform

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

func String(arg interface{}, buf *bytes.Buffer) {
	n := arg.(*Argument)
	buf.WriteString("∐ ([")
	for i, fvar := range n.FreeVars {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(fvar)
	}
	buf.WriteString("])")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	n := arg.(*Argument)
	n.ctr = new(Container)
	switch n.Type {
	case Bare:
		return n.ctr.processBare(n.FreeVars, proc)
	case Single:
		return n.ctr.processSingle(n.FreeVars, proc)
	default:
		if len(n.FreeVars) == 0 {
			return n.ctr.processBoundVars(proc)
		}
		return n.ctr.processFreeVars(n.FreeVars, proc)
	}
}

func (ctr *Container) processBare(fvars []string, proc *process.Process) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil {
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	for i, r := range bat.Rs {
		bat.Attrs = append(bat.Attrs, bat.As[i])
		vec := r.Eval(bat.Zs)
		vec.Ref = bat.Refs[i]
		bat.Vecs = append(bat.Vecs, vec)
	}
	bat.Rs = nil
	if len(fvars) > 0 {
		batch.Reduce(bat, fvars, proc.Mp)
	}
	return false, nil
}

func (ctr *Container) processSingle(fvars []string, proc *process.Process) (bool, error) {
	bat := proc.Reg.InputBatch
	if bat == nil {
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	for i, r := range bat.Rs {
		bat.Attrs = append(bat.Attrs, bat.As[i])
		vec := r.Eval(bat.Zs)
		vec.Ref = bat.Refs[i]
		bat.Vecs = append(bat.Vecs, vec)
	}
	bat.Rs = nil
	if len(fvars) > 0 {
		batch.Reduce(bat, fvars, proc.Mp)
	}
	for i := range bat.Zs {
		bat.Zs[i] = 1
	}
	return false, nil
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
					ctr.bat = new(batch.Batch)
					for k, r := range bat.Rs {
						ctr.bat.Rs = append(ctr.bat.Rs, r.Dup())
						ctr.bat.As = append(ctr.bat.As, bat.As[k])
						ctr.bat.Refs = append(ctr.bat.Refs, bat.Refs[k])
					}
					for _, r := range ctr.bat.Rs {
						if err := r.Grow(proc.Mp); err != nil {
							return false, err
						}
					}
					ctr.bat.Zs = append(ctr.bat.Zs, 0)
				}
				for k, z := range bat.Zs {
					ctr.bat.Zs[0] += z
					for j, r := range ctr.bat.Rs {
						r.Add(bat.Rs[j], 0, int64(k))
					}
				}
				batch.Clean(bat, proc.Mp)
			}
			ctr.state = Eval
		case Eval:
			if ctr.bat != nil {
				for i, r := range ctr.bat.Rs {
					ctr.bat.Attrs = append(ctr.bat.Attrs, ctr.bat.As[i])
					vec := r.Eval(ctr.bat.Zs)
					vec.Ref = ctr.bat.Refs[i]
					ctr.bat.Vecs = append(ctr.bat.Vecs, vec)
				}
				ctr.bat.Rs = nil
				for i := range ctr.bat.Zs {
					ctr.bat.Zs[i] = 1
				}
				proc.Reg.InputBatch = ctr.bat
				ctr.bat = nil
			}
			return true, nil
		}
	}

}

func (ctr *Container) processFreeVars(fvars []string, proc *process.Process) (bool, error) {
	for {
		switch ctr.state {
		case Fill:
			if err := ctr.fill(fvars, proc); err != nil {
				batch.Clean(ctr.bat, proc.Mp)
				proc.Reg.InputBatch = nil
				ctr.state = Eval
				return true, err
			}
			ctr.state = Eval
		case Eval:
			if ctr.bat != nil {
				for i, r := range ctr.bat.Rs {
					ctr.bat.Attrs = append(ctr.bat.Attrs, ctr.bat.As[i])
					vec := r.Eval(ctr.bat.Zs)
					vec.Ref = ctr.bat.Refs[i]
					ctr.bat.Vecs = append(ctr.bat.Vecs, vec)
				}
				ctr.bat.Rs = nil
				for i := range ctr.bat.Zs {
					ctr.bat.Zs[i] = 1
				}
				if len(fvars) > 0 {
					batch.Reduce(ctr.bat, fvars, proc.Mp)
				}
				proc.Reg.InputBatch = ctr.bat
				ctr.bat = nil
			}
			return true, nil
		}
	}
}

func (ctr *Container) fill(fvars []string, proc *process.Process) error {
	for i := 0; i < len(proc.Reg.MergeReceivers); i++ {
		bat := <-proc.Reg.MergeReceivers[i].Ch
		if bat == nil {
			continue
		}
		if len(bat.Zs) == 0 {
			i--
			continue
		}
		if err := ctr.fillBatch(fvars, bat, proc); err != nil {
			return err
		}
	}
	return nil
}

func (ctr *Container) fillBatch(fvars []string, bat *batch.Batch, proc *process.Process) error {
	if len(ctr.vars) == 0 {
		ctr.vars = append(ctr.vars, bat.Attrs...)
		size := 0
		ctr.bat = batch.New(true, bat.Attrs)
		for i, vec := range bat.Vecs {
			ctr.bat.Vecs[i] = vector.New(vec.Typ)
			ctr.bat.Vecs[i].Ref = vec.Ref
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
			ctr.inserted = make([]uint8, UnitLimit)
			ctr.zInserted = make([]uint8, UnitLimit)
			ctr.hashes = make([]uint64, UnitLimit)
			ctr.values = make([]*uint64, UnitLimit)
			ctr.h8.keys = make([]uint64, UnitLimit)
			ctr.h8.zKeys = make([]uint64, UnitLimit)
			ctr.h8.ht = &hashtable.Int64HashMap{}
			ctr.h8.ht.Init()
		case size <= 24:
			ctr.typ = H24
			ctr.keyOffs = make([]uint32, UnitLimit)
			ctr.zKeyOffs = make([]uint32, UnitLimit)
			ctr.inserted = make([]uint8, UnitLimit)
			ctr.zInserted = make([]uint8, UnitLimit)
			ctr.hashes = make([]uint64, UnitLimit)
			ctr.values = make([]*uint64, UnitLimit)
			ctr.h24.keys = make([][3]uint64, UnitLimit)
			ctr.h24.zKeys = make([][3]uint64, UnitLimit)
			ctr.h24.ht = &hashtable.String24HashMap{}
			ctr.h24.ht.Init()
		case size <= 32:
			ctr.typ = H32
			ctr.keyOffs = make([]uint32, UnitLimit)
			ctr.zKeyOffs = make([]uint32, UnitLimit)
			ctr.inserted = make([]uint8, UnitLimit)
			ctr.zInserted = make([]uint8, UnitLimit)
			ctr.hashes = make([]uint64, UnitLimit)
			ctr.values = make([]*uint64, UnitLimit)
			ctr.h32.keys = make([][4]uint64, UnitLimit)
			ctr.h32.zKeys = make([][4]uint64, UnitLimit)
			ctr.h32.ht = &hashtable.String32HashMap{}
			ctr.h32.ht.Init()
		case size <= 40:
			ctr.typ = H40
			ctr.keyOffs = make([]uint32, UnitLimit)
			ctr.zKeyOffs = make([]uint32, UnitLimit)
			ctr.inserted = make([]uint8, UnitLimit)
			ctr.zInserted = make([]uint8, UnitLimit)
			ctr.hashes = make([]uint64, UnitLimit)
			ctr.values = make([]*uint64, UnitLimit)
			ctr.h40.keys = make([][5]uint64, UnitLimit)
			ctr.h40.zKeys = make([][5]uint64, UnitLimit)
			ctr.h40.ht = &hashtable.String40HashMap{}
			ctr.h40.ht.Init()
		default:
			ctr.typ = HStr
			ctr.keyOffs = make([]uint32, UnitLimit)
			ctr.zKeyOffs = make([]uint32, UnitLimit)
			ctr.hstr.ht = &hashtable.StringHashMap{}
			ctr.hstr.ht.Init()
		}
		for k, r := range bat.Rs {
			ctr.bat.Rs = append(ctr.bat.Rs, r.Dup())
			ctr.bat.As = append(ctr.bat.As, bat.As[k])
			ctr.bat.Refs = append(ctr.bat.Refs, bat.Refs[k])
		}
	} else {
		batch.Reorder(bat, ctr.vars)
	}
	switch ctr.typ {
	case H8:
		return ctr.processH8(fvars, bat, proc)
	case H24:
		return ctr.processH24(fvars, bat, proc)
	case H32:
		return ctr.processH32(fvars, bat, proc)
	case H40:
		return ctr.processH40(fvars, bat, proc)
	default:
		return ctr.processHStr(fvars, bat, proc)
	}
}

func (ctr *Container) processH8(fvars []string, bat *batch.Batch, proc *process.Process) error {
	defer batch.Clean(bat, proc.Mp)
	vecs := bat.Vecs[:len(fvars)]
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
			case types.T_int8, types.T_uint8:
				vs := vecs[j].Col.([]int8)
				for k := int64(0); k < n; k++ {
					*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int16, types.T_uint16:
				vs := vecs[j].Col.([]int16)
				for k := int64(0); k < n; k++ {
					*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int32, types.T_uint32, types.T_float32:
				vs := vecs[j].Col.([]int32)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int64, types.T_uint64, types.T_float64:
				vs := vecs[j].Col.([]int64)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h8.keys[k]), ctr.keyOffs[k])) = vs[i+k]
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
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		ctr.h8.ht.InsertBatch(int(n), ctr.hashes, unsafe.Pointer(&ctr.h8.keys[0]), ctr.inserted, ctr.values)
		{ // batch
			cnt := 0
			for k, ok := range ctr.inserted[:n] {
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
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserted, proc.Mp); err != nil {
					return err
				}
			}
			for _, r := range ctr.bat.Rs {
				if err := r.Grows(cnt, proc.Mp); err != nil {
					return err
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchAdd(bat.Rs[j], i, ctr.inserted[:n], ctr.values)
			}
		}
	}
	return nil
}

func (ctr *Container) processH24(fvars []string, bat *batch.Batch, proc *process.Process) error {
	defer batch.Clean(bat, proc.Mp)
	vecs := bat.Vecs[:len(fvars)]
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h24.keys, ctr.h24.zKeys)
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8, types.T_uint8:
				vs := vecs[j].Col.([]int8)
				for k := int64(0); k < n; k++ {
					*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int16, types.T_uint16:
				vs := vecs[j].Col.([]int16)
				for k := int64(0); k < n; k++ {
					*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int32, types.T_uint32, types.T_float32:
				vs := vecs[j].Col.([]int32)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int64, types.T_uint64, types.T_float64:
				vs := vecs[j].Col.([]int64)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h24.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				vData := vs.Data
				vOff := vs.Offsets
				vLen := vs.Lengths
				for k := int64(0); k < n; k++ {
					copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h24.keys[k])), 24)[ctr.keyOffs[k]:], vData[vOff[i+k]:vOff[i+k]+vLen[i+k]])
					ctr.keyOffs[k] += vLen[i+k]
				}
			}
		}
		ctr.hashes[0] = 0
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		ctr.h24.ht.InsertBatch(ctr.hashes, ctr.h24.keys[:n], ctr.inserted, ctr.values)
		{ // batch
			cnt := 0
			for k, ok := range ctr.inserted[:n] {
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
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserted, proc.Mp); err != nil {
					return err
				}
			}
			for _, r := range ctr.bat.Rs {
				if err := r.Grows(cnt, proc.Mp); err != nil {
					return err
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchAdd(bat.Rs[j], i, ctr.inserted[:n], ctr.values)
			}
		}
	}
	return nil
}

func (ctr *Container) processH32(fvars []string, bat *batch.Batch, proc *process.Process) error {
	defer batch.Clean(bat, proc.Mp)
	vecs := bat.Vecs[:len(fvars)]
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h32.keys, ctr.h32.zKeys)
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8, types.T_uint8:
				vs := vecs[j].Col.([]int8)
				for k := int64(0); k < n; k++ {
					*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int16, types.T_uint16:
				vs := vecs[j].Col.([]int16)
				for k := int64(0); k < n; k++ {
					*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int32, types.T_uint32, types.T_float32:
				vs := vecs[j].Col.([]int32)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int64, types.T_uint64, types.T_float64:
				vs := vecs[j].Col.([]int64)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h32.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				vData := vs.Data
				vOff := vs.Offsets
				vLen := vs.Lengths
				for k := int64(0); k < n; k++ {
					copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h32.keys[k])), 32)[ctr.keyOffs[k]:], vData[vOff[i+k]:vOff[i+k]+vLen[i+k]])
					ctr.keyOffs[k] += vLen[i+k]
				}
			}
		}
		ctr.hashes[0] = 0
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		ctr.h32.ht.InsertBatch(ctr.hashes, ctr.h32.keys[:n], ctr.inserted, ctr.values)
		{ // batch
			cnt := 0
			for k, ok := range ctr.inserted[:n] {
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
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserted, proc.Mp); err != nil {
					return err
				}
			}
			for _, r := range ctr.bat.Rs {
				if err := r.Grows(cnt, proc.Mp); err != nil {
					return err
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchAdd(bat.Rs[j], i, ctr.inserted[:n], ctr.values)
			}
		}
	}
	return nil
}

func (ctr *Container) processH40(fvars []string, bat *batch.Batch, proc *process.Process) error {
	defer batch.Clean(bat, proc.Mp)
	vecs := bat.Vecs[:len(fvars)]
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit {
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.keyOffs, ctr.zKeyOffs)
		copy(ctr.h40.keys, ctr.h40.zKeys)
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8, types.T_uint8:
				vs := vecs[j].Col.([]int8)
				for k := int64(0); k < n; k++ {
					*(*int8)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(1, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int16, types.T_uint16:
				vs := vecs[j].Col.([]int16)
				for k := int64(0); k < n; k++ {
					*(*int16)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(2, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int32, types.T_uint32, types.T_float32:
				vs := vecs[j].Col.([]int32)
				for k := int64(0); k < n; k++ {
					*(*int32)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(4, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_int64, types.T_uint64, types.T_float64:
				vs := vecs[j].Col.([]int64)
				for k := int64(0); k < n; k++ {
					*(*int64)(unsafe.Add(unsafe.Pointer(&ctr.h40.keys[k]), ctr.keyOffs[k])) = vs[i+k]
				}
				add.Uint32AddScalar(8, ctr.keyOffs[:n], ctr.keyOffs[:n])
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				vData := vs.Data
				vOff := vs.Offsets
				vLen := vs.Lengths
				for k := int64(0); k < n; k++ {
					copy(unsafe.Slice((*byte)(unsafe.Pointer(&ctr.h40.keys[k])), 40)[ctr.keyOffs[k]:], vData[vOff[i+k]:vOff[i+k]+vLen[i+k]])
					ctr.keyOffs[k] += vLen[i+k]
				}
			}
		}
		ctr.hashes[0] = 0
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		ctr.h40.ht.InsertBatch(ctr.hashes, ctr.h40.keys[:n], ctr.inserted, ctr.values)
		{ // batch
			cnt := 0
			for k, ok := range ctr.inserted[:n] {
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
				if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserted, proc.Mp); err != nil {
					return err
				}
			}
			for _, r := range ctr.bat.Rs {
				if err := r.Grows(cnt, proc.Mp); err != nil {
					return err
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchAdd(bat.Rs[j], i, ctr.inserted[:n], ctr.values)
			}
		}
	}
	return nil
}

func (ctr *Container) processHStr(fvars []string, bat *batch.Batch, proc *process.Process) error {
	defer batch.Clean(bat, proc.Mp)
	vecs := bat.Vecs[:len(fvars)]
	count := int64(len(bat.Zs))
	for i := int64(0); i < count; i += UnitLimit { // batch
		n := count - i
		if n > UnitLimit {
			n = UnitLimit
		}
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		keys := make([][]byte, UnitLimit)
		cnt := 0
		for j, vec := range vecs {
			switch vec.Typ.Oid {
			case types.T_int8, types.T_uint8:
				vs := vecs[j].Col.([]int8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*1:(i+k+1)*1]...)
				}
			case types.T_int16, types.T_uint16:
				vs := vecs[j].Col.([]int16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*2:(i+k+1)*2]...)
				}
			case types.T_int32, types.T_uint32, types.T_float32:
				vs := vecs[j].Col.([]int32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*4]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			case types.T_int64, types.T_uint64, types.T_float64:
				vs := vecs[j].Col.([]int64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				vData := vs.Data
				vOff := vs.Offsets
				vLen := vs.Lengths
				for k := int64(0); k < n; k++ {
					keys[k] = append(keys[k], vData[vOff[i+k]:vOff[i+k]+vLen[i+k]]...)
				}
			}
		}
		for k := int64(0); k < n; k++ {
			ok, vp := ctr.hstr.ht.Insert(hashtable.StringRef{Ptr: &keys[k][0], Len: len(keys[k])})
			if ok {
				ctr.inserted[k] = 1
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
			if err := vector.UnionBatch(vec, vecs[j], i, cnt, ctr.inserted, proc.Mp); err != nil {
				return err
			}
		}
		for _, r := range ctr.bat.Rs {
			if err := r.Grows(cnt, proc.Mp); err != nil {
				return err
			}
		}
		for j, r := range ctr.bat.Rs {
			r.BatchAdd(bat.Rs[j], i, ctr.inserted[:n], ctr.values)
		}
	}
	return nil
}
