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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transformer"
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
		ctr.bat.Zs = []int64{0}
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
	for _, z := range bat.Zs {
		ctr.bat.Zs[0] += z
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
			ctr.keyOffs = make([]uint32, UnitLimit)
			ctr.zKeyOffs = make([]uint32, UnitLimit)
			ctr.inserted = make([]uint8, UnitLimit)
			ctr.zInserted = make([]uint8, UnitLimit)
			ctr.hashes = make([]uint64, UnitLimit)
			ctr.fakeKeys = make([][2]uint64, UnitLimit)
			ctr.values = make([]uint64, UnitLimit)
			switch {
			case size <= 8:
				ctr.typ = H8
				ctr.h8.keys = make([]uint64, UnitLimit)
				ctr.h8.zKeys = make([]uint64, UnitLimit)
				ctr.h8.ht = &hashtable.Int64HashMap{}
				ctr.h8.ht.Init()
			case size <= 24:
				ctr.typ = H24
				ctr.h24.keys = make([][3]uint64, UnitLimit)
				ctr.h24.zKeys = make([][3]uint64, UnitLimit)
				ctr.h24.ht = &hashtable.String24HashMap{}
				ctr.h24.ht.Init()
			case size <= 32:
				ctr.typ = H32
				ctr.h32.keys = make([][4]uint64, UnitLimit)
				ctr.h32.zKeys = make([][4]uint64, UnitLimit)
				ctr.h32.ht = &hashtable.String32HashMap{}
				ctr.h32.ht.Init()
			case size <= 40:
				ctr.typ = H40
				ctr.h40.keys = make([][5]uint64, UnitLimit)
				ctr.h40.zKeys = make([][5]uint64, UnitLimit)
				ctr.h40.ht = &hashtable.String40HashMap{}
				ctr.h40.ht.Init()
			default:
				ctr.typ = HStr
				ctr.hstr.keys = make([][]byte, UnitLimit)
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
	vecs := bat.Vecs[:ctr.n]
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
		{ // batch
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
				for _, r := range ctr.bat.Rs {
					if err := r.Grows(cnt, proc.Mp); err != nil {
						return err
					}
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchFill(i, ctr.inserted[:n], ctr.values, bat.Zs, bat.Vecs[ctr.Is[j]])
			}
		}
	}
	return nil
}

func (ctr *Container) processH24(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs[:ctr.n]
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
		{ // batch fill
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
				for _, r := range ctr.bat.Rs {
					if err := r.Grows(cnt, proc.Mp); err != nil {
						return err
					}
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchFill(i, ctr.inserted[:n], ctr.values, bat.Zs, bat.Vecs[ctr.Is[j]])
			}
		}
	}
	return nil
}

func (ctr *Container) processH32(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs[:ctr.n]
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
		ctr.h32.ht.InsertRawBatch(ctr.hashes, ctr.fakeKeys, ctr.h32.keys[:n], ctr.values)
		{ // batch
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
				for _, r := range ctr.bat.Rs {
					if err := r.Grows(cnt, proc.Mp); err != nil {
						return err
					}
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchFill(i, ctr.inserted[:n], ctr.values, bat.Zs, bat.Vecs[ctr.Is[j]])
			}
		}
	}
	return nil
}

func (ctr *Container) processH40(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs[:ctr.n]
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
		ctr.h40.ht.InsertRawBatch(ctr.hashes, ctr.fakeKeys, ctr.h40.keys[:n], ctr.values)
		{ // batch
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
				for _, r := range ctr.bat.Rs {
					if err := r.Grows(cnt, proc.Mp); err != nil {
						return err
					}
				}
			}
			for j, r := range ctr.bat.Rs {
				r.BatchFill(i, ctr.inserted[:n], ctr.values, bat.Zs, bat.Vecs[ctr.Is[j]])
			}
		}
	}
	return nil
}

func (ctr *Container) processHStr(bat *batch.Batch, proc *process.Process) error {
	vecs := bat.Vecs[:ctr.n]
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
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*1:(i+k+1)*1]...)
				}
			case types.T_uint8:
				vs := vecs[j].Col.([]uint8)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*1)[:len(vs)*1]
				for k := int64(0); k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*1:(i+k+1)*1]...)
				}
			case types.T_int16:
				vs := vecs[j].Col.([]int16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				for k := int64(0); k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*2:(i+k+1)*2]...)
				}
			case types.T_uint16:
				vs := vecs[j].Col.([]uint16)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*2)[:len(vs)*2]
				for k := int64(0); k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*2:(i+k+1)*2]...)
				}
			case types.T_int32:
				vs := vecs[j].Col.([]int32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				for k := int64(0); k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			case types.T_uint32:
				vs := vecs[j].Col.([]uint32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				for k := int64(0); k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			case types.T_float32:
				vs := vecs[j].Col.([]float32)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*4)[:len(vs)*4]
				for k := int64(0); k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*4:(i+k+1)*4]...)
				}
			case types.T_int64:
				vs := vecs[j].Col.([]int64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				for k := int64(0); k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			case types.T_uint64:
				vs := vecs[j].Col.([]uint64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				for k := int64(0); k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			case types.T_float64:
				vs := vecs[j].Col.([]float64)
				data := unsafe.Slice((*byte)(unsafe.Pointer(&vs[0])), cap(vs)*8)[:len(vs)*8]
				for k := int64(0); k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], data[(i+k)*8:(i+k+1)*8]...)
				}
			case types.T_char, types.T_varchar:
				vs := vecs[j].Col.(*types.Bytes)
				for k := int64(0); k < n; k++ {
					ctr.hstr.keys[k] = append(ctr.hstr.keys[k], vs.Get(i+k)...)
				}
			}
		}
		cnt := 0
		copy(ctr.inserted[:n], ctr.zInserted[:n])
		for k := int64(0); k < n; k++ {
			v := ctr.hstr.ht.Insert(hashtable.StringRef{Ptr: &ctr.hstr.keys[k][0], Len: len(ctr.hstr.keys[k])})
			ctr.hstr.keys[k] = ctr.hstr.keys[k][:0]
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
			for _, r := range ctr.bat.Rs {
				if err := r.Grows(cnt, proc.Mp); err != nil {
					return err
				}
			}
		}
		for j, r := range ctr.bat.Rs {
			r.BatchFill(i, ctr.inserted[:n], ctr.values, bat.Zs, bat.Vecs[ctr.Is[j]])
		}
	}
	return nil
}

func (ctr *Container) constructContainer(n *Argument, bat *batch.Batch) {
	ctr.n = len(n.FreeVars)
	mp := make(map[string]int)
	for i, attr := range bat.Attrs {
		mp[attr] = i
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
	for _, fvar := range n.FreeVars {
		delete(mp, fvar)
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
