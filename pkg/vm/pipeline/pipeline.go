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

package pipeline

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transform"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func New(cs []uint64, attrs []string, ins vm.Instructions) *Pipeline {
	return &Pipeline{
		refCnts:      cs,
		instructions: ins,
		attrs:        attrs,
	}
}

func NewMerge(ins vm.Instructions) *Pipeline {
	return &Pipeline{
		instructions: ins,
	}
}

func (p *Pipeline) String() string {
	var buf bytes.Buffer

	vm.String(p.instructions, &buf)
	return buf.String()
}

func (p *Pipeline) Run(r engine.Reader, proc *process.Process) (bool, error) {
	var end bool // exist flag
	var err error
	var bat *batch.Batch

	defer func() {
		if err != nil {
			for i, in := range p.instructions {
				if in.Op == vm.Connector {
					arg := p.instructions[i].Arg.(*connector.Argument)
					arg.Reg.Ch <- nil
					break
				}
			}
		} else {
			proc.Reg.InputBatch = nil
			vm.Run(p.instructions, proc)
		}
	}()
	//	r = exportRestrict(r, p.instructions)
	if err = vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	for {
		// read data from storage engine
		if bat, err = r.Read(p.refCnts, p.attrs); err != nil {
			return false, err
		}
		// processing the batch according to the instructions
		proc.Reg.InputBatch = bat
		if end, err = vm.Run(p.instructions, proc); err != nil || end { // end is true means pipeline successfully completed
			return end, err
		}
	}
}

func (p *Pipeline) RunMerge(proc *process.Process) (bool, error) {
	var end bool
	var err error

	defer func() {
		if err != nil {
			for i, in := range p.instructions {
				if in.Op == vm.Connector {
					arg := p.instructions[i].Arg.(*connector.Argument)
					arg.Reg.Ch <- nil
					break
				}
			}
		} else {
			proc.Reg.InputBatch = nil
			vm.Run(p.instructions, proc)
		}
		proc.Cancel()
	}()
	if err := vm.Prepare(p.instructions, proc); err != nil {
		return false, err
	}
	for {
		proc.Reg.InputBatch = nil
		if end, err = vm.Run(p.instructions, proc); err != nil || end {
			return end, err
		}
	}
}

func exportRestrict(r engine.Reader, ins vm.Instructions) engine.Reader {
	if ft := r.NewSparseFilter(); ft == nil {
		return r
	}

	for i, in := range ins {
		if in.Op == vm.Restrict {
			arg := ins[i].Arg.(*restrict.Argument)
			r = newReaderWithfilter(r, arg.E)
		}
		if in.Op == vm.Transform {
			arg := ins[i].Arg.(*transform.Argument)
			if arg.Restrict != nil {
				r = newReaderWithfilter(r, arg.Restrict.E)
			}
		}
	}
	return r
}

func newReaderWithfilter(r engine.Reader, e extend.Extend) engine.Reader {
	var es []extend.Extend

	es = extend.AndExtends(e, es)
	if len(es) == 0 {
		return r
	}
	for i := range es {
		if v, ok := es[i].(*extend.BinaryExtend); ok {
			switch v.Op {
			case overload.EQ:
				r = newReaderWithEq(r, v)
			case overload.NE:
				r = newReaderWithNe(r, v)
			case overload.LT:
				r = newReaderWithLt(r, v)
			case overload.LE:
				r = newReaderWithLe(r, v)
			case overload.GT:
				r = newReaderWithGt(r, v)
			case overload.GE:
				r = newReaderWithGe(r, v)
			}
		}
	}
	return r
}

func newReaderWithEq(r engine.Reader, e *extend.BinaryExtend) engine.Reader {
	if attr, ok := e.Left.(*extend.Attribute); ok {
		if val, ok := e.Right.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Eq(attr.Name, cast(val.V, attr.Type))
		}
	}
	if attr, ok := e.Right.(*extend.Attribute); ok {
		if val, ok := e.Left.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Eq(attr.Name, cast(val.V, attr.Type))
		}
	}
	return r
}

func newReaderWithNe(r engine.Reader, e *extend.BinaryExtend) engine.Reader {
	if attr, ok := e.Left.(*extend.Attribute); ok {
		if val, ok := e.Right.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Ne(attr.Name, cast(val.V, attr.Type))
		}
	}
	if attr, ok := e.Right.(*extend.Attribute); ok {
		if val, ok := e.Left.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Ne(attr.Name, cast(val.V, attr.Type))
		}
	}
	return r
}

func newReaderWithLt(r engine.Reader, e *extend.BinaryExtend) engine.Reader {
	if attr, ok := e.Left.(*extend.Attribute); ok {
		if val, ok := e.Right.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Lt(attr.Name, cast(val.V, attr.Type))
		}
	}
	if attr, ok := e.Right.(*extend.Attribute); ok {
		if val, ok := e.Left.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Lt(attr.Name, cast(val.V, attr.Type))
		}
	}
	return r
}

func newReaderWithLe(r engine.Reader, e *extend.BinaryExtend) engine.Reader {
	if attr, ok := e.Left.(*extend.Attribute); ok {
		if val, ok := e.Right.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Le(attr.Name, cast(val.V, attr.Type))
		}
	}
	if attr, ok := e.Right.(*extend.Attribute); ok {
		if val, ok := e.Left.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Le(attr.Name, cast(val.V, attr.Type))
		}
	}
	return r
}

func newReaderWithGt(r engine.Reader, e *extend.BinaryExtend) engine.Reader {
	if attr, ok := e.Left.(*extend.Attribute); ok {
		if val, ok := e.Right.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Gt(attr.Name, cast(val.V, attr.Type))
		}
	}
	if attr, ok := e.Right.(*extend.Attribute); ok {
		if val, ok := e.Left.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Gt(attr.Name, cast(val.V, attr.Type))
		}
	}
	return r
}

func newReaderWithGe(r engine.Reader, e *extend.BinaryExtend) engine.Reader {
	if attr, ok := e.Left.(*extend.Attribute); ok {
		if val, ok := e.Right.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Ge(attr.Name, cast(val.V, attr.Type))
		}
	}
	if attr, ok := e.Right.(*extend.Attribute); ok {
		if val, ok := e.Left.(*extend.ValueExtend); ok {
			filter := r.NewSparseFilter()
			r, _ = filter.Ge(attr.Name, cast(val.V, attr.Type))
		}
	}
	return r
}

func cast(vec *vector.Vector, typ types.T) interface{} {
	switch vec.Typ.Oid {
	case types.T_int8:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]int8)[0])
		case types.T_int16:
			return int16(vec.Col.([]int8)[0])
		case types.T_int32:
			return int32(vec.Col.([]int8)[0])
		case types.T_int64:
			return int64(vec.Col.([]int8)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]int8)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]int8)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]int8)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]int8)[0])
		case types.T_float32:
			return float32(vec.Col.([]int8)[0])
		case types.T_float64:
			return float64(vec.Col.([]int8)[0])
		case types.T_date:
			return types.Date(vec.Col.([]int8)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]int8)[0])
		}
	case types.T_int16:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]int16)[0])
		case types.T_int16:
			return int16(vec.Col.([]int16)[0])
		case types.T_int32:
			return int32(vec.Col.([]int16)[0])
		case types.T_int64:
			return int64(vec.Col.([]int16)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]int16)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]int16)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]int16)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]int16)[0])
		case types.T_float32:
			return float32(vec.Col.([]int16)[0])
		case types.T_float64:
			return float64(vec.Col.([]int16)[0])
		case types.T_date:
			return types.Date(vec.Col.([]int16)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]int16)[0])
		}
	case types.T_int32:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]int32)[0])
		case types.T_int16:
			return int16(vec.Col.([]int32)[0])
		case types.T_int32:
			return int32(vec.Col.([]int32)[0])
		case types.T_int64:
			return int64(vec.Col.([]int32)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]int32)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]int32)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]int32)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]int32)[0])
		case types.T_float32:
			return float32(vec.Col.([]int32)[0])
		case types.T_float64:
			return float64(vec.Col.([]int32)[0])
		case types.T_date:
			return types.Date(vec.Col.([]int32)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]int32)[0])
		}
	case types.T_int64:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]int64)[0])
		case types.T_int16:
			return int16(vec.Col.([]int64)[0])
		case types.T_int32:
			return int32(vec.Col.([]int64)[0])
		case types.T_int64:
			return int64(vec.Col.([]int64)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]int64)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]int64)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]int64)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]int64)[0])
		case types.T_float32:
			return float32(vec.Col.([]int64)[0])
		case types.T_float64:
			return float64(vec.Col.([]int64)[0])
		case types.T_date:
			return types.Date(vec.Col.([]int64)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]int64)[0])
		}
	case types.T_uint8:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]uint8)[0])
		case types.T_int16:
			return int16(vec.Col.([]uint8)[0])
		case types.T_int32:
			return int32(vec.Col.([]uint8)[0])
		case types.T_int64:
			return int64(vec.Col.([]uint8)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]uint8)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]uint8)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]uint8)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]uint8)[0])
		case types.T_float32:
			return float32(vec.Col.([]uint8)[0])
		case types.T_float64:
			return float64(vec.Col.([]uint8)[0])
		case types.T_date:
			return types.Date(vec.Col.([]uint8)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]uint8)[0])
		}
	case types.T_uint16:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]uint16)[0])
		case types.T_int16:
			return int16(vec.Col.([]uint16)[0])
		case types.T_int32:
			return int32(vec.Col.([]uint16)[0])
		case types.T_int64:
			return int64(vec.Col.([]uint16)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]uint16)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]uint16)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]uint16)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]uint16)[0])
		case types.T_float32:
			return float32(vec.Col.([]uint16)[0])
		case types.T_float64:
			return float64(vec.Col.([]uint16)[0])
		case types.T_date:
			return types.Date(vec.Col.([]uint16)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]uint16)[0])
		}
	case types.T_uint32:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]uint32)[0])
		case types.T_int16:
			return int16(vec.Col.([]uint32)[0])
		case types.T_int32:
			return int32(vec.Col.([]uint32)[0])
		case types.T_int64:
			return int64(vec.Col.([]uint32)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]uint32)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]uint32)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]uint32)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]uint32)[0])
		case types.T_float32:
			return float32(vec.Col.([]uint32)[0])
		case types.T_float64:
			return float64(vec.Col.([]uint32)[0])
		case types.T_date:
			return types.Date(vec.Col.([]uint32)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]uint32)[0])
		}
	case types.T_uint64:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]uint64)[0])
		case types.T_int16:
			return int16(vec.Col.([]uint64)[0])
		case types.T_int32:
			return int32(vec.Col.([]uint64)[0])
		case types.T_int64:
			return int64(vec.Col.([]uint64)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]uint64)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]uint64)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]uint64)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]uint64)[0])
		case types.T_float32:
			return float32(vec.Col.([]uint64)[0])
		case types.T_float64:
			return float64(vec.Col.([]uint64)[0])
		case types.T_date:
			return types.Date(vec.Col.([]uint64)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]uint64)[0])
		}
	case types.T_float32:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]float32)[0])
		case types.T_int16:
			return int16(vec.Col.([]float32)[0])
		case types.T_int32:
			return int32(vec.Col.([]float32)[0])
		case types.T_int64:
			return int64(vec.Col.([]float32)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]float32)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]float32)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]float32)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]float32)[0])
		case types.T_float32:
			return float32(vec.Col.([]float32)[0])
		case types.T_float64:
			return float64(vec.Col.([]float32)[0])
		case types.T_date:
			return types.Date(vec.Col.([]float32)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]float32)[0])
		}
	case types.T_float64:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]float64)[0])
		case types.T_int16:
			return int16(vec.Col.([]float64)[0])
		case types.T_int32:
			return int32(vec.Col.([]float64)[0])
		case types.T_int64:
			return int64(vec.Col.([]float64)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]float64)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]float64)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]float64)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]float64)[0])
		case types.T_float32:
			return float32(vec.Col.([]float64)[0])
		case types.T_float64:
			return float64(vec.Col.([]float64)[0])
		case types.T_date:
			return types.Date(vec.Col.([]float64)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]float64)[0])
		}
	case types.T_date:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]types.Date)[0])
		case types.T_int16:
			return int16(vec.Col.([]types.Date)[0])
		case types.T_int32:
			return int32(vec.Col.([]types.Date)[0])
		case types.T_int64:
			return int64(vec.Col.([]types.Date)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]types.Date)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]types.Date)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]types.Date)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]types.Date)[0])
		case types.T_float32:
			return float32(vec.Col.([]types.Date)[0])
		case types.T_float64:
			return float64(vec.Col.([]types.Date)[0])
		case types.T_date:
			return types.Date(vec.Col.([]types.Date)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]types.Date)[0])
		}
	case types.T_datetime:
		switch typ {
		case types.T_int8:
			return int8(vec.Col.([]types.Datetime)[0])
		case types.T_int16:
			return int16(vec.Col.([]types.Datetime)[0])
		case types.T_int32:
			return int32(vec.Col.([]types.Datetime)[0])
		case types.T_int64:
			return int64(vec.Col.([]types.Datetime)[0])
		case types.T_uint8:
			return uint8(vec.Col.([]types.Datetime)[0])
		case types.T_uint16:
			return uint16(vec.Col.([]types.Datetime)[0])
		case types.T_uint32:
			return uint32(vec.Col.([]types.Datetime)[0])
		case types.T_uint64:
			return uint64(vec.Col.([]types.Datetime)[0])
		case types.T_float32:
			return float32(vec.Col.([]types.Datetime)[0])
		case types.T_float64:
			return float64(vec.Col.([]types.Datetime)[0])
		case types.T_date:
			return types.Date(vec.Col.([]types.Datetime)[0])
		case types.T_datetime:
			return types.Datetime(vec.Col.([]types.Datetime)[0])
		}
	case types.T_char, types.T_varchar:
		switch typ {
		case types.T_date:
			v, _ := types.ParseDate(string(vec.Data))
			return v
		case types.T_datetime:
			v, _ := types.ParseDatetime(string(vec.Data))
			return v
		}
		return vec.Data
	}
	return nil
}
