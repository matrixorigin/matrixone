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

package fill

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "fill"

func (fill *Fill) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": fill")
}

func (fill *Fill) OpType() vm.OpType {
	return vm.Fill
}

func (fill *Fill) Prepare(proc *process.Process) (err error) {
	if fill.OpAnalyzer == nil {
		fill.OpAnalyzer = process.NewAnalyzer(fill.GetIdx(), fill.IsFirst, fill.IsLast, "fill")
	} else {
		fill.OpAnalyzer.Reset()
	}

	ctr := &fill.ctr

	switch fill.FillType {
	case plan.Node_VALUE:
		// the batch just for eval const value
		b := batch.NewWithSize(1)
		defer b.Clean(proc.Mp())
		b.SetVector(0, vector.NewVec(types.T_varchar.ToType()))
		batch.SetLength(b, 1)
		if len(ctr.exes) == 0 {
			ctr.valVecs = make([]*vector.Vector, len(fill.FillVal))
			for _, val := range fill.FillVal {
				exe, err := colexec.NewExpressionExecutor(proc, val)
				if err != nil {
					return err
				}
				ctr.exes = append(ctr.exes, exe)
			}
		}
		for i := range fill.FillVal {
			ctr.valVecs[i], err = ctr.exes[i].Eval(proc, []*batch.Batch{b}, nil)
			if err != nil {
				return err
			}
		}
		ctr.process = processValue
	case plan.Node_PREV:
		if len(ctr.prevVecs) == 0 {
			ctr.prevVecs = make([]*vector.Vector, fill.ColLen)
		}
		ctr.process = processPrev
	case plan.Node_NEXT:
		ctr.status = receiveBat
		ctr.subStatus = findNull
		ctr.process = processNext
	case plan.Node_LINEAR:
		if len(ctr.exes) == 0 {
			ctr.valVecs = make([]*vector.Vector, len(fill.FillVal))
			for _, v := range fill.FillVal {
				resetColRef(v, 0)
				exe, err := colexec.NewExpressionExecutor(proc, v)
				if err != nil {
					return err
				}
				ctr.exes = append(ctr.exes, exe)
			}
		}
		ctr.process = processLinear
	default:
		ctr.process = processDefault
	}

	if fill.ProjectList != nil {
		err := fill.PrepareProjection(proc)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fill *Fill) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	analyzer := fill.OpAnalyzer
	analyzer.Start()
	defer analyzer.Stop()

	ctr := &fill.ctr

	result, err := ctr.process(ctr, fill, proc, analyzer)

	if fill.ProjectList != nil {
		result.Batch, err = fill.EvalProjection(result.Batch, proc)
	}

	return result, err
}

func resetColRef(expr *plan.Expr, idx int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		exprImpl.Col.RelPos = -1
		exprImpl.Col.ColPos = int32(idx)

	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			resetColRef(arg, i)
		}
	}
}

func processValue(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var err error
	result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}
	if result.Batch == nil {
		result.Status = vm.ExecStop
		return result, nil
	}
	if ctr.buf != nil {
		ctr.buf.CleanOnlyData()
	}
	ctr.buf, err = ctr.buf.AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
	if err != nil {
		return result, err
	}

	for i := 0; i < ap.ColLen; i++ {
		for j := 0; j < ctr.buf.Vecs[i].Length(); j++ {
			if ctr.buf.Vecs[i].IsNull(uint64(j)) {
				if err = setValue(ctr.buf.Vecs[i], ctr.valVecs[i], j, 0, proc); err != nil {
					return result, err
				}
			}
		}
	}

	result.Batch = ctr.buf
	analyzer.Output(result.Batch)
	return result, nil
}

func (ctr *container) initIndex() {
	ctr.nullIdx = 0
	ctr.nullRow = 0
	ctr.preIdx = 0
	ctr.preRow = 0
	ctr.curIdx = 0
	ctr.curRow = 0
	ctr.status = receiveBat
	ctr.subStatus = findNullPre
}

func processNextCol(ctr *container, idx int, proc *process.Process) error {
	var err error
	ctr.initIndex()
	k := 0
	for {
		switch ctr.status {
		case receiveBat:
			if k == len(ctr.bats) {
				return nil
			}
			k++
			if ctr.subStatus == findNull {
				ctr.status = findNull
			} else {
				ctr.status = findValue
			}

		case findNull:
			b := ctr.bats[ctr.preIdx]
			hasNull := false
			for ; ctr.preRow < b.RowCount(); ctr.preRow++ {
				if b.Vecs[idx].IsNull(uint64(ctr.preRow)) {
					hasNull = true
					break
				}
			}
			ctr.subStatus = findNull
			if hasNull {
				ctr.status = findValue
			} else {
				ctr.preIdx++
				ctr.preRow = 0
				ctr.status = receiveBat
			}
		case findValue:
			ctr.curIdx = ctr.preIdx
			ctr.curRow = ctr.preRow
			b := ctr.bats[ctr.curIdx]
			hasValue := false
			for ; ctr.curRow < b.RowCount(); ctr.curRow++ {
				if !b.Vecs[idx].IsNull(uint64(ctr.curRow)) {
					hasValue = true
					break
				}
			}
			ctr.subStatus = findValue
			if hasValue {
				ctr.status = fillValue
			} else {
				ctr.status = receiveBat
			}
		case fillValue:
			for ; ctr.preIdx < ctr.curIdx-1; ctr.preIdx++ {
				for ; ctr.preRow < ctr.bats[ctr.preIdx].RowCount(); ctr.preRow++ {
					vec := ctr.bats[ctr.preIdx].Vecs[idx]
					if !vec.IsNull(uint64(ctr.preRow)) {
						continue
					}
					err = setValue(vec, ctr.bats[ctr.curIdx].Vecs[idx], ctr.preRow, ctr.curRow, proc)
					if err != nil {
						return err
					}

				}
				if ctr.preRow == ctr.bats[ctr.preRow].RowCount() {
					ctr.preRow = 0
				}
			}

			for ; ctr.preRow < ctr.curRow; ctr.preRow++ {
				vec := ctr.bats[ctr.preIdx].Vecs[idx]
				if !vec.IsNull(uint64(ctr.preRow)) {
					continue
				}
				err = setValue(vec, ctr.bats[ctr.curIdx].Vecs[idx], ctr.preRow, ctr.curRow, proc)
				if err != nil {
					return err
				}
			}

			ctr.status = findNull

		}
	}
}

func processPrev(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var err error
	result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}
	if result.Batch == nil {
		result.Status = vm.ExecStop
		return result, nil
	}

	if ctr.buf != nil {
		ctr.buf.CleanOnlyData()
	}
	ctr.buf, err = ctr.buf.AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
	if err != nil {
		return result, err
	}

	for i := 0; i < ap.ColLen; i++ {
		for j := 0; j < ctr.buf.Vecs[i].Length(); j++ {
			if ctr.buf.Vecs[i].IsNull(uint64(j)) {
				if ctr.prevVecs[i] != nil {
					if err = setValue(ctr.buf.Vecs[i], ctr.prevVecs[i], j, 0, proc); err != nil {
						return result, err
					}
				}
			} else {
				if ctr.prevVecs[i] == nil {
					ctr.prevVecs[i] = vector.NewVec(*ctr.buf.Vecs[i].GetType())
					err = appendValue(ctr.prevVecs[i], ctr.buf.Vecs[i], j, proc)
					if err != nil {
						return result, err
					}
				} else {
					if err = setValue(ctr.prevVecs[i], ctr.buf.Vecs[i], 0, j, proc); err != nil {
						return result, err
					}
				}

			}
		}
	}
	result.Batch = ctr.buf
	analyzer.Output(result.Batch)
	return result, nil
}

func processLinearCol(ctr *container, proc *process.Process, idx int) error {
	var err error

	ctr.initIndex()
	k := 0

	for {
		switch ctr.status {
		case receiveBat:
			if k == len(ctr.bats) {
				return nil
			}
			k++
			if ctr.subStatus == findNullPre {
				ctr.status = findNullPre
			} else {
				ctr.status = findValue
			}
		case findNullPre:
			b := ctr.bats[ctr.nullIdx]

			if ctr.nullIdx > ctr.preRow {
				if b.Vecs[idx].IsNull(uint64(ctr.nullIdx)) {
					ctr.status = findValue
					continue
				}
			}

			hasNullPre := false
			ctr.preRow = ctr.nullRow - 1
			for ; ctr.nullRow < b.RowCount(); ctr.nullRow++ {
				if ctr.preRow >= 0 && b.Vecs[idx].IsNull(uint64(ctr.nullRow)) && !b.Vecs[idx].IsNull(uint64(ctr.preRow)) {
					hasNullPre = true
					break
				}
				ctr.preRow++
			}

			ctr.subStatus = findNullPre
			if hasNullPre {
				ctr.status = findValue
			} else {
				if b.Vecs[idx].IsNull(uint64(ctr.preRow)) {
					ctr.preIdx++
					ctr.nullRow = 0
				}
				ctr.nullIdx++
				ctr.nullRow = 0
				ctr.status = receiveBat
			}
		case findValue:
			ctr.curIdx = ctr.nullIdx
			ctr.curRow = ctr.nullRow
			b := ctr.bats[ctr.curIdx]
			hasValue := false
			for ; ctr.curRow < b.RowCount(); ctr.curRow++ {
				if !b.Vecs[idx].IsNull(uint64(ctr.curRow)) {
					hasValue = true
					break
				}
			}
			ctr.subStatus = findValue
			if hasValue {
				ctr.status = fillValue
			} else {
				ctr.status = receiveBat
			}
		case fillValue:
			b := batch.NewWithSize(2)
			b.Vecs[0] = vector.NewVec(*ctr.bats[ctr.preIdx].Vecs[idx].GetType())
			err = appendValue(b.Vecs[0], ctr.bats[ctr.preIdx].Vecs[idx], ctr.preRow, proc)
			if err != nil {
				return err
			}
			b.Vecs[1] = vector.NewVec(*ctr.bats[ctr.curIdx].Vecs[idx].GetType())
			err = appendValue(b.Vecs[1], ctr.bats[ctr.curIdx].Vecs[idx], ctr.curRow, proc)
			if err != nil {
				return err
			}
			b.SetRowCount(1)
			ctr.valVecs[idx], err = ctr.exes[idx].Eval(proc, []*batch.Batch{b}, nil)
			if err != nil {
				return err
			}

			ctr.preRow++
			for ; ctr.preIdx < ctr.curIdx-1; ctr.preIdx++ {
				for ; ctr.preRow < ctr.bats[ctr.preIdx].RowCount(); ctr.preRow++ {

					vec := ctr.bats[ctr.preIdx].Vecs[idx]
					if !vec.IsNull(uint64(ctr.preRow)) {
						continue
					}
					err = setValue(vec, ctr.valVecs[idx], ctr.preRow, 0, proc)
					if err != nil {
						return err
					}

				}
				if ctr.preRow == ctr.bats[ctr.preRow].RowCount() {
					ctr.preRow = 0
				}
			}

			for ; ctr.preRow < ctr.curRow; ctr.preRow++ {
				vec := ctr.bats[ctr.preIdx].Vecs[idx]
				if !vec.IsNull(uint64(ctr.preRow)) {
					continue
				}
				err = setValue(vec, ctr.valVecs[idx], ctr.preRow, 0, proc)
				if err != nil {
					return err
				}
			}

			ctr.nullIdx = ctr.preIdx
			ctr.nullRow = ctr.preRow
			ctr.status = findNullPre
		}
	}
}

func processNext(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var err error
	result := vm.NewCallResult()
	if ctr.done {
		if ctr.idx == len(ctr.bats) {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
		result.Batch = ctr.bats[ctr.idx]
		result.Status = vm.ExecNext
		ctr.idx++
		analyzer.Output(result.Batch)
		return result, nil
	}
	for i := 0; ; i++ {
		result, err = vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
		if err != nil {
			return result, err
		}
		if result.Batch == nil {
			break
		}
		if len(ctr.bats) > i {
			if ctr.bats[i] != nil {
				ctr.bats[i].CleanOnlyData()
			}
			ctr.bats[i], err = ctr.bats[i].AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
			if err != nil {
				return result, err
			}
		} else {
			appBat, err := result.Batch.Dup(proc.Mp())
			if err != nil {
				return result, err
			}
			analyzer.Alloc(int64(appBat.Size()))
			ctr.bats = append(ctr.bats, appBat)
		}
	}
	if len(ctr.bats) == 0 {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}

	for i := range ctr.bats[0].Vecs {
		if err = processNextCol(ctr, i, proc); err != nil {
			return result, err
		}
	}
	ctr.done = true
	result.Batch = ctr.bats[ctr.idx]
	result.Status = vm.ExecNext
	ctr.idx++

	analyzer.Output(result.Batch)
	return result, nil
}

func processLinear(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var err error
	result := vm.NewCallResult()
	if ctr.done {
		if ctr.idx == len(ctr.bats) {
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
		result.Batch = ctr.bats[ctr.idx]
		result.Status = vm.ExecNext
		ctr.idx++
		analyzer.Output(result.Batch)
		return result, nil
	}
	for i := 0; ; i++ {
		result, err = vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
		if err != nil {
			return result, err
		}
		if result.Batch == nil {
			break
		}

		if len(ctr.bats) > i {
			if ctr.bats[i] != nil {
				ctr.bats[i].CleanOnlyData()
			}
			ctr.buf, err = ctr.buf.AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
			if err != nil {
				return result, err
			}
		} else {
			appBat, err := result.Batch.Dup(proc.Mp())
			if err != nil {
				return result, err
			}
			analyzer.Alloc(int64(appBat.Size()))
			ctr.bats = append(ctr.bats, appBat)
		}
	}
	if len(ctr.bats) == 0 {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}
	for i := range ctr.bats[0].Vecs {
		if err = processLinearCol(ctr, proc, i); err != nil {
			return result, err
		}
	}
	ctr.done = true
	result.Batch = ctr.bats[ctr.idx]
	result.Status = vm.ExecNext
	ctr.idx++

	analyzer.Output(result.Batch)
	return result, nil
}

func processDefault(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
	if err != nil {
		return result, err
	}
	if result.Batch == nil {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}
	return result, nil
}

func appendValue(v, w *vector.Vector, j int, proc *process.Process) error {
	var err error
	switch v.GetType().Oid {
	case types.T_bool:
		err = vector.AppendFixed[bool](v, vector.GetFixedAtNoTypeCheck[bool](w, j), false, proc.Mp())
	case types.T_bit:
		err = vector.AppendFixed[uint64](v, vector.GetFixedAtNoTypeCheck[uint64](w, j), false, proc.Mp())
	case types.T_int8:
		err = vector.AppendFixed[int8](v, vector.GetFixedAtNoTypeCheck[int8](w, j), false, proc.Mp())
	case types.T_int16:
		err = vector.AppendFixed[int16](v, vector.GetFixedAtNoTypeCheck[int16](w, j), false, proc.Mp())
	case types.T_int32:
		err = vector.AppendFixed[int32](v, vector.GetFixedAtNoTypeCheck[int32](w, j), false, proc.Mp())
	case types.T_int64:
		err = vector.AppendFixed[int64](v, vector.GetFixedAtNoTypeCheck[int64](w, j), false, proc.Mp())
	case types.T_uint8:
		err = vector.AppendFixed[uint8](v, vector.GetFixedAtNoTypeCheck[uint8](w, j), false, proc.Mp())
	case types.T_uint16:
		err = vector.AppendFixed[uint16](v, vector.GetFixedAtNoTypeCheck[uint16](w, j), false, proc.Mp())
	case types.T_uint32:
		err = vector.AppendFixed[uint32](v, vector.GetFixedAtNoTypeCheck[uint32](w, j), false, proc.Mp())
	case types.T_uint64:
		err = vector.AppendFixed[uint64](v, vector.GetFixedAtNoTypeCheck[uint64](w, j), false, proc.Mp())
	case types.T_float32:
		err = vector.AppendFixed[float32](v, vector.GetFixedAtNoTypeCheck[float32](w, j), false, proc.Mp())
	case types.T_float64:
		err = vector.AppendFixed[float64](v, vector.GetFixedAtNoTypeCheck[float64](w, j), false, proc.Mp())
	case types.T_date:
		err = vector.AppendFixed[types.Date](v, vector.GetFixedAtNoTypeCheck[types.Date](w, j), false, proc.Mp())
	case types.T_datetime:
		err = vector.AppendFixed[types.Datetime](v, vector.GetFixedAtNoTypeCheck[types.Datetime](w, j), false, proc.Mp())
	case types.T_time:
		err = vector.AppendFixed[types.Time](v, vector.GetFixedAtNoTypeCheck[types.Time](w, j), false, proc.Mp())
	case types.T_timestamp:
		err = vector.AppendFixed[types.Timestamp](v, vector.GetFixedAtNoTypeCheck[types.Timestamp](w, j), false, proc.Mp())
	case types.T_enum:
		err = vector.AppendFixed[types.Enum](v, vector.GetFixedAtNoTypeCheck[types.Enum](w, j), false, proc.Mp())
	case types.T_decimal64:
		err = vector.AppendFixed[types.Decimal64](v, vector.GetFixedAtNoTypeCheck[types.Decimal64](w, j), false, proc.Mp())
	case types.T_decimal128:
		err = vector.AppendFixed[types.Decimal128](v, vector.GetFixedAtNoTypeCheck[types.Decimal128](w, j), false, proc.Mp())
	case types.T_uuid:
		err = vector.AppendFixed[types.Uuid](v, vector.GetFixedAtNoTypeCheck[types.Uuid](w, j), false, proc.Mp())
	case types.T_TS:
		err = vector.AppendFixed[types.TS](v, vector.GetFixedAtNoTypeCheck[types.TS](w, j), false, proc.Mp())
	case types.T_Rowid:
		err = vector.AppendFixed[types.Rowid](v, vector.GetFixedAtNoTypeCheck[types.Rowid](w, j), false, proc.Mp())
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		err = vector.AppendBytes(v, w.GetBytesAt(j), false, proc.Mp())
	default:
		panic(fmt.Sprintf("unexpect type %s for function set value in fill query", v.GetType()))
	}
	return err
}

func setValue(v, w *vector.Vector, i, j int, proc *process.Process) error {
	if v.HasNull() {
		v.GetNulls().Del(uint64(i))
	}
	var err error
	switch v.GetType().Oid {
	case types.T_bool:
		err = vector.SetFixedAtNoTypeCheck[bool](v, i, vector.GetFixedAtNoTypeCheck[bool](w, j))
	case types.T_bit:
		err = vector.SetFixedAtNoTypeCheck[uint64](v, i, vector.GetFixedAtNoTypeCheck[uint64](w, j))
	case types.T_int8:
		err = vector.SetFixedAtNoTypeCheck[int8](v, i, vector.GetFixedAtNoTypeCheck[int8](w, j))
	case types.T_int16:
		err = vector.SetFixedAtNoTypeCheck[int16](v, i, vector.GetFixedAtNoTypeCheck[int16](w, j))
	case types.T_int32:
		err = vector.SetFixedAtNoTypeCheck[int32](v, i, vector.GetFixedAtNoTypeCheck[int32](w, j))
	case types.T_int64:
		err = vector.SetFixedAtNoTypeCheck[int64](v, i, vector.GetFixedAtNoTypeCheck[int64](w, j))
	case types.T_uint8:
		err = vector.SetFixedAtNoTypeCheck[uint8](v, i, vector.GetFixedAtNoTypeCheck[uint8](w, j))
	case types.T_uint16:
		err = vector.SetFixedAtNoTypeCheck[uint16](v, i, vector.GetFixedAtNoTypeCheck[uint16](w, j))
	case types.T_uint32:
		err = vector.SetFixedAtNoTypeCheck[uint32](v, i, vector.GetFixedAtNoTypeCheck[uint32](w, j))
	case types.T_uint64:
		err = vector.SetFixedAtNoTypeCheck[uint64](v, i, vector.GetFixedAtNoTypeCheck[uint64](w, j))
	case types.T_float32:
		err = vector.SetFixedAtNoTypeCheck[float32](v, i, vector.GetFixedAtNoTypeCheck[float32](w, j))
	case types.T_float64:
		err = vector.SetFixedAtNoTypeCheck[float64](v, i, vector.GetFixedAtNoTypeCheck[float64](w, j))
	case types.T_date:
		err = vector.SetFixedAtNoTypeCheck[types.Date](v, i, vector.GetFixedAtNoTypeCheck[types.Date](w, j))
	case types.T_datetime:
		err = vector.SetFixedAtNoTypeCheck[types.Datetime](v, i, vector.GetFixedAtNoTypeCheck[types.Datetime](w, j))
	case types.T_time:
		err = vector.SetFixedAtNoTypeCheck[types.Time](v, i, vector.GetFixedAtNoTypeCheck[types.Time](w, j))
	case types.T_timestamp:
		err = vector.SetFixedAtNoTypeCheck[types.Timestamp](v, i, vector.GetFixedAtNoTypeCheck[types.Timestamp](w, j))
	case types.T_enum:
		err = vector.SetFixedAtNoTypeCheck[types.Enum](v, i, vector.GetFixedAtNoTypeCheck[types.Enum](w, j))
	case types.T_decimal64:
		err = vector.SetFixedAtNoTypeCheck[types.Decimal64](v, i, vector.GetFixedAtNoTypeCheck[types.Decimal64](w, j))
	case types.T_decimal128:
		err = vector.SetFixedAtNoTypeCheck[types.Decimal128](v, i, vector.GetFixedAtNoTypeCheck[types.Decimal128](w, j))
	case types.T_uuid:
		err = vector.SetFixedAtNoTypeCheck[types.Uuid](v, i, vector.GetFixedAtNoTypeCheck[types.Uuid](w, j))
	case types.T_TS:
		err = vector.SetFixedAtNoTypeCheck[types.TS](v, i, vector.GetFixedAtNoTypeCheck[types.TS](w, j))
	case types.T_Rowid:
		err = vector.SetFixedAtNoTypeCheck[types.Rowid](v, i, vector.GetFixedAtNoTypeCheck[types.Rowid](w, j))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		err = vector.SetBytesAt(v, i, w.GetBytesAt(j), proc.Mp())
	default:
		panic(fmt.Sprintf("unexpect type %s for function set value in fill query", v.GetType()))
	}
	return err
}
