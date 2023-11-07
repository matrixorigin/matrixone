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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString("fill")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	ap := arg
	ap.ctr = new(container)
	ctr := ap.ctr
	ctr.InitReceiver(proc, true)

	f := true
	for i, id := range ap.AggIds {
		if id == function.MAX || id == function.MIN || id == function.SUM || id == function.AVG {
			ctr.colIdx = i
			f = false
			break
		}
	}
	if f {
		ap.FillType = plan.Node_NONE
	}

	switch ap.FillType {
	case plan.Node_VALUE:
		b := batch.NewWithSize(1)
		b.SetVector(0, proc.GetVector(types.T_varchar.ToType()))
		batch.SetLength(b, 1)
		ctr.valVecs = make([]*vector.Vector, len(ap.FillVal))
		for i, val := range ap.FillVal {
			exe, err := colexec.NewExpressionExecutor(proc, val)
			if err != nil {
				return err
			}
			ctr.valVecs[i], err = exe.EvalWithoutResultReusing(proc, []*batch.Batch{b})
			if err != nil {
				return err
			}
			exe.Free()
		}
		ctr.process = processValue
	case plan.Node_PREV:
		ctr.prevVecs = make([]*vector.Vector, ap.ColLen)
		ctr.process = processPrev
	case plan.Node_NEXT:
		ctr.status = receiveBat
		ctr.subStatus = findNull
		ctr.process = processNext
	case plan.Node_LINEAR:
		for _, v := range ap.FillVal {
			resetColRef(v, 0)
			exe, err := colexec.NewExpressionExecutor(proc, v)
			if err != nil {
				return err
			}
			ctr.exes = append(ctr.exes, exe)
			ctr.valVecs = make([]*vector.Vector, len(ap.FillVal))
		}
		ctr.process = processLinear
	default:
		ctr.process = processDefault
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	anal := proc.GetAnalyze(arg.info.Idx)
	anal.Start()
	defer anal.Stop()
	ctr := arg.ctr

	return ctr.process(ctr, arg, proc, anal)
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

func processValue(ctr *container, ap *Argument, proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	var err error
	result := vm.NewCallResult()
	if ctr.buf != nil {
		proc.PutBatch(ctr.buf)
		ctr.buf = nil
	}
	ctr.buf, _, err = ctr.ReceiveFromAllRegs(anal)
	if err != nil {
		return result, err
	}
	if ctr.buf == nil {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
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
	return result, nil
}

func processNext(ctr *container, _ *Argument, proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	var err error
	result := vm.NewCallResult()
	for {
		switch ctr.status {
		case receiveBat:
			bat, _, err := ctr.ReceiveFromAllRegs(anal)
			if err != nil {
				return result, err
			}
			if bat == nil {
				ctr.status = withoutNewBat
				continue
			}
			ctr.bats = append(ctr.bats, bat)
			if ctr.subStatus == findNull {
				ctr.status = findNull
			} else {
				ctr.status = findValue
			}

		case findNull:
			if ctr.preIdx > 0 {
				ctr.preIdx--
				if ctr.buf != nil {
					proc.PutBatch(ctr.buf)
					ctr.buf = nil
				}
				ctr.buf = ctr.bats[0]
				ctr.bats = ctr.bats[1:]
				result.Batch = ctr.buf
				return result, nil
			}
			b := ctr.bats[ctr.preIdx]
			hasNull := false
			for ; ctr.preRow < b.RowCount(); ctr.preRow++ {
				if b.Vecs[ctr.colIdx].IsNull(uint64(ctr.preRow)) {
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
				if !b.Vecs[ctr.colIdx].IsNull(uint64(ctr.curRow)) {
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

					for i, vec := range ctr.bats[ctr.preIdx].Vecs {
						if !vec.IsNull(uint64(ctr.preRow)) {
							continue
						}
						err = setValue(vec, ctr.bats[ctr.curIdx].Vecs[i], ctr.preRow, ctr.curRow, proc)
						if err != nil {
							return result, err
						}
					}

				}
				if ctr.preRow == ctr.bats[ctr.preRow].RowCount() {
					ctr.preRow = 0
				}
			}

			for ; ctr.preRow < ctr.curRow; ctr.preRow++ {
				for i, vec := range ctr.bats[ctr.preIdx].Vecs {
					if !vec.IsNull(uint64(ctr.preRow)) {
						continue
					}
					err = setValue(vec, ctr.bats[ctr.curIdx].Vecs[i], ctr.preRow, ctr.curRow, proc)
					if err != nil {
						return result, err
					}
				}
			}

			ctr.status = findNull

		case withoutNewBat:
			if len(ctr.bats) > 0 {
				if ctr.buf != nil {
					proc.PutBatch(ctr.buf)
					ctr.buf = nil
				}
				ctr.buf = ctr.bats[0]
				ctr.bats = ctr.bats[1:]
				result.Batch = ctr.buf
				return result, nil
			}
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func processPrev(ctr *container, ap *Argument, proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	var err error
	result := vm.NewCallResult()
	if ctr.buf != nil {
		proc.PutBatch(ctr.buf)
		ctr.buf = nil
	}
	ctr.buf, _, err = ctr.ReceiveFromAllRegs(anal)
	if err != nil {
		return result, err
	}
	if ctr.buf == nil {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
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
					ctr.prevVecs[i] = proc.GetVector(*ctr.buf.Vecs[i].GetType())
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
	return result, nil
}

func processLinear(ctr *container, ap *Argument, proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	var err error
	result := vm.NewCallResult()
	for {
		switch ctr.status {
		case receiveBat:
			bat, _, err := ctr.ReceiveFromAllRegs(anal)
			if err != nil {
				return result, err
			}
			if bat == nil {
				ctr.status = withoutNewBat
				continue
			}
			ctr.bats = append(ctr.bats, bat)
			if ctr.subStatus == findNullPre {
				ctr.status = findNullPre
			} else {
				ctr.status = findValue
			}
		case findNullPre:
			if ctr.preIdx > 0 {
				ctr.preIdx--
				ctr.nullIdx--
				if ctr.buf != nil {
					proc.PutBatch(ctr.buf)
					ctr.buf = nil
				}
				ctr.buf = ctr.bats[0]
				ctr.bats = ctr.bats[1:]
				result.Batch = ctr.buf
				return result, nil
			}
			b := ctr.bats[ctr.nullIdx]

			if ctr.nullIdx > ctr.preRow {
				if b.Vecs[ctr.colIdx].IsNull(uint64(ctr.nullIdx)) {
					ctr.status = findValue
					continue
				}
			}

			hasNullPre := false
			ctr.preRow = ctr.nullRow - 1
			for ; ctr.nullRow < b.RowCount(); ctr.nullRow++ {
				if ctr.preRow >= 0 && b.Vecs[ctr.colIdx].IsNull(uint64(ctr.nullRow)) && !b.Vecs[ctr.colIdx].IsNull(uint64(ctr.preRow)) {
					hasNullPre = true
					break
				}
				ctr.preRow++
			}

			ctr.subStatus = findNullPre
			if hasNullPre {
				ctr.status = findValue
			} else {
				if b.Vecs[ctr.colIdx].IsNull(uint64(ctr.preRow)) {
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
				if !b.Vecs[ctr.colIdx].IsNull(uint64(ctr.curRow)) {
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
			for i, exe := range ctr.exes {
				b := batch.NewWithSize(2)
				b.Vecs[0] = proc.GetVector(*ctr.bats[ctr.preIdx].Vecs[i].GetType())
				err = appendValue(b.Vecs[0], ctr.bats[ctr.preIdx].Vecs[i], ctr.preRow, proc)
				if err != nil {
					return result, err
				}
				b.Vecs[1] = proc.GetVector(*ctr.bats[ctr.curIdx].Vecs[i].GetType())
				err = appendValue(b.Vecs[1], ctr.bats[ctr.curIdx].Vecs[i], ctr.curRow, proc)
				if err != nil {
					return result, err
				}
				b.SetRowCount(1)
				ctr.valVecs[i], err = exe.EvalWithoutResultReusing(proc, []*batch.Batch{b})
				if err != nil {
					return result, err
				}
			}
			ctr.preRow++
			for ; ctr.preIdx < ctr.curIdx-1; ctr.preIdx++ {
				for ; ctr.preRow < ctr.bats[ctr.preIdx].RowCount(); ctr.preRow++ {

					for i, vec := range ctr.bats[ctr.preIdx].Vecs {
						if !vec.IsNull(uint64(ctr.preRow)) {
							continue
						}
						err = setValue(vec, ctr.valVecs[i], ctr.preRow, 0, proc)
						if err != nil {
							return result, err
						}
					}

				}
				if ctr.preRow == ctr.bats[ctr.preRow].RowCount() {
					ctr.preRow = 0
				}
			}

			for ; ctr.preRow < ctr.curRow; ctr.preRow++ {
				for i, vec := range ctr.bats[ctr.preIdx].Vecs {
					if !vec.IsNull(uint64(ctr.preRow)) {
						continue
					}
					err = setValue(vec, ctr.valVecs[i], ctr.preRow, 0, proc)
					if err != nil {
						return result, err
					}
				}
			}

			ctr.nullIdx = ctr.preIdx
			ctr.nullRow = ctr.preRow
			ctr.status = findNullPre
		case withoutNewBat:
			if len(ctr.bats) > 0 {
				if ctr.buf != nil {
					proc.PutBatch(ctr.buf)
					ctr.buf = nil
				}
				ctr.buf = ctr.bats[0]
				ctr.bats = ctr.bats[1:]
				result.Batch = ctr.buf
				return result, nil
			}
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func processDefault(ctr *container, ap *Argument, proc *process.Process, anal process.Analyze) (vm.CallResult, error) {
	var err error
	result := vm.NewCallResult()
	if ctr.buf != nil {
		proc.PutBatch(ctr.buf)
		ctr.buf = nil
	}
	ctr.buf, _, err = ctr.ReceiveFromAllRegs(anal)
	if err != nil {
		return result, err
	}
	if ctr.buf == nil {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}
	result.Batch = ctr.buf
	return result, nil
}

func appendValue(v, w *vector.Vector, j int, proc *process.Process) error {
	var err error
	switch v.GetType().Oid {
	case types.T_bool:
		err = vector.AppendFixed[bool](v, vector.GetFixedAt[bool](w, j), false, proc.Mp())
	case types.T_int8:
		err = vector.AppendFixed[int8](v, vector.GetFixedAt[int8](w, j), false, proc.Mp())
	case types.T_int16:
		err = vector.AppendFixed[int16](v, vector.GetFixedAt[int16](w, j), false, proc.Mp())
	case types.T_int32:
		err = vector.AppendFixed[int32](v, vector.GetFixedAt[int32](w, j), false, proc.Mp())
	case types.T_int64:
		err = vector.AppendFixed[int64](v, vector.GetFixedAt[int64](w, j), false, proc.Mp())
	case types.T_uint8:
		err = vector.AppendFixed[uint8](v, vector.GetFixedAt[uint8](w, j), false, proc.Mp())
	case types.T_uint16:
		err = vector.AppendFixed[uint16](v, vector.GetFixedAt[uint16](w, j), false, proc.Mp())
	case types.T_uint32:
		err = vector.AppendFixed[uint32](v, vector.GetFixedAt[uint32](w, j), false, proc.Mp())
	case types.T_uint64:
		err = vector.AppendFixed[uint64](v, vector.GetFixedAt[uint64](w, j), false, proc.Mp())
	case types.T_float32:
		err = vector.AppendFixed[float32](v, vector.GetFixedAt[float32](w, j), false, proc.Mp())
	case types.T_float64:
		err = vector.AppendFixed[float64](v, vector.GetFixedAt[float64](w, j), false, proc.Mp())
	case types.T_date:
		err = vector.AppendFixed[types.Date](v, vector.GetFixedAt[types.Date](w, j), false, proc.Mp())
	case types.T_datetime:
		err = vector.AppendFixed[types.Datetime](v, vector.GetFixedAt[types.Datetime](w, j), false, proc.Mp())
	case types.T_time:
		err = vector.AppendFixed[types.Time](v, vector.GetFixedAt[types.Time](w, j), false, proc.Mp())
	case types.T_timestamp:
		err = vector.AppendFixed[types.Timestamp](v, vector.GetFixedAt[types.Timestamp](w, j), false, proc.Mp())
	case types.T_enum:
		err = vector.AppendFixed[types.Enum](v, vector.GetFixedAt[types.Enum](w, j), false, proc.Mp())
	case types.T_decimal64:
		err = vector.AppendFixed[types.Decimal64](v, vector.GetFixedAt[types.Decimal64](w, j), false, proc.Mp())
	case types.T_decimal128:
		err = vector.AppendFixed[types.Decimal128](v, vector.GetFixedAt[types.Decimal128](w, j), false, proc.Mp())
	case types.T_uuid:
		err = vector.AppendFixed[types.Uuid](v, vector.GetFixedAt[types.Uuid](w, j), false, proc.Mp())
	case types.T_TS:
		err = vector.AppendFixed[types.TS](v, vector.GetFixedAt[types.TS](w, j), false, proc.Mp())
	case types.T_Rowid:
		err = vector.AppendFixed[types.Rowid](v, vector.GetFixedAt[types.Rowid](w, j), false, proc.Mp())
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64:
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
		err = vector.SetFixedAt[bool](v, i, vector.GetFixedAt[bool](w, j))
	case types.T_int8:
		err = vector.SetFixedAt[int8](v, i, vector.GetFixedAt[int8](w, j))
	case types.T_int16:
		err = vector.SetFixedAt[int16](v, i, vector.GetFixedAt[int16](w, j))
	case types.T_int32:
		err = vector.SetFixedAt[int32](v, i, vector.GetFixedAt[int32](w, j))
	case types.T_int64:
		err = vector.SetFixedAt[int64](v, i, vector.GetFixedAt[int64](w, j))
	case types.T_uint8:
		err = vector.SetFixedAt[uint8](v, i, vector.GetFixedAt[uint8](w, j))
	case types.T_uint16:
		err = vector.SetFixedAt[uint16](v, i, vector.GetFixedAt[uint16](w, j))
	case types.T_uint32:
		err = vector.SetFixedAt[uint32](v, i, vector.GetFixedAt[uint32](w, j))
	case types.T_uint64:
		err = vector.SetFixedAt[uint64](v, i, vector.GetFixedAt[uint64](w, j))
	case types.T_float32:
		err = vector.SetFixedAt[float32](v, i, vector.GetFixedAt[float32](w, j))
	case types.T_float64:
		err = vector.SetFixedAt[float64](v, i, vector.GetFixedAt[float64](w, j))
	case types.T_date:
		err = vector.SetFixedAt[types.Date](v, i, vector.GetFixedAt[types.Date](w, j))
	case types.T_datetime:
		err = vector.SetFixedAt[types.Datetime](v, i, vector.GetFixedAt[types.Datetime](w, j))
	case types.T_time:
		err = vector.SetFixedAt[types.Time](v, i, vector.GetFixedAt[types.Time](w, j))
	case types.T_timestamp:
		err = vector.SetFixedAt[types.Timestamp](v, i, vector.GetFixedAt[types.Timestamp](w, j))
	case types.T_enum:
		err = vector.SetFixedAt[types.Enum](v, i, vector.GetFixedAt[types.Enum](w, j))
	case types.T_decimal64:
		err = vector.SetFixedAt[types.Decimal64](v, i, vector.GetFixedAt[types.Decimal64](w, j))
	case types.T_decimal128:
		err = vector.SetFixedAt[types.Decimal128](v, i, vector.GetFixedAt[types.Decimal128](w, j))
	case types.T_uuid:
		err = vector.SetFixedAt[types.Uuid](v, i, vector.GetFixedAt[types.Uuid](w, j))
	case types.T_TS:
		err = vector.SetFixedAt[types.TS](v, i, vector.GetFixedAt[types.TS](w, j))
	case types.T_Rowid:
		err = vector.SetFixedAt[types.Rowid](v, i, vector.GetFixedAt[types.Rowid](w, j))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64:
		err = vector.SetBytesAt(v, i, w.GetBytesAt(j), proc.Mp())
	default:
		panic(fmt.Sprintf("unexpect type %s for function set value in fill query", v.GetType()))
	}
	return err
}
