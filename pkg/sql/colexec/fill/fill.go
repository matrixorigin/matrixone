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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
			ctr.prevValid = make([]bool, fill.ColLen)
		}
		ctr.process = processPrev
	case plan.Node_NEXT:
		ctr.i = 0
		ctr.idx = 0
		ctr.process = processNext
	case plan.Node_LINEAR:
		ctr.i = 0
		ctr.idx = 0
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
	analyzer := fill.OpAnalyzer

	ctr := &fill.ctr

	result, err := ctr.process(ctr, fill, proc, analyzer)

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
	return result, nil
}

func (ctr *container) initIndex() {
	ctr.idx = 0
	ctr.i = 0
}

// partKeyAt reads one partition-key cell, tolerating the constant vectors the
// time window emits when it broadcasts a key across a flushed batch.
func partKeyAt(vec *vector.Vector, row int) (val []byte, isNull bool) {
	if vec.IsConstNull() {
		return nil, true
	}
	if vec.IsConst() {
		row = 0
	}
	if vec.IsNull(uint64(row)) {
		return nil, true
	}
	return vec.GetRawBytesAt(row), false
}

// samePartitionRows reports whether two rows carry the same partition key.
// NULL keys compare equal, matching how GROUP BY folded them into one group.
func samePartitionRows(partIdx []int32, batA *batch.Batch, rowA int, batB *batch.Batch, rowB int) bool {
	for _, col := range partIdx {
		valA, nullA := partKeyAt(batA.Vecs[col], rowA)
		valB, nullB := partKeyAt(batB.Vecs[col], rowB)
		if nullA || nullB {
			if nullA != nullB {
				return false
			}
			continue
		}
		if !bytes.Equal(valA, valB) {
			return false
		}
	}
	return true
}

// snapshotPartKey copies a row's partition key out of the batch, so the next
// batch can detect a boundary after this one has been recycled.
func (ctr *container) snapshotPartKey(partIdx []int32, bat *batch.Batch, row int) {
	if cap(ctr.prevPartKey) < len(partIdx) {
		ctr.prevPartKey = make([][]byte, len(partIdx))
		ctr.prevPartNull = make([]bool, len(partIdx))
	}
	ctr.prevPartKey = ctr.prevPartKey[:len(partIdx)]
	ctr.prevPartNull = ctr.prevPartNull[:len(partIdx)]
	for i, col := range partIdx {
		val, isNull := partKeyAt(bat.Vecs[col], row)
		ctr.prevPartNull[i] = isNull
		ctr.prevPartKey[i] = append(ctr.prevPartKey[i][:0], val...)
	}
	ctr.prevPartSet = true
}

// matchesSnapshot compares a row against the saved cross-batch partition key.
func (ctr *container) matchesSnapshot(partIdx []int32, bat *batch.Batch, row int) bool {
	if !ctr.prevPartSet {
		return true
	}
	for i, col := range partIdx {
		val, isNull := partKeyAt(bat.Vecs[col], row)
		if isNull || ctr.prevPartNull[i] {
			if isNull != ctr.prevPartNull[i] {
				return false
			}
			continue
		}
		if !bytes.Equal(val, ctr.prevPartKey[i]) {
			return false
		}
	}
	return true
}

// gatherBats materializes the whole child stream into ctr.bats. NEXT and
// LINEAR read values that lie after the gap being filled, so they cannot
// stream; materializing first also means no batch is ever dropped because an
// earlier one happened to contain no NULL.
func (ctr *container) gatherBats(ap *Fill, proc *process.Process, analyzer process.Analyzer) error {
	for {
		result, err := vm.ChildrenCall(ap.GetChildren(0), proc, analyzer)
		if err != nil {
			return err
		}
		if result.Batch == nil {
			return nil
		}
		if len(ctr.bats) > ctr.i {
			if ctr.bats[ctr.i] != nil {
				ctr.bats[ctr.i].CleanOnlyData()
			}
			ctr.bats[ctr.i], err = ctr.bats[ctr.i].AppendWithCopy(proc.Ctx, proc.Mp(), result.Batch)
			if err != nil {
				return err
			}
		} else {
			appBat, err := result.Batch.Dup(proc.Mp())
			if err != nil {
				return err
			}
			if analyzer != nil {
				analyzer.Alloc(int64(appBat.Size()))
			}
			ctr.bats = append(ctr.bats, appBat)
		}
		ctr.i++
	}
}

// fillNextCol replaces every NULL with the nearest following non-NULL value of
// the same partition. Walking backwards keeps that candidate in one cursor,
// and a partition boundary simply drops it: the NULLs at the tail of the
// previous partition have no next value of their own.
func (ctr *container) fillNextCol(ap *Fill, col int, proc *process.Process) error {
	nextBat, nextRow := -1, -1
	for b := len(ctr.bats) - 1; b >= 0; b-- {
		vec := ctr.bats[b].Vecs[col]
		for r := ctr.bats[b].RowCount() - 1; r >= 0; r-- {
			if !vec.IsNull(uint64(r)) {
				nextBat, nextRow = b, r
				continue
			}
			if nextBat < 0 {
				continue
			}
			if len(ap.PartitionColIdx) > 0 &&
				!samePartitionRows(ap.PartitionColIdx, ctr.bats[b], r, ctr.bats[nextBat], nextRow) {
				nextBat, nextRow = -1, -1
				continue
			}
			if err := setValue(vec, ctr.bats[nextBat].Vecs[col], r, nextRow, proc); err != nil {
				return err
			}
		}
	}
	return nil
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

	if len(ctr.prevValid) < ap.ColLen {
		ctr.prevValid = make([]bool, ap.ColLen)
	}

	// A previous value must not leak across a partition boundary, so find
	// where new partitions start before filling: row 0 against the key saved
	// from the previous batch, later rows against their predecessor.
	rowCount := ctr.buf.RowCount()
	var newSegment []bool
	if len(ap.PartitionColIdx) > 0 && rowCount > 0 {
		newSegment = make([]bool, rowCount)
		newSegment[0] = !ctr.matchesSnapshot(ap.PartitionColIdx, ctr.buf, 0)
		for j := 1; j < rowCount; j++ {
			newSegment[j] = !samePartitionRows(ap.PartitionColIdx, ctr.buf, j-1, ctr.buf, j)
		}
	}

	for i := 0; i < ap.ColLen; i++ {
		for j := 0; j < ctr.buf.Vecs[i].Length(); j++ {
			if newSegment != nil && newSegment[j] {
				// The new partition has no previous value yet. The vector is
				// kept for reuse; only its validity is dropped.
				ctr.prevValid[i] = false
			}
			if ctr.buf.Vecs[i].IsNull(uint64(j)) {
				if ctr.prevVecs[i] != nil && ctr.prevValid[i] {
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
				ctr.prevValid[i] = true
			}
		}
	}
	if len(ap.PartitionColIdx) > 0 && rowCount > 0 {
		ctr.snapshotPartKey(ap.PartitionColIdx, ctr.buf, rowCount-1)
	}
	result.Batch = ctr.buf
	return result, nil
}

// fillLinearCol fills each run of NULLs that sits strictly between two
// non-NULL values of the same partition with their midpoint. A run touching a
// partition boundary on either side stays NULL: its neighbour belongs to a
// different group, so there is nothing to interpolate between.
func (ctr *container) fillLinearCol(ap *Fill, col int, proc *process.Process) error {
	type pos struct{ bat, row int }
	preBat, preRow := -1, -1
	var run []pos

	for b := 0; b < len(ctr.bats); b++ {
		vec := ctr.bats[b].Vecs[col]
		for r := 0; r < ctr.bats[b].RowCount(); r++ {
			if preBat >= 0 && len(ap.PartitionColIdx) > 0 &&
				!samePartitionRows(ap.PartitionColIdx, ctr.bats[b], r, ctr.bats[preBat], preRow) {
				preBat, preRow = -1, -1
				run = run[:0]
			}

			if vec.IsNull(uint64(r)) {
				// Only NULLs with a known previous value can ever be filled.
				if preBat >= 0 {
					run = append(run, pos{b, r})
				}
				continue
			}

			if len(run) > 0 {
				valVec, owned, err := linearFillValue(ctr, proc, col, preBat, preRow, b, r)
				if err != nil {
					return err
				}
				for _, p := range run {
					if err = setValue(ctr.bats[p.bat].Vecs[col], valVec, p.row, 0, proc); err != nil {
						if owned {
							valVec.Free(proc.Mp())
						}
						return err
					}
				}
				if owned {
					valVec.Free(proc.Mp())
				}
			}
			preBat, preRow = b, r
			run = run[:0]
		}
	}
	return nil
}

func linearFillValue(ctr *container, proc *process.Process, idx, preIdx, preRow, curIdx, curRow int) (*vector.Vector, bool, error) {
	preVec := ctr.bats[preIdx].Vecs[idx]
	curVec := ctr.bats[curIdx].Vecs[idx]
	if preVec.GetType().Oid == types.T_decimal128 && curVec.GetType().Oid == types.T_decimal128 {
		result := vector.NewVec(*preVec.GetType())
		left := vector.GetFixedAtNoTypeCheck[types.Decimal128](preVec, preRow)
		right := vector.GetFixedAtNoTypeCheck[types.Decimal128](curVec, curRow)
		value, err := decimal128LinearMidpoint(left, right, preVec.GetType().Scale)
		if err != nil {
			result.Free(proc.Mp())
			return nil, false, err
		}
		if err = vector.AppendFixed(result, value, false, proc.Mp()); err != nil {
			result.Free(proc.Mp())
			return nil, false, err
		}
		return result, true, nil
	}

	b := batch.NewWithSize(2)
	b.Vecs[0] = vector.NewVec(*preVec.GetType())
	if err := appendValue(b.Vecs[0], preVec, preRow, proc); err != nil {
		b.Clean(proc.Mp())
		return nil, false, err
	}
	b.Vecs[1] = vector.NewVec(*curVec.GetType())
	if err := appendValue(b.Vecs[1], curVec, curRow, proc); err != nil {
		b.Clean(proc.Mp())
		return nil, false, err
	}
	b.SetRowCount(1)
	defer b.Clean(proc.Mp())
	result, err := ctr.exes[idx].Eval(proc, []*batch.Batch{b}, nil)
	return result, false, err
}

func decimal128LinearMidpoint(left, right types.Decimal128, scale int32) (types.Decimal128, error) {
	sum, err := left.Add128(right)
	if err != nil {
		return types.Decimal128{}, err
	}
	value, valueScale, err := sum.Div(types.Decimal128FromInt64(2), scale, 0)
	if err != nil {
		return types.Decimal128{}, err
	}
	return value.Scale(scale - valueScale)
}

func processNext(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var err error
	result := vm.NewCallResult()
	if !ctr.done {
		if err = ctr.gatherBats(ap, proc, analyzer); err != nil {
			return result, err
		}
		// Only the aggregate prefix is filled: the columns behind it are the
		// window boundaries and partition keys, whose NULLs are data.
		for col := 0; col < ap.ColLen; col++ {
			if err = ctr.fillNextCol(ap, col, proc); err != nil {
				return result, err
			}
		}
		ctr.done = true
	}
	if ctr.idx == len(ctr.bats) {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}
	result.Batch = ctr.bats[ctr.idx]
	result.Status = vm.ExecNext
	ctr.idx++
	return result, nil
}

func processLinear(ctr *container, ap *Fill, proc *process.Process, analyzer process.Analyzer) (vm.CallResult, error) {
	var err error
	result := vm.NewCallResult()
	if !ctr.done {
		if err = ctr.gatherBats(ap, proc, analyzer); err != nil {
			return result, err
		}
		// Only the aggregate prefix is filled: the columns behind it are the
		// window boundaries and partition keys, whose NULLs are data.
		for col := 0; col < ap.ColLen; col++ {
			if err = ctr.fillLinearCol(ap, col, proc); err != nil {
				return result, err
			}
		}
		ctr.done = true
	}
	if ctr.idx == len(ctr.bats) {
		result.Batch = nil
		result.Status = vm.ExecStop
		return result, nil
	}
	result.Batch = ctr.bats[ctr.idx]
	result.Status = vm.ExecNext
	ctr.idx++
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
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[bool](w, j), false, proc.Mp())
	case types.T_bit:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[uint64](w, j), false, proc.Mp())
	case types.T_int8:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[int8](w, j), false, proc.Mp())
	case types.T_int16:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[int16](w, j), false, proc.Mp())
	case types.T_int32:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[int32](w, j), false, proc.Mp())
	case types.T_int64:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[int64](w, j), false, proc.Mp())
	case types.T_uint8:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[uint8](w, j), false, proc.Mp())
	case types.T_uint16:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[uint16](w, j), false, proc.Mp())
	case types.T_uint32:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[uint32](w, j), false, proc.Mp())
	case types.T_uint64:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[uint64](w, j), false, proc.Mp())
	case types.T_float32:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[float32](w, j), false, proc.Mp())
	case types.T_float64:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[float64](w, j), false, proc.Mp())
	case types.T_date:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[types.Date](w, j), false, proc.Mp())
	case types.T_datetime:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[types.Datetime](w, j), false, proc.Mp())
	case types.T_time:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[types.Time](w, j), false, proc.Mp())
	case types.T_timestamp:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[types.Timestamp](w, j), false, proc.Mp())
	case types.T_enum:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[types.Enum](w, j), false, proc.Mp())
	case types.T_decimal64:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[types.Decimal64](w, j), false, proc.Mp())
	case types.T_decimal128:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[types.Decimal128](w, j), false, proc.Mp())
	case types.T_uuid:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[types.Uuid](w, j), false, proc.Mp())
	case types.T_TS:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[types.TS](w, j), false, proc.Mp())
	case types.T_Rowid:
		err = vector.AppendFixed(v, vector.GetFixedAtNoTypeCheck[types.Rowid](w, j), false, proc.Mp())
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
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[bool](w, j))
	case types.T_bit:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[uint64](w, j))
	case types.T_int8:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[int8](w, j))
	case types.T_int16:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[int16](w, j))
	case types.T_int32:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[int32](w, j))
	case types.T_int64:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[int64](w, j))
	case types.T_uint8:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[uint8](w, j))
	case types.T_uint16:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[uint16](w, j))
	case types.T_uint32:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[uint32](w, j))
	case types.T_uint64:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[uint64](w, j))
	case types.T_float32:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[float32](w, j))
	case types.T_float64:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[float64](w, j))
	case types.T_date:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Date](w, j))
	case types.T_datetime:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Datetime](w, j))
	case types.T_time:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Time](w, j))
	case types.T_timestamp:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Timestamp](w, j))
	case types.T_enum:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Enum](w, j))
	case types.T_decimal64:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Decimal64](w, j))
	case types.T_decimal128:
		err = setDecimal128Value(v, w, i, j)
	case types.T_uuid:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Uuid](w, j))
	case types.T_TS:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.TS](w, j))
	case types.T_Rowid:
		err = vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Rowid](w, j))
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_datalink:
		err = vector.SetBytesAt(v, i, w.GetBytesAt(j), proc.Mp())
	default:
		panic(fmt.Sprintf("unexpect type %s for function set value in fill query", v.GetType()))
	}
	return err
}

func setDecimal128Value(v, w *vector.Vector, i, j int) error {
	if v.GetType().Oid == w.GetType().Oid && v.GetType().Scale == w.GetType().Scale {
		return vector.SetFixedAtNoTypeCheck(v, i, vector.GetFixedAtNoTypeCheck[types.Decimal128](w, j))
	}

	var (
		value types.Decimal128
		err   error
	)
	targetScale := v.GetType().Scale
	switch w.GetType().Oid {
	case types.T_int8:
		value = types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[int8](w, j)))
	case types.T_int16:
		value = types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[int16](w, j)))
	case types.T_int32:
		value = types.Decimal128FromInt64(int64(vector.GetFixedAtNoTypeCheck[int32](w, j)))
	case types.T_int64:
		value = types.Decimal128FromInt64(vector.GetFixedAtNoTypeCheck[int64](w, j))
	case types.T_uint8:
		value = types.Decimal128{B0_63: uint64(vector.GetFixedAtNoTypeCheck[uint8](w, j))}
	case types.T_uint16:
		value = types.Decimal128{B0_63: uint64(vector.GetFixedAtNoTypeCheck[uint16](w, j))}
	case types.T_uint32:
		value = types.Decimal128{B0_63: uint64(vector.GetFixedAtNoTypeCheck[uint32](w, j))}
	case types.T_uint64:
		value = types.Decimal128{B0_63: vector.GetFixedAtNoTypeCheck[uint64](w, j)}
	case types.T_float32:
		value, err = types.Decimal128FromFloat64(float64(vector.GetFixedAtNoTypeCheck[float32](w, j)), v.GetType().Width, targetScale)
	case types.T_float64:
		value, err = types.Decimal128FromFloat64(vector.GetFixedAtNoTypeCheck[float64](w, j), v.GetType().Width, targetScale)
	case types.T_decimal64:
		value = types.Decimal128FromDecimal64(vector.GetFixedAtNoTypeCheck[types.Decimal64](w, j), w.GetType().Scale)
		value, err = value.Scale(targetScale - w.GetType().Scale)
	case types.T_decimal128:
		value = vector.GetFixedAtNoTypeCheck[types.Decimal128](w, j)
		value, err = value.Scale(targetScale - w.GetType().Scale)
	default:
		return moerr.NewInternalErrorNoCtxf("cannot set decimal128 fill value from %s", w.GetType())
	}
	if err != nil {
		return err
	}
	if w.GetType().Oid != types.T_float32 && w.GetType().Oid != types.T_float64 &&
		w.GetType().Oid != types.T_decimal64 && w.GetType().Oid != types.T_decimal128 {
		value, err = value.Scale(targetScale)
		if err != nil {
			return err
		}
	}
	return vector.SetFixedAtNoTypeCheck(v, i, value)
}
