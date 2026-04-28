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

package top

import (
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "top"

func (top *Top) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": top([")
	for i, f := range top.Fs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(f.String())
	}
	fmt.Fprintf(buf, "], %v)", top.Limit)
}

func (top *Top) OpType() vm.OpType {
	return vm.Top
}

func (top *Top) Prepare(proc *process.Process) (err error) {
	if top.OpAnalyzer == nil {
		top.OpAnalyzer = process.NewAnalyzer(top.GetIdx(), top.IsFirst, top.IsLast, "top")
	} else {
		top.OpAnalyzer.Reset()
	}

	// limit executor
	if top.ctr.limitExecutor == nil {
		top.ctr.limitExecutor, err = colexec.NewExpressionExecutor(proc, top.Limit)
		if err != nil {
			return err
		}
	}
	vec, err := top.ctr.limitExecutor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return err
	}
	top.ctr.limit = vector.MustFixedColWithTypeCheck[uint64](vec)[0]

	if top.ctr.limit > 1024 {
		top.ctr.sels = make([]int64, 0, 1024)
	} else {
		top.ctr.sels = make([]int64, 0, top.ctr.limit)
	}
	top.ctr.poses = make([]int32, 0, len(top.Fs))

	if len(top.ctr.executorsForOrderColumn) != len(top.Fs) {
		top.ctr.executorsForOrderColumn = make([]colexec.ExpressionExecutor, len(top.Fs))
		for i := range top.ctr.executorsForOrderColumn {
			top.ctr.executorsForOrderColumn[i], err = colexec.NewExpressionExecutor(proc, top.Fs[i].Expr)
			if err != nil {
				return err
			}
		}
	}

	typ := top.Fs[0].Expr.Typ
	if top.TopValueTag > 0 {
		top.ctr.desc = top.Fs[0].Flag&plan.OrderBySpec_DESC != 0
		top.ctr.topValueZM = objectio.NewZM(types.T(typ.Id), typ.Scale)
	}

	if top.ctr.limit > topSpillThreshold {
		top.ctr.spilling = true
		top.ctr.rowRefs = make([]rowRef, 0, min(top.ctr.limit, 1024*1024))
	}

	return nil
}

func (top *Top) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := top.OpAnalyzer

	if top.ctr.limit == 0 {
		result := vm.NewCallResult()
		result.Status = vm.ExecStop
		return result, nil
	}

	if top.ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(top.GetChildren(0), proc, analyzer)
			if err != nil {
				return result, err
			}
			bat := result.Batch

			if bat == nil {
				top.ctr.state = vm.Eval
				break
			}
			if bat.IsEmpty() {
				continue
			}

			//because ctr.build will change input batch(append new Vector)
			if top.ctr.buildBat == nil {
				top.ctr.n = len(bat.Vecs)
				top.ctr.buildBat = batch.NewWithSize(top.ctr.n)
			} else {
				top.ctr.buildBat.Vecs = top.ctr.buildBat.Vecs[:len(bat.Vecs)]
			}
			top.ctr.buildBat.Recursive = bat.Recursive
			top.ctr.buildBat.ShuffleIDX = bat.ShuffleIDX
			top.ctr.buildBat.Attrs = bat.Attrs
			if len(bat.ExtraBuf) > 0 {
				return result, moerr.NewInternalError(proc.Ctx, "top build should not have extra buffers")
			}
			copy(top.ctr.buildBat.Vecs, bat.Vecs)
			top.ctr.buildBat.SetRowCount(bat.RowCount())

			err = top.ctr.build(top, top.ctr.buildBat, proc, analyzer)
			if err != nil {
				return result, err
			}
			if top.TopValueTag > 0 && top.updateTopValueZM() {
				message.SendMessage(message.TopValueMessage{TopValueZM: top.ctr.topValueZM, Tag: top.TopValueTag}, proc.GetMessageBoard())
			}
		}
	}

	result := vm.NewCallResult()
	if top.ctr.state == vm.Eval {
		top.ctr.state = vm.End
		if top.ctr.bat != nil {
			err := top.ctr.eval(top.ctr.limit, top.ctr.n, proc, &result)
			if err != nil {
				return result, err
			}
		}
		return result, nil
	}

	if top.ctr.state == vm.End {
		return vm.CancelResult, nil
	}

	panic("bug")
}

func (ctr *container) build(ap *Top, bat *batch.Batch, proc *process.Process, analyzer process.Analyzer) error {
	ctr.poses = ctr.poses[:0]
	for i := range ap.Fs {
		vec, err := ctr.executorsForOrderColumn[i].Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		aNewOrderColumn := true
		for j := range bat.Vecs {
			if bat.Vecs[j] == vec {
				aNewOrderColumn = false
				ctr.poses = append(ctr.poses, int32(j))
				break
			}
		}
		if aNewOrderColumn {
			ctr.poses = append(ctr.poses, int32(len(bat.Vecs)))
			bat.Vecs = append(bat.Vecs, vec)
		}
	}

	if len(ctr.cmps) == 0 {
		mp := make(map[int]int)
		for i, pos := range ctr.poses {
			mp[int(pos)] = i
		}

		if ctr.bat == nil {
			if ctr.spilling {
				ctr.bat = batch.NewOffHeapWithSize(len(ctr.poses))
				for idx, pos := range ctr.poses {
					ctr.bat.Vecs[idx] = vector.NewOffHeapVecWithType(*bat.Vecs[pos].GetType())
				}
			} else {
				batNew, vecNew := batch.NewWithSize, vector.NewVec
				if ap.ctr.limit > 10240 {
					batNew, vecNew = batch.NewOffHeapWithSize, vector.NewOffHeapVecWithType
				}
				ctr.bat = batNew(len(bat.Vecs))
				for i, vec := range bat.Vecs {
					ctr.bat.Vecs[i] = vecNew(*vec.GetType())
				}
			}
		}

		if ctr.spilling {
			ctr.spillCmpPoses = make([]int32, len(ctr.poses))
			for idx := range ctr.poses {
				ctr.spillCmpPoses[idx] = int32(idx)
				var desc, nullsLast bool
				pos := ctr.poses[idx]
				if posIdx, ok := mp[int(pos)]; ok {
					desc = ap.Fs[posIdx].Flag&plan.OrderBySpec_DESC != 0
					if ap.Fs[posIdx].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
						nullsLast = false
					} else if ap.Fs[posIdx].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
						nullsLast = true
					} else {
						nullsLast = desc
					}
				}
				ctr.cmps = append(
					ctr.cmps,
					compare.New(*bat.Vecs[pos].GetType(), desc, nullsLast),
				)
			}
		} else {
			for i := 0; i < len(bat.Vecs); i++ {
				var desc, nullsLast bool
				if pos, ok := mp[i]; ok {
					desc = ap.Fs[pos].Flag&plan.OrderBySpec_DESC != 0
					if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_FIRST != 0 {
						nullsLast = false
					} else if ap.Fs[pos].Flag&plan.OrderBySpec_NULLS_LAST != 0 {
						nullsLast = true
					} else {
						nullsLast = desc
					}
				}
				ctr.cmps = append(
					ctr.cmps,
					compare.New(*bat.Vecs[i].GetType(), desc, nullsLast),
				)
			}
		}
	}

	if ctr.spilling {
		return ctr.processBatchSpill(ap.ctr.limit, bat, proc, analyzer)
	}
	return ctr.processBatch(ap.ctr.limit, bat, proc)
}

func (ctr *container) processBatch(limit uint64, bat *batch.Batch, proc *process.Process) error {
	rowCount := int64(bat.RowCount())
	toFillCount := int64(limit) - int64(len(ctr.sels))

	processCount := min(int64(toFillCount), rowCount)

	if processCount > 0 {
		for j, vec := range ctr.bat.Vecs {
			if err := vec.UnionBatch(
				bat.Vecs[j],
				0,
				int(processCount),
				nil,
				proc.Mp(),
			); err != nil {
				return err
			}
		}
		baseSel := int64(len(ctr.sels))
		for i := range processCount {
			ctr.sels = append(ctr.sels, baseSel+i)
		}
		ctr.bat.AddRowCount(int(processCount))

		if uint64(len(ctr.sels)) == limit {
			ctr.sort()
		}
	}

	if processCount == rowCount {
		return nil
	}

	// bat is still have items
	for i, cmp := range ctr.cmps {
		cmp.Set(1, bat.Vecs[i])
	}
	for i, j := processCount, rowCount; i < j; i++ {
		if ctr.compare(1, 0, i, ctr.sels[0]) < 0 {
			for _, cmp := range ctr.cmps {
				if err := cmp.Copy(1, 0, i, ctr.sels[0], proc); err != nil {
					return err
				}
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}

func (ctr *container) spillBatch(bat *batch.Batch, proc *process.Process, analyzer process.Analyzer) error {
	if ctr.spillFile == nil {
		f, err := os.CreateTemp("", "mo-top-spill-*")
		if err != nil {
			return err
		}
		ctr.spillFile = f
	}

	offset, _ := ctr.spillFile.Seek(0, io.SeekCurrent)

	// Only serialize the original n columns (excluding appended order columns).
	origBat := batch.NewWithSize(ctr.n)
	if len(bat.Attrs) >= ctr.n {
		origBat.Attrs = bat.Attrs[:ctr.n]
	}
	copy(origBat.Vecs, bat.Vecs[:ctr.n])
	origBat.SetRowCount(bat.RowCount())

	ctr.spillBuf.Reset()
	data, err := origBat.MarshalBinaryWithBuffer(&ctr.spillBuf, false)
	if err != nil {
		return err
	}
	if _, err := ctr.spillFile.Write(data); err != nil {
		return err
	}

	analyzer.Spill(int64(len(data)))
	analyzer.SpillRows(int64(bat.RowCount()))

	ctr.spillIndex = append(ctr.spillIndex, spilledBatchInfo{
		offset: offset,
		size:   int64(len(data)),
		rows:   int32(bat.RowCount()),
	})
	ctr.spillBatIdx++
	return nil
}

func (ctr *container) processBatchSpill(limit uint64, bat *batch.Batch, proc *process.Process, analyzer process.Analyzer) error {
	batchIdx := ctr.spillBatIdx
	if err := ctr.spillBatch(bat, proc, analyzer); err != nil {
		return err
	}

	rowCount := int64(bat.RowCount())
	toFillCount := int64(limit) - int64(len(ctr.sels))
	processCount := min(int64(toFillCount), rowCount)

	if processCount > 0 {
		for idx, pos := range ctr.poses {
			if err := ctr.bat.Vecs[idx].UnionBatch(
				bat.Vecs[pos],
				0,
				int(processCount),
				nil,
				proc.Mp(),
			); err != nil {
				return err
			}
		}
		baseSel := int64(len(ctr.sels))
		for i := range processCount {
			ctr.sels = append(ctr.sels, baseSel+i)
			ctr.rowRefs = append(ctr.rowRefs, rowRef{
				batchIdx: batchIdx,
				rowIdx:   int32(i),
			})
		}
		ctr.bat.AddRowCount(int(processCount))

		if uint64(len(ctr.sels)) == limit {
			ctr.sortSpill()
		}
	}

	if processCount == rowCount {
		return nil
	}

	// heap is full, compare and replace
	for idx, pos := range ctr.poses {
		ctr.cmps[idx].Set(1, bat.Vecs[pos])
	}
	for i, j := processCount, rowCount; i < j; i++ {
		if ctr.compare(1, 0, i, ctr.sels[0]) < 0 {
			for idx := range ctr.cmps {
				if err := ctr.cmps[idx].Copy(1, 0, i, ctr.sels[0], proc); err != nil {
					return err
				}
			}
			ctr.rowRefs[ctr.sels[0]] = rowRef{
				batchIdx: batchIdx,
				rowIdx:   int32(i),
			}
			heap.Fix(ctr, 0)
		}
	}
	return nil
}

func (ctr *container) eval(limit uint64, n int, proc *process.Process, result *vm.CallResult) error {
	if ctr.spilling {
		return ctr.evalSpill(limit, n, proc, result)
	}
	return ctr.evalInMemory(limit, n, proc, result)
}

func (ctr *container) evalInMemory(limit uint64, n int, proc *process.Process, result *vm.CallResult) error {
	if uint64(len(ctr.sels)) < limit {
		ctr.sort()
	}
	for i, cmp := range ctr.cmps {
		ctr.bat.Vecs[i] = cmp.Vector()
	}
	sels := make([]int64, len(ctr.sels))
	for i, j := 0, len(ctr.sels); i < j; i++ {
		sels[len(sels)-1-i] = heap.Pop(ctr).(int64)
	}
	if err := ctr.bat.Shuffle(sels, proc.Mp()); err != nil {
		return err
	}
	for i := n; i < len(ctr.bat.Vecs); i++ {
		ctr.bat.Vecs[i].Free(proc.Mp())
	}
	ctr.bat.Vecs = ctr.bat.Vecs[:n]
	result.Batch = ctr.bat
	return nil
}

func (ctr *container) evalSpill(limit uint64, n int, proc *process.Process, result *vm.CallResult) error {
	if uint64(len(ctr.sels)) < limit {
		ctr.sortSpill()
	}

	// Pop all elements from heap in sorted order (reverse pop gives ascending).
	orderedSels := make([]int64, len(ctr.sels))
	for i, j := 0, len(ctr.sels); i < j; i++ {
		orderedSels[len(orderedSels)-1-i] = heap.Pop(ctr).(int64)
	}

	// Collect refs for each spilled batch.
	type batchRow struct {
		outputIdx int
		rowIdx    int32
	}
	batchRows := make(map[int32][]batchRow)
	for outputIdx, sel := range orderedSels {
		ref := ctr.rowRefs[sel]
		batchRows[ref.batchIdx] = append(batchRows[ref.batchIdx], batchRow{
			outputIdx: outputIdx,
			rowIdx:    ref.rowIdx,
		})
	}

	// Build the output batch.
	outputBat := batch.NewOffHeapWithSize(n)
	totalRows := len(orderedSels)

	reuseBat := batch.NewOffHeapWithSize(0)
	defer reuseBat.Clean(proc.Mp())

	for bIdx, rows := range batchRows {
		info := ctr.spillIndex[bIdx]
		data := make([]byte, info.size)
		if _, err := ctr.spillFile.ReadAt(data, info.offset); err != nil {
			return err
		}

		reuseBat.CleanOnlyData()
		if err := reuseBat.UnmarshalBinaryWithAnyMp(data, proc.Mp()); err != nil {
			return err
		}

		if outputBat.Vecs[0] == nil {
			for i := 0; i < n; i++ {
				outputBat.Vecs[i] = vector.NewOffHeapVecWithType(*reuseBat.Vecs[i].GetType())
				if err := outputBat.Vecs[i].PreExtend(totalRows, proc.Mp()); err != nil {
					return err
				}
				outputBat.Vecs[i].SetLength(totalRows)
			}
			if len(reuseBat.Attrs) > 0 {
				outputBat.Attrs = make([]string, n)
				copy(outputBat.Attrs, reuseBat.Attrs[:n])
			}
		}

		for _, r := range rows {
			for col := 0; col < n; col++ {
				if err := outputBat.Vecs[col].Copy(reuseBat.Vecs[col], int64(r.outputIdx), int64(r.rowIdx), proc.Mp()); err != nil {
					return err
				}
			}
		}
	}

	outputBat.SetRowCount(totalRows)

	// Clean the key-only heap batch and replace with the full output.
	ctr.bat.Clean(proc.Mp())
	ctr.bat = outputBat
	result.Batch = ctr.bat
	return nil
}

// do sort work for heap, and result order will be set in container.sels
func (ctr *container) sort() {
	for i, cmp := range ctr.cmps {
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	heap.Init(ctr)
}

func (ctr *container) sortSpill() {
	for i, cmp := range ctr.cmps {
		cmp.Set(0, ctr.bat.Vecs[i])
	}
	heap.Init(ctr)
}

func (top *Top) updateTopValueZM() bool {
	v, ok := top.getTopValue()
	if !ok {
		return false
	}
	zm := top.ctr.topValueZM
	if !zm.IsInited() {
		index.UpdateZM(zm, v)
		return true
	}
	newZM := objectio.NewZM(zm.GetType(), zm.GetScale())
	index.UpdateZM(newZM, v)
	if top.ctr.desc && newZM.CompareMax(zm) > 0 {
		top.ctr.topValueZM = newZM
		return true
	}
	if !top.ctr.desc && newZM.CompareMin(zm) < 0 {
		top.ctr.topValueZM = newZM
		return true
	}
	return false
}

func (top *Top) getTopValue() ([]byte, bool) {
	// not enough items in the heap.
	if uint64(len(top.ctr.sels)) < top.ctr.limit {
		return nil, false
	}
	x := int(top.ctr.sels[0])
	ctr := &top.ctr
	var vec *vector.Vector
	if ctr.spilling {
		vec = ctr.cmps[0].Vector()
	} else {
		vec = ctr.cmps[ctr.poses[0]].Vector()
	}
	if vec.GetType().IsVarlen() {
		return vec.GetBytesAt(x), true
	}
	switch vec.GetType().Oid {
	case types.T_int8:
		v := vector.GetFixedAtNoTypeCheck[int8](vec, x)
		return types.EncodeInt8(&v), true
	case types.T_int16:
		v := vector.GetFixedAtNoTypeCheck[int16](vec, x)
		return types.EncodeInt16(&v), true
	case types.T_int32:
		v := vector.GetFixedAtNoTypeCheck[int32](vec, x)
		return types.EncodeInt32(&v), true
	case types.T_int64:
		v := vector.GetFixedAtNoTypeCheck[int64](vec, x)
		return types.EncodeInt64(&v), true
	case types.T_uint8:
		v := vector.GetFixedAtNoTypeCheck[uint8](vec, x)
		return types.EncodeUint8(&v), true
	case types.T_uint16:
		v := vector.GetFixedAtNoTypeCheck[uint16](vec, x)
		return types.EncodeUint16(&v), true
	case types.T_uint32:
		v := vector.GetFixedAtNoTypeCheck[uint32](vec, x)
		return types.EncodeUint32(&v), true
	case types.T_uint64:
		v := vector.GetFixedAtNoTypeCheck[uint64](vec, x)
		return types.EncodeUint64(&v), true
	case types.T_float32:
		v := vector.GetFixedAtNoTypeCheck[float32](vec, x)
		return types.EncodeFloat32(&v), true
	case types.T_float64:
		v := vector.GetFixedAtNoTypeCheck[float64](vec, x)
		return types.EncodeFloat64(&v), true
	case types.T_date:
		v := vector.GetFixedAtNoTypeCheck[types.Date](vec, x)
		return types.EncodeDate(&v), true
	case types.T_datetime:
		v := vector.GetFixedAtNoTypeCheck[types.Datetime](vec, x)
		return types.EncodeDatetime(&v), true
	case types.T_timestamp:
		v := vector.GetFixedAtNoTypeCheck[types.Timestamp](vec, x)
		return types.EncodeTimestamp(&v), true
	case types.T_time:
		v := vector.GetFixedAtNoTypeCheck[types.Time](vec, x)
		return types.EncodeTime(&v), true
	case types.T_decimal64:
		v := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, x)
		return types.EncodeDecimal64(&v), true
	case types.T_decimal128:
		v := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, x)
		return types.EncodeDecimal128(&v), true
	case types.T_enum:
		v := vector.GetFixedAtNoTypeCheck[types.Enum](vec, x)
		return types.EncodeEnum(&v), true
	case types.T_year:
		v := vector.GetFixedAtNoTypeCheck[types.MoYear](vec, x)
		return types.EncodeMoYear(&v), true
	}
	return nil, false
}
