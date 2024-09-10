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

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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

	return nil
}

func (top *Top) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(top.GetIdx(), top.GetParallelIdx(), top.GetParallelMajor())
	anal.Start()
	defer func() {
		anal.Stop()
	}()

	if top.ctr.limit == 0 {
		result := vm.NewCallResult()
		result.Status = vm.ExecStop
		return result, nil
	}

	if top.ctr.state == vm.Build {
		for {
			result, err := vm.ChildrenCall(top.GetChildren(0), proc, anal)
			if err != nil {
				return result, err
			}
			bat := result.Batch
			anal.Input(bat, top.IsFirst)

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
			top.ctr.buildBat.Ro = bat.Ro
			top.ctr.buildBat.ShuffleIDX = bat.ShuffleIDX
			top.ctr.buildBat.Attrs = bat.Attrs
			top.ctr.buildBat.Aggs = bat.Aggs
			copy(top.ctr.buildBat.Vecs, bat.Vecs)
			top.ctr.buildBat.SetRowCount(bat.RowCount())

			err = top.ctr.build(top, top.ctr.buildBat, proc)
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
			err := top.ctr.eval(top.ctr.limit, proc, &result)
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

func (ctr *container) build(ap *Top, bat *batch.Batch, proc *process.Process) error {
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
			ctr.bat = batch.NewWithSize(len(bat.Vecs))
			for i, vec := range bat.Vecs {
				ctr.bat.Vecs[i] = vector.NewVec(*vec.GetType())
			}
		}

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

	err := ctr.processBatch(ap.ctr.limit, bat, proc)
	return err
}

func (ctr *container) processBatch(limit uint64, bat *batch.Batch, proc *process.Process) error {
	var start int64

	length := int64(bat.RowCount())
	if n := uint64(len(ctr.sels)); n < limit {
		start = int64(limit - n)
		if start > length {
			start = length
		}
		for i := int64(0); i < start; i++ {
			for j, vec := range ctr.bat.Vecs {
				if err := vec.UnionOne(bat.Vecs[j], i, proc.Mp()); err != nil {
					return err
				}
			}
			ctr.sels = append(ctr.sels, int64(n))
			n++
		}
		ctr.bat.AddRowCount(int(start))

		if n == limit {
			ctr.sort()
		}
	}
	if start == length {
		return nil
	}

	// bat is still have items
	for i, cmp := range ctr.cmps {
		cmp.Set(1, bat.Vecs[i])
	}
	for i, j := start, length; i < j; i++ {
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

func (ctr *container) eval(limit uint64, proc *process.Process, result *vm.CallResult) error {
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
	for i := ctr.n; i < len(ctr.bat.Vecs); i++ {
		ctr.bat.Vecs[i].Free(proc.Mp())
	}
	ctr.bat.Vecs = ctr.bat.Vecs[:ctr.n]
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
	vec := top.ctr.cmps[top.ctr.poses[0]].Vector()
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
	}
	return nil, false
}
