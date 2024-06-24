// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const addBatchSize int64 = 8191

func generateSeriesString(buf *bytes.Buffer) {
	buf.WriteString("generate_series")
}

func generateSeriesPrepare(proc *process.Process, arg *Argument) (err error) {
	arg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, arg.Args)
	arg.ctr.generateSeries = new(generateSeriesArg)
	return err
}

func resetGenerateSeriesState(proc *process.Process, arg *Argument) error {
	if arg.ctr.generateSeries.state == initArg {
		var startVec, endVec, stepVec, startVecTmp, endVecTmp *vector.Vector
		var err error
		arg.ctr.generateSeries.state = genBatch

		defer func() {
			if startVecTmp != nil {
				startVecTmp.Free(proc.Mp())
			}
			if endVecTmp != nil {
				endVecTmp.Free(proc.Mp())
			}
		}()

		if len(arg.ctr.executorsForArgs) == 1 {
			endVec, err = arg.ctr.executorsForArgs[0].Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			if err != nil {
				return err
			}
			startVec, err = vector.NewConstFixed(types.T_int64.ToType(), int64(1), 1, proc.Mp())
		} else {
			startVec, err = arg.ctr.executorsForArgs[0].Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			if err != nil {
				return err
			}
			endVec, err = arg.ctr.executorsForArgs[1].Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		}
		if err != nil {
			return err
		}
		if len(arg.Args) == 3 {
			stepVec, err = arg.ctr.executorsForArgs[2].Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			if err != nil {
				return err
			}
		}
		if !startVec.IsConst() || !endVec.IsConst() || (stepVec != nil && !stepVec.IsConst()) {
			return moerr.NewInvalidInput(proc.Ctx, "generate_series only support scalar")
		}
		arg.ctr.generateSeries.startVecType = startVec.GetType()
		switch arg.ctr.generateSeries.startVecType.Oid {
		case types.T_int32:
			if endVec.GetType().Oid != types.T_int32 || (stepVec != nil && stepVec.GetType().Oid != types.T_int32) {
				return moerr.NewInvalidInput(proc.Ctx, "generate_series arguments must be of the same type, type1: %s, type2: %s", startVec.GetType().Oid.String(), endVec.GetType().Oid.String())
			}
			initStartAndEnd[int32](arg, startVec, endVec, stepVec)
		case types.T_int64:
			if endVec.GetType().Oid != types.T_int64 || (stepVec != nil && stepVec.GetType().Oid != types.T_int64) {
				return moerr.NewInvalidInput(proc.Ctx, "generate_series arguments must be of the same type, type1: %s, type2: %s", startVec.GetType().Oid.String(), endVec.GetType().Oid.String())
			}
			initStartAndEnd[int64](arg, startVec, endVec, stepVec)
		case types.T_datetime:
			if endVec.GetType().Oid != types.T_datetime || (stepVec != nil && stepVec.GetType().Oid != types.T_varchar) {
				return moerr.NewInvalidInput(proc.Ctx, "generate_series arguments must be of the same type, type1: %s, type2: %s", startVec.GetType().Oid.String(), endVec.GetType().Oid.String())
			}
			startSlice := vector.MustFixedCol[types.Datetime](startVec)
			endSlice := vector.MustFixedCol[types.Datetime](endVec)
			arg.ctr.generateSeries.start = startSlice[0]
			arg.ctr.generateSeries.end = endSlice[0]
			arg.ctr.generateSeries.last = endSlice[0]
			if stepVec == nil {
				return moerr.NewInvalidInput(proc.Ctx, "generate_series datetime must specify step")
			}
			arg.ctr.generateSeries.step = stepVec.GetStringAt(0)
		case types.T_varchar:
			if stepVec == nil {
				return moerr.NewInvalidInput(proc.Ctx, "generate_series must specify step")
			}
			startStr := startVec.GetStringAt(0)
			endStr := endVec.GetStringAt(0)
			scale := int32(findScale(startStr, endStr))
			startTmp, err := types.ParseDatetime(startStr, scale)
			if err != nil {
				return err
			}

			endTmp, err := types.ParseDatetime(endStr, scale)
			if err != nil {
				return err
			}
			if startVecTmp, err = vector.NewConstFixed(types.T_datetime.ToType(), startTmp, 1, proc.Mp()); err != nil {
				return err
			}
			if endVecTmp, err = vector.NewConstFixed(types.T_datetime.ToType(), endTmp, 1, proc.Mp()); err != nil {
				return err
			}

			newStartSlice := vector.MustFixedCol[types.Datetime](startVecTmp)
			newEndSlice := vector.MustFixedCol[types.Datetime](endVecTmp)
			arg.ctr.generateSeries.scale = scale
			arg.ctr.generateSeries.start = newStartSlice[0]
			arg.ctr.generateSeries.end = newEndSlice[0]
			arg.ctr.generateSeries.last = newEndSlice[0]
			arg.ctr.generateSeries.step = stepVec.GetStringAt(0)
		default:
			return moerr.NewNotSupported(proc.Ctx, "generate_series not support type %s", arg.ctr.generateSeries.startVecType.Oid.String())

		}
	}

	if arg.ctr.generateSeries.state == genBatch {
		switch arg.ctr.generateSeries.startVecType.Oid {
		case types.T_int32:
			computeNewStartAndEnd[int32](arg)
		case types.T_int64:
			computeNewStartAndEnd[int64](arg)
		case types.T_varchar, types.T_datetime:
			//todo split datetime batch
			arg.ctr.generateSeries.state = genFinish
		default:
			arg.ctr.generateSeries.state = genFinish
		}
	}

	return nil
}

func generateSeriesCall(_ int, proc *process.Process, arg *Argument, result *vm.CallResult) (bool, error) {
	var (
		err  error
		rbat *batch.Batch
	)
	defer func() {
		if err != nil && rbat != nil {
			rbat.Clean(proc.Mp())
		}
	}()

	if arg.ctr.generateSeries.state == genFinish {
		return true, nil
	}

	err = resetGenerateSeriesState(proc, arg)
	if err != nil {
		return false, err
	}

	rbat = batch.NewWithSize(len(arg.Attrs))
	rbat.Attrs = arg.Attrs
	for i := range arg.Attrs {
		rbat.Vecs[i] = proc.GetVector(arg.ctr.retSchema[i])
	}

	switch arg.ctr.generateSeries.startVecType.Oid {
	case types.T_int32:
		start := arg.ctr.generateSeries.start.(int32)
		end := arg.ctr.generateSeries.end.(int32)
		step := arg.ctr.generateSeries.step.(int32)
		err = handleInt(start, end, step, generateInt32, proc, rbat)
		if err != nil {
			return false, err
		}
	case types.T_int64:
		start := arg.ctr.generateSeries.start.(int64)
		end := arg.ctr.generateSeries.end.(int64)
		step := arg.ctr.generateSeries.step.(int64)
		err = handleInt(start, end, step, generateInt64, proc, rbat)
		if err != nil {
			return false, err
		}
	case types.T_datetime:
		start := arg.ctr.generateSeries.start.(types.Datetime)
		end := arg.ctr.generateSeries.end.(types.Datetime)
		step := arg.ctr.generateSeries.step.(string)

		err = handleDatetime(start, end, step, -1, proc, rbat)
	case types.T_varchar:
		start := arg.ctr.generateSeries.start.(types.Datetime)
		end := arg.ctr.generateSeries.end.(types.Datetime)
		step := arg.ctr.generateSeries.step.(string)
		scale := arg.ctr.generateSeries.scale
		rbat.Vecs[0].GetType().Scale = scale

		err = handleDatetime(start, end, step, scale, proc, rbat)
		if err != nil {
			return false, err
		}

	default:
		return false, moerr.NewNotSupported(proc.Ctx, "generate_series not support type %s", arg.ctr.generateSeries.startVecType.Oid.String())

	}
	result.Batch = rbat
	return false, nil
}

func judgeArgs[T generateSeriesNumber](ctx context.Context, start, end, step T) ([]T, error) {
	if step == 0 {
		return nil, moerr.NewInvalidInput(ctx, "step size cannot equal zero")
	}
	if start == end {
		return []T{start}, nil
	}
	s1 := step > 0
	s2 := end > start
	if s1 != s2 {
		return []T{}, nil
	}
	return nil, nil
}

func initStartAndEnd[T generateSeriesNumber](arg *Argument, startVec, endVec, stepVec *vector.Vector) {
	startSlice := vector.MustFixedCol[T](startVec)
	endSlice := vector.MustFixedCol[T](endVec)
	start := startSlice[0]
	end := startSlice[0]
	last := endSlice[0]
	var step T
	if stepVec != nil {
		stepSlice := vector.MustFixedCol[T](stepVec)
		step = stepSlice[0]
	} else {
		if startSlice[0] < endSlice[0] {
			step = T(1)
		} else {
			step = T(-1)
		}
	}
	end = end - step

	arg.ctr.generateSeries.start = start
	arg.ctr.generateSeries.end = end
	arg.ctr.generateSeries.last = last
	arg.ctr.generateSeries.step = step
}

func computeNewStartAndEnd[T generateSeriesNumber](arg *Argument) {
	step := arg.ctr.generateSeries.step.(T)
	newStart := arg.ctr.generateSeries.end.(T) + step
	last := arg.ctr.generateSeries.last.(T)
	newEnd := newStart + step*T(addBatchSize)
	if step > 0 {
		if newEnd < newStart {
			newEnd = last
		} else {
			if newEnd > last {
				newEnd = last
			}
		}
	} else {
		if newEnd > newStart {
			newEnd = last
		} else {
			if newEnd < last {
				newEnd = last
			}
		}
	}
	if newEnd == last {
		arg.ctr.generateSeries.state = genFinish
	}
	arg.ctr.generateSeries.start = newStart
	arg.ctr.generateSeries.end = newEnd
}

func trimStep(step string) string {
	step = strings.TrimSpace(step)
	step = strings.TrimSuffix(step, "s")
	step = strings.TrimSuffix(step, "(s)")
	return step
}

func genStep(ctx context.Context, step string) (num int64, tp types.IntervalType, err error) {
	step = trimStep(step)
	s := strings.Split(step, " ")
	if len(s) != 2 {
		err = moerr.NewInvalidInput(ctx, "invalid step '%s'", step)
		return
	}
	num, err = strconv.ParseInt(s[0], 10, 64)
	if err != nil {
		err = moerr.NewInvalidInput(ctx, "invalid step '%s'", step)
		return
	}
	tp, err = types.IntervalTypeOf(s[1])
	return
}

func generateInt32(ctx context.Context, start, end, step int32) ([]int32, error) {
	res, err := judgeArgs(ctx, start, end, step)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return res, nil
	}
	if step > 0 {
		for i := start; i <= end; i += step {
			res = append(res, i)
			if i > 0 && math.MaxInt32-i < step {
				break
			}
		}
	} else {
		for i := start; i >= end; i += step {
			res = append(res, i)
			if i < 0 && math.MinInt32-i > step {
				break
			}
		}
	}
	return res, nil
}

func generateInt64(ctx context.Context, start, end, step int64) ([]int64, error) {
	res, err := judgeArgs(ctx, start, end, step)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return res, nil
	}
	if step > 0 {
		for i := start; i <= end; i += step {
			res = append(res, i)
			if i > 0 && math.MaxInt64-i < step {
				break
			}
		}
	} else {
		for i := start; i >= end; i += step {
			res = append(res, i)
			if i < 0 && math.MinInt64-i > step {
				break
			}
		}
	}
	return res, nil
}

func generateDatetime(ctx context.Context, start, end types.Datetime, stepStr string) ([]types.Datetime, error) {
	step, tp, err := genStep(ctx, stepStr)
	if err != nil {
		return nil, err
	}
	var res []types.Datetime
	res, err = judgeArgs(ctx, start, end, types.Datetime(step)) // here, transfer step to types.Datetime may change the inner behavior of datetime, but we just care the sign of step.
	if err != nil {
		return nil, err
	}
	if res != nil {
		return res, nil
	}
	if step > 0 {
		for i := start; i <= end; {
			res = append(res, i)
			var ok bool
			i, ok = i.AddInterval(step, tp, types.DateTimeType)
			if !ok {
				return nil, moerr.NewInvalidInput(ctx, "invalid step '%s'", stepStr)
			}
		}
	} else {
		for i := start; i >= end; {
			res = append(res, i)
			var ok bool
			i, ok = i.AddInterval(step, tp, types.DateTimeType)
			if !ok {
				return nil, moerr.NewInvalidInput(ctx, "invalid step '%s'", stepStr)
			}
		}
	}
	return res, nil
}

func handleInt[T int32 | int64](start, end, step T, genFn func(context.Context, T, T, T) ([]T, error), proc *process.Process, rbat *batch.Batch) error {
	res, err := genFn(proc.Ctx, start, end, step)
	if err != nil {
		return err
	}
	for i := range res {
		err = vector.AppendFixed(rbat.Vecs[0], res[i], false, proc.Mp())
		if err != nil {
			return err
		}
	}
	rbat.SetRowCount(len(res))
	return nil
}

func handleDatetime(start, end types.Datetime, step string, scale int32, proc *process.Process, rbat *batch.Batch) error {
	res, err := generateDatetime(proc.Ctx, start, end, step)
	if err != nil {
		return err
	}
	for i := range res {
		if scale >= 0 {
			err = vector.AppendBytes(rbat.Vecs[0], []byte(res[i].String2(scale)), false, proc.Mp())
		} else {
			err = vector.AppendFixed(rbat.Vecs[0], res[i], false, proc.Mp())
		}

		if err != nil {
			return err
		}
	}
	rbat.SetRowCount(len(res))
	return nil
}

func findScale(s1, s2 string) int {
	p1 := 0
	if strings.Contains(s1, ".") {
		p1 = len(s1) - strings.LastIndex(s1, ".")
	}
	p2 := 0
	if strings.Contains(s2, ".") {
		p2 = len(s2) - strings.LastIndex(s2, ".")
	}
	if p2 > p1 {
		p1 = p2
	}
	if p1 > 6 {
		p1 = 6
	}
	return p1
}
