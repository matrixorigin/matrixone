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
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func generateSeriesString(arg any, buf *bytes.Buffer) {
	buf.WriteString("generate_series")
}

func generateSeriesPrepare(_ *process.Process, arg *Argument) error {
	return nil
}

func generateSeriesCall(_ int, proc *process.Process, arg *Argument) (bool, error) {
	var (
		err                                               error
		startVec, endVec, stepVec, startVecTmp, endVecTmp *vector.Vector
		rbat                                              *batch.Batch
	)
	defer func() {
		if err != nil && rbat != nil {
			rbat.Clean(proc.Mp())
		}
		if startVec != nil {
			startVec.Free(proc.Mp())
		}
		if endVec != nil {
			endVec.Free(proc.Mp())
		}
		if stepVec != nil {
			stepVec.Free(proc.Mp())
		}
		if startVecTmp != nil {
			startVecTmp.Free(proc.Mp())
		}
		if endVecTmp != nil {
			endVecTmp.Free(proc.Mp())
		}
	}()
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	startVec, err = colexec.EvalExpr(bat, proc, arg.Args[0])
	if err != nil {
		return false, err
	}
	endVec, err = colexec.EvalExpr(bat, proc, arg.Args[1])
	if err != nil {
		return false, err
	}
	rbat = batch.New(false, arg.Attrs)
	rbat.Cnt = 1
	for i := range arg.Attrs {
		rbat.Vecs[i] = vector.NewVec(dupType(plan.MakePlan2Type(startVec.GetType())))
	}
	if len(arg.Args) == 3 {
		stepVec, err = colexec.EvalExpr(bat, proc, arg.Args[2])
		if err != nil {
			return false, err
		}
	}
	if !startVec.IsConst() || !endVec.IsConst() || (stepVec != nil && !stepVec.IsConst()) {
		return false, moerr.NewInvalidInput(proc.Ctx, "generate_series only support scalar")
	}
	switch startVec.GetType().Oid {
	case types.T_int32:
		if endVec.GetType().Oid != types.T_int32 || (stepVec != nil && stepVec.GetType().Oid != types.T_int32) {
			return false, moerr.NewInvalidInput(proc.Ctx, "generate_series arguments must be of the same type, type1: %s, type2: %s", startVec.GetType().Oid.String(), endVec.GetType().Oid.String())
		}
		err = handleInt(startVec, endVec, stepVec, generateInt32, false, proc, rbat)
		if err != nil {
			return false, err
		}
	case types.T_int64:
		if endVec.GetType().Oid != types.T_int64 || (stepVec != nil && stepVec.GetType().Oid != types.T_int64) {
			return false, moerr.NewInvalidInput(proc.Ctx, "generate_series arguments must be of the same type, type1: %s, type2: %s", startVec.GetType().Oid.String(), endVec.GetType().Oid.String())
		}
		err = handleInt(startVec, endVec, stepVec, generateInt64, false, proc, rbat)
		if err != nil {
			return false, err
		}
	case types.T_datetime:
		if endVec.GetType().Oid != types.T_datetime || (stepVec != nil && stepVec.GetType().Oid != types.T_varchar) {
			return false, moerr.NewInvalidInput(proc.Ctx, "generate_series arguments must be of the same type, type1: %s, type2: %s", startVec.GetType().Oid.String(), endVec.GetType().Oid.String())
		}
		err = handleDatetime(startVec, endVec, stepVec, false, proc, rbat)
	case types.T_varchar:
		if stepVec == nil {
			return false, moerr.NewInvalidInput(proc.Ctx, "generate_series must specify step")
		}
		startSlice := vector.MustStrCol(startVec)
		endSlice := vector.MustStrCol(endVec)
		stepSlice := vector.MustStrCol(stepVec)
		startStr := startSlice[0]
		endStr := endSlice[0]
		stepStr := stepSlice[0]
		scale := int32(findScale(startStr, endStr))
		rbat.Vecs[0].GetType().Scale = scale
		start, err := types.ParseDatetime(startStr, scale)
		if err != nil {
			err = tryInt(startStr, endStr, stepStr, proc, rbat)
			if err != nil {
				return false, err
			}
			break
		}

		end, err := types.ParseDatetime(endStr, scale)
		if err != nil {
			return false, err
		}
		startVecTmp = vector.NewConstFixed(types.T_datetime.ToType(), start, 1, proc.Mp())
		endVecTmp = vector.NewConstFixed(types.T_datetime.ToType(), end, 1, proc.Mp())

		err = handleDatetime(startVecTmp, endVecTmp, stepVec, true, proc, rbat)
		if err != nil {
			return false, err
		}

	default:
		return false, moerr.NewNotSupported(proc.Ctx, "generate_series not support type %s", startVec.GetType().Oid.String())

	}
	proc.SetInputBatch(rbat)
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

func generateDatetime(ctx context.Context, start, end types.Datetime, stepStr string, scale int32) ([]types.Datetime, error) {
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

func handleInt[T int32 | int64](startVec, endVec, stepVec *vector.Vector, genFn func(context.Context, T, T, T) ([]T, error), toString bool, proc *process.Process, rbat *batch.Batch) error {
	var (
		start, end, step T
	)
	startSlice := vector.MustFixedCol[T](startVec)
	endSlice := vector.MustFixedCol[T](endVec)
	start = startSlice[0]
	end = endSlice[0]
	if stepVec != nil {
		stepSlice := vector.MustFixedCol[T](stepVec)
		step = stepSlice[0]
	} else {
		if start < end {
			step = 1
		} else {
			step = -1
		}
	}
	res, err := genFn(proc.Ctx, start, end, step)
	if err != nil {
		return err
	}
	for i := range res {
		if toString {
			err = vector.AppendBytes(rbat.Vecs[0], []byte(strconv.FormatInt(int64(res[i]), 10)), false, proc.Mp())
		} else {
			err = vector.AppendFixed(rbat.Vecs[0], res[i], false, proc.Mp())
		}
		if err != nil {
			return err
		}
	}
	rbat.InitZsOne(len(res))
	return nil
}

func handleDatetime(startVec, endVec, stepVec *vector.Vector, toString bool, proc *process.Process, rbat *batch.Batch) error {
	var (
		start, end types.Datetime
		step       string
	)
	startSlice := vector.MustFixedCol[types.Datetime](startVec)
	endSlice := vector.MustFixedCol[types.Datetime](endVec)
	start = startSlice[0]
	end = endSlice[0]
	if stepVec == nil {
		return moerr.NewInvalidInput(proc.Ctx, "generate_series datetime must specify step")
	}
	stepSlice := vector.MustStrCol(stepVec)
	step = stepSlice[0]
	res, err := generateDatetime(proc.Ctx, start, end, step, startVec.GetType().Scale)
	if err != nil {
		return err
	}
	for i := range res {
		if toString {
			err = vector.AppendBytes(rbat.Vecs[0], []byte(res[i].String2(rbat.Vecs[0].GetType().Scale)), false, proc.Mp())
		} else {
			err = vector.AppendFixed(rbat.Vecs[0], res[i], false, proc.Mp())
		}
		if err != nil {
			return err
		}
	}
	rbat.InitZsOne(len(res))
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

func tryInt(startStr, endStr, stepStr string, proc *process.Process, rbat *batch.Batch) error {
	var (
		startVec, endVec, stepVec *vector.Vector
		err                       error
	)
	defer func() {
		if startVec != nil {
			startVec.Free(proc.Mp())
		}
		if endVec != nil {
			endVec.Free(proc.Mp())
		}
		if stepVec != nil {
			stepVec.Free(proc.Mp())
		}
	}()
	startInt, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil {
		return err
	}
	endInt, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil {
		return err
	}
	stepInt, err := strconv.ParseInt(stepStr, 10, 64)
	if err != nil {
		return err
	}

	startVec = vector.NewConstFixed(types.T_int64.ToType(), startInt, 1, proc.Mp())
	endVec = vector.NewConstFixed(types.T_int64.ToType(), endInt, 1, proc.Mp())
	stepVec = vector.NewConstFixed(types.T_int64.ToType(), stepInt, 1, proc.Mp())

	err = handleInt(startVec, endVec, stepVec, generateInt64, true, proc, rbat)
	if err != nil {
		return err
	}
	return nil
}
