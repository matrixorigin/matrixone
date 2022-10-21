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

package generate_series

import (
	"bytes"
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
	"strconv"
	"strings"
)

func String(arg any, buf *bytes.Buffer) {
	buf.WriteString("generate_series")
}

func Prepare(_ *process.Process, arg any) error {
	return nil
}

func Call(_ int, proc *process.Process, arg any) (bool, error) {
	var err error
	param := arg.(*Argument).Es
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	dt := bat.GetVector(0).GetBytes(0)
	var exArgs []string
	err = json.Unmarshal(dt, &exArgs)
	if err != nil {
		return false, err
	}
	rbat := batch.New(false, param.Attrs)
	rbat.Cnt = 1
	defer func() {
		if err != nil {
			rbat.Clean(proc.Mp())
		}
	}()
	for i := range param.Cols {
		rbat.Vecs[i] = vector.New(dupType(param.Cols[i].Typ))
	}
	if len(exArgs) == 2 {
		param.start, param.end = exArgs[0], exArgs[1]
	} else {
		param.start, param.end, param.step = exArgs[0], exArgs[1], exArgs[2]
	}
	switch param.Cols[0].Typ.Id {
	case int32(types.T_int32):
		var start, end, step int64
		start, err = strconv.ParseInt(param.start, 10, 32)
		if err != nil {
			return false, err
		}
		end, err = strconv.ParseInt(param.end, 10, 32)
		if err != nil {
			return false, err
		}
		if len(param.step) == 0 {
			step = 1
			if start > end {
				step = -1
			}
		} else {
			step, err = strconv.ParseInt(param.step, 10, 32)
			if err != nil {
				return false, err
			}
		}
		res, err := generateInt32(int32(start), int32(end), int32(step))
		if err != nil {
			return false, err
		}
		for i := range res {
			err = rbat.Vecs[0].Append(res[i], false, proc.Mp())
			if err != nil {
				return false, err
			}
		}
		rbat.InitZsOne(len(res))
	case int32(types.T_int64):
		var start, end, step int64
		start, err = strconv.ParseInt(param.start, 10, 64)
		if err != nil {
			return false, err
		}
		end, err = strconv.ParseInt(param.end, 10, 64)
		if err != nil {
			return false, err
		}
		if len(param.step) == 0 {
			step = 1
			if start > end {
				step = -1
			}
		} else {
			step, err = strconv.ParseInt(param.step, 10, 64)
			if err != nil {
				return false, err
			}
		}
		res, err := generateInt64(start, end, step)
		if err != nil {
			return false, err
		}
		for i := range res {
			err = rbat.Vecs[0].Append(res[i], false, proc.Mp())
			if err != nil {
				return false, err
			}
		}
		rbat.InitZsOne(len(res))
	case int32(types.T_datetime):
		res, err := generateDatetime(param.start, param.end, param.step, param.Cols[0].Typ.Precision)
		if err != nil {
			return false, err
		}
		for i := range res {
			err = rbat.Vecs[0].Append(res[i], false, proc.Mp())
			if err != nil {
				return false, err
			}
		}
		rbat.InitZsOne(len(res))
	}
	proc.SetInputBatch(rbat)
	return false, nil
}

func dupType(typ *plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale, typ.Precision)
}

func judgeArgs[T Number](start, end, step T) ([]T, error) {
	if step == 0 {
		return nil, moerr.NewInvalidInput("step size cannot equal zero")
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

func genStep(step string) (num int64, tp types.IntervalType, err error) {
	step = trimStep(step)
	s := strings.Split(step, " ")
	if len(s) != 2 {
		err = moerr.NewInvalidInput("invalid step '%s'", step)
		return
	}
	num, err = strconv.ParseInt(s[0], 10, 64)
	if err != nil {
		err = moerr.NewInvalidInput("invalid step '%s'", step)
		return
	}
	tp, err = types.IntervalTypeOf(s[1])
	return
}

func generateInt32(start, end, step int32) ([]int32, error) {
	res, err := judgeArgs(start, end, step)
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

func generateInt64(start, end, step int64) ([]int64, error) {
	res, err := judgeArgs(start, end, step)
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

func generateDatetime(startStr, endStr, stepStr string, precision int32) ([]types.Datetime, error) {
	start, err := types.ParseDatetime(startStr, precision)
	if err != nil {
		return nil, err
	}
	end, err := types.ParseDatetime(endStr, precision)
	if err != nil {
		return nil, err
	}
	step, tp, err := genStep(stepStr)
	if err != nil {
		return nil, err
	}
	var res []types.Datetime
	res, err = judgeArgs(start, end, types.Datetime(step)) // here, transfer step to types.Datetime may change the inner behavior of datetime, but we just care the sign of step.
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
				return nil, moerr.NewInvalidInput("invalid step '%s'", stepStr)
			}
		}
	} else {
		for i := start; i >= end; {
			res = append(res, i)
			var ok bool
			i, ok = i.AddInterval(step, tp, types.DateTimeType)
			if !ok {
				return nil, moerr.NewInvalidInput("invalid step '%s'", stepStr)
			}
		}
	}
	return res, nil
}
