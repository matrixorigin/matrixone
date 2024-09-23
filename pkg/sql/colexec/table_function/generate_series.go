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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type genNumState[T int32 | int64] struct {
	start, end, step, next T
}

type genDatetimeState struct {
	start, end, next types.Datetime
	step             int64
	tp               types.IntervalType // used by handleDateTime
	scale            int32              // used by handleDateTime
}

type generateSeriesArg struct {
	i64State genNumState[int64]
	dtState  genDatetimeState

	// holding output batch of generate_series, we own it.
	batch *batch.Batch
}

func initStartAndEndNumNoTypeCheck[T int32 | int64](gs *genNumState[int64], proc *process.Process, startVec, endVec, stepVec *vector.Vector, nth int) error {
	if startVec == nil {
		gs.start = 1
	} else {
		gs.start = int64(vector.GetFixedAtNoTypeCheck[T](startVec, nth))
	}

	// end vec is always not null
	gs.end = int64(vector.GetFixedAtNoTypeCheck[T](endVec, nth))

	if stepVec == nil {
		if gs.start < gs.end {
			gs.step = 1
		} else {
			gs.step = -1
		}
	} else {
		gs.step = int64(vector.GetFixedAtNoTypeCheck[T](stepVec, nth))
	}
	if gs.step == 0 {
		return moerr.NewInvalidInput(proc.Ctx, "generate_series step cannot be zero")
	}
	gs.next = gs.start
	return nil
}

func initDateTimeStep(gs *genDatetimeState,
	proc *process.Process, stepVec *vector.Vector, nthRow int) error {
	var err error
	if stepVec == nil || stepVec.GetType().Oid != types.T_varchar {
		return moerr.NewInvalidInput(proc.Ctx, "generate_series datetime must specify step using varchar")
	}
	stepStr := stepVec.GetStringAt(nthRow)
	stepStr = strings.TrimSpace(stepStr)
	stepStr = strings.TrimSuffix(stepStr, "s")
	stepStr = strings.TrimSuffix(stepStr, "(s)")
	s := strings.Split(stepStr, " ")
	if len(s) != 2 {
		return moerr.NewInvalidInputf(proc.Ctx, "invalid step '%s'", stepStr)
	}
	gs.step, err = strconv.ParseInt(s[0], 10, 64)
	if err != nil {
		return err
	}
	if gs.step == 0 {
		return moerr.NewInvalidInput(proc.Ctx, "generate_series step cannot be zero")
	}
	gs.tp, err = types.IntervalTypeOf(s[1])
	return err
}

func initStartAndEndDatetime(gs *genDatetimeState,
	startVec, endVec *vector.Vector, nthRow int) error {
	gs.start = vector.GetFixedAtWithTypeCheck[types.Datetime](startVec, nthRow)
	gs.end = vector.GetFixedAtWithTypeCheck[types.Datetime](endVec, nthRow)
	gs.next = gs.start
	return nil
}

// init start, end, step with varchar types.
// XXX: varchar type is always converted to datetime.  we should for example, support
// timestamp time.
func initStartAndEndVarChar(gs *genDatetimeState,
	startVec, endVec *vector.Vector, nthRow int) error {
	var err error
	startStr := startVec.UnsafeGetStringAt(nthRow)
	endStr := endVec.UnsafeGetStringAt(nthRow)
	gs.scale = int32(findScale(startStr, endStr))
	gs.start, err = types.ParseDatetime(startStr, gs.scale)
	if err != nil {
		return err
	}
	gs.end, err = types.ParseDatetime(endStr, gs.scale)
	gs.next = gs.start
	return err
}

func generateSeriesPrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	st := new(generateSeriesArg)
	var err error
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	tableFunction.ctr.argVecs = make([]*vector.Vector, len(tableFunction.Args))
	return st, err
}

func (g *generateSeriesArg) reset(tf *TableFunction, proc *process.Process) {
	if g.batch != nil {
		g.batch.CleanOnlyData()
	}
}

func (g *generateSeriesArg) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if g.batch != nil {
		g.batch.Clean(proc.Mp())
	}
}

func (g *generateSeriesArg) start(tf *TableFunction, proc *process.Process, nthRow int) error {
	var err error
	var startVec, endVec, stepVec *vector.Vector
	// get result type, this should happen in parpare.
	// no matter how many args, the first arg is always the correct output type.
	if len(tf.ctr.executorsForArgs) == 1 {
		endVec = tf.ctr.argVecs[0]
	} else if len(tf.ctr.executorsForArgs) == 2 {
		startVec = tf.ctr.argVecs[0]
		endVec = tf.ctr.argVecs[1]
	} else {
		startVec = tf.ctr.argVecs[0]
		endVec = tf.ctr.argVecs[1]
		stepVec = tf.ctr.argVecs[2]
	}

	resTyp := tf.ctr.argVecs[0].GetType()
	switch resTyp.Oid {
	case types.T_int32:
		if err = initStartAndEndNumNoTypeCheck[int32](&g.i64State, proc, startVec, endVec, stepVec, nthRow); err != nil {
			return err
		}
	case types.T_int64:
		if err = initStartAndEndNumNoTypeCheck[int64](&g.i64State, proc, startVec, endVec, stepVec, nthRow); err != nil {
			return err
		}
	case types.T_datetime:
		if err = initDateTimeStep(&g.dtState, proc, stepVec, nthRow); err != nil {
			return err
		}
		// step has been handled, here pass in nil
		if err = initStartAndEndDatetime(&g.dtState, startVec, endVec, nthRow); err != nil {
			return err
		}
	case types.T_varchar:
		// call step first, then we know startVec and endVec are not nil
		if err = initDateTimeStep(&g.dtState, proc, stepVec, nthRow); err != nil {
			return err
		}
		// convert varchar to datetime
		if err = initStartAndEndVarChar(&g.dtState, startVec, endVec, nthRow); err != nil {
			return err
		}
		// reset schema
		typ := types.T_datetime.ToType()
		typ.Scale = g.dtState.scale
		tf.Rets[0].Typ = plan2.MakePlan2Type(&typ)
		tf.ctr.retSchema[0] = typ
	default:
		return moerr.NewNotSupportedf(proc.Ctx, "generate_series not support type %s", resTyp.Oid.String())
	}

	if g.batch == nil {
		g.batch = tf.createResultBatch()
	} else {
		g.batch.CleanOnlyData()
	}
	return nil
}

func buildNextNumBatch[T int32 | int64](g *genNumState[int64], rbat *batch.Batch, maxSz int, proc *process.Process) {
	cnt := 0
	for cnt = 0; cnt < maxSz; cnt++ {
		if (g.step > 0 && (g.next < g.start || g.next > g.end)) || (g.step < 0 && (g.next > g.start || g.next < g.end)) {
			break
		} else {
			vector.AppendFixed(rbat.Vecs[0], g.next, false, proc.Mp())
			g.next += g.step
		}
	}
	rbat.SetRowCount(cnt)
}

func buildNextDatetimeBatch(g *genDatetimeState, rbat *batch.Batch, maxSz int, proc *process.Process) error {
	var ok bool
	cnt := 0
	for cnt = 0; cnt < maxSz; cnt++ {
		if (g.step > 0 && (g.next < g.start || g.next > g.end)) || (g.step < 0 && (g.next > g.start || g.next < g.end)) {
			break
		}
		vector.AppendFixed(rbat.Vecs[0], g.next, false, proc.Mp())
		g.next, ok = g.next.AddInterval(g.step, g.tp, types.DateTimeType)
		if !ok {
			return moerr.NewInvalidInputf(proc.Ctx, "invalid step '%v %v'", g.step, g.tp)
		}
	}
	rbat.SetRowCount(cnt)
	return nil
}

func (g *generateSeriesArg) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	// clean up previous batch
	g.batch.CleanOnlyData()
	switch g.batch.Vecs[0].GetType().Oid {
	case types.T_int32:
		buildNextNumBatch[int32](&g.i64State, g.batch, 8192, proc)
	case types.T_int64:
		buildNextNumBatch[int64](&g.i64State, g.batch, 8192, proc)
	case types.T_datetime:
		if err := buildNextDatetimeBatch(&g.dtState, g.batch, 8192, proc); err != nil {
			return vm.CancelResult, err
		}
	case types.T_varchar:
		if err := buildNextDatetimeBatch(&g.dtState, g.batch, 8192, proc); err != nil {
			return vm.CancelResult, err
		}
	}
	if g.batch.RowCount() == 0 {
		// we are done
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: g.batch}, nil
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
