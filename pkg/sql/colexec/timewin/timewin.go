// Copyright 2022 Matrix Origin
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

package timewin

import (
	"bytes"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

const argName = "time_window"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": time window")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	arg.ctr = new(container)
	ctr := arg.ctr
	ctr.InitReceiver(proc, true)

	ctr.aggExe = make([]colexec.ExpressionExecutor, len(arg.Aggs))
	for i, ag := range arg.Aggs {
		if expressions := ag.GetArgExpressions(); len(expressions) > 0 {
			ctr.aggExe[i], err = colexec.NewExpressionExecutor(proc, expressions[0])
			if err != nil {
				return err
			}
		}
	}
	// ctr.aggVec = make([][]*vector.Vector, len(ap.Aggs))

	ctr.tsExe, err = colexec.NewExpressionExecutor(proc, arg.Ts)
	if err != nil {
		return err
	}

	ctr.status = initTag
	ctr.tsOid = types.T(arg.Ts.Typ.Id)
	ctr.group = -1

	ctr.colCnt = len(arg.Aggs)
	if arg.WStart {
		ctr.colCnt++
	}
	if arg.WEnd {
		ctr.colCnt++
	}

	switch ctr.tsOid {
	case types.T_date:
		ctr.calRes = calRes[types.Date]
		ctr.eval = eval[types.Date]
	case types.T_datetime:
		ctr.calRes = calRes[types.Datetime]
		ctr.eval = eval[types.Datetime]
	case types.T_time:
		ctr.calRes = calRes[types.Time]
		ctr.eval = eval[types.Time]
	case types.T_timestamp:
		ctr.calRes = calRes[types.Timestamp]
		ctr.eval = eval[types.Timestamp]
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	ctr := arg.ctr
	var err error
	var bat *batch.Batch

	result := vm.NewCallResult()
	for {

		switch ctr.status {
		case dataTag:
			bat, _, err = ctr.ReceiveFromAllRegs(anal)
			if err != nil {
				return result, err
			}
			if bat == nil {
				if ctr.cur == hasGrow {
					ctr.status = evalLastCur
					continue
				}
				result.Status = vm.ExecStop
				return result, nil
			}
			ctr.pushBatch(bat)
			if err = ctr.evalVecs(proc); err != nil {
				return result, err
			}

			ctr.curIdx = ctr.curIdx - ctr.preIdx
			ctr.bats = ctr.bats[ctr.preIdx:]
			ctr.tsVec = ctr.tsVec[ctr.preIdx:]
			ctr.aggVec = ctr.aggVec[ctr.preIdx:]
			ctr.preIdx = 0

			ctr.status = evalTag
		case initTag:
			bat, _, err = ctr.ReceiveFromAllRegs(anal)
			if err != nil {
				return result, err
			}
			if bat == nil {
				result.Batch = nil
				result.Status = vm.ExecStop
				return result, nil
			}
			ctr.pushBatch(bat)
			if err = ctr.evalVecs(proc); err != nil {
				return result, err
			}
			if err = ctr.firstWindow(arg, proc); err != nil {
				return result, err
			}
			ctr.aggs = make([]aggexec.AggFuncExec, len(arg.Aggs))
			for i, ag := range arg.Aggs {
				ctr.aggs[i] = aggexec.MakeAgg(proc, ag.GetAggID(), ag.IsDistinct(), arg.Types[i])
				if config := ag.GetExtraConfig(); config != nil {
					if err = ctr.aggs[i].SetExtraInformation(config, 0); err != nil {
						return result, err
					}
				}
			}
			ctr.status = evalTag
		case nextTag:
			if err = ctr.nextWindow(arg, proc); err != nil {
				return result, err
			}
			ctr.status = evalTag
		case evalTag:

			if err = ctr.eval(ctr, arg, proc); err != nil {
				return result, err
			}

		case resultTag:

			ctr.status = nextTag
			result.Batch = ctr.rbat
			return result, nil

		case evalLastCur:

			if err = ctr.calRes(ctr, arg, proc); err != nil {
				return result, err
			}
			if ctr.pre == hasPre {
				ctr.wstart = nil
				ctr.wend = nil
				ctr.status = evalLastPre
			} else {
				ctr.status = endTag
			}

			result.Batch = ctr.rbat
			return result, nil

		case evalLastPre:

			if err = ctr.nextWindow(arg, proc); err != nil {
				return result, err
			}
			ctr.aggs = make([]aggexec.AggFuncExec, len(arg.Aggs))
			for i, ag := range arg.Aggs {
				ctr.aggs[i] = aggexec.MakeAgg(proc, ag.GetAggID(), ag.IsDistinct(), arg.Types[i])
				if config := ag.GetExtraConfig(); config != nil {
					if err = ctr.aggs[i].SetExtraInformation(config, 0); err != nil {
						return result, err
					}
				}
			}
			ctr.wstart = append(ctr.wstart, ctr.start)
			ctr.wend = append(ctr.wend, ctr.end)
			for _, ag := range ctr.aggs {
				if err = ag.GroupGrow(1); err != nil {
					return result, err
				}
			}

			for i := ctr.preIdx; i < len(ctr.bats); i++ {
				for k := ctr.preRow; k < ctr.bats[i].RowCount(); k++ {
					for j, agg := range ctr.aggs {
						if err = agg.Fill(0, k, []*vector.Vector{ctr.aggVec[i][j]}); err != nil {
							return result, err
						}
					}
				}
				ctr.preRow = 0
			}

			if err = ctr.calRes(ctr, arg, proc); err != nil {
				return result, err
			}

			ctr.status = endTag
			result.Batch = ctr.rbat
			return result, nil

		case endTag:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}

	}
}

const maxTimeWindowRows = 8192

func eval[T constraints.Integer](ctr *container, ap *Argument, proc *process.Process) (err error) {
	end := T(ctr.end)
	ts := vector.MustFixedCol[T](ctr.tsVec[ctr.curIdx])
	for ; ctr.curRow < len(ts); ctr.curRow++ {
		if ts[ctr.curRow] >= T(ctr.nextStart) && ctr.pre == withoutPre {
			ctr.preRow = ctr.curRow
			ctr.preIdx = ctr.curIdx
			ctr.pre = hasPre
		}
		if ts[ctr.curRow] < end {
			if ctr.cur == withoutGrow {
				ctr.wstart = append(ctr.wstart, ctr.start)
				ctr.wend = append(ctr.wend, ctr.end)
				for _, ag := range ctr.aggs {
					if err = ag.GroupGrow(1); err != nil {
						return err
					}
				}
				ctr.cur = hasGrow
				ctr.group++
			}
			for j, agg := range ctr.aggs {
				if err = agg.Fill(ctr.group, ctr.curRow, []*vector.Vector{ctr.aggVec[ctr.curIdx][j]}); err != nil {
					return err
				}
			}
		} else {
			break
		}
	}

	if ctr.curRow < len(ts) {

		ctr.cur = withoutGrow
		ctr.pre = withoutPre
		ctr.curIdx = ctr.preIdx
		ctr.curRow = ctr.preRow

		if ctr.cur == hasGrow {
			if ctr.group > maxTimeWindowRows {
				if err = calRes[T](ctr, ap, proc); err != nil {
					return err
				}
				ctr.aggs = make([]aggexec.AggFuncExec, len(ap.Aggs))
				for i, ag := range ap.Aggs {
					ctr.aggs[i] = aggexec.MakeAgg(proc, ag.GetAggID(), ag.IsDistinct(), ap.Types[i])
					if config := ag.GetExtraConfig(); config != nil {
						if err = ctr.aggs[i].SetExtraInformation(config, 0); err != nil {
							return err
						}
					}
				}
				ctr.group = 0
				ctr.status = resultTag
				ctr.wstart = nil
				ctr.wend = nil
			}
		} else {
			ctr.status = nextTag
		}

	} else {
		ctr.curIdx++
		ctr.curRow = 0
		ctr.status = dataTag
	}

	return
}

func calRes[T constraints.Integer](ctr *container, ap *Argument, proc *process.Process) (err error) {
	ctr.rbat = batch.NewWithSize(ctr.colCnt)
	i := 0
	for _, agg := range ctr.aggs {
		vec, err := agg.Flush()
		if err != nil {
			return err
		}
		ctr.rbat.SetVector(int32(i), vec)
		i++
	}
	ctr.aggs = nil
	if ap.WStart {
		wstart := make([]T, len(ctr.wstart))
		for t, v := range ctr.wstart {
			wstart[t] = T(v)
		}
		vec := proc.GetVector(*ctr.tsTyp)
		err = vector.AppendFixedList(vec, wstart, nil, proc.Mp())
		if err != nil {
			return err
		}
		ctr.rbat.SetVector(int32(i), vec)
		i++
	}
	if ap.WEnd {
		wend := make([]T, len(ctr.wend))
		for t, v := range ctr.wend {
			wend[t] = T(v)
		}
		vec := proc.GetVector(*ctr.tsTyp)
		err = vector.AppendFixedList(vec, wend, nil, proc.Mp())
		if err != nil {
			return err
		}
		ctr.rbat.SetVector(int32(i), vec)
	}
	batch.SetLength(ctr.rbat, ctr.rbat.Vecs[0].Length())
	return nil
}

func (ctr *container) peekBatch(i int) *batch.Batch {
	return ctr.bats[i]
}

//func (ctr *container) popBatch() {
//	ctr.bats = ctr.bats[1:]
//}

func (ctr *container) pushBatch(bat *batch.Batch) {
	ctr.bats = append(ctr.bats, bat)
}

//func (ctr *container) fill(proc *process.Process) (err error) {
//	ctr.wstart = append(ctr.wstart, ctr.start)
//	ctr.wend = append(ctr.wend, ctr.end)
//	for _, ag := range ctr.aggs {
//		if err = ag.Grows(1, proc.Mp()); err != nil {
//			return err
//		}
//	}
//	for _, idx := range ctr.idxs {
//		for j, agg := range ctr.aggs {
//			if err = agg.Fill(int64(ctr.group), int64(idx), []*vector.Vector{ctr.aggVec[ctr.curIdx][j]}); err != nil {
//				return err
//			}
//		}
//	}
//
//	return nil
//}

func (ctr *container) evalVecs(proc *process.Process) error {
	vec, err := ctr.tsExe.Eval(proc, []*batch.Batch{ctr.peekBatch(ctr.curIdx)})
	if err != nil {
		return err
	}
	ctr.tsTyp = vec.GetType()
	ctr.tsVec = append(ctr.tsVec, vec)
	if err = ctr.evalAggVector(ctr.peekBatch(ctr.curIdx), proc); err != nil {
		return err
	}
	return nil
}

func (ctr *container) evalAggVector(bat *batch.Batch, proc *process.Process) error {
	vs := make([]*vector.Vector, len(ctr.aggExe))
	for i := range ctr.aggExe {
		if ctr.aggExe[i] != nil {
			vec, err := ctr.aggExe[i].Eval(proc, []*batch.Batch{bat})
			if err != nil {
				return err
			}
			vs[i] = vec
		}
	}
	ctr.aggVec = append(ctr.aggVec, vs)
	return nil
}

func (ctr *container) nextWindow(ap *Argument, proc *process.Process) error {
	m := ap.Interval
	if ap.Sliding != nil {
		m = ap.Sliding
	}
	switch ctr.tsOid {
	case types.T_date:
		ctr.start = ctr.nextStart

		nextStart, err := doDateAdd(types.Date(ctr.start), m.Val, m.Typ)
		if err != nil {
			return err
		}
		ctr.nextStart = int64(nextStart)

		end, err := doDateAdd(types.Date(ctr.start), ap.Interval.Val, ap.Interval.Typ)
		if err != nil {
			return err
		}
		ctr.end = int64(end)
	case types.T_datetime:
		ctr.start = ctr.nextStart

		nextStart, err := doDatetimeAdd(types.Datetime(ctr.start), m.Val, m.Typ)
		if err != nil {
			return err
		}
		ctr.nextStart = int64(nextStart)

		end, err := doDatetimeAdd(types.Datetime(ctr.start), ap.Interval.Val, ap.Interval.Typ)
		if err != nil {
			return err
		}
		ctr.end = int64(end)
	case types.T_time:
		ctr.start = ctr.nextStart

		nextStart, err := doTimeAdd(types.Time(ctr.start), m.Val, m.Typ)
		if err != nil {
			return err
		}
		ctr.nextStart = int64(nextStart)

		end, err := doTimeAdd(types.Time(ctr.start), ap.Interval.Val, ap.Interval.Typ)
		if err != nil {
			return err
		}
		ctr.end = int64(end)
	case types.T_timestamp:
		ctr.start = ctr.nextStart

		nextStart, err := doTimestampAdd(proc.SessionInfo.TimeZone, types.Timestamp(ctr.start), m.Val, m.Typ)
		if err != nil {
			return err
		}
		ctr.nextStart = int64(nextStart)

		end, err := doTimestampAdd(proc.SessionInfo.TimeZone, types.Timestamp(ctr.start), ap.Interval.Val, ap.Interval.Typ)
		if err != nil {
			return err
		}
		ctr.end = int64(end)
	}
	return nil
}

func (ctr *container) firstWindow(ap *Argument, proc *process.Process) (err error) {
	m := ap.Interval
	if ap.Sliding != nil {
		m = ap.Sliding
	}

	vec := ctr.tsVec[ctr.curIdx]
	switch ctr.tsOid {
	case types.T_date:
		ts := vector.MustFixedCol[types.Date](vec)[0]
		start, err := doDateSub(ts, ap.Interval.Val/2, ap.Interval.Typ)
		if err != nil {
			return err
		}

		end, err := doDateSub(start, ap.Interval.Val, ap.Interval.Typ)
		if err != nil {
			return err
		}

		ctr.start = int64(start)
		ctr.end = int64(end)
	case types.T_datetime:
		ts := vector.MustFixedCol[types.Datetime](vec)[0]
		start, err := doDatetimeSub(ts, ap.Interval.Val/2, ap.Interval.Typ)
		if err != nil {
			return err
		}

		end, err := doDatetimeSub(start, ap.Interval.Val, ap.Interval.Typ)
		if err != nil {
			return err
		}

		ctr.start = int64(start)
		ctr.end = int64(end)
	case types.T_time:
		ts := vector.MustFixedCol[types.Time](vec)[0]
		start, err := doTimeSub(ts, ap.Interval.Val/2, ap.Interval.Typ)
		if err != nil {
			return err
		}

		end, err := doTimeAdd(start, ap.Interval.Val, ap.Interval.Typ)
		if err != nil {
			return err
		}

		ctr.start = int64(start)
		ctr.end = int64(end)
	case types.T_timestamp:
		ts := vector.MustFixedCol[types.Timestamp](vec)[0]

		itv, err := doTimestampAdd(proc.SessionInfo.TimeZone, ts, ap.Interval.Val, ap.Interval.Typ)
		if err != nil {
			return err
		}
		sld, err := doTimestampAdd(proc.SessionInfo.TimeZone, ts, m.Val, m.Typ)
		if err != nil {
			return err
		}
		if sld > itv {
			return moerr.NewInvalidInput(proc.Ctx, "sliding value should be smaller than the interval value")
		}

		start, err := roundTimestamp(proc.SessionInfo.TimeZone, ts, ap.Interval.Val, ap.Interval.Typ, proc)
		if err != nil {
			return err
		}
		ctr.start = int64(start)

		nextStart, err := doTimestampAdd(proc.SessionInfo.TimeZone, start, m.Val, m.Typ)
		if err != nil {
			return err
		}
		ctr.nextStart = int64(nextStart)

		end, err := doTimestampAdd(proc.SessionInfo.TimeZone, start, ap.Interval.Val, ap.Interval.Typ)
		if err != nil {
			return err
		}
		ctr.end = int64(end)
	default:
		return moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("%s as ts in time window", vec.GetType().Oid.String()))
	}
	return nil
}

func doTimeAdd(start types.Time, diff int64, iTyp types.IntervalType) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(diff, iTyp)
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}

func doDatetimeAdd(start types.Datetime, diff int64, iTyp types.IntervalType) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(diff, iTyp, types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

func doDateAdd(start types.Date, diff int64, iTyp types.IntervalType) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(diff, iTyp, types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

func doTimestampAdd(loc *time.Location, start types.Timestamp, diff int64, iTyp types.IntervalType) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime(loc).AddInterval(diff, iTyp, types.DateTimeType)
	if success {
		return dt.ToTimestamp(loc), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
	}
}

func roundTimestamp(loc *time.Location, start types.Timestamp, diff int64, iTyp types.IntervalType, proc *process.Process) (types.Timestamp, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt := start.ToDatetime(loc)
	var num int64
	switch iTyp {
	//case types.MicroSecond:
	//	num = diff
	case types.Second:
		num = diff * types.MicroSecsPerSec
	case types.Minute:
		num = diff * types.SecsPerMinute * types.MicroSecsPerSec
	case types.Hour:
		num = diff * types.SecsPerHour * types.MicroSecsPerSec
	case types.Day:
		num = diff * types.SecsPerDay * types.MicroSecsPerSec
	default:
		return 0, moerr.NewNotSupported(proc.Ctx, "Time Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit")
	}
	ts := types.Datetime(int64(dt) - (int64(dt)+num)%num)
	return ts.ToTimestamp(loc), nil
}

func doDateSub(start types.Date, diff int64, iTyp types.IntervalType) (types.Date, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.ToDatetime().AddInterval(-diff, iTyp, types.DateType)
	if success {
		return dt.ToDate(), nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("date", "")
	}
}

func doDatetimeSub(start types.Datetime, diff int64, iTyp types.IntervalType) (types.Datetime, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	dt, success := start.AddInterval(-diff, iTyp, types.DateTimeType)
	if success {
		return dt, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "")
	}
}

//func doTimestampSub(loc *time.Location, start types.Timestamp, diff int64, iTyp types.IntervalType) (types.Timestamp, error) {
//	err := types.JudgeIntervalNumOverflow(diff, iTyp)
//	if err != nil {
//		return 0, err
//	}
//	dt, success := start.ToDatetime(loc).AddInterval(-diff, iTyp, types.DateTimeType)
//	if success {
//		return dt.ToTimestamp(loc), nil
//	} else {
//		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "")
//	}
//}

func doTimeSub(start types.Time, diff int64, iTyp types.IntervalType) (types.Time, error) {
	err := types.JudgeIntervalNumOverflow(diff, iTyp)
	if err != nil {
		return 0, err
	}
	t, success := start.AddInterval(-diff, iTyp)
	if success {
		return t, nil
	} else {
		return 0, moerr.NewOutOfRangeNoCtx("time", "")
	}
}
