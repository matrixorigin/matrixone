package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// builtInStrToDateDynamic handles a runtime format whose result domain cannot
// be known during prepare. It returns MySQL's textual temporal representation
// while preserving DATE/TIME/DATETIME shape at execution time.
func builtInStrToDateDynamic(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[types.Varlena](result)
	rs.TempSetType(types.New(types.T_varchar, 29, strToDateMaxFsp))
	tm := NewGeneralTime()
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		tm.ResetTime()
		if !coreStrToDate(proc.Ctx, tm, functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		isTime, isDate, scale := dynamicStrToDateFormatType(functionUtil.QuickBytesToStr(v2))
		var output string
		if isTime && !isDate && types.ValidTime(uint64(tm.hour), uint64(tm.minute), uint64(tm.second)) {
			output = types.TimeFromClock(false, uint64(tm.hour), tm.minute, tm.second, tm.microsecond).String2(int32(scale))
		} else if isDate && !isTime && types.ValidDate(int32(tm.year), tm.month, tm.day) {
			output = types.DateFromCalendar(int32(tm.year), tm.month, tm.day).String()
		} else if isDate && isTime && types.ValidDatetime(int32(tm.year), tm.month, tm.day) && types.ValidTimeInDay(tm.hour, tm.minute, tm.second) {
			output = types.DatetimeFromClock(int32(tm.year), tm.month, tm.day, tm.hour, tm.minute, tm.second, tm.microsecond).String2(int32(scale))
		} else {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if err := rs.AppendBytes(functionUtil.QuickStrToBytes(output), false); err != nil {
			return err
		}
	}
	return nil
}

func dynamicStrToDateFormatType(format string) (isTime, isDate bool, scale int) {
	for i := 0; i+1 < len(format); i++ {
		if format[i] != '%' {
			continue
		}
		switch format[i+1] {
		case 'h', 'H', 'i', 'I', 's', 'S', 'k', 'l', 'f', 'r', 'T':
			isTime = true
			if format[i+1] == 'f' {
				scale = strToDateMaxFsp
			}
		case 'y', 'Y', 'm', 'M', 'c', 'b', 'D', 'd', 'e':
			isDate = true
		}
	}
	return
}
