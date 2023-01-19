package multi

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
)

func trimBoth(src, cuts string) string {
	return trimLeading(trimTrailing(src, cuts), cuts)
}

func trimLeading(src, cuts string) string {
	for strings.HasPrefix(src, cuts) {
		src = src[len(cuts):]
	}
	return src
}

func trimTrailing(src, cuts string) string {
	for strings.HasSuffix(src, cuts) {
		src = src[:len(src)-len(cuts)]
	}
	return src
}

func Trim(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	tp := strings.ToLower(vector.MustStrCols(parameters[0])[0])
	switch tp {
	case "both":
		return trim(parameters[1:], result, length, trimBoth)
	case "leading":
		return trim(parameters[1:], result, length, trimLeading)
	case "trailing":
		return trim(parameters[1:], result, length, trimTrailing)
	default:
		return moerr.NewNotSupported(proc.Ctx, "trim", tp)
	}
}

func trim(parameters []*vector.Vector, result vector.FunctionResultWrapper, length int, trimFn func(string, string) string) error {
	cutsets := vector.GenerateFunctionStrParameter(parameters[0])
	origin := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		cutset, cIsNull := cutsets.GetStrValue(i)
		orig, oIsNull := origin.GetStrValue(i)
		if cIsNull || oIsNull {
			if err := rs.AppendStr(nil, true); err != nil {
				return err
			}
			continue
		}
		if err := rs.AppendStr([]byte(trimFn(string(orig), string(cutset))), false); err != nil {
			return err
		}
	}
	return nil
}
