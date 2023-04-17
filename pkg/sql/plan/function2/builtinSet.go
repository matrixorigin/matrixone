// Copyright 2021 - 2022 Matrix Origin
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

package function2

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func builtInDateDiff(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length uint64) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Date](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Date](parameters[1])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if err := rs.Append(int64(v1-v2), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInCurrentTimestamp(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Timestamp](result)

	resultValue := types.UnixNanoToTimestamp(proc.UnixTime)
	for i := uint64(0); i < uint64(length); i++ {
		if err := rs.Append(resultValue, false); err != nil {
			return err
		}
	}

	return nil
}

const (
	onUpdateExpr = iota
	defaultExpr
	typNormal
	typWithLen
)

func builtInMoShowVisibleBin(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[uint8](parameters[1])

	tp, null := p2.GetValue(0)
	if null {
		return moerr.NewNotSupported(proc.Ctx, "show visible bin, the second argument must be in [0, 3], but got NULL")
	}
	if tp > 3 {
		return moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("show visible bin, the second argument must be in [0, 3], but got %d", tp))
	}

	var f func(s []byte) ([]byte, error)
	rs := vector.MustFunctionResult[types.Varlena](result)
	switch tp {
	case onUpdateExpr:
		f = func(s []byte) ([]byte, error) {
			update := new(plan.OnUpdate)
			err := types.Decode(s, update)
			if err != nil {
				return nil, err
			}
			return function2Util.QuickStrToBytes(update.OriginString), nil
		}
	case defaultExpr:
		f = func(s []byte) ([]byte, error) {
			def := new(plan.Default)
			err := types.Decode(s, def)
			if err != nil {
				return nil, err
			}
			return function2Util.QuickStrToBytes(def.OriginString), nil
		}
	case typNormal:
		f = func(s []byte) ([]byte, error) {
			typ := new(types.Type)
			err := types.Decode(s, typ)
			if err != nil {
				return nil, err
			}
			return function2Util.QuickStrToBytes(typ.String()), nil
		}
	case typWithLen:
		f = func(s []byte) ([]byte, error) {
			typ := new(types.Type)
			err := types.Decode(s, typ)
			if err != nil {
				return nil, err
			}
			ret := fmt.Sprintf("%s(%d)", typ.String(), typ.Width)
			return function2Util.QuickStrToBytes(ret), nil
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		if null1 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		} else {
			if len(v1) == 0 {
				if err := rs.AppendBytes([]byte{}, false); err != nil {
					return err
				}
			} else {
				b, err := f(v1)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(b, false); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func builtInInternalCharLength(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsMySQLString() {
				if err := rs.Append(int64(typ.Width), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalCharSize(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsMySQLString() {
				if err := rs.Append(int64(typ.GetSize()*typ.Width), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalNumericPrecision(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsDecimal() {
				if err := rs.Append(int64(typ.Width), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalNumericScale(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid.IsDecimal() {
				if err := rs.Append(int64(typ.Scale), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalDatetimeScale(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid == types.T_datetime {
				if err := rs.Append(int64(typ.Scale), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInInternalCharacterSet(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[int64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if !null {
			typ := types.Type{}
			if err := types.Decode(v, &typ); err != nil {
				return err
			}
			if typ.Oid == types.T_varchar || typ.Oid == types.T_char ||
				typ.Oid == types.T_blob || typ.Oid == types.T_text {
				if err := rs.Append(int64(typ.Scale), false); err != nil {
					return err
				}
				continue
			}
		}
		if err := rs.Append(0, true); err != nil {
			return err
		}
	}
	return nil
}

func builtInConcatCheck(overloads []overload, inputs []types.Type) checkResult {
	if len(inputs) > 1 {
		shouldCast := false

		ret := make([]types.Type, len(inputs))
		for i, source := range inputs {
			if !source.Oid.IsMySQLString() {
				c, _ := tryToMatch([]types.Type{source}, []types.T{types.T_varchar})
				if c == matchFailed {
					return newCheckResultWithFailure(failedFunctionParametersWrong)
				}
				if c == matchByCast {
					shouldCast = true
					ret[i] = types.T_varchar.ToType()
				}
			} else {
				ret[i] = source
			}
		}
		if shouldCast {
			return newCheckResultWithCast(0, ret)
		}
		return newCheckResultWithSuccess(0)
	}
	return newCheckResultWithFailure(failedFunctionParametersWrong)
}

func builtInConcat(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)
	ps := make([]vector.FunctionParameterWrapper[types.Varlena], len(parameters))
	for i := range ps {
		ps[i] = vector.GenerateFunctionStrParameter(parameters[i])
	}

	for i := uint64(0); i < uint64(length); i++ {
		vs := make([]byte, 16)
		apv := true

		for _, p := range ps {
			v, null := p.GetStrValue(i)
			if null {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				apv = false
				break
			} else {
				vs = append(vs, v...)
			}
		}
		if apv {
			if err := rs.AppendBytes(vs, false); err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	ZeroDate   = "0001-01-01"
	formatMask = "%Y/%m/%d"
	regexpMask = `\d{1,4}/\d{1,2}/\d{1,2}`
)

// MOLogDate parse 'YYYY/MM/DD' date from input string.
// return '0001-01-01' if input string not container 'YYYY/MM/DD' substr, until DateParse Function support return NULL for invalid date string.
func builtInMoLogDate(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Date](result)
	p1 := vector.GenerateFunctionStrParameter(parameters[0])

	container := function2Util.NewRegContainer()
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		}
		expr := function2Util.QuickBytesToStr(v)
		match, parsedInput, err := container.RegularSubstr(expr, regexpMask, 1, 1)
		if err != nil {
			return err
		}
		if !match {
			if err = rs.Append(0, true); err != nil {
				return err
			}
		}
		val, err := types.ParseDateCast(parsedInput)
		if err != nil {
			return err
		}
		if err = rs.Append(val, false); err != nil {
			return err
		}
	}

	return nil
}

func builtInRegexpSubstr(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[types.Varlena](result)
	container := function2Util.NewRegContainer()
	switch len(parameters) {
	case 2:
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := function2Util.QuickBytesToStr(v1), function2Util.QuickBytesToStr(v2)
				match, res, err := container.RegularSubstr(expr, pat, 1, 1)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(function2Util.QuickStrToBytes(res), !match); err != nil {
					return err
				}
			}
		}

	case 3:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			if null1 || null2 || null3 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := function2Util.QuickBytesToStr(v1), function2Util.QuickBytesToStr(v2)
				match, res, err := container.RegularSubstr(expr, pat, pos, 1)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(function2Util.QuickStrToBytes(res), !match); err != nil {
					return err
				}
			}
		}

	case 4:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		occurrences := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			ocur, null4 := occurrences.GetValue(i)
			if null1 || null2 || null3 || null4 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := function2Util.QuickBytesToStr(v1), function2Util.QuickBytesToStr(v2)
				match, res, err := container.RegularSubstr(expr, pat, pos, ocur)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(function2Util.QuickStrToBytes(res), !match); err != nil {
					return err
				}
			}
		}
		return nil

	}
	return nil
}

func builtInDatabase(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := uint64(0); i < uint64(length); i++ {
		db := proc.SessionInfo.GetDatabase()
		if err := rs.AppendBytes(function2Util.QuickStrToBytes(db), false); err != nil {
			return err
		}
	}
	return nil
}
