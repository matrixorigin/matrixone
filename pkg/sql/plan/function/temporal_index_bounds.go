// Copyright 2026 Matrix Origin
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

package function

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type temporalBoundValue interface {
	types.Datetime | types.Timestamp
}

func temporalDatetimeBound(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selections *FunctionSelectList) error {
	return temporalBoundEndpoint[types.Timestamp, types.Datetime](parameters, result, proc, length, selections, types.T_datetime)
}

func temporalTimestampBound(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selections *FunctionSelectList) error {
	return temporalBoundEndpoint[types.Datetime, types.Timestamp](parameters, result, proc, length, selections, types.T_timestamp)
}

func temporalBoundEndpoint[Input, Output temporalBoundValue](parameters []*vector.Vector, result vector.FunctionResultWrapper,
	proc *process.Process, length int, selections *FunctionSelectList, key types.T,
) error {
	values := vector.GenerateFunctionFixedTypeParameter[Input](parameters[0])
	ops := vector.GenerateFunctionFixedTypeParameter[int32](parameters[1])
	scales := vector.GenerateFunctionFixedTypeParameter[int32](parameters[2])
	selectors := vector.GenerateFunctionFixedTypeParameter[int32](parameters[3])
	permissions := vector.GenerateFunctionFixedTypeParameter[bool](parameters[4])
	out := vector.MustFunctionResult[Output](result)
	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selections, row) {
			if err := out.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if err := proc.Ctx.Err(); err != nil {
			return err
		}
		value, valueNull := values.GetValue(row)
		op, opNull := ops.GetValue(row)
		scale, scaleNull := scales.GetValue(row)
		selector, selectorNull := selectors.GetValue(row)
		permission, permissionNull := permissions.GetValue(row)
		candidate := types.TemporalCandidate{Lo: math.MinInt64, Hi: math.MaxInt64, Sentinel: math.MinInt64, Full: true}
		if !opNull && !scaleNull && !selectorNull && !permissionNull && permission && op >= 0 && op <= int32(types.TemporalGreaterEqual) && selector >= 0 && selector <= 2 {
			var err error
			candidate, err = types.BuildTemporalCandidate(proc.Ctx, key, scale, int64(value), valueNull, types.TemporalOp(op), proc.GetSessionInfo().TimeZone)
			if err != nil {
				return err
			}
		}
		selected := candidate.Lo
		if selector == 1 {
			selected = candidate.Hi
		}
		if selector == 2 {
			selected = candidate.Sentinel
		}
		if err := out.Append(Output(selected), false); err != nil {
			return err
		}
	}
	return nil
}

func temporalTimestampBoundNotEmpty(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selections *FunctionSelectList) error {
	values := vector.GenerateFunctionFixedTypeParameter[types.Timestamp](parameters[0])
	ops := vector.GenerateFunctionFixedTypeParameter[int32](parameters[1])
	scales := vector.GenerateFunctionFixedTypeParameter[int32](parameters[2])
	out := vector.MustFunctionResult[bool](result)
	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selections, row) {
			if err := out.Append(false, true); err != nil {
				return err
			}
			continue
		}
		value, isNull := values.GetValue(row)
		op, opNull := ops.GetValue(row)
		scale, scaleNull := scales.GetValue(row)
		notEmpty := !isNull
		if notEmpty && !opNull && !scaleNull {
			notEmpty = TemporalTimestampBoundNotEmpty(value, op, scale)
		}
		if err := out.Append(notEmpty, false); err != nil {
			return err
		}
	}
	return nil
}

func temporalDatetimeBoundNotEmpty(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selections *FunctionSelectList) error {
	values := vector.GenerateFunctionFixedTypeParameter[types.Datetime](parameters[0])
	out := vector.MustFunctionResult[bool](result)
	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selections, row) {
			if err := out.Append(false, true); err != nil {
				return err
			}
			continue
		}
		_, isNull := values.GetValue(row)
		if err := out.Append(!isNull, false); err != nil {
			return err
		}
	}
	return nil
}

// TemporalTimestampBoundNotEmpty is the non-NULL scalar part of the runtime
// guard. It is distinct from BuildTemporalCandidate.Empty: bound overflow may
// conservatively produce Full even when equality is off the timestamp grid.
func TemporalTimestampBoundNotEmpty(value types.Timestamp, op, scale int32) bool {
	if op == int32(types.TemporalEqual) && scale >= 0 && scale < 6 {
		q := int64(1)
		for i := scale; i < 6; i++ {
			q *= 10
		}
		return int64(value) <= q || int64(value)%q == 0
	}
	return true
}
