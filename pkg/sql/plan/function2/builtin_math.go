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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/momath"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
)

func builtInSin(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Sin(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInSinh(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Sinh(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInCos(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Cos(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInCot(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Cot(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInTan(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Tan(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInExp(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Exp(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInACos(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Acos(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInATan(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Atan(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInATan2(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[1])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			if v1 == 0 {
				return moerr.NewInvalidArg(proc.Ctx, "Atan first input", 0)
			}
			if err := rs.Append(math.Atan(v2/v1), false); err != nil {
				return err
			}
		}
	}
	return nil
}

func builtInLn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[float64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v, null := p1.GetValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			sinValue, err := momath.Ln(v)
			if err != nil {
				return err
			}
			if err = rs.Append(sinValue, false); err != nil {
				return err
			}
		}
	}
	return nil
}
