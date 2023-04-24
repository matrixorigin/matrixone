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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math/rand"
)

type opBuiltInRand struct {
	seed *rand.Rand
}

func newOpBuiltInRand() *opBuiltInRand {
	return new(opBuiltInRand)
}

func builtInRand(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[float64](result)
	for i := uint64(0); i < uint64(length); i++ {
		v := rand.Float64()
		if err := rs.Append(v, false); err != nil {
			return err
		}
	}
	return nil
}

func (op *opBuiltInRand) builtInRand(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	if !parameters[0].IsConst() {
		return moerr.NewInvalidArg(proc.Ctx, "parameter of rand", "column")
	}
	if parameters[0].IsConstNull() {
		return moerr.NewInvalidArg(proc.Ctx, "parameter of rand", "null")
	}

	p1 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[0])
	rs := vector.MustFunctionResult[float64](result)

	seedNumber, _ := p1.GetValue(0)
	if op.seed == nil {
		op.seed = rand.New(rand.NewSource(seedNumber))
	}
	for i := uint64(0); i < uint64(length); i++ {
		v := op.seed.Float64()
		if err := rs.Append(v, false); err != nil {
			return err
		}
	}
	return nil
}
