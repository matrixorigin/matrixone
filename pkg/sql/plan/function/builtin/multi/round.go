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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/round"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RoundUint64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalMathMulti("round", vecs, proc, round.RoundUint64)
}

func RoundInt64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalMathMulti("round", vecs, proc, round.RoundInt64)
}

func RoundFloat64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalMathMulti("round", vecs, proc, round.RoundFloat64)
}

func RoundDecimal64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	scale := vecs[0].GetType().Scale
	cb := func(vs []types.Decimal64, rs []types.Decimal64, digits int64) []types.Decimal64 {
		return round.RoundDecimal64(vs, rs, digits, scale)
	}
	return generalMathMulti("round", vecs, proc, cb)
}

func RoundDecimal128(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	scale := vecs[0].GetType().Scale
	cb := func(vs []types.Decimal128, rs []types.Decimal128, digits int64) []types.Decimal128 {
		return round.RoundDecimal128(vs, rs, digits, scale)
	}
	return generalMathMulti("round", vecs, proc, cb)
}
