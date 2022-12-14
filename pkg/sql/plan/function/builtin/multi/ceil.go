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
	"github.com/matrixorigin/matrixone/pkg/vectorize/ceil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func CeilUint64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalMathMulti("ceil", vecs, proc, ceil.CeilUint64)
}

func CeilInt64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalMathMulti("ceil", vecs, proc, ceil.CeilInt64)
}

func CeilFloat64(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalMathMulti("ceil", vecs, proc, ceil.CeilFloat64)
}

func CeilDecimal128(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	scale := vecs[0].GetType().Scale
	cb := func(vs []types.Decimal128, rs []types.Decimal128, digits int64) []types.Decimal128 {
		return ceil.CeilDecimal128(scale, vs, rs, digits)
	}
	return generalMathMulti("ceil", vecs, proc, cb)
}
