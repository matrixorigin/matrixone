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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/neg"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func UnaryMinus[T constraints.Signed | constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVector := vectors[0]
	srcValues := srcVector.Col.([]T)
	resultElementSize := srcVector.Typ.Oid.FixedLength()

	// in this case , I don't think we need to distinguish whether the parameter is constant or not
	resVector, err := process.Get2(proc, int64(resultElementSize*len(srcValues)), srcVector.Typ)
	if err != nil {
		return nil, err
	}
	resValues := encoding.DecodeFixedSlice[T](resVector.Data)
	nulls.Set(resVector.Nsp, srcVector.Nsp)
	vector.SetCol(srcVector, neg.NumericNeg[T](srcValues, resValues))
	if srcVector.IsConst {
		resVector.IsConst = true
		resVector.Length = srcVector.Length
	}
	return resVector, nil
}

// There is no case where the access parameter is of bool type
func Not(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	srcVector := vectors[0]
	srcValues := srcVector.Col.([]types.Bool)
	resultElementSize := types.T_bool.FixedLength()

	// Apply for space for bool type results, which needs to be discussed here
	resVector, err := process.Get(proc, int64(resultElementSize*len(srcValues)), types.Type{
		Oid:  types.T_bool,
		Size: 1,
	})
	if err != nil {
		return nil, err
	}
	resValues := encoding.DecodeBoolSlice(resVector.Data)
	vector.SetCol(resVector, resValues)
	// When the input parameter is null, the return value should be false. This treatment may not be correct
	nulls.Set(srcVector.Nsp, resVector.Nsp)
	if srcVector.IsConst {
		resVector.IsConst = true
		resVector.Length = srcVector.Length
	}
	return resVector, nil
}
