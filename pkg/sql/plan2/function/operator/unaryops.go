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

	if srcVector.IsScalar() {
		if srcVector.IsScalarNull() {
			return proc.AllocScalarNullVector(srcVector.Typ), nil
		}
		resVector := proc.AllocScalarVector(srcVector.Typ)
		resValues := make([]T, 1)
		nulls.Set(resVector.Nsp, srcVector.Nsp)
		vector.SetCol(resVector, neg.NumericNeg(srcValues, resValues))
		return resVector, nil
	} else {
		resVector, err := proc.AllocVector(srcVector.Typ, int64(resultElementSize*len(srcValues)))
		if err != nil {
			return nil, err
		}
		resValues := encoding.DecodeFixedSlice[T](resVector.Data, resultElementSize)
		nulls.Set(resVector.Nsp, srcVector.Nsp)
		vector.SetCol(resVector, neg.NumericNeg(srcValues, resValues))
		return resVector, nil
	}
}
