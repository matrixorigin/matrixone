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

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/bin"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Bin[T constraints.Unsigned | constraints.Signed](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalBin(vectors, proc, bin.Bin[T])
}

func BinFloat[T constraints.Float](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalBin(vectors, proc, bin.BinFloat[T])
}

type binT interface {
	constraints.Unsigned | constraints.Signed | constraints.Float
}

type binFun[T binT] func([]T, []string, *process.Process) error

func generalBin[T binT](ivecs []*vector.Vector, proc *process.Process, cb binFun[T]) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_varchar.ToType()
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		var rs [1]string
		err := cb(vector.MustFixedCol[T](inputVector), rs[:], proc)
		if err != nil {
			return nil, moerr.NewInvalidInput(proc.Ctx, "The input value is out of range")
		}
		return vector.NewConstBytes(rtyp, []byte(rs[0]), ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, 0, inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rs := make([]string, inputVector.Length())
		err = cb(vector.MustFixedCol[T](inputVector), rs, proc)
		if err != nil {
			return nil, moerr.NewInvalidInput(proc.Ctx, "The input value is out of range")
		}
		err = vector.AppendStringList(rvec, rs, nil, proc.Mp())
		if err != nil {
			return nil, moerr.NewInvalidInput(proc.Ctx, "The input value is out of range")
		}
		return rvec, nil
	}

}
