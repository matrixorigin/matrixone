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
	"github.com/matrixorigin/matrixone/pkg/vectorize/oct"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func Oct[T constraints.Unsigned | constraints.Signed](ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.Type{Oid: types.T_decimal128, Size: 16}
	ivals := vector.MustFixedCol[T](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		var rvals [1]types.Decimal128
		_, err := oct.Oct(ivals, rvals[:])
		if err != nil {
			return nil, err
		}
		return vector.NewConstFixed(rtyp, rvals[0], ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[types.Decimal128](rvec)
		_, err = oct.Oct(ivals, rvals)
		if err != nil {
			return nil, err
		}
		return rvec, nil
	}
}

func OctFloat[T constraints.Float](ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.Type{Oid: types.T_decimal128, Size: 16}
	ivals := vector.MustFixedCol[T](inputVector)
	if inputVector.IsConst() {
		if inputVector.IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		var rvals [1]types.Decimal128
		_, err := oct.OctFloat(ivals, rvals[:])
		if err != nil {
			return nil, moerr.NewInternalError(proc.Ctx, "the input value is out of integer range")
		}
		return vector.NewConstFixed(rtyp, rvals[0], ivecs[0].Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), inputVector.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[types.Decimal128](rvec)
		_, err = oct.OctFloat(ivals, rvals)
		if err != nil {
			return nil, moerr.NewInternalError(proc.Ctx, "the input value is out of integer range")
		}
		return rvec, nil
	}
}
