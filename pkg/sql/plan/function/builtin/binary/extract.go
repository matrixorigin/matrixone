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

package binary

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/extract"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

/*
// when implicit cast from varchar to date is ready, get rid of this
func ExtractFromString(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	rtyp := types.T_uint32.ToType()
	resultElementSize := int(rtyp.Size)
	switch {
	case left.IsConst() && right.IsConst():
		if left.IsConstNull() || right.IsConstNull() {
			return proc.AllocScalarNullVector(rtyp), nil
		}
		leftValues, rightValues := left.Col.(*types.Bytes), right.Col.(*types.Bytes)
		rvec := vector.NewConst(rtyp)
		rvals := make([]uint32, 1)
		unit := string(leftValues.Data)
		inputDate, err := types.ParseDateCast(string(rightValues.Get(0)))
		if err != nil {
			return nil, moerr.NewInternalError("invalid input")
		}
		rvals, err = extract.ExtractFromDate(unit, []types.Date{inputDate}, rvals)
		if err != nil {
			return nil, moerr.NewInternalError("invalid input")
		}
		vector.SetCol(rvec, rvals)
		return rvec, nil
	case left.IsConst() && !right.IsConst():
		if left.IsConstNull() {
			return proc.AllocScalarNullVector(rtyp), nil
		}
		leftValues, rightValues := left.Col.(*types.Bytes), right.Col.(*types.Bytes)
		unit := string(leftValues.Data)
		rvals, err := proc.AllocVector(rtyp, int64(resultElementSize) * int64(len(rightValues.Lengths)))
		if

		result, resultNsp, err := extract.ExtractFromInputBytes(unit, rightValues, right.GetNulls(), )

	default:
		return nil, moerr.NewInternalError("invalid input")
	}
}
*/

func ExtractFromDate(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	rtyp := types.T_uint32.ToType()
	leftValues, rightValues := vector.MustStrCol(left), vector.MustFixedCol[types.Date](right)
	switch {
	case left.IsConstNull() || right.IsConstNull():
		return vector.NewConstNull(rtyp, left.Length(), proc.Mp()), nil
	case left.IsConst() && right.IsConst():
		var rvals [1]uint32
		unit := leftValues[0]
		_, err := extract.ExtractFromDate(unit, rightValues, rvals[:])
		if err != nil {
			return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
		}
		return vector.NewConstFixed(rtyp, rvals[0], left.Length(), proc.Mp()), nil
	case left.IsConst() && !right.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, len(rightValues), nil)
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[uint32](rvec)
		unit := leftValues[0]
		_, err = extract.ExtractFromDate(unit, rightValues, rvals)
		if err != nil {
			return nil, err
		}
		nulls.Or(left.GetNulls(), right.GetNulls(), rvec.GetNulls())
		return rvec, nil
	default:
		return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
	}
}

func ExtractFromDatetime(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	rtyp := types.T_varchar.ToType()
	leftValues, rightValues := vector.MustStrCol(left), vector.MustFixedCol[types.Datetime](right)
	switch {
	case left.IsConstNull() || right.IsConstNull():
		return vector.NewConstNull(rtyp, left.Length(), proc.Mp()), nil
	case left.IsConst() && right.IsConst():
		var rvals [1]string
		unit := leftValues[0]
		_, err := extract.ExtractFromDatetime(unit, rightValues, rvals[:])
		if err != nil {
			return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
		}
		return vector.NewConstBytes(rtyp, []byte(rvals[0]), left.Length(), proc.Mp()), nil
	case left.IsConst() && !right.IsConst():
		rvals := make([]string, len(rightValues))
		unit := leftValues[0]
		rvals, err := extract.ExtractFromDatetime(unit, rightValues, rvals)
		if err != nil {
			return nil, err
		}
		rvec := vector.NewVec(rtyp)
		vector.AppendStringList(rvec, rvals, nil, proc.Mp())
		nulls.Or(left.GetNulls(), right.GetNulls(), rvec.GetNulls())
		return rvec, nil
	default:
		return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
	}
}

func ExtractFromTime(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	rtyp := types.T_varchar.ToType()
	leftValues, rightValues := vector.MustStrCol(left), vector.MustFixedCol[types.Time](right)
	switch {
	case left.IsConstNull() || right.IsConstNull():
		return vector.NewConstNull(rtyp, left.Length(), proc.Mp()), nil
	case left.IsConst() && right.IsConst():
		var rvals [1]string
		unit := leftValues[0]
		_, err := extract.ExtractFromTime(unit, rightValues, rvals[:])
		if err != nil {
			return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
		}
		return vector.NewConstBytes(rtyp, []byte(rvals[0]), left.Length(), proc.Mp()), nil
	case left.IsConst() && !right.IsConst():
		rvals := make([]string, len(rightValues))
		unit := leftValues[0]
		rvals, err := extract.ExtractFromTime(unit, rightValues, rvals)
		if err != nil {
			return nil, err
		}
		rvec := vector.NewVec(rtyp)
		vector.AppendStringList(rvec, rvals, nil, proc.Mp())
		nulls.Or(left.GetNulls(), right.GetNulls(), rvec.GetNulls())
		return rvec, nil
	default:
		return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
	}
}

func ExtractFromVarchar(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	rtyp := types.T_varchar.ToType()
	leftValues, rightValues := vector.MustStrCol(left), vector.MustStrCol(right)
	switch {
	case left.IsConstNull() || right.IsConstNull():
		return vector.NewConstNull(rtyp, left.Length(), proc.Mp()), nil
	case left.IsConst() && right.IsConst():
		var rvals [1]string
		unit := leftValues[0]
		_, err := extract.ExtractFromString(unit, rightValues, rvals[:], vectors[1].GetType().Scale)
		if err != nil {
			return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
		}
		return vector.NewConstBytes(rtyp, []byte(rvals[0]), left.Length(), proc.Mp()), nil
	case left.IsConst() && !right.IsConst():
		rvals := make([]string, len(rightValues))
		unit := leftValues[0]
		rvals, err := extract.ExtractFromString(unit, rightValues, rvals, vectors[1].GetType().Scale)
		if err != nil {
			return nil, err
		}
		rvec := vector.NewVec(rtyp)
		vector.AppendStringList(rvec, rvals, nil, proc.Mp())
		nulls.Or(left.GetNulls(), right.GetNulls(), rvec.GetNulls())
		return rvec, nil
	default:
		return nil, moerr.NewInternalError(proc.Ctx, "invalid input")
	}
}
