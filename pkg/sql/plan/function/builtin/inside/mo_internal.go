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

package inside

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type typeFunc func(typ types.Type) (bool, int32)

// InternalCharSize Implementation of mo internal function 'internal_char_size'
func InternalCharSize(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalInternalType("internal_char_size", vectors, proc, getTypeCharSize)
}

// InternalCharLength Implementation of mo internal function 'internal_char_length'
func InternalCharLength(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalInternalType("internal_char_length", vectors, proc, getTypeCharLength)
}

// InternalNumericPrecision Implementation of mo internal function 'internal_numeric_precision'
func InternalNumericPrecision(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalInternalType("internal_numeric_precision", vectors, proc, getTypeNumericPrecision)
}

// InternalNumericScale Implementation of mo internal function 'internal_numeric_scale'
func InternalNumericScale(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalInternalType("internal_numeric_scale", vectors, proc, getTypeNumericScale)
}

// InternalDatetimeScale Implementation of mo internal function 'internal_datetime_scale'
func InternalDatetimeScale(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalInternalType("internal_datetime_scale", vectors, proc, getTypeDatetimeScale)
}

// InternalColumnCharacterSet  Implementation of mo internal function 'internal_column_character_set'
func InternalColumnCharacterSet(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return generalInternalType("internal_column_character_set", vectors, proc, getTypeCharacterSet)
}

// Mo General function for obtaining type information
func generalInternalType(funName string, ivecs []*vector.Vector, proc *process.Process, typefunc typeFunc) (*vector.Vector, error) {
	ivec := ivecs[0]
	rtyp := types.T_int64.ToType()
	ivals := vector.MustStrCol(ivec)
	if ivec.IsConst() {
		if ivec.IsConstNull() {
			return vector.NewConstNull(rtyp, ivec.Length(), proc.Mp()), nil
		} else {
			var typ types.Type
			bytes := []byte(ivals[0])
			err := types.Decode(bytes, &typ)
			if err != nil {
				return nil, err
			}
			isVaild, val := typefunc(typ)
			if isVaild {
				return vector.NewConstFixed(rtyp, int64(val), ivec.Length(), proc.Mp()), nil
			} else {
				return vector.NewConstNull(rtyp, ivec.Length(), proc.Mp()), nil
			}
		}
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, len(ivals), nil)
		nulls.Set(rvec.GetNulls(), ivec.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[int64](rvec)
		for i, typeValue := range ivals {
			if rvec.GetNulls().Contains(uint64(i)) {
				continue
			}
			var typ types.Type
			bytes := []byte(typeValue)
			err = types.Decode(bytes, &typ)
			if err != nil {
				return nil, err
			}
			isVaild, val := typefunc(typ)
			if isVaild {
				rvals[i] = int64(val)
			} else {
				nulls.Add(rvec.GetNulls(), uint64(i))
			}
		}
		return rvec, nil
	}
}

// 'internal_char_lengh' function operator
func getTypeCharLength(typ types.Type) (bool, int32) {
	if typ.Oid.IsMySQLString() {
		return true, typ.Width
	} else {
		return false, -1
	}
}

// 'internal_char_size' function operator
func getTypeCharSize(typ types.Type) (bool, int32) {
	if typ.Oid.IsMySQLString() {
		return true, typ.GetSize() * typ.Width
	} else {
		return false, -1
	}
}

// 'internal_numeric_precision' function operator
func getTypeNumericPrecision(typ types.Type) (bool, int32) {
	if typ.Oid.IsDecimal() {
		return true, typ.Width
	} else {
		return false, -1
	}
}

// 'internal_numeric_scale' function operator
func getTypeNumericScale(typ types.Type) (bool, int32) {
	if typ.Oid.IsDecimal() {
		return true, typ.Scale
	} else {
		return false, -1
	}
}

// 'internal_datetime_scale' function operator
func getTypeDatetimeScale(typ types.Type) (bool, int32) {
	if typ.Oid == types.T_datetime {
		return true, typ.Scale
	} else {
		return false, -1
	}
}

// 'internal_column_character_set_name' function operator
func getTypeCharacterSet(typ types.Type) (bool, int32) {
	if typ.Oid == types.T_varchar ||
		typ.Oid == types.T_char ||
		typ.Oid == types.T_blob ||
		typ.Oid == types.T_text {
		return true, int32(typ.Charset)
	} else {
		return false, -1
	}
}
