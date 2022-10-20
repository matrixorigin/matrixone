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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/builtin/binary"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Rpad(origVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if origVecs[0].IsScalarNull() || origVecs[1].IsScalarNull() || origVecs[2].IsScalarNull() {
		return proc.AllocScalarNullVector(origVecs[0].Typ), nil
	}

	isConst := []bool{origVecs[0].IsScalar(), origVecs[1].IsScalar(), origVecs[2].IsScalar()}

	// gets all args
	strs := vector.GetStrVectorValues(origVecs[0])
	sizes := origVecs[1].Col
	if _, ok := sizes.([]types.Varlena); ok {
		sizes = vector.MustStrCols(origVecs[1])
	}

	var padstrs interface{}
	// resolve padstrs,
	if origVecs[2].GetType().IsVarlen() {
		padstrs = vector.GetStrVectorValues(origVecs[2])
	} else {
		// keep orig type
		padstrs = origVecs[2].Col
	}
	oriNsps := []*nulls.Nulls{origVecs[0].Nsp, origVecs[1].Nsp, origVecs[2].Nsp}

	// gets a new vector to store our result
	rowCount := vector.Length(origVecs[0])

	if origVecs[0].IsScalar() && origVecs[1].IsScalar() && origVecs[2].IsScalar() {
		//evaluate the result
		result, nsp, err := rpad(rowCount, strs, sizes, padstrs, isConst, oriNsps)
		if err != nil {
			return nil, err
		}
		resultVec := vector.NewWithStrings(origVecs[0].Typ, result, nsp, proc.Mp())
		return resultVec, nil
	}

	result, nsp, err := rpad(rowCount, strs, sizes, padstrs, isConst, oriNsps)
	if err != nil {
		return nil, err
	}
	resultVec := vector.NewWithStrings(origVecs[0].Typ, result, nsp, proc.Mp())
	return resultVec, nil
}

var (
	MaxPad int64
)

const UINT16_MAX = ^uint16(0)

func init() {
	MaxPad = int64(16 * 1024 * 1024)
}

// rpad returns a *types.Bytes containing the padded strings and a corresponding bitmap *nulls.Nulls.
// rpad is multibyte-safe
func rpad(rowCount int, strs []string, sizes interface{}, pads interface{}, isConst []bool, oriNsp []*nulls.Nulls) ([]string, *nulls.Nulls, error) {
	// typecast
	var padstrs []string
	var err error
	switch pd := pads.(type) {
	case []string:
		padstrs = pd
	case []int64:
		padstrs = make([]string, len(pd))
		_, err = binary.Int64ToBytes(pd, padstrs)
	case []int32:
		padstrs = make([]string, len(pd))
		_, err = binary.Int32ToBytes(pd, padstrs)
	case []int16:
		padstrs = make([]string, len(pd))
		_, err = binary.Int16ToBytes(pd, padstrs)
	case []int8:
		padstrs = make([]string, len(pd))
		_, err = binary.Int8ToBytes(pd, padstrs)
	case []uint64:
		padstrs = make([]string, len(pd))
		_, err = binary.Uint64ToBytes(pd, padstrs)
	case []uint32:
		padstrs = make([]string, len(pd))
		_, err = binary.Uint32ToBytes(pd, padstrs)
	case []uint16:
		padstrs = make([]string, len(pd))
		_, err = binary.Uint16ToBytes(pd, padstrs)
	case []uint8:
		padstrs = make([]string, len(pd))
		_, err = binary.Uint8ToBytes(pd, padstrs)
	case []float32:
		padstrs = make([]string, len(pd))
		_, err = binary.Float32ToBytes(pd, padstrs)
	case []float64:
		padstrs = make([]string, len(pd))
		_, err = binary.Float64ToBytes(pd, padstrs)
	default:
		// empty string
		padstrs = append(padstrs, "")
		isConst[2] = true
	}
	if err != nil {
		return nil, nil, err
	}

	// do rpad
	var result []string
	var nsp *nulls.Nulls
	var err2 error
	switch sz := sizes.(type) {
	case []int64:
		result, nsp = rpadInt64(rowCount, strs, sz, padstrs, isConst, oriNsp)
	case []int32:
		sizesInt64 := make([]int64, len(sz))
		sizesInt64, err2 = binary.Int32ToInt64(sz, sizesInt64)
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp)
	case []int16:
		sizesInt64 := make([]int64, len(sz))
		sizesInt64, err2 = binary.Int16ToInt64(sz, sizesInt64)
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp)
	case []int8:
		sizesInt64 := make([]int64, len(sz))
		sizesInt64, err2 = binary.Int8ToInt64(sz, sizesInt64)
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp)
	case []float64:
		sizesInt64 := make([]int64, len(sz))
		isEmptyStringOrNull := make([]int, len(sz))
		sizesInt64, err2 = binary.Float64ToInt64(sz, sizesInt64, isEmptyStringOrNull)
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp, isEmptyStringOrNull)
	case []float32:
		sizesInt64 := make([]int64, len(sz))
		sizesInt64, err2 = binary.Float32ToInt64(sz, sizesInt64)
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp)
	case []uint64:
		result, nsp = rpadUint64(rowCount, strs, sz, padstrs, isConst, oriNsp)
	case []uint32:
		sizesUint64 := make([]uint64, len(sz))
		sizesUint64, err2 = binary.Uint32ToUint64(sz, sizesUint64)
		result, nsp = rpadUint64(rowCount, strs, sizesUint64, padstrs, isConst, oriNsp)
	case []uint16:
		sizesUint64 := make([]uint64, len(sz))
		sizesUint64, err2 = binary.Uint16ToUint64(sz, sizesUint64)
		result, nsp = rpadUint64(rowCount, strs, sizesUint64, padstrs, isConst, oriNsp)
	case []uint8:
		sizesUint64 := make([]uint64, len(sz))
		sizesUint64, err2 = binary.Uint8ToUint64(sz, sizesUint64)
		result, nsp = rpadUint64(rowCount, strs, sizesUint64, padstrs, isConst, oriNsp)
	case []string:
		// XXX What is this code?
		sizesFloat64 := make([]float64, len(sz))
		isEmptyStringOrNull := make([]int, len(sz))
		sizesFloat64, err2 = binary.BytesToFloat(sz, sizesFloat64, false, isEmptyStringOrNull)
		sizesInt64 := make([]int64, len(sz))
		for i, val := range sizesFloat64 { //for func rpad,like '1.8', is 1, not 2.
			sizesInt64[i] = int64(math.Floor(val))
		}
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp, isEmptyStringOrNull)
	default:
		// return empty strings if sizes is a non-numerical type slice
		nsp = new(nulls.Nulls)
		nulls.Set(nsp, oriNsp[0])
		result = make([]string, len(strs))
	}
	if err2 != nil {
		return nil, nil, err2
	}
	return result, nsp, nil
}

// note that: for flag:
// 0: nothing todo
// 1: is an overflow flag
// 2: is an parse_error flag
func rpadInt64(rowCount int, strs []string, sizes []int64, padstrs []string, isConst []bool, oriNsp []*nulls.Nulls, isEmptyStringOrNull ...[]int) ([]string, *nulls.Nulls) {
	results := make([]string, rowCount)
	resultNsp := new(nulls.Nulls)
	usedEmptyStringOrNull := len(isEmptyStringOrNull) > 0
	for i := 0; i < rowCount; i++ {
		var newSize int64
		var EmptyStringOrNull int //we use flag1 to see if we need to give "" but not NULL
		if isConst[1] {
			if usedEmptyStringOrNull {
				EmptyStringOrNull = isEmptyStringOrNull[0][0]
			}
			// accepts a constant literal
			newSize = sizes[0]
		} else {
			if usedEmptyStringOrNull {
				EmptyStringOrNull = isEmptyStringOrNull[0][i]
			}
			// accepts an attribute name
			newSize = sizes[i]
		}
		if EmptyStringOrNull == 2 {
			continue
		}
		// gets NULL if any arg is NULL or the newSize < 0
		if row := uint64(i); nulls.Contains(oriNsp[0], row) || nulls.Contains(oriNsp[1], row) || nulls.Contains(oriNsp[2], row) || newSize < 0 || newSize > int64(UINT16_MAX) || newSize > MaxPad {
			nulls.Add(resultNsp, row)
			continue
		}

		var padRunes []rune
		if isConst[2] {
			padRunes = []rune(padstrs[0])
		} else {
			padRunes = []rune(padstrs[i])
		}
		var oriRunes []rune
		if isConst[0] {
			oriRunes = []rune(strs[0])
		} else {
			oriRunes = []rune(strs[i])
		}
		// gets the padded string
		if int(newSize) <= len(oriRunes) {
			// truncates the original string
			tmp := string(oriRunes[:newSize])
			results[i] = tmp
		} else {
			if len(padRunes) == 0 {
				// gets an empty string if the padRunes is also an empty string and newSize > len(oriRunes)
				// E.x. in mysql 8.0
				// select rpad("test",5,"");
				// +-----------------+
				// |rpad("test",5,"")|
				// +-----------------+
				// |                 |
				// +-----------------+
				// results[i] is still empty
			} else {
				padding := int(newSize) - len(oriRunes)
				// builds a padded string
				var tmp string
				if isConst[0] {
					tmp += strs[0]
				} else {
					tmp += strs[i]
				}
				// adds some pads
				for j := 0; j < padding/len(padRunes); j++ {
					tmp += string(padRunes)
				}
				// adds the remaining part
				tmp += string(padRunes[:padding%len(padRunes)])
				results[i] = tmp
			}
		}
	}
	return results, resultNsp
}

func rpadUint64(rowCount int, strs []string, sizes []uint64, padstrs []string, isConst []bool, oriNsp []*nulls.Nulls) ([]string, *nulls.Nulls) {
	isz := make([]int64, len(sizes))
	for i, s := range sizes {
		isz[i] = int64(s)
	}
	return rpadInt64(rowCount, strs, isz, padstrs, isConst, oriNsp)
}
