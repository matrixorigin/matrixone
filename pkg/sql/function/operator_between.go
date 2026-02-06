// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"bytes"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func betweenImpl(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return opBetweenBool(parameters, rs, proc, length)

	case types.T_bit:
		return opBetweenFixed[uint64](parameters, rs, proc, length)
	case types.T_int8:
		return opBetweenFixed[int8](parameters, rs, proc, length)
	case types.T_int16:
		return opBetweenFixed[int16](parameters, rs, proc, length)
	case types.T_int32:
		return opBetweenFixed[int32](parameters, rs, proc, length)
	case types.T_int64:
		return opBetweenFixed[int64](parameters, rs, proc, length)
	case types.T_uint8:
		return opBetweenFixed[uint8](parameters, rs, proc, length)
	case types.T_uint16:
		return opBetweenFixed[uint16](parameters, rs, proc, length)
	case types.T_uint32:
		return opBetweenFixed[uint32](parameters, rs, proc, length)
	case types.T_uint64:
		return opBetweenFixed[uint64](parameters, rs, proc, length)
	case types.T_float32:
		return opBetweenFixed[float32](parameters, rs, proc, length)
	case types.T_float64:
		return opBetweenFixed[float64](parameters, rs, proc, length)
	case types.T_date:
		return opBetweenFixed[types.Date](parameters, rs, proc, length)
	case types.T_datetime:
		return opBetweenFixed[types.Datetime](parameters, rs, proc, length)
	case types.T_time:
		return opBetweenFixed[types.Time](parameters, rs, proc, length)
	case types.T_timestamp:
		return opBetweenFixed[types.Timestamp](parameters, rs, proc, length)

	case types.T_uuid:
		return opBetweenFixedWithFn(parameters, rs, proc, length, types.CompareUuid)
	case types.T_decimal64:
		return opBetweenFixedWithFn(parameters, rs, proc, length, types.CompareDecimal64)
	case types.T_decimal128:
		return opBetweenFixedWithFn(parameters, rs, proc, length, types.CompareDecimal128)
	case types.T_Rowid:
		return opBetweenFixedWithFn(parameters, rs, proc, length, func(lhs, rhs types.Rowid) int {
			return lhs.Compare(&rhs)
		})

	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		return opBetweenBytesWithFunc(parameters, rs, proc, length, selectList, bytes.Compare)
	}

	panic("unreached code")
}

func opBetweenBool(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
) error {
	ivec := parameters[0]
	icol := vector.MustFixedColWithTypeCheck[bool](ivec)
	lval := vector.GetFixedAtWithTypeCheck[bool](parameters[1], 0)
	rval := vector.GetFixedAtWithTypeCheck[bool](parameters[2], 0)
	res := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())

	alwaysTrue := lval != rval

	if ivec.IsConstNull() {
		nulls.AddRange(result.GetResultVector().GetNulls(), 0, uint64(length))
	} else if ivec.IsConst() {
		if alwaysTrue {
			for i := range length {
				res[i] = true
			}
		} else {
			r := icol[0] == lval

			for i := range length {
				res[i] = r
			}
		}
	} else if ivec.HasNull() {
		iNulls := ivec.GetNulls()
		rNulls := result.GetResultVector().GetNulls()
		for i := range length {
			if iNulls.Contains(uint64(i)) {
				res[i] = false
				rNulls.Add(uint64(i))
			} else {
				res[i] = icol[i] == lval
			}
		}
	} else if alwaysTrue {
		for i := range length {
			res[i] = true
		}
	} else {
		for i := range length {
			res[i] = icol[i] == lval
		}
	}

	return nil
}

func opBetweenFixed[T constraints.Integer | constraints.Float](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
) error {
	ivec := parameters[0]
	icol := vector.MustFixedColWithTypeCheck[T](ivec)
	lval := vector.GetFixedAtWithTypeCheck[T](parameters[1], 0)
	rval := vector.GetFixedAtWithTypeCheck[T](parameters[2], 0)
	res := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())

	if ivec.IsConst() {
		if ivec.IsConstNull() {
			nulls.AddRange(result.GetResultVector().GetNulls(), 0, uint64(length))
		} else {
			val := icol[0]
			r := val >= lval && val <= rval

			for i := range length {
				res[i] = r
			}
		}
	} else if ivec.HasNull() {
		iNulls := ivec.GetNulls()
		rNulls := result.GetResultVector().GetNulls()
		for i := range length {
			if iNulls.Contains(uint64(i)) {
				res[i] = false
				rNulls.Add(uint64(i))
			} else {
				val := icol[i]
				res[i] = val >= lval && val <= rval
			}
		}
	} else if ivec.GetSorted() {
		lowerBound := sort.Search(len(icol), func(i int) bool {
			return icol[i] >= lval
		})

		upperBound := sort.Search(len(icol), func(i int) bool {
			return icol[i] > rval
		})

		for i := range lowerBound {
			res[i] = false
		}
		for i := lowerBound; i < upperBound; i++ {
			res[i] = true
		}
		for i := upperBound; i < length; i++ {
			res[i] = false
		}
	} else {
		for i := range length {
			val := icol[i]
			res[i] = val >= lval && val <= rval
		}
	}

	return nil
}

func opBetweenFixedWithFn[T types.FixedSizeTExceptStrType](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	compareFunc func(v1, v2 T) int,
) error {
	ivec := parameters[0]
	icol := vector.MustFixedColWithTypeCheck[T](ivec)
	lval := vector.GetFixedAtWithTypeCheck[T](parameters[1], 0)
	rval := vector.GetFixedAtWithTypeCheck[T](parameters[2], 0)
	res := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())

	if ivec.IsConst() {
		if ivec.IsConstNull() {
			nulls.AddRange(result.GetResultVector().GetNulls(), 0, uint64(length))
		} else {
			val := icol[0]
			r := compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0

			for i := range length {
				res[i] = r
			}
		}
	} else if ivec.HasNull() {
		iNulls := ivec.GetNulls()
		rNulls := result.GetResultVector().GetNulls()
		for i := range length {
			if iNulls.Contains(uint64(i)) {
				res[i] = false
				rNulls.Add(uint64(i))
			} else {
				val := icol[i]
				res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0
			}
		}
	} else if ivec.GetSorted() {
		lowerBound := sort.Search(len(icol), func(i int) bool {
			return compareFunc(icol[i], lval) >= 0
		})

		upperBound := sort.Search(len(icol), func(i int) bool {
			return compareFunc(icol[i], rval) > 0
		})

		for i := range lowerBound {
			res[i] = false
		}
		for i := lowerBound; i < upperBound; i++ {
			res[i] = true
		}
		for i := upperBound; i < length; i++ {
			res[i] = false
		}
	} else {
		for i := range length {
			val := icol[i]
			res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0
		}
	}

	return nil
}

func opBetweenBytesWithFunc(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	_ *FunctionSelectList,
	compareFunc func(v1, v2 []byte) int,
) error {
	ivec := parameters[0]
	icol, iarea := vector.MustVarlenaRawData(ivec)
	lval := parameters[1].GetBytesAt(0)
	rval := parameters[2].GetBytesAt(0)
	res := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())

	if ivec.IsConst() {
		if ivec.IsConstNull() {
			nulls.AddRange(result.GetResultVector().GetNulls(), 0, uint64(length))
		} else {
			val := icol[0].GetByteSlice(iarea)
			r := compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0

			for i := range length {
				res[i] = r
			}
		}
	} else if ivec.HasNull() {
		iNulls := ivec.GetNulls()
		rNulls := result.GetResultVector().GetNulls()
		for i := range length {
			if iNulls.Contains(uint64(i)) {
				res[i] = false
				rNulls.Add(uint64(i))
			} else {
				val := icol[i].GetByteSlice(iarea)
				res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0
			}
		}
	} else if ivec.GetSorted() {
		lowerBound := sort.Search(len(icol), func(i int) bool {
			return compareFunc(icol[i].GetByteSlice(iarea), lval) >= 0
		})

		upperBound := sort.Search(len(icol), func(i int) bool {
			return compareFunc(icol[i].GetByteSlice(iarea), rval) > 0
		})

		for i := range lowerBound {
			res[i] = false
		}
		for i := lowerBound; i < upperBound; i++ {
			res[i] = true
		}
		for i := upperBound; i < length; i++ {
			res[i] = false
		}
	} else {
		for i := range length {
			val := icol[i].GetByteSlice(iarea)
			res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0
		}
	}

	return nil
}

func inRangeImpl(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return inRangeBool(parameters, rs, proc, length)

	case types.T_bit:
		return inRangeFixed[uint64](parameters, rs, proc, length)
	case types.T_int8:
		return inRangeFixed[int8](parameters, rs, proc, length)
	case types.T_int16:
		return inRangeFixed[int16](parameters, rs, proc, length)
	case types.T_int32:
		return inRangeFixed[int32](parameters, rs, proc, length)
	case types.T_int64:
		return inRangeFixed[int64](parameters, rs, proc, length)
	case types.T_uint8:
		return inRangeFixed[uint8](parameters, rs, proc, length)
	case types.T_uint16:
		return inRangeFixed[uint16](parameters, rs, proc, length)
	case types.T_uint32:
		return inRangeFixed[uint32](parameters, rs, proc, length)
	case types.T_uint64:
		return inRangeFixed[uint64](parameters, rs, proc, length)
	case types.T_float32:
		return inRangeFixed[float32](parameters, rs, proc, length)
	case types.T_float64:
		return inRangeFixed[float64](parameters, rs, proc, length)
	case types.T_date:
		return inRangeFixed[types.Date](parameters, rs, proc, length)
	case types.T_datetime:
		return inRangeFixed[types.Datetime](parameters, rs, proc, length)
	case types.T_time:
		return inRangeFixed[types.Time](parameters, rs, proc, length)
	case types.T_timestamp:
		return inRangeFixed[types.Timestamp](parameters, rs, proc, length)

	case types.T_uuid:
		return inRangeFixedWithFunc(parameters, rs, proc, length, types.CompareUuid)
	case types.T_decimal64:
		return inRangeFixedWithFunc(parameters, rs, proc, length, types.CompareDecimal64)
	case types.T_decimal128:
		return inRangeFixedWithFunc(parameters, rs, proc, length, types.CompareDecimal128)
	case types.T_Rowid:
		return inRangeFixedWithFunc(parameters, rs, proc, length, func(lhs, rhs types.Rowid) int {
			return lhs.Compare(&rhs)
		})

	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		return inRangeBytesWithFunc(parameters, rs, proc, length, selectList, bytes.Compare)
	}

	panic("unreached code")
}

func inRangeBool(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
) error {
	ivec := parameters[0]
	icol := vector.MustFixedColWithTypeCheck[bool](ivec)
	lval := vector.GetFixedAtWithTypeCheck[bool](parameters[1], 0)
	rval := vector.GetFixedAtWithTypeCheck[bool](parameters[2], 0)
	flag := vector.MustFixedColWithTypeCheck[uint8](parameters[3])[0]
	res := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())

	alwaysTrue := flag == 0 && lval != rval
	alwaysFalse := flag == 3 || (flag != 0 && lval == rval)

	if ivec.IsConstNull() {
		nulls.AddRange(result.GetResultVector().GetNulls(), 0, uint64(length))
	} else if ivec.IsConst() {
		if alwaysTrue {
			for i := range length {
				res[i] = true
			}
		} else if alwaysFalse {
			for i := range length {
				res[i] = false
			}
		} else {
			val := icol[0]
			var r bool
			switch flag {
			case 0:
				r = val == lval
			case 1:
				r = val
			case 2:
				r = !val
			}

			for i := range length {
				res[i] = r
			}
		}
	} else if ivec.HasNull() {
		iNulls := ivec.GetNulls()
		rNulls := result.GetResultVector().GetNulls()
		for i := range length {
			if iNulls.Contains(uint64(i)) {
				res[i] = false
				rNulls.Add(uint64(i))
			} else {
				val := icol[i]
				switch flag {
				case 0:
					res[i] = val == lval
				case 1:
					res[i] = val
				case 2:
					res[i] = !val
				}
			}
		}
	} else if alwaysTrue {
		for i := range length {
			res[i] = true
		}
	} else if alwaysFalse {
		for i := range length {
			res[i] = false
		}
	} else {
		switch flag {
		case 0:
			for i := range length {
				res[i] = icol[i] == lval
			}
		case 1:
			for i := range length {
				res[i] = icol[i]
			}
		case 2:
			for i := range length {
				res[i] = !icol[i]
			}
		}
	}

	return nil
}

func inRangeFixed[T constraints.Integer | constraints.Float](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
) error {
	ivec := parameters[0]
	icol := vector.MustFixedColWithTypeCheck[T](ivec)
	lval := vector.GetFixedAtWithTypeCheck[T](parameters[1], 0)
	rval := vector.GetFixedAtWithTypeCheck[T](parameters[2], 0)
	flag := vector.MustFixedColWithTypeCheck[uint8](parameters[3])[0]
	res := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())

	if ivec.IsConstNull() {
		nulls.AddRange(result.GetResultVector().GetNulls(), 0, uint64(length))
	} else if ivec.IsConst() {
		val := icol[0]
		var r bool
		switch flag {
		case 0:
			r = val >= lval && val <= rval
		case 1:
			r = val > lval && val <= rval
		case 2:
			r = val >= lval && val < rval
		case 3:
			r = val > lval && val < rval
		}

		for i := range length {
			res[i] = r
		}
	} else if ivec.HasNull() {
		iNulls := ivec.GetNulls()
		rNulls := result.GetResultVector().GetNulls()
		for i := range length {
			if iNulls.Contains(uint64(i)) {
				res[i] = false
				rNulls.Add(uint64(i))
			} else {
				val := icol[i]
				switch flag {
				case 0:
					res[i] = val >= lval && val <= rval
				case 1:
					res[i] = val > lval && val <= rval
				case 2:
					res[i] = val >= lval && val < rval
				case 3:
					res[i] = val > lval && val < rval
				}
			}
		}
	} else if ivec.GetSorted() {
		var lowerBoundFunc func(i int) bool
		if flag&1 == 0 {
			lowerBoundFunc = func(i int) bool {
				return icol[i] >= lval
			}
		} else {
			lowerBoundFunc = func(i int) bool {
				return icol[i] > lval
			}
		}
		lowerBound := sort.Search(len(icol), lowerBoundFunc)

		var upperBoundFunc func(i int) bool
		if flag&2 == 0 {
			upperBoundFunc = func(i int) bool {
				return icol[i] > rval
			}
		} else {
			upperBoundFunc = func(i int) bool {
				return icol[i] >= rval
			}
		}
		upperBound := sort.Search(len(icol), upperBoundFunc)

		for i := range lowerBound {
			res[i] = false
		}
		for i := lowerBound; i < upperBound; i++ {
			res[i] = true
		}
		for i := upperBound; i < length; i++ {
			res[i] = false
		}
	} else {
		switch flag {
		case 0:
			for i := range length {
				val := icol[i]
				res[i] = val >= lval && val <= rval
			}
		case 1:
			for i := range length {
				val := icol[i]
				res[i] = val > lval && val <= rval
			}
		case 2:
			for i := range length {
				val := icol[i]
				res[i] = val >= lval && val < rval
			}
		case 3:
			for i := range length {
				val := icol[i]
				res[i] = val > lval && val < rval
			}
		}
	}

	return nil
}

func inRangeFixedWithFunc[T types.FixedSizeTExceptStrType](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	compareFunc func(v1, v2 T) int,
) error {
	ivec := parameters[0]
	icol := vector.MustFixedColWithTypeCheck[T](ivec)
	lval := vector.GetFixedAtWithTypeCheck[T](parameters[1], 0)
	rval := vector.GetFixedAtWithTypeCheck[T](parameters[2], 0)
	flag := vector.MustFixedColWithTypeCheck[uint8](parameters[3])[0]
	res := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())

	if ivec.IsConstNull() {
		nulls.AddRange(result.GetResultVector().GetNulls(), 0, uint64(length))
	} else if ivec.IsConst() {
		val := icol[0]
		var r bool
		switch flag {
		case 0:
			r = compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0
		case 1:
			r = compareFunc(val, lval) > 0 && compareFunc(val, rval) <= 0
		case 2:
			r = compareFunc(val, lval) >= 0 && compareFunc(val, rval) < 0
		case 3:
			r = compareFunc(val, lval) > 0 && compareFunc(val, rval) < 0
		}

		for i := range length {
			res[i] = r
		}
	} else if ivec.HasNull() {
		iNulls := ivec.GetNulls()
		rNulls := result.GetResultVector().GetNulls()
		for i := range length {
			if iNulls.Contains(uint64(i)) {
				res[i] = false
				rNulls.Add(uint64(i))
			} else {
				val := icol[i]
				switch flag {
				case 0:
					res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0
				case 1:
					res[i] = compareFunc(val, lval) > 0 && compareFunc(val, rval) <= 0
				case 2:
					res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) < 0
				case 3:
					res[i] = compareFunc(val, lval) > 0 && compareFunc(val, rval) < 0
				}
			}
		}
	} else if ivec.GetSorted() {
		var lowerBoundFunc func(i int) bool
		if flag&1 == 0 {
			lowerBoundFunc = func(i int) bool {
				return compareFunc(icol[i], lval) >= 0
			}
		} else {
			lowerBoundFunc = func(i int) bool {
				return compareFunc(icol[i], lval) > 0
			}
		}
		lowerBound := sort.Search(len(icol), lowerBoundFunc)

		var upperBoundFunc func(i int) bool
		if flag&2 == 0 {
			upperBoundFunc = func(i int) bool {
				return compareFunc(icol[i], rval) > 0
			}
		} else {
			upperBoundFunc = func(i int) bool {
				return compareFunc(icol[i], rval) >= 0
			}
		}
		upperBound := sort.Search(len(icol), upperBoundFunc)

		for i := range lowerBound {
			res[i] = false
		}
		for i := lowerBound; i < upperBound; i++ {
			res[i] = true
		}
		for i := upperBound; i < length; i++ {
			res[i] = false
		}
	} else {
		switch flag {
		case 0:
			for i := range length {
				val := icol[i]
				res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0
			}
		case 1:
			for i := range length {
				val := icol[i]
				res[i] = compareFunc(val, lval) > 0 && compareFunc(val, rval) <= 0
			}
		case 2:
			for i := range length {
				val := icol[i]
				res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) < 0
			}
		case 3:
			for i := range length {
				val := icol[i]
				res[i] = compareFunc(val, lval) > 0 && compareFunc(val, rval) < 0
			}
		}
	}

	return nil
}

func inRangeBytesWithFunc(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	_ *FunctionSelectList,
	compareFunc func(v1, v2 []byte) int,
) error {
	ivec := parameters[0]
	icol, iarea := vector.MustVarlenaRawData(ivec)
	lval := parameters[1].GetBytesAt(0)
	rval := parameters[2].GetBytesAt(0)
	flag := vector.MustFixedColWithTypeCheck[uint8](parameters[3])[0]
	res := vector.MustFixedColWithTypeCheck[bool](result.GetResultVector())

	if ivec.IsConst() {
		if ivec.IsConstNull() {
			nulls.AddRange(result.GetResultVector().GetNulls(), 0, uint64(length))
		} else {
			val := icol[0].GetByteSlice(iarea)
			var r bool
			switch flag {
			case 0:
				r = compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0
			case 1:
				r = compareFunc(val, lval) > 0 && compareFunc(val, rval) <= 0
			case 2:
				r = compareFunc(val, lval) >= 0 && compareFunc(val, rval) < 0
			case 3:
				r = compareFunc(val, lval) > 0 && compareFunc(val, rval) < 0
			}

			for i := range length {
				res[i] = r
			}
		}
	} else if ivec.HasNull() {
		iNulls := ivec.GetNulls()
		rNulls := result.GetResultVector().GetNulls()
		for i := range length {
			if iNulls.Contains(uint64(i)) {
				res[i] = false
				rNulls.Add(uint64(i))
			} else {
				val := icol[i].GetByteSlice(iarea)
				switch flag {
				case 0:
					res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0
				case 1:
					res[i] = compareFunc(val, lval) > 0 && compareFunc(val, rval) <= 0
				case 2:
					res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) < 0
				case 3:
					res[i] = compareFunc(val, lval) > 0 && compareFunc(val, rval) < 0
				}
			}
		}
	} else if ivec.GetSorted() {
		var lowerBoundFunc func(i int) bool
		if flag&1 == 0 {
			lowerBoundFunc = func(i int) bool {
				return compareFunc(icol[i].GetByteSlice(iarea), lval) >= 0
			}
		} else {
			lowerBoundFunc = func(i int) bool {
				return compareFunc(icol[i].GetByteSlice(iarea), lval) > 0
			}
		}
		lowerBound := sort.Search(len(icol), lowerBoundFunc)

		var upperBoundFunc func(i int) bool
		if flag&2 == 0 {
			upperBoundFunc = func(i int) bool {
				return compareFunc(icol[i].GetByteSlice(iarea), rval) > 0
			}
		} else {
			upperBoundFunc = func(i int) bool {
				return compareFunc(icol[i].GetByteSlice(iarea), rval) >= 0
			}
		}
		upperBound := sort.Search(len(icol), upperBoundFunc)

		for i := range lowerBound {
			res[i] = false
		}
		for i := lowerBound; i < upperBound; i++ {
			res[i] = true
		}
		for i := upperBound; i < length; i++ {
			res[i] = false
		}
	} else {
		switch flag {
		case 0:
			for i := range length {
				val := icol[i].GetByteSlice(iarea)
				res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) <= 0
			}
		case 1:
			for i := range length {
				val := icol[i].GetByteSlice(iarea)
				res[i] = compareFunc(val, lval) > 0 && compareFunc(val, rval) <= 0
			}
		case 2:
			for i := range length {
				val := icol[i].GetByteSlice(iarea)
				res[i] = compareFunc(val, lval) >= 0 && compareFunc(val, rval) < 0
			}
		case 3:
			for i := range length {
				val := icol[i].GetByteSlice(iarea)
				res[i] = compareFunc(val, lval) > 0 && compareFunc(val, rval) < 0
			}
		}
	}

	return nil
}
