// Copyright 2024 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func PrefixEq(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	lvec := parameters[0]
	rval := parameters[1].GetBytesAt(0)
	res := vector.MustFixedCol[bool](result.GetResultVector())

	lcol, larea := vector.MustVarlenaRawData(lvec)

	if lvec.GetSorted() {
		lowerBound := sort.Search(len(lcol), func(i int) bool {
			return bytes.Compare(rval, lcol[i].GetByteSlice(larea)) <= 0
		})

		upperBound := lowerBound
		for upperBound < len(lcol) && bytes.HasPrefix(lcol[upperBound].GetByteSlice(larea), rval) {
			upperBound++
		}

		for i := 0; i < lowerBound; i++ {
			res[i] = false
		}
		for i := lowerBound; i < upperBound; i++ {
			res[i] = true
		}
		for i := upperBound; i < length; i++ {
			res[i] = false
		}
	} else {
		for i := 0; i < length; i++ {
			res[i] = bytes.HasPrefix(lcol[i].GetByteSlice(larea), rval)
		}
	}

	return nil
}

func PrefixBetween(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	ivec := parameters[0]
	lval := parameters[1].GetBytesAt(0)
	rval := parameters[2].GetBytesAt(0)
	res := vector.MustFixedCol[bool](result.GetResultVector())

	icol, iarea := vector.MustVarlenaRawData(ivec)

	if ivec.GetSorted() {
		lowerBound := sort.Search(len(icol), func(i int) bool {
			return index.PrefixCompare(icol[i].GetByteSlice(iarea), lval) >= 0
		})

		upperBound := sort.Search(len(icol), func(i int) bool {
			return index.PrefixCompare(icol[i].GetByteSlice(iarea), rval) > 0
		})

		for i := 0; i < lowerBound; i++ {
			res[i] = false
		}
		for i := lowerBound; i < upperBound; i++ {
			res[i] = true
		}
		for i := upperBound; i < length; i++ {
			res[i] = false
		}
	} else {
		for i := 0; i < length; i++ {
			val := icol[i].GetByteSlice(iarea)
			res[i] = index.PrefixCompare(val, lval) >= 0 && index.PrefixCompare(val, rval) <= 0
		}
	}

	return nil
}

func PrefixIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	lvec := parameters[0]
	rvec := parameters[1]
	res := vector.MustFixedCol[bool](result.GetResultVector())

	lcol, larea := vector.MustVarlenaRawData(lvec)
	rcol, rarea := vector.MustVarlenaRawData(rvec)

	if lvec.GetSorted() {
		rval := rcol[0].GetByteSlice(rarea)
		rpos := 0
		rlen := rvec.Length()

		for i := 0; i < length; i++ {
			lval := lcol[i].GetByteSlice(larea)
			for index.PrefixCompare(lval, rval) > 0 {
				rpos++
				if rpos == rlen {
					for j := i; j < length; j++ {
						res[j] = false
					}
					return nil
				}

				rval = rcol[rpos].GetByteSlice(rarea)
			}

			res[i] = bytes.HasPrefix(lval, rval)
		}
	} else {
		for i := 0; i < length; i++ {
			lval := lcol[i].GetByteSlice(larea)
			rpos, _ := sort.Find(len(rcol), func(j int) int {
				return index.PrefixCompare(lval, rcol[j].GetByteSlice(rarea))
			})

			res[i] = rpos < len(rcol) && bytes.HasPrefix(lval, rcol[rpos].GetByteSlice(rarea))
		}
	}

	return nil
}
