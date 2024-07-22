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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func PrefixEq(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if parameters[1].AllNull() {
		result.GetResultVector().GetNulls().AddRange(0, uint64(length))
		return nil
	}

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

func PrefixBetween(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	ivec := parameters[0]
	lval := parameters[1].GetBytesAt(0)
	rval := parameters[2].GetBytesAt(0)
	res := vector.MustFixedCol[bool](result.GetResultVector())

	icol, iarea := vector.MustVarlenaRawData(ivec)

	if ivec.GetSorted() {
		lowerBound := sort.Search(len(icol), func(i int) bool {
			return types.PrefixCompare(icol[i].GetByteSlice(iarea), lval) >= 0
		})

		upperBound := sort.Search(len(icol), func(i int) bool {
			return types.PrefixCompare(icol[i].GetByteSlice(iarea), rval) > 0
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
			res[i] = types.PrefixCompare(val, lval) >= 0 && types.PrefixCompare(val, rval) <= 0
		}
	}

	return nil
}

type implPrefixIn struct {
	ready bool
	vals  [][]byte
}

func newImplPrefixIn() *implPrefixIn {
	return &implPrefixIn{ready: false}
}

func (op *implPrefixIn) init(rvec *vector.Vector) {
	op.ready = true
	op.vals = make([][]byte, rvec.Length())
	vlen := 0
	rcol, rarea := vector.MustVarlenaRawData(rvec)
	for i := 0; i < rvec.Length(); i++ {
		rval := rcol[i].GetByteSlice(rarea)
		if vlen == 0 || !bytes.HasPrefix(rval, op.vals[vlen-1]) {
			op.vals[vlen] = rval
			vlen++
		}
	}
	op.vals = op.vals[:vlen]
}

func (op *implPrefixIn) doPrefixIn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if !op.ready {
		op.init(parameters[1])
	}

	lvec := parameters[0]
	res := vector.MustFixedCol[bool](result.GetResultVector())

	lcol, larea := vector.MustVarlenaRawData(lvec)

	if lvec.GetSorted() {
		rval := op.vals[0]
		rpos := 0
		rlen := len(op.vals)

		for i := 0; i < length; i++ {
			lval := lcol[i].GetByteSlice(larea)
			for types.PrefixCompare(lval, rval) > 0 {
				rpos++
				if rpos == rlen {
					for j := i; j < length; j++ {
						res[j] = false
					}
					return nil
				}

				rval = op.vals[rpos]
			}

			res[i] = bytes.HasPrefix(lval, rval)
		}
	} else {
		for i := 0; i < length; i++ {
			lval := lcol[i].GetByteSlice(larea)
			rpos, _ := sort.Find(len(op.vals), func(j int) int {
				return types.PrefixCompare(lval, op.vals[j])
			})

			res[i] = rpos < len(op.vals) && bytes.HasPrefix(lval, op.vals[rpos])
		}
	}

	return nil
}
