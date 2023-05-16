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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type number interface {
	constraints.Unsigned | constraints.Signed | constraints.Float
}

func FieldString(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec0 := ivecs[0]
	vals0 := vector.MustStrCol(vec0)

	vecLen := vec0.Length()

	//return vector
	rtyp := types.T_uint64.ToType()
	rvec, err := proc.AllocVectorOfRows(rtyp, vecLen, nil)
	if err != nil {
		return nil, err
	}
	rvals := vector.MustFixedCol[uint64](rvec)

	//if first vector is null, the return value is 0
	if vec0.IsConstNull() {
		return vector.NewConstFixed(rtyp, uint64(0), ivecs[0].Length(), proc.Mp()), err
	}

	//if first vector is scalar
	if vec0.IsConst() {

		//detect index
		startIdx := 1

		//detect in pre scalar vector
		for i := 1; i < len(ivecs); i++ {
			vec := ivecs[i]
			if vec.IsConst() {
				if !vec.IsConstNull() {
					if vals0[0] == vec.GetStringAt(0) {
						return vector.NewConstFixed(rtyp, uint64(i), ivecs[0].Length(), proc.Mp()), err
					}
				}
			} else {
				startIdx = i
				break
			}
		}

		//notFound represents the non-null counts
		notFound := vecLen

		for i := startIdx; i < len(ivecs); i++ {
			vec := ivecs[i]
			vals := vector.MustStrCol(vec)

			if vec.IsConst() {
				if !vec.IsConstNull() {
					if vals0[0] == vals[0] {
						for j := 0; j < vecLen; j++ {
							if rvals[j] == 0 {
								rvals[j] = uint64(i)
								notFound--
							}
						}
						break
					}
				}
			} else {
				for j := 0; j < vecLen; j++ {
					if !nulls.Contains(vec.GetNulls(), uint64(j)) && rvals[j] == 0 && vals0[0] == vals[j] {
						rvals[j] = uint64(i)
						notFound--
					}
				}
			}
			if notFound == 0 {
				break
			}
		}
		return rvec, nil
	} else {
		//if the first vector is null
		nullsLength := vec0.GetNulls().Count()
		if nullsLength == vecLen {
			return rvec, nil
		}

		//notFound represents the non-null counts
		notFound := vecLen - nullsLength

		for i := 1; i < len(ivecs); i++ {
			vec := ivecs[i]
			vals := vector.MustStrCol(vec)

			if vec.IsConst() {
				if !vec.IsConstNull() {
					for j := 0; j < vecLen; j++ {
						if rvals[j] == 0 && vals0[j] == vals[0] {
							rvals[j] = uint64(i)
							notFound--
						}
					}
				}
			} else {
				for j := 0; j < vecLen; j++ {
					if !nulls.Contains(vec.GetNulls(), uint64(j)) && rvals[j] == 0 && vals0[j] == vals[j] {
						rvals[j] = uint64(i)
						notFound--
					}
				}
			}

			if notFound == 0 {
				break
			}

		}

		return rvec, nil
	}
}

func FieldNumber[T number](ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	vec0 := ivecs[0]
	vals0 := vector.MustFixedCol[T](vec0)

	vecLen := vec0.Length()

	//return vector
	rtyp := types.T_uint64.ToType()
	rvec, err := proc.AllocVectorOfRows(rtyp, vecLen, nil)
	if err != nil {
		return nil, err
	}
	rvals := vector.MustFixedCol[uint64](rvec)

	//if first vector is null, the return value is 0
	if vec0.IsConstNull() {
		return vector.NewConstFixed(rtyp, uint64(0), ivecs[0].Length(), proc.Mp()), err
	}

	//if first vector is scalar
	if vec0.IsConst() {

		//detect index
		startIdx := 1

		//detect in pre scalar vector
		for i := 1; i < len(ivecs); i++ {
			vec := ivecs[i]
			if vec.IsConst() {
				if !vec.IsConstNull() {
					if vals0[0] == vector.GetFixedAt[T](vec, 0) {
						return vector.NewConstFixed(rtyp, uint64(i), ivecs[0].Length(), proc.Mp()), err
					}
				}
			} else {
				startIdx = i
				break
			}
		}

		//notFound represents the non-null counts
		notFound := vecLen

		for i := startIdx; i < len(ivecs); i++ {
			vec := ivecs[i]
			vals := vector.MustFixedCol[T](vec)

			if vec.IsConst() {
				if !vec.IsConstNull() {
					if vals0[0] == vals[0] {
						for j := 0; j < vecLen; j++ {
							if rvals[j] == 0 {
								rvals[j] = uint64(i)
								notFound--
							}
						}
						break
					}
				}
			} else {
				for j := 0; j < vecLen; j++ {
					if !nulls.Contains(vec.GetNulls(), uint64(j)) && rvals[j] == 0 && vals0[0] == vals[j] {
						rvals[j] = uint64(i)
						notFound--
					}
				}
			}
			if notFound == 0 {
				break
			}
		}
		return rvec, nil
	} else {
		//if the first vector is null
		nullsLength := vec0.GetNulls().Count()
		if nullsLength == vecLen {
			return rvec, nil
		}

		//notFound represents the non-null counts
		notFound := vecLen - nullsLength

		for i := 1; i < len(ivecs); i++ {
			vec := ivecs[i]
			vals := vector.MustFixedCol[T](vec)

			if vec.IsConst() {
				if !vec.IsConstNull() {
					for j := 0; j < vecLen; j++ {
						if rvals[j] == 0 && vals0[j] == vals[0] {
							rvals[j] = uint64(i)
							notFound--
						}
					}
				}
			} else {
				for j := 0; j < vecLen; j++ {
					if !nulls.Contains(vec.GetNulls(), uint64(j)) && rvals[j] == 0 && vals0[j] == vals[j] {
						rvals[j] = uint64(i)
						notFound--
					}
				}
			}

			if notFound == 0 {
				break
			}

		}

		return rvec, nil
	}
}
