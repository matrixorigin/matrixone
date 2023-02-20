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

func FieldString(vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vs[0]
	firstValues := vector.MustStrCols(firstVector)

	vecLen := vector.Length(firstVector)

	//return vector
	returnType := types.T_uint64.ToType()
	resultVector, err := proc.AllocVector(returnType, int64(vecLen*returnType.Oid.TypeLen()))
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[uint64](resultVector)

	//if first vector is scalar
	if firstVector.IsScalar() {

		//if first vector is null, the return value is 0
		if firstVector.IsScalarNull() {
			return vector.NewConstFixed(returnType, vecLen, uint64(0), proc.Mp()), err
		}

		//detect index
		startIdx := 1

		//detect in pre scalar vector
		for i := 1; i < len(vs); i++ {
			input := vs[i]
			if input.IsScalar() {
				if !input.IsScalarNull() {
					cols := vector.MustStrCols(input)
					if firstValues[0] == cols[0] {
						return vector.NewConstFixed(returnType, input.Length(), uint64(i), proc.Mp()), err
					}
				}
			} else {
				startIdx = i
				break
			}
		}

		//shouldReturn represents the non-null counts
		shouldReturn := vecLen

		for i := startIdx; i < len(vs); i++ {
			input := vs[i]
			cols := vector.MustStrCols(input)

			if input.IsScalar() {
				if !input.IsScalarNull() {
					if firstValues[0] == cols[0] {
						for j := 0; j < vecLen; j++ {
							if rs[j] == 0 {
								rs[j] = uint64(i)
							}
						}
						break
					}
				}
			} else {
				for j := 0; j < vecLen; j++ {
					if !nulls.Contains(input.Nsp, uint64(j)) && rs[j] == 0 && firstValues[0] == cols[j] {
						rs[j] = uint64(i)
						shouldReturn--
					}
				}
				if shouldReturn == 0 {
					break
				}
			}
		}
		return resultVector, nil
	} else {

		//if the first vector is null
		nullsLength := nulls.Length(firstVector.Nsp)
		if nullsLength == vecLen {
			return resultVector, nil
		}

		//shouldReturn represents the non-null counts
		shouldReturn := vecLen - nullsLength

		for i := 1; i < len(vs); i++ {
			input := vs[i]
			cols := vector.MustStrCols(input)

			if input.IsScalar() {
				if !input.IsScalarNull() {
					for j := 0; j < vecLen; j++ {
						if rs[j] == 0 && firstValues[j] == cols[0] {
							rs[j] = uint64(i)
							shouldReturn--
						}
					}
				}
			} else {
				for j := 0; j < vecLen; j++ {
					if !nulls.Contains(input.Nsp, uint64(j)) && rs[j] == 0 && firstValues[j] == cols[j] {
						rs[j] = uint64(i)
						shouldReturn--
					}
				}
			}

			if shouldReturn == 0 {
				break
			}

		}

		return resultVector, nil
	}
}

func FieldNumber[T number](vs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := vs[0]
	firstValues := vector.MustTCols[T](firstVector)

	vecLen := vector.Length(firstVector)

	//return vector
	returnType := types.T_uint64.ToType()
	resultVector, err := proc.AllocVector(returnType, int64(vecLen*returnType.Oid.TypeLen()))
	if err != nil {
		return nil, err
	}
	rs := vector.MustTCols[uint64](resultVector)

	//if first vector is scalar
	if firstVector.IsScalar() {

		//if first vector is null, the return value is 0
		if firstVector.IsScalarNull() {
			return vector.NewConstFixed(returnType, vecLen, uint64(0), proc.Mp()), err
		}

		//detect index
		startIdx := 1

		//detect in pre scalar vector
		for i := 1; i < len(vs); i++ {
			input := vs[i]
			if input.IsScalar() {
				if !input.IsScalarNull() {
					cols := vector.MustTCols[T](input)
					if firstValues[0] == cols[0] {
						return vector.NewConstFixed(returnType, input.Length(), uint64(i), proc.Mp()), err
					}
				}
			} else {
				startIdx = i
				break
			}
		}

		//shouldReturn represents the non-null counts
		shouldReturn := vecLen

		for i := startIdx; i < len(vs); i++ {
			input := vs[i]
			cols := vector.MustTCols[T](input)

			if input.IsScalar() {
				if !input.IsScalarNull() {
					if firstValues[0] == cols[0] {
						for j := 0; j < vecLen; j++ {
							if rs[j] == 0 {
								rs[j] = uint64(i)
							}
						}
						break
					}
				}
			} else {
				for j := 0; j < vecLen; j++ {
					if !nulls.Contains(input.Nsp, uint64(j)) && rs[j] == 0 && firstValues[0] == cols[j] {
						rs[j] = uint64(i)
						shouldReturn--
					}
				}
				if shouldReturn == 0 {
					break
				}
			}
		}
		return resultVector, nil
	} else {

		//if the first vector is null
		nullsLength := nulls.Length(firstVector.Nsp)
		if nullsLength == vecLen {
			return resultVector, nil
		}

		//shouldReturn represents the non-null counts
		shouldReturn := vecLen - nullsLength

		for i := 1; i < len(vs); i++ {
			input := vs[i]
			cols := vector.MustTCols[T](input)

			if input.IsScalar() {
				if !input.IsScalarNull() {
					for j := 0; j < vecLen; j++ {
						if rs[j] == 0 && firstValues[j] == cols[0] {
							rs[j] = uint64(i)
							shouldReturn--
						}
					}
				}
			} else {
				for j := 0; j < vecLen; j++ {
					if !nulls.Contains(input.Nsp, uint64(j)) && rs[j] == 0 && firstValues[j] == cols[j] {
						rs[j] = uint64(i)
						shouldReturn--
					}
				}
			}

			if shouldReturn == 0 {
				break
			}

		}
		return resultVector, nil
	}
}
