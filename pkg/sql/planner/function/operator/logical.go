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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/logical"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type logicFn func(v1, v2, r *vector.Vector) error

type logicType int8

const (
	AND logicType = 0
	OR  logicType = 1
	XOR logicType = 2
)

func Logic(ivecs []*vector.Vector, proc *process.Process, cfn logicFn, op logicType) (*vector.Vector, error) {
	left, right := ivecs[0], ivecs[1]
	if left.IsConstNull() {
		switch op {
		case AND:
			if right.IsConstNull() {
				return vector.NewConstNull(boolType, right.Length(), proc.Mp()), nil
			} else if right.IsConst() {
				if vector.MustFixedCol[bool](right)[0] {
					return vector.NewConstNull(boolType, right.Length(), proc.Mp()), nil
				} else {
					return vector.NewConstFixed(boolType, false, right.Length(), proc.Mp()), nil
				}
			} else {
				length := right.Length()
				rvec := allocateBoolVector(length, proc)
				value := vector.MustFixedCol[bool](right)
				for i := 0; i < int(length); i++ {
					if value[i] || right.GetNulls().Contains(uint64(i)) {
						nulls.Add(rvec.GetNulls(), uint64(i))
					}
				}
				return rvec, nil
			}

		case OR:
			if right.IsConstNull() {
				return vector.NewConstNull(boolType, right.Length(), proc.Mp()), nil
			} else if right.IsConst() {
				if vector.MustFixedCol[bool](right)[0] {
					return vector.NewConstFixed(boolType, true, right.Length(), proc.Mp()), nil
				} else {
					return vector.NewConstNull(boolType, right.Length(), proc.Mp()), nil
				}
			} else {
				length := right.Length()
				rvec := allocateBoolVector(length, proc)
				value := vector.MustFixedCol[bool](right)
				rvals := vector.MustFixedCol[bool](rvec)
				for i := 0; i < int(length); i++ {
					if value[i] && !right.GetNulls().Contains(uint64(i)) {
						rvals[i] = true
					} else {
						nulls.Add(rvec.GetNulls(), uint64(i))
					}
				}
				return rvec, nil
			}

		default:
			return vector.NewConstNull(boolType, right.Length(), proc.Mp()), nil
		}
	} else if right.IsConstNull() {
		switch op {
		case AND:
			if left.IsConst() {
				if vector.MustFixedCol[bool](left)[0] {
					return vector.NewConstNull(boolType, left.Length(), proc.Mp()), nil
				} else {
					return vector.NewConstFixed(boolType, false, left.Length(), proc.Mp()), nil
				}
			} else {
				length := left.Length()
				rvec := allocateBoolVector(length, proc)
				value := vector.MustFixedCol[bool](left)
				for i := 0; i < int(length); i++ {
					if value[i] || left.GetNulls().Contains(uint64(i)) {
						nulls.Add(rvec.GetNulls(), uint64(i))
					}
				}
				return rvec, nil
			}

		case OR:
			if left.IsConst() {
				if vector.MustFixedCol[bool](left)[0] {
					return vector.NewConstFixed(boolType, true, left.Length(), proc.Mp()), nil
				} else {
					return vector.NewConstNull(boolType, left.Length(), proc.Mp()), nil
				}
			} else {
				length := left.Length()
				rvec := allocateBoolVector(length, proc)
				value := vector.MustFixedCol[bool](left)
				rvals := vector.MustFixedCol[bool](rvec)
				for i := 0; i < int(length); i++ {
					if value[i] && !left.GetNulls().Contains(uint64(i)) {
						rvals[i] = true
					} else {
						nulls.Add(rvec.GetNulls(), uint64(i))
					}
				}
				return rvec, nil
			}

		default:
			return vector.NewConstNull(boolType, left.Length(), proc.Mp()), nil
		}
	} else if left.IsConst() && right.IsConst() {
		vec := vector.NewConstFixed(boolType, false, ivecs[0].Length(), proc.Mp())
		if err := cfn(left, right, vec); err != nil {
			return nil, err
		}
		return vec, nil
	} else {
		length := left.Length()
		rvec := allocateBoolVector(length, proc)
		nulls.Or(left.GetNulls(), right.GetNulls(), rvec.GetNulls())

		if err := cfn(left, right, rvec); err != nil {
			return nil, err
		}
		return rvec, nil
	}
}

func LogicAnd(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Logic(args, proc, logical.And, AND)
}

func LogicOr(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Logic(args, proc, logical.Or, OR)
}

func LogicXor(args []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return Logic(args, proc, logical.Xor, XOR)
}

func LogicNot(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	v1 := ivecs[0]
	if v1.IsConstNull() {
		return vector.NewConstNull(boolType, v1.Length(), proc.Mp()), nil
	}
	if v1.IsConst() {
		return vector.NewConstFixed(boolType, !vector.GetFixedAt[bool](v1, 0), v1.Length(), proc.Mp()), nil
	}
	length := v1.Length()
	vec := allocateBoolVector(length, proc)
	nulls.Or(v1.GetNulls(), nil, vec.GetNulls())
	if err := logical.Not(v1, vec); err != nil {
		return nil, err
	}
	return vec, nil
}
