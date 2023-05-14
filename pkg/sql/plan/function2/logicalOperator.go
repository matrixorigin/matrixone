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

package function2

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func andFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[1])
	rs := vector.MustFunctionResult[bool](result)

	if parameters[0].IsConst() {
		v1, null1 := p1.GetValue(0)
		if null1 {
			for i := uint64(0); i < uint64(length); i++ {
				v2, null2 := p2.GetValue(i)
				if err := rs.Append(v2, v2 || null2); err != nil {
					return err
				}
			}
		} else {
			if v1 {
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetValue(i)
					if err := rs.Append(v2, null2); err != nil {
						return err
					}
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					if err := rs.Append(false, false); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	if parameters[1].IsConst() {
		v2, null2 := p2.GetValue(0)
		if null2 {
			for i := uint64(0); i < uint64(length); i++ {
				v1, null1 := p1.GetValue(i)
				if err := rs.Append(v1, v1 || null1); err != nil {
					return err
				}
			}
		} else {
			if v2 {
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					if err := rs.Append(v1, null1); err != nil {
						return err
					}
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					if err := rs.Append(false, false); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if null1 {
			if err := rs.Append(v2, v2 || null2); err != nil {
				return err
			}
		} else {
			if v1 {
				if err := rs.Append(v2, null2); err != nil {
					return err
				}
			} else {
				if err := rs.Append(false, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func orFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[1])
	rs := vector.MustFunctionResult[bool](result)

	if parameters[0].IsConst() {
		v1, null1 := p1.GetValue(0)
		if v1 {
			for i := uint64(0); i < uint64(length); i++ {
				if err := rs.Append(true, false); err != nil {
					return err
				}
			}
		} else {
			if null1 {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					if v2 {
						if err := rs.Append(true, false); err != nil {
							return err
						}
					} else {
						if err := rs.Append(false, true); err != nil {
							return err
						}
					}
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetValue(i)
					if v2 {
						if err := rs.Append(true, false); err != nil {
							return err
						}
					} else {
						if err := rs.Append(false, null2); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	}

	if parameters[1].IsConst() {
		v2, null2 := p2.GetValue(0)
		if v2 {
			for i := uint64(0); i < uint64(length); i++ {
				if err := rs.Append(true, false); err != nil {
					return err
				}
			}
		} else {
			if null2 {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetValue(i)
					if v1 {
						if err := rs.Append(true, false); err != nil {
							return err
						}
					} else {
						if err := rs.Append(false, true); err != nil {
							return err
						}
					}
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					if v1 {
						if err := rs.Append(true, false); err != nil {
							return err
						}
					} else {
						if err := rs.Append(false, null1); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if v1 || v2 {
			if err := rs.Append(true, false); err != nil {
				return err
			}
		} else {
			if err := rs.Append(false, null1 || null2); err != nil {
				return err
			}
		}

	}
	return nil
}

func xorFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[1])
	rs := vector.MustFunctionResult[bool](result)

	if parameters[0].IsConst() {
		v1, null1 := p1.GetValue(0)
		if null1 {
			for i := uint64(0); i < uint64(length); i++ {
				if err := rs.Append(false, true); err != nil {
					return err
				}
			}
		} else {
			for i := uint64(0); i < uint64(length); i++ {
				v2, null2 := p2.GetValue(i)
				if err := rs.Append(v1 != v2, null2); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if parameters[1].IsConst() {
		v2, null2 := p2.GetValue(0)
		if null2 {
			for i := uint64(0); i < uint64(length); i++ {
				if err := rs.Append(false, true); err != nil {
					return err
				}
			}
		} else {
			for i := uint64(0); i < uint64(length); i++ {
				v1, null1 := p1.GetValue(i)
				if err := rs.Append(v2 != v1, null1); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		v2, null2 := p2.GetValue(i)
		if err := rs.Append(v1 != v2, null1 || null2); err != nil {
			return err
		}
	}
	return nil
}

func notFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
	rs := vector.MustFunctionResult[bool](result)

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetValue(i)
		if err := rs.Append(!v1, null1); err != nil {
			return err
		}
	}
	return nil
}
