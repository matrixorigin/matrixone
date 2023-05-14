// Copyright 2021 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ = optimizedTpsToTrFn[bool, bool]

type templateTp interface {
	bool
}

type templateTr interface {
	bool
}

// XXX it's just a function template file.
// Still in test phase, plz use it carefully.
func optimizedTpsToTrFn[T1 templateTp, T2 templateTr](
	fValueNull func(v1 T1) (T2, bool), alwaysNull1 bool,
	fNullValue func(v2 T1) (T2, bool), alwaysNull2 bool,
	fValueValue func(v1, v2 T1) (T2, bool), neverNull bool,
	fNullNull func() (T1, bool), alwaysNull3 bool, canFold bool) func(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	if !canFold {
		panic("cannot support template for function cannot fold now")
	}

	// very basic template.
	// if any one of params was null, result is null.
	// and never occurs null if all params were not null.
	if alwaysNull1 && alwaysNull2 && alwaysNull3 && neverNull {
		return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
			p1 := vector.GenerateFunctionFixedTypeParameter[T1](parameters[0])
			p2 := vector.GenerateFunctionFixedTypeParameter[T1](parameters[1])
			rs := vector.MustFunctionResult[T2](result)
			rsVec := rs.GetResultVector()
			rss := vector.MustFixedCol[T2](rsVec)

			c1, c2 := parameters[0].IsConst(), parameters[0].IsConst()
			if c1 && c2 {
				v1, null1 := p1.GetValue(0)
				v2, null2 := p2.GetValue(0)
				ifNull := null1 || null2
				if ifNull {
					nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
				} else {
					r, _ := fValueValue(v1, v2)
					for i := uint64(0); i < uint64(length); i++ {
						rss[i] = r
					}
				}
				return nil
			}

			if c1 {
				v1, null1 := p1.GetValue(0)
				if null1 {
					nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
				} else {
					if p2.WithAnyNullValue() {
						rsVec.GetNulls().Or(parameters[1].GetNulls())
						for i := uint64(0); i < uint64(length); i++ {
							v2, null2 := p2.GetValue(i)
							if null2 {
								continue
							}
							r, _ := fValueValue(v1, v2)
							rss[i] = r
						}
					} else {
						for i := uint64(0); i < uint64(length); i++ {
							v2, _ := p2.GetValue(i)
							rss[i], _ = fValueValue(v1, v2)
						}
					}
				}
				return nil
			}

			if c2 {
				v2, null2 := p2.GetValue(0)
				if null2 {
					nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
				} else {
					if p1.WithAnyNullValue() {
						rsVec.GetNulls().Or(parameters[0].GetNulls())
						for i := uint64(0); i < uint64(length); i++ {
							v1, null1 := p2.GetValue(i)
							if null1 {
								continue
							}
							r, _ := fValueValue(v1, v2)
							rss[i] = r
						}
					} else {
						for i := uint64(0); i < uint64(length); i++ {
							v1, _ := p1.GetValue(i)
							rss[i], _ = fValueValue(v1, v2)
						}
					}
				}
				return nil
			}

			// basic case.
			if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
				nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
			}

			for i := uint64(0); i < uint64(length); i++ {
				v1, _ := p1.GetValue(i)
				v2, _ := p2.GetValue(i)
				rss[i], _ = fValueValue(v1, v2)
			}
			return nil
		}
	}

	// return null if all params were null. but not certain if only one param was null.
	// result will be not null if all params were not null.
	if alwaysNull3 && neverNull && (!alwaysNull1 && !alwaysNull2) {
		return func(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
			p1 := vector.GenerateFunctionFixedTypeParameter[T1](parameters[0])
			p2 := vector.GenerateFunctionFixedTypeParameter[T1](parameters[1])
			rs := vector.MustFunctionResult[T2](result)
			rsVec := rs.GetResultVector()
			rss := vector.MustFixedCol[T2](rsVec)

			c1, c2 := parameters[0].IsConst(), parameters[0].IsConst()

			if c1 && c2 {
				v1, null1 := p1.GetValue(0)
				v2, null2 := p2.GetValue(0)
				if null1 && null2 {
					nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
				} else if null1 {
					v, rnull := fNullValue(v2)
					if rnull {
						nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
					} else {
						for i := uint64(0); i < uint64(length); i++ {
							rss[i] = v
						}
					}
				} else if null2 {
					v, rnull := fNullValue(v1)
					if rnull {
						nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
					} else {
						for i := uint64(0); i < uint64(length); i++ {
							rss[i] = v
						}
					}
				} else {
					v, _ := fValueValue(v1, v2)
					for i := uint64(0); i < uint64(length); i++ {
						rss[i] = v
					}
				}

				return nil
			}

			if c1 {
				v1, null1 := p1.GetValue(0)
				if !null1 {
					if p2.WithAnyNullValue() {
						rsVec.GetNulls().Or(parameters[1].GetNulls())
						for i := uint64(0); i < uint64(length); i++ {
							v2, null2 := p2.GetValue(i)
							if null2 {
								r, ifnull := fValueNull(v1)
								if !ifnull {
									rsVec.GetNulls().Np.Remove(i)
									rss[i] = r
								}
							} else {
								rss[i], _ = fValueValue(v1, v2)
							}
						}
					} else {
						for i := uint64(0); i < uint64(length); i++ {
							v2, _ := p2.GetValue(i)
							rss[i], _ = fValueValue(v1, v2)
						}
					}
					return nil

				} else {
					if p2.WithAnyNullValue() {
						rsVec.GetNulls().Or(parameters[1].GetNulls())
					}
					for i := uint64(0); i < uint64(length); i++ {
						v2, null2 := p2.GetValue(i)
						if null2 {
							continue
						} else {
							r, ifnull := fNullValue(v2)
							if ifnull {
								rsVec.GetNulls().Np.Add(i)
							} else {
								rss[i] = r
							}
						}
					}
				}
				return nil
			}

			if c2 {
				v2, null2 := p2.GetValue(0)
				if !null2 {
					if p1.WithAnyNullValue() {
						rsVec.GetNulls().Or(parameters[0].GetNulls())
						for i := uint64(0); i < uint64(length); i++ {
							v1, null1 := p2.GetValue(i)
							if null1 {
								r, ifnull := fNullValue(v2)
								if !ifnull {
									rsVec.GetNulls().Np.Remove(i)
									rss[i] = r
								}
							} else {
								rss[i], _ = fValueValue(v1, v2)
							}
						}
					} else {
						for i := uint64(0); i < uint64(length); i++ {
							v1, _ := p1.GetValue(i)
							rss[i], _ = fValueValue(v1, v2)
						}
					}
					return nil

				} else {
					if p1.WithAnyNullValue() {
						rsVec.GetNulls().Or(parameters[0].GetNulls())
					}
					for i := uint64(0); i < uint64(length); i++ {
						v1, null1 := p1.GetValue(i)
						if null1 {
							continue
						} else {
							r, ifnull := fValueNull(v1)
							if ifnull {
								rsVec.GetNulls().Np.Add(i)
							} else {
								rss[i] = r
							}
						}
					}
				}
				return nil
			}

			// normal vector op normal vector
			if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
				nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())

				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					v2, null2 := p2.GetValue(i)
					if null1 && !null2 {
						r, rnull := fNullValue(v2)
						if !rnull {
							rsVec.GetNulls().Np.Remove(i)
							rss[i] = r
						}
					} else if null2 && !null1 {
						r, rnull := fValueNull(v1)
						if !rnull {
							rsVec.GetNulls().Np.Remove(i)
							rss[i] = r
						}
					} else if !null1 && !null2 {
						rss[i], _ = fValueValue(v1, v2)
					}
				}
				return nil
			}

			for i := uint64(0); i < uint64(length); i++ {
				if rsVec.GetNulls().Contains(i) {
					continue
				}
				v1, _ := p1.GetValue(i)
				v2, _ := p2.GetValue(i)
				rss[i], _ = fValueValue(v1, v2)
			}
			return nil
		}
	}

	panic("unsupported function template.")
}
