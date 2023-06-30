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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type templateTp1 interface {
	bool
}

type templateTr1 interface {
	bool
}

// For sca.
var _ = opBinaryFixedStrToFixedWithErrorCheck[bool, bool]
var _ = opNoneParamToBytesWithErrorCheck

// I hope it can generate all functions according to some easy parameters.
// not yet ok. and may change soon. plz use it carefully if you really need it.
func generalFunctionTemplateFactor[T1 templateTp1, T2 templateTr1](
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

			c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
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
						nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
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
						nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
						for i := uint64(0); i < uint64(length); i++ {
							v1, null1 := p1.GetValue(i)
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

			c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()

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
					v, rnull := fValueNull(v1)
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
						nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
						for i := uint64(0); i < uint64(length); i++ {
							v2, null2 := p2.GetValue(i)
							if null2 {
								r, ifnull := fValueNull(v1)
								if !ifnull {
									rsVec.GetNulls().Del(i)
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
				} else {
					if p2.WithAnyNullValue() {
						nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
						for i := uint64(0); i < uint64(length); i++ {
							v2, null2 := p2.GetValue(i)
							if null2 {
								continue
							} else {
								r, ifnull := fNullValue(v2)
								if ifnull {
									rsVec.GetNulls().Add(i)
								} else {
									rss[i] = r
								}
							}
						}
					} else {
						if rsVec.GetNulls() == nil {
							rsVec.SetNulls(nulls.NewWithSize(0))
						}
						for i := uint64(0); i < uint64(length); i++ {
							v2, _ := p2.GetValue(i)
							r, ifnull := fNullValue(v2)
							if ifnull {
								rsVec.GetNulls().Add(i)
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
						nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
						for i := uint64(0); i < uint64(length); i++ {
							v1, null1 := p1.GetValue(i)
							if null1 {
								r, ifnull := fNullValue(v2)
								if !ifnull {
									rsVec.GetNulls().Del(i)
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
						nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
						for i := uint64(0); i < uint64(length); i++ {
							v1, null1 := p1.GetValue(i)
							if null1 {
								continue
							} else {
								r, ifnull := fValueNull(v1)
								if ifnull {
									rsVec.GetNulls().Add(i)
								} else {
									rss[i] = r
								}
							}
						}
					} else {
						if rsVec.GetNulls() == nil {
							rsVec.SetNulls(nulls.NewWithSize(0))
						}
						for i := uint64(0); i < uint64(length); i++ {
							v1, _ := p1.GetValue(i)
							r, ifnull := fValueNull(v1)
							if ifnull {
								rsVec.GetNulls().Add(i)
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
							rsVec.GetNulls().Del(i)
							rss[i] = r
						}
					} else if null2 && !null1 {
						r, rnull := fValueNull(v1)
						if !rnull {
							rsVec.GetNulls().Del(i)
							rss[i] = r
						}
					} else if !null1 && !null2 {
						rss[i], _ = fValueValue(v1, v2)
					}
				}
				return nil
			}

			for i := uint64(0); i < uint64(length); i++ {
				v1, _ := p1.GetValue(i)
				v2, _ := p2.GetValue(i)
				rss[i], _ = fValueValue(v1, v2)
			}
			return nil
		}
	}

	panic("unsupported function template.")
}

type templateDec interface {
	types.Decimal64 | types.Decimal128
}

func decimalArith[T templateDec](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	arithFn func(v1, v2 T, scale1, scale2 int32) (T, error)) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[T](rsVec)

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetValue(0)
		v2, null2 := p2.GetValue(0)
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r, err := arithFn(v1, v2, scale1, scale2)
			if err != nil {
				return err
			}
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
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetValue(i)
					if null2 {
						continue
					}
					r, err := arithFn(v1, v2, scale1, scale2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					r, err := arithFn(v1, v2, scale1, scale2)
					if err != nil {
						return err
					}
					rss[i] = r
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
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					if null1 {
						continue
					}
					r, err := arithFn(v1, v2, scale1, scale2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetValue(i)
					r, err := arithFn(v1, v2, scale1, scale2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())

		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 {
				continue
			}
			r, err := arithFn(v1, v2, scale1, scale2)
			if err != nil {
				return err
			}
			rss[i] = r
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		v2, _ := p2.GetValue(i)
		r, err := arithFn(v1, v2, scale1, scale2)
		if err != nil {
			return err
		}
		rss[i] = r
	}
	return nil
}

// XXX For decimal64 / decimal64, decimal64 * decimal64
func decimalArith2(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	arithFn func(v1, v2 types.Decimal128, scale1, scale2 int32) (types.Decimal128, error)) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[types.Decimal64](parameters[1])
	rs := vector.MustFunctionResult[types.Decimal128](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[types.Decimal128](rsVec)

	scale1 := p1.GetType().Scale
	scale2 := p2.GetType().Scale
	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetValue(0)
		v2, null2 := p2.GetValue(0)
		if null1 || null2 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			x, y := functionUtil.ConvertD64ToD128(v1), functionUtil.ConvertD64ToD128(v2)
			r, err := arithFn(x, y, scale1, scale2)
			if err != nil {
				return err
			}
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
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())

				x := functionUtil.ConvertD64ToD128(v1)
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetValue(i)
					if null2 {
						continue
					}
					y := functionUtil.ConvertD64ToD128(v2)
					r, err := arithFn(x, y, scale1, scale2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				x := functionUtil.ConvertD64ToD128(v1)
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					y := functionUtil.ConvertD64ToD128(v2)
					r, err := arithFn(x, y, scale1, scale2)
					if err != nil {
						return err
					}
					rss[i] = r
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
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())

				y := functionUtil.ConvertD64ToD128(v2)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					if null1 {
						continue
					}
					x := functionUtil.ConvertD64ToD128(v1)
					r, err := arithFn(x, y, scale1, scale2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				y := functionUtil.ConvertD64ToD128(v2)
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetValue(i)
					x := functionUtil.ConvertD64ToD128(v1)
					r, err := arithFn(x, y, scale1, scale2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())

		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 {
				continue
			}
			x, y := functionUtil.ConvertD64ToD128(v1), functionUtil.ConvertD64ToD128(v2)
			r, err := arithFn(x, y, scale1, scale2)
			if err != nil {
				return err
			}
			rss[i] = r
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		v2, _ := p2.GetValue(i)
		x, y := functionUtil.ConvertD64ToD128(v1), functionUtil.ConvertD64ToD128(v2)
		r, err := arithFn(x, y, scale1, scale2)
		if err != nil {
			return err
		}
		rss[i] = r
	}
	return nil
}

// opBinaryFixedFixedToFixed for binary functions whose
// result of f(x, y) is null if any one of x, y is null value.
// and if x, y were all normal value, result will not be an error.
func opBinaryFixedFixedToFixed[
	T1 types.FixedSizeTExceptStrType,
	T2 types.FixedSizeTExceptStrType,
	Tr types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v1 T1, v2 T2) Tr) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T1](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T2](parameters[1])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetValue(0)
		v2, null2 := p2.GetValue(0)
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := resultFn(v1, v2)
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
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetValue(i)
					if null2 {
						continue
					}
					rss[i] = resultFn(v1, v2)
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					rss[i] = resultFn(v1, v2)
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
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					if null1 {
						continue
					}
					rss[i] = resultFn(v1, v2)
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetValue(i)
					rss[i] = resultFn(v1, v2)
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 {
				continue
			}
			rss[i] = resultFn(v1, v2)
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		v2, _ := p2.GetValue(i)
		rss[i] = resultFn(v1, v2)
	}
	return nil
}

func opBinaryFixedFixedToFixedWithErrorCheck[
	T1 types.FixedSizeTExceptStrType,
	T2 types.FixedSizeTExceptStrType,
	Tr types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v1 T1, v2 T2) (Tr, error)) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T1](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T2](parameters[1])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetValue(0)
		v2, null2 := p2.GetValue(0)
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r, err := resultFn(v1, v2)
			if err != nil {
				return err
			}
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
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetValue(i)
					if null2 {
						continue
					}
					r, err := resultFn(v1, v2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					r, err := resultFn(v1, v2)
					if err != nil {
						return err
					}
					rss[i] = r
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
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					if null1 {
						continue
					}
					r, err := resultFn(v1, v2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetValue(i)
					r, err := resultFn(v1, v2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 {
				continue
			}
			r, err := resultFn(v1, v2)
			if err != nil {
				return err
			}
			rss[i] = r
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		v2, _ := p2.GetValue(i)
		r, err := resultFn(v1, v2)
		if err != nil {
			return err
		}
		rss[i] = r
	}
	return nil
}

func opBinaryStrFixedToFixedWithErrorCheck[
	T2 types.FixedSizeTExceptStrType,
	Tr types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v1 string, v2 T2) (Tr, error)) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T2](parameters[1])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetStrValue(0)
		v2, null2 := p2.GetValue(0)
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r, err := resultFn(functionUtil.QuickBytesToStr(v1), v2)
			if err != nil {
				return err
			}
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			if p2.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetValue(i)
					if null2 {
						continue
					}
					r, err := resultFn(functionUtil.QuickBytesToStr(v1), v2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				rv1 := functionUtil.QuickBytesToStr(v1)
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					r, err := resultFn(rv1, v2)
					if err != nil {
						return err
					}
					rss[i] = r
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
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					if null1 {
						continue
					}
					r, err := resultFn(functionUtil.QuickBytesToStr(v1), v2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetStrValue(i)
					r, err := resultFn(functionUtil.QuickBytesToStr(v1), v2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 {
				continue
			}
			r, err := resultFn(functionUtil.QuickBytesToStr(v1), v2)
			if err != nil {
				return err
			}
			rss[i] = r
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		v2, _ := p2.GetValue(i)
		r, err := resultFn(functionUtil.QuickBytesToStr(v1), v2)
		if err != nil {
			return err
		}
		rss[i] = r
	}
	return nil
}

func opBinaryFixedStrToFixedWithErrorCheck[
	T1 types.FixedSizeTExceptStrType,
	Tr types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v1 T1, v2 string) (Tr, error)) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T1](parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetValue(0)
		v2, null2 := p2.GetStrValue(0)
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r, err := resultFn(v1, functionUtil.QuickBytesToStr(v2))
			if err != nil {
				return err
			}
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
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetStrValue(i)
					if null2 {
						continue
					}
					r, err := resultFn(v1, functionUtil.QuickBytesToStr(v2))
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetStrValue(i)
					r, err := resultFn(v1, functionUtil.QuickBytesToStr(v2))
					if err != nil {
						return err
					}
					rss[i] = r
				}
			}
		}
		return nil
	}

	if c2 {
		v2, null2 := p2.GetStrValue(0)
		if null2 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			rv2 := functionUtil.QuickBytesToStr(v2)
			if p1.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					if null1 {
						continue
					}
					r, err := resultFn(v1, rv2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetValue(i)
					r, err := resultFn(v1, rv2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				continue
			}
			r, err := resultFn(v1, functionUtil.QuickBytesToStr(v2))
			if err != nil {
				return err
			}
			rss[i] = r
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		v2, _ := p2.GetStrValue(i)
		r, err := resultFn(v1, functionUtil.QuickBytesToStr(v2))
		if err != nil {
			return err
		}
		rss[i] = r
	}
	return nil
}

func specialTemplateForModFunction[
	T constraints.Integer | constraints.Float](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	modFn func(v1, v2 T) T) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[T](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetValue(0)
		v2, null2 := p2.GetValue(0)
		ifNull := null1 || null2
		if ifNull || v2 == 0 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := modFn(v1, v2)
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
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetValue(i)
					if null2 {
						continue
					}
					if v2 == 0 {
						rsVec.GetNulls().Add(i)
					} else {
						rss[i] = modFn(v1, v2)
					}
				}
			} else {
				if rsVec.GetNulls() == nil {
					rsVec.SetNulls(nulls.NewWithSize(0))
				}
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					if v2 == 0 {
						rsVec.GetNulls().Add(i)
					} else {
						rss[i] = modFn(v1, v2)
					}
				}
			}
		}
		return nil
	}

	if c2 {
		v2, null2 := p2.GetValue(0)
		if null2 || v2 == 0 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			if p1.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					if null1 {
						continue
					}
					rss[i] = modFn(v1, v2)
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetValue(i)
					rss[i] = modFn(v1, v2)
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 {
				continue
			}
			if v2 == 0 {
				rsVec.GetNulls().Add(i)
			} else {
				rss[i] = modFn(v1, v2)
			}
		}
		return nil
	}

	if rsVec.GetNulls() == nil {
		rsVec.SetNulls(nulls.NewWithSize(0))
	}
	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		v2, _ := p2.GetValue(i)
		if v2 == 0 {
			rsVec.GetNulls().Add(i)
		} else {
			rss[i] = modFn(v1, v2)
		}
	}
	return nil
}

func specialTemplateForDivFunction[
	T constraints.Float, T2 constraints.Float | int64](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	divFn func(v1, v2 T) T2) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	rs := vector.MustFunctionResult[T2](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[T2](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetValue(0)
		v2, null2 := p2.GetValue(0)
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			if v2 == 0 {
				return moerr.NewDivByZeroNoCtx()
			}
			r := divFn(v1, v2)
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
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetValue(i)
					if null2 {
						continue
					}
					if v2 == 0 {
						return moerr.NewDivByZeroNoCtx()
					} else {
						rss[i] = divFn(v1, v2)
					}
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					if v2 == 0 {
						return moerr.NewDivByZeroNoCtx()
					} else {
						rss[i] = divFn(v1, v2)
					}
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
			if v2 == 0 {
				return moerr.NewDivByZeroNoCtx()
			}
			if p1.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					if null1 {
						continue
					}
					rss[i] = divFn(v1, v2)
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetValue(i)
					rss[i] = divFn(v1, v2)
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 {
				continue
			}
			if v2 == 0 {
				return moerr.NewDivByZeroNoCtx()
			} else {
				rss[i] = divFn(v1, v2)
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		v2, _ := p2.GetValue(i)
		if v2 == 0 {
			return moerr.NewDivByZeroNoCtx()
		} else {
			rss[i] = divFn(v1, v2)
		}
	}
	return nil
}

func opBinaryBytesBytesToFixed[Tr types.FixedSizeTExceptStrType](
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	arithFn func(v1, v2 []byte) Tr) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetStrValue(0)
		v2, null2 := p2.GetStrValue(0)
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := arithFn(v1, v2)
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			if p2.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetStrValue(i)
					if null2 {
						continue
					}
					rss[i] = arithFn(v1, v2)
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetStrValue(i)
					rss[i] = arithFn(v1, v2)
				}
			}
		}
		return nil
	}

	if c2 {
		v2, null2 := p2.GetStrValue(0)
		if null2 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			if p1.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					if null1 {
						continue
					}
					rss[i] = arithFn(v1, v2)
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetStrValue(i)
					rss[i] = arithFn(v1, v2)
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				continue
			}
			rss[i] = arithFn(v1, v2)
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		v2, _ := p2.GetStrValue(i)
		rss[i] = arithFn(v1, v2)
	}
	return nil
}

func opBinaryBytesBytesToFixedWithErrorCheck[Tr types.FixedSizeTExceptStrType](
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	fn func(v1, v2 []byte) (Tr, error)) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetStrValue(0)
		v2, null2 := p2.GetStrValue(0)
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r, err := fn(v1, v2)
			if err != nil {
				return err
			}
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	var err error
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			if p2.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetStrValue(i)
					if null2 {
						continue
					}
					rss[i], err = fn(v1, v2)
					if err != nil {
						return err
					}
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetStrValue(i)
					rss[i], err = fn(v1, v2)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	if c2 {
		v2, null2 := p2.GetStrValue(0)
		if null2 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			if p1.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					if null1 {
						continue
					}
					rss[i], err = fn(v1, v2)
					if err != nil {
						return err
					}
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetStrValue(i)
					rss[i], err = fn(v1, v2)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				continue
			}
			rss[i], err = fn(v1, v2)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		v2, _ := p2.GetStrValue(i)
		rss[i], err = fn(v1, v2)
		if err != nil {
			return err
		}
	}
	return nil
}

func compareVarlenaEqual(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[bool](rsVec)

	v1, v2 := parameters[0], parameters[1]
	if v1.IsConstNull() || v2.IsConstNull() {
		nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		return nil
	}
	col1, _ := vector.MustVarlenaRawData(v1)
	col2, _ := vector.MustVarlenaRawData(v2)

	if v1.IsConst() && v2.IsConst() {
		p1 := col1[0].UnsafePtr()
		p2 := col2[0].UnsafePtr()
		ret := *(*[3]int64)(p1) == *(*[3]int64)(p2)
		for i := uint64(0); i < uint64(length); i++ {
			rss[i] = ret
		}
		return nil
	}

	if !v1.IsConst() && !v2.IsConst() {
		for i := 0; i < length; i++ {
			p1 := col1[i].UnsafePtr()
			p2 := col2[i].UnsafePtr()
			rss[i] = *(*[3]int64)(p1) == *(*[3]int64)(p2)
		}
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
	} else if v1.IsConst() {
		p1 := col1[0].UnsafePtr()
		for i := 0; i < length; i++ {
			p2 := col2[i].UnsafePtr()
			rss[i] = *(*[3]int64)(p1) == *(*[3]int64)(p2)
		}
		nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
	} else {
		p2 := col2[0].UnsafePtr()
		for i := 0; i < length; i++ {
			p1 := col1[i].UnsafePtr()
			rss[i] = *(*[3]int64)(p1) == *(*[3]int64)(p2)
		}
		nulls.Or(parameters[0].GetNulls(), rsVec.GetNulls(), rsVec.GetNulls())
	}
	return nil
}

func opBinaryStrStrToFixed[Tr types.FixedSizeTExceptStrType](
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	arithFn func(v1, v2 string) Tr) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetStrValue(0)
		v2, null2 := p2.GetStrValue(0)
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := arithFn(functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2))
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			x := functionUtil.QuickBytesToStr(v1)
			if p2.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetStrValue(i)
					if null2 {
						continue
					}
					rss[i] = arithFn(x, functionUtil.QuickBytesToStr(v2))
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetStrValue(i)
					rss[i] = arithFn(x, functionUtil.QuickBytesToStr(v2))
				}
			}
		}
		return nil
	}

	if c2 {
		v2, null2 := p2.GetStrValue(0)
		if null2 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			y := functionUtil.QuickBytesToStr(v2)
			if p1.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					if null1 {
						continue
					}
					rss[i] = arithFn(functionUtil.QuickBytesToStr(v1), y)
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetStrValue(i)
					rss[i] = arithFn(functionUtil.QuickBytesToStr(v1), y)
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				continue
			}
			rss[i] = arithFn(functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2))
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		v2, _ := p2.GetStrValue(i)
		rss[i] = arithFn(functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2))
	}
	return nil
}

func opBinaryStrStrToFixedWithErrorCheck[Tr types.FixedSizeTExceptStrType](
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	fn func(v1, v2 string) (Tr, error)) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1, c2 := parameters[0].IsConst(), parameters[1].IsConst()
	if c1 && c2 {
		v1, null1 := p1.GetStrValue(0)
		v2, null2 := p2.GetStrValue(0)
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r, err := fn(functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2))
			if err != nil {
				return err
			}
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	var err error
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			x := functionUtil.QuickBytesToStr(v1)
			if p2.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetStrValue(i)
					if null2 {
						continue
					}
					rss[i], err = fn(x, functionUtil.QuickBytesToStr(v2))
					if err != nil {
						return err
					}
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetStrValue(i)
					rss[i], err = fn(x, functionUtil.QuickBytesToStr(v2))
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	if c2 {
		v2, null2 := p2.GetStrValue(0)
		if null2 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			y := functionUtil.QuickBytesToStr(v2)
			if p1.WithAnyNullValue() {
				nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					if null1 {
						continue
					}
					rss[i], err = fn(functionUtil.QuickBytesToStr(v1), y)
					if err != nil {
						return err
					}
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetStrValue(i)
					rss[i], err = fn(functionUtil.QuickBytesToStr(v1), y)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() || p2.WithAnyNullValue() {
		nulls.Or(parameters[0].GetNulls(), parameters[1].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				continue
			}
			rss[i], err = fn(functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2))
			if err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		v2, _ := p2.GetStrValue(i)
		rss[i], err = fn(functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2))
		if err != nil {
			return err
		}
	}
	return nil
}

// opUnaryFixedToFixed for unary functions whose result of f(x) is null if x is null.
// and if x was not null, result will be not null.
func opUnaryFixedToFixed[
	T types.FixedSizeTExceptStrType,
	Tr types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v T) Tr) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := resultFn(v1)
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			if null1 {
				continue
			}
			rss[i] = resultFn(v1)
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		rss[i] = resultFn(v1)
	}
	return nil
}

func opUnaryBytesToFixed[
	Tr types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v []byte) Tr) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := resultFn(v1)
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			if null1 {
				continue
			}
			rss[i] = resultFn(v1)
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		rss[i] = resultFn(v1)
	}
	return nil
}

func opUnaryStrToFixed[
	Tr types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v string) Tr) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := resultFn(functionUtil.QuickBytesToStr(v1))
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			if null1 {
				continue
			}
			rss[i] = resultFn(functionUtil.QuickBytesToStr(v1))
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		rss[i] = resultFn(functionUtil.QuickBytesToStr(v1))
	}
	return nil
}

func opUnaryBytesToBytes(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v []byte) []byte) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	rsVec := rs.GetResultVector()

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := resultFn(v1)

			for i := uint64(0); i < uint64(length); i++ {
				if err := rs.AppendMustBytesValue(r); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			if null1 {
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
			} else {
				r := resultFn(v1)
				if err := rs.AppendMustBytesValue(r); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		r := resultFn(v1)
		if err := rs.AppendMustBytesValue(r); err != nil {
			return err
		}
	}
	return nil
}

func opUnaryBytesToStr(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v []byte) string) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	rsVec := rs.GetResultVector()

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := resultFn(v1)

			for i := uint64(0); i < uint64(length); i++ {
				if err := rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(r)); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			if null1 {
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
			} else {
				r := resultFn(v1)
				if err := rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(r)); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		r := resultFn(v1)
		if err := rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(r)); err != nil {
			return err
		}
	}
	return nil
}

func opUnaryStrToStr(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v string) string) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	rsVec := rs.GetResultVector()

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := resultFn(functionUtil.QuickBytesToStr(v1))

			for i := uint64(0); i < uint64(length); i++ {
				if err := rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(r)); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			if null1 {
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
			} else {
				r := resultFn(functionUtil.QuickBytesToStr(v1))
				if err := rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(r)); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		r := resultFn(functionUtil.QuickBytesToStr(v1))
		if err := rs.AppendMustBytesValue(functionUtil.QuickStrToBytes(r)); err != nil {
			return err
		}
	}
	return nil
}

func opUnaryFixedToStr[
	T types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v T) string) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	rsVec := rs.GetResultVector()

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			rb := resultFn(v1)
			r := functionUtil.QuickStrToBytes(rb)

			for i := uint64(0); i < uint64(length); i++ {
				if err := rs.AppendMustBytesValue(r); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			if null1 {
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
			} else {
				rb := resultFn(v1)
				r := functionUtil.QuickStrToBytes(rb)
				if err := rs.AppendMustBytesValue(r); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		rb := resultFn(v1)
		r := functionUtil.QuickStrToBytes(rb)
		if err := rs.AppendMustBytesValue(r); err != nil {
			return err
		}
	}
	return nil
}

func opUnaryFixedToStrWithErrorCheck[
	T types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v T) (string, error)) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	rsVec := rs.GetResultVector()

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			rb, err := resultFn(v1)
			if err != nil {
				return err
			}
			r := functionUtil.QuickStrToBytes(rb)

			for i := uint64(0); i < uint64(length); i++ {
				if err = rs.AppendMustBytesValue(r); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			if null1 {
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
			} else {
				rb, err := resultFn(v1)
				if err != nil {
					return err
				}
				r := functionUtil.QuickStrToBytes(rb)
				if err = rs.AppendMustBytesValue(r); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		rb, err := resultFn(v1)
		if err != nil {
			return err
		}
		r := functionUtil.QuickStrToBytes(rb)
		if err = rs.AppendMustBytesValue(r); err != nil {
			return err
		}
	}
	return nil
}

func opUnaryStrToBytesWithErrorCheck(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v string) ([]byte, error)) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	rsVec := rs.GetResultVector()

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r, err := resultFn(functionUtil.QuickBytesToStr(v1))
			if err != nil {
				return err
			}

			for i := uint64(0); i < uint64(length); i++ {
				if err = rs.AppendMustBytesValue(r); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			if null1 {
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
			} else {
				r, err := resultFn(functionUtil.QuickBytesToStr(v1))
				if err != nil {
					return err
				}
				if err = rs.AppendMustBytesValue(r); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		r, err := resultFn(functionUtil.QuickBytesToStr(v1))
		if err != nil {
			return err
		}
		if err = rs.AppendMustBytesValue(r); err != nil {
			return err
		}
	}
	return nil
}

func opUnaryBytesToStrWithErrorCheck(
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v []byte) (string, error)) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	rsVec := rs.GetResultVector()

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			rb, err := resultFn(v1)
			if err != nil {
				return err
			}
			r := functionUtil.QuickStrToBytes(rb)
			for i := uint64(0); i < uint64(length); i++ {
				if err = rs.AppendMustBytesValue(r); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// basic case.
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			if null1 {
				if err := rs.AppendMustNullForBytesResult(); err != nil {
					return err
				}
			} else {
				rb, err := resultFn(v1)
				if err != nil {
					return err
				}
				r := functionUtil.QuickStrToBytes(rb)
				if err = rs.AppendMustBytesValue(r); err != nil {
					return err
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		rb, err := resultFn(v1)
		if err != nil {
			return err
		}
		r := functionUtil.QuickStrToBytes(rb)
		if err = rs.AppendMustBytesValue(r); err != nil {
			return err
		}
	}
	return nil
}

func opUnaryFixedToFixedWithErrorCheck[
	T types.FixedSizeTExceptStrType,
	Tr types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v T) (Tr, error)) error {
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r, err := resultFn(v1)
			if err != nil {
				return err
			}
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	// basic case.
	var err error
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetValue(i)
			if null1 {
				continue
			}
			rss[i], err = resultFn(v1)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		rss[i], err = resultFn(v1)
		if err != nil {
			return err
		}
	}
	return nil
}

func opUnaryBytesToFixedWithErrorCheck[
	Tr types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v []byte) (Tr, error)) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r, err := resultFn(v1)
			if err != nil {
				return err
			}
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	// basic case.
	var err error
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			if null1 {
				continue
			}
			rss[i], err = resultFn(v1)
			if err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		rss[i], err = resultFn(v1)
		if err != nil {
			return err
		}
	}
	return nil
}

func opUnaryStrToFixedWithErrorCheck[
	Tr types.FixedSizeTExceptStrType](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	resultFn func(v string) (Tr, error)) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	c1 := parameters[0].IsConst()
	if c1 {
		v1, null1 := p1.GetStrValue(0)
		if null1 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r, err := resultFn(functionUtil.QuickBytesToStr(v1))
			if err != nil {
				return err
			}
			for i := uint64(0); i < uint64(length); i++ {
				rss[i] = r
			}
		}
		return nil
	}

	// basic case.
	var err error
	if p1.WithAnyNullValue() {
		nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			if null1 {
				continue
			}
			rss[i], err = resultFn(functionUtil.QuickBytesToStr(v1))
			if err != nil {
				return err
			}
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetStrValue(i)
		rss[i], err = resultFn(functionUtil.QuickBytesToStr(v1))
		if err != nil {
			return err
		}
	}
	return nil
}

func opNoneParamToFixed[Tr types.FixedSizeTExceptStrType](
	result vector.FunctionResultWrapper, proc *process.Process, length int, resultFn func() Tr) error {
	rs := vector.MustFunctionResult[Tr](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[Tr](rsVec)

	for i := 0; i < length; i++ {
		rss[i] = resultFn()
	}
	return nil
}

func opNoneParamToBytes(
	result vector.FunctionResultWrapper, proc *process.Process, length int, resultFn func() []byte) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := 0; i < length; i++ {
		if err := rs.AppendMustBytesValue(resultFn()); err != nil {
			return err
		}
	}
	return nil
}

func opNoneParamToBytesWithErrorCheck(
	result vector.FunctionResultWrapper, proc *process.Process, length int, resultFn func() ([]byte, error)) error {
	rs := vector.MustFunctionResult[types.Varlena](result)

	for i := 0; i < length; i++ {
		r, err := resultFn()
		if err != nil {
			return err
		}
		if err = rs.AppendMustBytesValue(r); err != nil {
			return err
		}
	}
	return nil
}
