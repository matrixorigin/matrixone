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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function2/function2Util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

type templateTp1 interface {
	bool
}

type templateTr1 interface {
	bool
}

// XXX it's just a function template file.
// Still in test phase, plz use it carefully.
func optimizedTpsToTrFn[T1 templateTp1, T2 templateTr1](
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
									rsVec.GetNulls().Np.Add(i)
								} else {
									rss[i] = r
								}
							}
						}
					} else {
						if rsVec.GetNulls().Np == nil {
							rsVec.SetNulls(nulls.NewWithSize(0))
						}
						for i := uint64(0); i < uint64(length); i++ {
							v2, _ := p2.GetValue(i)
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
						nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
						for i := uint64(0); i < uint64(length); i++ {
							v1, null1 := p1.GetValue(i)
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
						nulls.Or(rsVec.GetNulls(), parameters[0].GetNulls(), rsVec.GetNulls())
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
					} else {
						if rsVec.GetNulls().Np == nil {
							rsVec.SetNulls(nulls.NewWithSize(0))
						}
						for i := uint64(0); i < uint64(length); i++ {
							v1, _ := p1.GetValue(i)
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
		ifNull := null1 || null2
		if ifNull {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			x, y := function2Util.ConvertD64ToD128(v1), function2Util.ConvertD64ToD128(v2)
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

				x := function2Util.ConvertD64ToD128(v1)
				for i := uint64(0); i < uint64(length); i++ {
					v2, null2 := p2.GetValue(i)
					if null2 {
						continue
					}
					y := function2Util.ConvertD64ToD128(v2)
					r, err := arithFn(x, y, scale1, scale2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				x := function2Util.ConvertD64ToD128(v1)
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					y := function2Util.ConvertD64ToD128(v2)
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

				y := function2Util.ConvertD64ToD128(v2)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetValue(i)
					if null1 {
						continue
					}
					x := function2Util.ConvertD64ToD128(v1)
					r, err := arithFn(x, y, scale1, scale2)
					if err != nil {
						return err
					}
					rss[i] = r
				}
			} else {
				y := function2Util.ConvertD64ToD128(v2)
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetValue(i)
					x := function2Util.ConvertD64ToD128(v1)
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
			x, y := function2Util.ConvertD64ToD128(v1), function2Util.ConvertD64ToD128(v2)
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
		x, y := function2Util.ConvertD64ToD128(v1), function2Util.ConvertD64ToD128(v2)
		r, err := arithFn(x, y, scale1, scale2)
		if err != nil {
			return err
		}
		rss[i] = r
	}
	return nil
}

func optimizedTypeArith1[
	T constraints.Integer | constraints.Float | types.Date | types.Datetime | types.Time | types.Timestamp | types.Uuid | bool,
	T2 constraints.Integer | constraints.Float | bool](parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int,
	arithFn func(v1, v2 T) T2) error {
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
			r := arithFn(v1, v2)
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
					rss[i] = arithFn(v1, v2)
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					rss[i] = arithFn(v1, v2)
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
					rss[i] = arithFn(v1, v2)
				}
			} else {
				for i := uint64(0); i < uint64(length); i++ {
					v1, _ := p1.GetValue(i)
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
			v1, null1 := p1.GetValue(i)
			v2, null2 := p2.GetValue(i)
			if null1 || null2 {
				continue
			}
			rss[i] = arithFn(v1, v2)
		}
		return nil
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		v2, _ := p2.GetValue(i)
		rss[i] = arithFn(v1, v2)
	}
	return nil
}

func optimizedTypeArithMod[
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
						rsVec.GetNulls().Np.Add(i)
					} else {
						rss[i] = modFn(v1, v2)
					}
				}
			} else {
				if rsVec.GetNulls().Np == nil {
					rsVec.SetNulls(nulls.NewWithSize(0))
				}
				for i := uint64(0); i < uint64(length); i++ {
					v2, _ := p2.GetValue(i)
					if v2 == 0 {
						rsVec.GetNulls().Np.Add(i)
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
				rsVec.GetNulls().Np.Add(i)
			} else {
				rss[i] = modFn(v1, v2)
			}
		}
		return nil
	}

	if rsVec.GetNulls().Np == nil {
		rsVec.SetNulls(nulls.NewWithSize(0))
	}
	for i := uint64(0); i < uint64(length); i++ {
		v1, _ := p1.GetValue(i)
		v2, _ := p2.GetValue(i)
		if v2 == 0 {
			rsVec.GetNulls().Np.Add(i)
		} else {
			rss[i] = modFn(v1, v2)
		}
	}
	return nil
}

func optimizedTypeArithDiv[
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
