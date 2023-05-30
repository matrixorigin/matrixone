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

package function

import (
	"bytes"
	"fmt"
	"regexp"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	DefaultEscapeChar = '\\'
)

type opBuiltInRegexp struct {
	regMap regexpSet
}

func newOpBuiltInRegexp() *opBuiltInRegexp {
	return &opBuiltInRegexp{
		regMap: regexpSet{
			mp: make(map[string]*regexp.Regexp),
		},
	}
}

func (op *opBuiltInRegexp) likeFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)

	// optimize rule for some special case.
	if parameters[1].IsConst() {
		canOptimize, err := optimizeRuleForLike(p1, p2, rs, length, func(i []byte) []byte {
			return i
		})
		if canOptimize {
			return err
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			rs.AppendMustNull()
		} else {
			match, err := op.regMap.regularMatchForLikeOp(v2, v1)
			if err != nil {
				return err
			}
			rs.AppendMustValue(match)
		}
	}
	return nil
}

func (op *opBuiltInRegexp) iLikeFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)

	// optimize rule for some special case.
	if parameters[1].IsConst() {
		canOptimize, err := optimizeRuleForLike(p1, p2, rs, length, func(i []byte) []byte {
			return bytes.ToLower(i)
		})
		if canOptimize {
			return err
		}
	}

	for i := uint64(0); i < uint64(length); i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			rs.AppendMustNull()
		} else {
			match, err := op.regMap.regularMatchForLikeOp(bytes.ToLower(v2), bytes.ToLower(v1))
			if err != nil {
				return err
			}
			rs.AppendMustValue(match)
		}
	}
	return nil
}

func optimizeRuleForLike(p1, p2 vector.FunctionParameterWrapper[types.Varlena], rs *vector.FunctionResult[bool], length int,
	specialFnForV func([]byte) []byte) (bool, error) {
	pat, null := p2.GetStrValue(0)
	if null {
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.Append(false, true); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	pat = specialFnForV(pat)

	n := len(pat)
	// opt rule #1: if expr is empty string, only empty string like empty string.
	if n == 0 {
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v1 = specialFnForV(v1)
			if err := rs.Append(len(v1) == 0, null1); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	// opt rule #2.1: anything matches %
	if n == 1 && pat[0] == '%' {
		for i := uint64(0); i < uint64(length); i++ {
			_, null1 := p1.GetStrValue(i)
			if err := rs.Append(true, null1); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	// opt rule #2.2: single char matches _
	// XXX in UTF8 world, should we do single RUNE matches _?
	if n == 1 && pat[0] == '_' {
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v1 = specialFnForV(v1)
			if err := rs.Append(len(v1) == 1, null1); err != nil {
				return true, err
			}
		}
		return true, nil
	}
	// opt rule #2.3: single char, no wild card, so it is a simple compare eq.
	if n == 1 && pat[0] != '_' && pat[0] != '%' {
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v1 = specialFnForV(v1)
			if err := rs.Append(len(v1) == 1 && v1[0] == pat[0], null1); err != nil {
				return true, err
			}
		}
		return true, nil
	}

	// opt rule #3: [_%]somethingInBetween[_%]
	if n > 1 {
		c0, c1 := pat[0], pat[n-1]
		if !bytes.ContainsAny(pat[1:len(pat)-1], "_%") {
			if n > 2 && pat[n-2] == DefaultEscapeChar {
				c1 = DefaultEscapeChar
			}
			switch {
			case !(c0 == '%' || c0 == '_') && !(c1 == '%' || c1 == '_'):
				// Rule 4.1: no wild card, so it is a simple compare eq.
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					if err := rs.Append(len(v1) == n && bytes.Equal(pat, v1), null1); err != nil {
						return true, err
					}
				}
				return true, nil

			case c0 == '_' && !(c1 == '%' || c1 == '_'):
				// Rule 4.2: _foobarzoo,
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					if err := rs.Append(len(v1) == n && bytes.Equal(pat[1:], v1[1:]), null1); err != nil {
						return true, err
					}
				}
				return true, nil

			case c0 == '%' && !(c1 == '%' || c1 == '_'):
				// Rule 4.3, %foobarzoo, it turns into a suffix match.
				suffix := functionUtil.RemoveEscapeChar(pat[1:], DefaultEscapeChar)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					if err := rs.Append(bytes.HasSuffix(v1, suffix), null1); err != nil {
						return true, err
					}
				}
				return true, nil

			case c1 == '_' && !(c0 == '%' || c0 == '_'):
				// Rule 4.4, foobarzoo_, it turns into eq ingoring last char.
				prefix := functionUtil.RemoveEscapeChar(pat[:n-1], DefaultEscapeChar)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					if err := rs.Append(len(v1) == n && bytes.Equal(prefix, v1[:n-1]), null1); err != nil {
						return true, err
					}
				}
				return true, nil

			case c1 == '%' && !(c0 == '%' || c0 == '_'):
				// Rule 4.5 foobarzoo%, prefix match
				prefix := functionUtil.RemoveEscapeChar(pat[:n-1], DefaultEscapeChar)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					if err := rs.Append(bytes.HasPrefix(v1, prefix), null1); err != nil {
						return true, err
					}
				}
				return true, nil

			case c0 == '%' && c1 == '%':
				// Rule 4.6 %foobarzoo%, now it is contains
				substr := functionUtil.RemoveEscapeChar(pat[1:n-1], DefaultEscapeChar)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					if err := rs.Append(bytes.Contains(v1, substr), null1); err != nil {
						return true, err
					}
				}
				return true, nil

			case c0 == '%' && c1 == '_':
				// Rule 4.7 %foobarzoo_,
				suffix := functionUtil.RemoveEscapeChar(pat[1:n-1], DefaultEscapeChar)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					if err := rs.Append(len(v1) > 0 && bytes.HasSuffix(v1[:len(v1)-1], suffix), null1); err != nil {
						return true, err
					}
				}
				return true, nil

			case c0 == '_' && c1 == '%':
				// Rule 4.8 _foobarzoo%
				prefix := functionUtil.RemoveEscapeChar(pat[1:n-1], DefaultEscapeChar)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					if err := rs.Append(len(v1) > 0 && bytes.HasPrefix(v1[1:], prefix), null1); err != nil {
						return true, err
					}
				}
				return true, nil
			}
		} else if c0 == '%' && c1 == '%' && !bytes.Contains(pat[1:len(pat)-1], []byte{'_'}) && !bytes.Contains(pat, []byte{'\\', '%'}) {
			pat0 := pat[1:]
			var subpats [][]byte
			for {
				idx := bytes.IndexByte(pat0, '%')
				if idx == -1 {
					break
				}
				subpats = append(subpats, pat0[:idx])
				pat0 = pat0[idx+1:]
			}

		outer:
			for i := uint64(0); i < uint64(length); i++ {
				v1, null1 := p1.GetStrValue(i)
				if null1 {
					rs.AppendMustNull()
				} else {
					for _, sp := range subpats {
						idx := bytes.Index(v1, sp)
						if idx == -1 {
							rs.AppendMustValue(false)
							continue outer
						}
						v1 = v1[idx+len(sp):]
					}
					rs.AppendMustValue(true)
				}
			}
			return true, nil
		}
	}
	return false, nil
}

func (op *opBuiltInRegexp) builtInRegMatch(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return generalRegMatch(op, parameters, result, proc, uint64(length), true)
}

func (op *opBuiltInRegexp) builtInNotRegMatch(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	return generalRegMatch(op, parameters, result, proc, uint64(length), false)
}

func (op *opBuiltInRegexp) builtInRegexpSubstr(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[types.Varlena](result)
	switch len(parameters) {
	case 2:
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 || len(v2) == 0 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				match, res, err := op.regMap.regularSubstr(pat, expr, 1, 1)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), !match); err != nil {
					return err
				}
			}
		}

	case 3:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			if null1 || null2 || null3 || len(v2) == 0 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				match, res, err := op.regMap.regularSubstr(pat, expr, pos, 1)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), !match); err != nil {
					return err
				}
			}
		}

	case 4:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		occurrences := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			ocur, null4 := occurrences.GetValue(i)
			if null1 || null2 || null3 || null4 || len(v2) == 0 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				match, res, err := op.regMap.regularSubstr(pat, expr, pos, ocur)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), !match); err != nil {
					return err
				}
			}
		}
		return nil

	}
	return nil
}

func (op *opBuiltInRegexp) builtInRegexpInstr(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[int64](result)
	switch len(parameters) {
	case 2:
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				index, err := op.regMap.regularInstr(pat, expr, 1, 1, 0)
				if err != nil {
					return err
				}
				if err = rs.Append(index, false); err != nil {
					return err
				}
			}
		}

	case 3:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			if null1 || null2 || null3 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				index, err := op.regMap.regularInstr(pat, expr, pos, 1, 0)
				if err != nil {
					return err
				}
				if err = rs.Append(index, false); err != nil {
					return err
				}
			}
		}

	case 4:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		occurrences := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			ocur, null4 := occurrences.GetValue(i)
			if null1 || null2 || null3 || null4 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				index, err := op.regMap.regularInstr(pat, expr, pos, ocur, 0)
				if err != nil {
					return err
				}
				if err = rs.Append(index, false); err != nil {
					return err
				}
			}
		}
		return nil

	case 5:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		occurrences := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		resultOption := vector.GenerateFunctionFixedTypeParameter[int8](parameters[4])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			ocur, null4 := occurrences.GetValue(i)
			resOp, null5 := resultOption.GetValue(i)
			if null1 || null2 || null3 || null4 || null5 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				index, err := op.regMap.regularInstr(pat, expr, pos, ocur, resOp)
				if err != nil {
					return err
				}
				if err = rs.Append(index, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (op *opBuiltInRegexp) builtInRegexpLike(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)

	if len(parameters) == 2 {
		for i := uint64(0); i < uint64(length); i++ {
			expr, null1 := p1.GetStrValue(i)
			pat, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				if err := rs.Append(false, true); err != nil {
					return err
				}
			} else {
				match, err := op.regMap.regularLike(string(pat), string(expr), "c")
				if err != nil {
					return err
				}
				if err = rs.Append(match, false); err != nil {
					return err
				}
			}
		}
	} else if len(parameters) == 3 {
		if parameters[2].IsConstNull() {
			for i := uint64(0); i < uint64(length); i++ {
				if err := rs.Append(false, true); err != nil {
					return err
				}
			}
			return nil
		}

		p3 := vector.GenerateFunctionStrParameter(parameters[2])
		for i := uint64(0); i < uint64(length); i++ {
			expr, null1 := p1.GetStrValue(i)
			pat, null2 := p2.GetStrValue(i)
			mt, null3 := p3.GetStrValue(i)
			if null1 || null2 || null3 {
				if err := rs.Append(false, true); err != nil {
					return err
				}
			} else {
				match, err := op.regMap.regularLike(string(pat), string(expr), string(mt))
				if err != nil {
					return err
				}
				if err = rs.Append(match, false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (op *opBuiltInRegexp) builtInRegexpReplace(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0]) // expr
	p2 := vector.GenerateFunctionStrParameter(parameters[1]) // pat
	p3 := vector.GenerateFunctionStrParameter(parameters[2]) // repl
	rs := vector.MustFunctionResult[types.Varlena](result)

	if parameters[0].IsConstNull() || parameters[1].IsConstNull() || parameters[2].IsConstNull() {
		for i := uint64(0); i < uint64(length); i++ {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
		}
		return nil
	}

	switch len(parameters) {
	case 3:
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			v3, null3 := p3.GetStrValue(i)
			if null1 || null2 || null3 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				val, err := op.regMap.regularReplace(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v3), 1, 0)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes([]byte(val), false); err != nil {
					return err
				}
			}
		}

	case 4:
		p4 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			v3, null3 := p3.GetStrValue(i)
			v4, null4 := p4.GetValue(i)
			if null1 || null2 || null3 || null4 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				val, err := op.regMap.regularReplace(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v3), v4, 0)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes([]byte(val), false); err != nil {
					return err
				}
			}
		}

	case 5:
		p4 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		p5 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[4])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			v3, null3 := p3.GetStrValue(i)
			v4, null4 := p4.GetValue(i)
			v5, null5 := p5.GetValue(i)
			if null1 || null2 || null3 || null4 || null5 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				val, err := op.regMap.regularReplace(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v3), v4, v5)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes([]byte(val), false); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func generalRegMatch(
	op *opBuiltInRegexp,
	parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length uint64, isReg bool) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)
	if parameters[1].IsConst() {
		v, null := p2.GetStrValue(0)
		if null {
			for i := uint64(0); i < length; i++ {
				if err := rs.Append(false, true); err != nil {
					return err
				}
			}
			return nil
		} else {
			reg, err := regexp.Compile(functionUtil.QuickBytesToStr(v))
			if err != nil {
				return err
			}
			for i := uint64(0); i < length; i++ {
				v1, null1 := p1.GetStrValue(i)
				if null1 {
					if err = rs.Append(false, null1); err != nil {
						return err
					}
				} else {
					err = rs.Append(reg.MatchString(functionUtil.QuickBytesToStr(v1)) == isReg, false)
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}

	for i := uint64(0); i < length; i++ {
		v1, null1 := p1.GetStrValue(i)
		v2, null2 := p2.GetStrValue(i)
		if null1 || null2 {
			if err := rs.Append(false, true); err != nil {
				return err
			}
		} else {
			rval, err := op.regMap.regularMatch(functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2))
			if err != nil {
				return err
			}
			if err = rs.Append(rval == isReg, false); err != nil {
				return err
			}
		}
	}
	return nil
}

type regexpSet struct {
	mp map[string]*regexp.Regexp
}

func (rs *regexpSet) regularMatchForLikeOp(pat []byte, str []byte) (match bool, err error) {
	replace := func(s string) string {
		var oldCharactor rune

		r := make([]byte, len(s)*2)
		w := 0
		start := 0
		for len(s) > start {
			character, wid := utf8.DecodeRuneInString(s[start:])
			if oldCharactor == '\\' {
				w += copy(r[w:], s[start:start+wid])
				start += wid
				oldCharactor = 0
				continue
			}
			switch character {
			case '_':
				w += copy(r[w:], []byte{'.'})
			case '%':
				w += copy(r[w:], []byte{'.', '*'})
			case '(':
				w += copy(r[w:], []byte{'\\', '('})
			case ')':
				w += copy(r[w:], []byte{'\\', ')'})
			case '\\':
			default:
				w += copy(r[w:], s[start:start+wid])
			}
			start += wid
			oldCharactor = character
		}
		return string(r[:w])
	}
	convert := func(expr []byte) string {
		return fmt.Sprintf("^(?s:%s)$", replace(util.UnsafeBytesToString(expr)))
	}

	realPat := convert(pat)
	reg, ok := rs.mp[realPat]
	if !ok {
		reg, err = regexp.Compile(realPat)
		if err != nil {
			return false, err
		}
		rs.mp[realPat] = reg
	}
	return reg.Match(str), nil
}

// return if str matched pat.
func (rs *regexpSet) regularMatch(pat string, str string) (match bool, err error) {
	reg, ok := rs.mp[pat]
	if !ok {
		reg, err = regexp.Compile(pat)
		if err != nil {
			return false, moerr.NewInternalErrorNoCtx(fmt.Sprintf("%s is not a regexp expression", pat))
		}
		rs.mp[pat] = reg
	}
	return reg.MatchString(str), nil
}

// if str[pos:] matched pat.
// return Nth (N = occurrence here) of match result
func (rs *regexpSet) regularSubstr(pat string, str string, pos, occurrence int64) (match bool, substr string, err error) {
	if pos < 1 || occurrence < 1 || pos >= int64(len(str)) {
		return false, "", moerr.NewInvalidInputNoCtx("regexp_substr have invalid input")
	}

	reg, ok := rs.mp[pat]
	if !ok {
		reg, err = regexp.Compile(pat)
		if err != nil {
			return false, "", moerr.NewInvalidArgNoCtx("regexp_substr have invalid regexp pattern arg", pat)
		}
		rs.mp[pat] = reg
	}

	// match and return
	matches := reg.FindAllString(str[pos-1:], -1)
	if l := int64(len(matches)); l < occurrence {
		return false, "", nil
	}
	return true, matches[occurrence-1], nil
}

func (rs *regexpSet) regularReplace(pat string, str string, repl string, pos, occurrence int64) (r string, err error) {
	if pos < 1 || occurrence < 0 || pos >= int64(len(str)) {
		return "", moerr.NewInvalidInputNoCtx("regexp_replace have invalid input")
	}
	reg, ok := rs.mp[pat]
	if !ok {
		reg, err = regexp.Compile(pat)
		if err != nil {
			pat = "[" + pat + "]"
			return "", moerr.NewInvalidArgNoCtx("regexp_replace have invalid regexp pattern arg", pat)
		}
		rs.mp[pat] = reg
	}
	//match result indexs
	matchRes := reg.FindAllStringIndex(str, -1)
	if matchRes == nil {
		return str, nil
	} //find the match position
	index := 0
	for int64(matchRes[index][0]) < pos-1 {
		index++
		if index == len(matchRes) {
			return str, nil
		}
	}
	matchRes = matchRes[index:]
	if int64(len(matchRes)) < occurrence {
		return str, nil
	}
	if occurrence == 0 {
		return reg.ReplaceAllLiteralString(str, repl), nil
	} else if occurrence == int64(len(matchRes)) {
		// the string won't be replaced
		notRepl := str[:matchRes[occurrence-1][0]]
		// the string will be replaced
		replace := str[matchRes[occurrence-1][0]:]
		return notRepl + reg.ReplaceAllLiteralString(replace, repl), nil
	} else {
		// the string won't be replaced
		notRepl := str[:matchRes[occurrence-1][0]]
		// the string will be replaced
		replace := str[matchRes[occurrence-1][0]:matchRes[occurrence][0]]
		left := str[matchRes[occurrence][0]:]
		return notRepl + reg.ReplaceAllLiteralString(replace, repl) + left, nil
	}
}

// regularInstr return an index indicating the starting or ending position of the match.
// it depends on the value of retOption, if 0 then return start, if 1 then return end.
// return 0 if match failed.
func (rs *regexpSet) regularInstr(pat string, str string, pos, occurrence int64, retOption int8) (index int64, err error) {
	if pos < 1 || occurrence < 1 || retOption > 1 || pos >= int64(len(str)) {
		return 0, moerr.NewInvalidInputNoCtx("regexp_instr have invalid input")
	}

	reg, ok := rs.mp[pat]
	if !ok {
		reg, err = regexp.Compile(pat)
		if err != nil {
			pat = "[" + pat + "]"
			return 0, moerr.NewInvalidArgNoCtx("regexp_instr have invalid regexp pattern arg", pat)
		}
		rs.mp[pat] = reg
	}
	matches := reg.FindAllStringIndex(str[pos-1:], -1)
	if int64(len(matches)) < occurrence {
		return 0, nil
	}
	return int64(matches[occurrence-1][retOption]) + pos, nil
}

func (rs *regexpSet) regularLike(pat string, str string, matchType string) (bool, error) {
	mt, err := getPureMatchType(matchType)
	if err != nil {
		return false, err
	}
	rule := fmt.Sprintf("(?%s)%s", mt, pat)
	reg, ok := rs.mp[rule]
	if !ok {
		reg, err = regexp.Compile(rule)
		if err != nil {
			return false, err
		}
		rs.mp[rule] = reg
	}
	match := reg.MatchString(str)
	return match, nil
}

// Support four arguments:
// i: case insensitive.
// c: case sensitive.
// m: multiple line mode.
// n: '.' can match line terminator.
func getPureMatchType(input string) (string, error) {
	retstring := ""
	caseType := ""
	foundn := false
	foundm := false

	for _, c := range input {
		switch string(c) {
		case "i":
			caseType = "i"
		case "c":
			caseType = ""
		case "m":
			if !foundm {
				retstring += "m"
				foundm = true
			}
		case "n":
			if !foundn {
				retstring += "s"
				foundn = true
			}
		default:
			return "", moerr.NewInvalidInputNoCtx("regexp_like got invalid match_type input!")
		}
	}

	retstring += caseType

	return retstring, nil
}
