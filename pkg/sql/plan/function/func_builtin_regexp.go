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
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/dlclark/regexp2"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	DefaultEscapeChar = '\\'

	mapSizeForRegexp = 100
)

type opBuiltInRegexp struct {
	regMap regexpSet
}

func newOpBuiltInRegexp() *opBuiltInRegexp {
	return &opBuiltInRegexp{
		regMap: regexpSet{
			mp:      make(map[string]*regexp.Regexp, mapSizeForRegexp),
			mysqlMp: make(map[string]*regexp2.Regexp, mapSizeForRegexp),
		},
	}
}

func (op *opBuiltInRegexp) likeFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(parameters) == 3 {
		return op.likeFnWithEscape(parameters, result, proc, length, selectList, false)
	}

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

	return opBinaryBytesBytesToFixedWithErrorCheck[bool](parameters, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return op.regMap.regularMatchForLikeOp(v2, v1)
	}, selectList)
}

func (op *opBuiltInRegexp) iLikeFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(parameters) == 3 {
		return op.likeFnWithEscape(parameters, result, proc, length, selectList, true)
	}

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

	return opBinaryBytesBytesToFixedWithErrorCheck[bool](parameters, result, proc, length, func(v1, v2 []byte) (bool, error) {
		return op.regMap.regularMatchForLikeOp(bytes.ToLower(v2), bytes.ToLower(v1))
	}, selectList)
}

func (op *opBuiltInRegexp) likeFnWithEscape(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	caseInsensitive bool,
) error {
	if !parameters[2].IsConst() {
		return moerr.NewInvalidInputNoCtx("Incorrect arguments to ESCAPE")
	}

	var escapeBytes []byte
	escapeIsNull := parameters[2].IsConstNull()
	if !escapeIsNull {
		escapeParam := vector.GenerateFunctionStrParameter(parameters[2])
		var isNull bool
		escapeBytes, isNull = escapeParam.GetStrValue(0)
		escapeIsNull = isNull
	}

	escapeEnabled := false
	var escape rune
	if !escapeIsNull {
		if !utf8.Valid(escapeBytes) || utf8.RuneCount(escapeBytes) > 1 {
			return moerr.NewInvalidInputNoCtx("Incorrect arguments to ESCAPE")
		}
		if len(escapeBytes) == 0 && likeNoBackslashEscapes(proc) {
			return moerr.NewInvalidInputNoCtx("Incorrect arguments to ESCAPE")
		}

		escapeEnabled = len(escapeBytes) != 0
		if escapeEnabled {
			escape, _ = utf8.DecodeRune(escapeBytes)
		}
	}
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](parameters[:2], result, proc, length, func(value, pattern []byte) (bool, error) {
		return op.regMap.regularMatchForLikeOpWithEscape(pattern, value, escape, escapeEnabled, caseInsensitive)
	}, selectList)
}

func likeNoBackslashEscapes(proc *process.Process) bool {
	if proc == nil || proc.Base == nil {
		return false
	}

	mode := proc.GetSessionInfo().SqlMode
	if resolver := proc.GetResolveVariableFunc(); resolver != nil {
		if value, err := resolver("sql_mode", true, false); err == nil {
			if sessionMode, ok := value.(string); ok {
				mode = sessionMode
			}
		}
	}
	if mode == process.EmptySqlModeSentinel {
		mode = ""
	}
	return mysql.HasSQLMode(mode, "NO_BACKSLASH_ESCAPES")
}

func optimizeRuleForLike(p1, p2 vector.FunctionParameterWrapper[types.Varlena], rs *vector.FunctionResult[bool], length int,
	specialFnForV func([]byte) []byte) (bool, error) {
	pat, null := p2.GetStrValue(0)
	if null {
		nulls.AddRange(rs.GetResultVector().GetNulls(), 0, uint64(length))
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
				literal := functionUtil.RemoveEscapeChar(pat, DefaultEscapeChar)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					if err := rs.Append(len(v1) == len(literal) && bytes.Equal(literal, v1), null1); err != nil {
						return true, err
					}
				}
				return true, nil

			case c0 == '_' && !(c1 == '%' || c1 == '_'):
				// Rule 4.2: _foobarzoo,
				literal := functionUtil.RemoveEscapeChar(pat[1:], DefaultEscapeChar)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					if err := rs.Append(len(v1) == len(literal)+1 && bytes.Equal(literal, v1[1:]), null1); err != nil {
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
					if err := rs.Append(len(v1) == len(prefix)+1 && bytes.Equal(prefix, v1[:len(prefix)]), null1); err != nil {
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

func (op *opBuiltInRegexp) builtInRegMatch(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryStrStrToFixedWithErrorCheck[bool](parameters, result, proc, length, func(v1, v2 string) (bool, error) {
		reg, err := op.regMap.getRegularMatcherForMatch(v2)
		if err != nil {
			return false, err
		}
		return reg.MatchString(v1), nil
	}, selectList)
}

func (op *opBuiltInRegexp) builtInNotRegMatch(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return opBinaryStrStrToFixedWithErrorCheck[bool](parameters, result, proc, length, func(v1, v2 string) (bool, error) {
		reg, err := op.regMap.getRegularMatcherForMatch(v2)
		if err != nil {
			return false, err
		}
		return !reg.MatchString(v1), nil
	}, selectList)
}

func (op *opBuiltInRegexp) builtInRegexpSubstr(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[types.Varlena](result)
	switch len(parameters) {
	case 2:
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				if !null2 {
					if err := op.regMap.validateMySQLRegexp(functionUtil.QuickBytesToStr(v2), "c", "regexp_substr"); err != nil {
						return err
					}
				}
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
			if null1 || null2 || null3 {
				if !null2 {
					if err := op.regMap.validateMySQLRegexp(functionUtil.QuickBytesToStr(v2), "c", "regexp_substr"); err != nil {
						return err
					}
				}
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
			if null1 || null2 || null3 || null4 {
				if !null2 {
					if err := op.regMap.validateMySQLRegexp(functionUtil.QuickBytesToStr(v2), "c", "regexp_substr"); err != nil {
						return err
					}
				}
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

	case 5:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		occurrences := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		matchTypes := vector.GenerateFunctionStrParameter(parameters[4])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			ocur, null4 := occurrences.GetValue(i)
			matchType, null5 := matchTypes.GetStrValue(i)
			if null1 || null2 || null3 || null4 || null5 {
				if !null2 && !null5 {
					if err := op.regMap.validateMySQLRegexp(
						functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(matchType), "regexp_substr"); err != nil {
						return err
					}
				}
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				match, res, err := op.regMap.regularSubstrWithMatchType(
					pat, expr, pos, ocur, functionUtil.QuickBytesToStr(matchType))
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

func (op *opBuiltInRegexp) builtInRegexpInstr(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, _ *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[int64](result)
	switch len(parameters) {
	case 2:
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				if !null2 {
					if err := op.regMap.validateMySQLRegexp(functionUtil.QuickBytesToStr(v2), "c", "regexp_instr"); err != nil {
						return err
					}
				}
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
			index, err := op.regMap.regularInstr(
				functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), 1, 1, 0)
			if err != nil {
				return err
			}
			if err = rs.Append(index, false); err != nil {
				return err
			}
		}
		return nil

	case 3:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			if null1 || null2 || null3 {
				if !null2 {
					if err := op.regMap.validateMySQLRegexp(functionUtil.QuickBytesToStr(v2), "c", "regexp_instr"); err != nil {
						return err
					}
				}
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
				if !null2 {
					if err := op.regMap.validateMySQLRegexp(functionUtil.QuickBytesToStr(v2), "c", "regexp_instr"); err != nil {
						return err
					}
				}
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
				if !null2 {
					if err := op.regMap.validateMySQLRegexp(functionUtil.QuickBytesToStr(v2), "c", "regexp_instr"); err != nil {
						return err
					}
				}
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

	case 6:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		occurrences := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		resultOption := vector.GenerateFunctionFixedTypeParameter[int8](parameters[4])
		matchTypes := vector.GenerateFunctionStrParameter(parameters[5])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			ocur, null4 := occurrences.GetValue(i)
			resOp, null5 := resultOption.GetValue(i)
			matchType, null6 := matchTypes.GetStrValue(i)
			if null1 || null2 || null3 || null4 || null5 || null6 {
				if !null2 && !null6 {
					if err := op.regMap.validateMySQLRegexp(
						functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(matchType), "regexp_instr"); err != nil {
						return err
					}
				}
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				index, err := op.regMap.regularInstrWithMatchType(
					pat, expr, pos, ocur, resOp, functionUtil.QuickBytesToStr(matchType))
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

func (op *opBuiltInRegexp) builtInRegexpLike(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, _ *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)

	if len(parameters) == 2 {
		for i := uint64(0); i < uint64(length); i++ {
			expr, null1 := p1.GetStrValue(i)
			pat, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				if !null2 {
					if err := op.regMap.validateMySQLRegexp(string(pat), "c", "regexp_like"); err != nil {
						return err
					}
				}
				if err := rs.Append(false, true); err != nil {
					return err
				}
				continue
			}
			match, err := op.regMap.regularLike(string(pat), string(expr), "c")
			if err != nil {
				return err
			}
			if err = rs.Append(match, false); err != nil {
				return err
			}
		}
		return nil
	} else if len(parameters) == 3 {
		if parameters[2].IsConstNull() {
			nulls.AddRange(rs.GetResultVector().GetNulls(), 0, uint64(length))
			return nil
		}

		p3 := vector.GenerateFunctionStrParameter(parameters[2])
		for i := uint64(0); i < uint64(length); i++ {
			expr, null1 := p1.GetStrValue(i)
			pat, null2 := p2.GetStrValue(i)
			mt, null3 := p3.GetStrValue(i)
			if null1 || null2 || null3 {
				if !null2 && !null3 {
					if err := op.regMap.validateMySQLRegexp(string(pat), string(mt), "regexp_like"); err != nil {
						return err
					}
				}
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

func (op *opBuiltInRegexp) builtInRegexpReplace(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0]) // expr
	p2 := vector.GenerateFunctionStrParameter(parameters[1]) // pat
	p3 := vector.GenerateFunctionStrParameter(parameters[2]) // repl
	rs := vector.MustFunctionResult[types.Varlena](result)

	switch len(parameters) {
	case 3:
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			v3, null3 := p3.GetStrValue(i)
			if null1 || null2 || null3 {
				if !null2 && !null3 {
					if err := op.regMap.validateMySQLRegexpReplacement(
						functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v3), "c"); err != nil {
						return err
					}
				}
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
				if !null2 && !null3 {
					if err := op.regMap.validateMySQLRegexpReplacement(
						functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v3), "c"); err != nil {
						return err
					}
				}
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
				if !null2 && !null3 {
					if err := op.regMap.validateMySQLRegexpReplacement(
						functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v3), "c"); err != nil {
						return err
					}
				}
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

	case 6:
		p4 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		p5 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[4])
		matchTypes := vector.GenerateFunctionStrParameter(parameters[5])
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			v3, null3 := p3.GetStrValue(i)
			v4, null4 := p4.GetValue(i)
			v5, null5 := p5.GetValue(i)
			matchType, null6 := matchTypes.GetStrValue(i)
			if null1 || null2 || null3 || null4 || null5 || null6 {
				if !null2 && !null3 && !null6 {
					if err := op.regMap.validateMySQLRegexpReplacement(
						functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v3),
						functionUtil.QuickBytesToStr(matchType)); err != nil {
						return err
					}
				}
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				val, err := op.regMap.regularReplaceWithMatchType(
					functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1),
					functionUtil.QuickBytesToStr(v3), v4, v5, functionUtil.QuickBytesToStr(matchType))
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

type regexpSet struct {
	mp      map[string]*regexp.Regexp
	mysqlMp map[string]*regexp2.Regexp
}

func (rs *regexpSet) getRegularMatcher(pat string) (*regexp.Regexp, error) {
	var err error

	reg, ok := rs.mp[pat]
	if !ok {
		if len(rs.mp) == mapSizeForRegexp {
			for key := range rs.mp {
				delete(rs.mp, key)
				break
			}
		}

		reg, err = regexp.Compile(pat)
		if err != nil {
			return nil, err
		}
		rs.mp[pat] = reg
	}
	return reg, nil
}

func (rs *regexpSet) getRegularMatcherForMatch(pat string) (*regexp.Regexp, error) {
	if pat == "" {
		return nil, moerr.NewRegexpIllegalArgumentNoCtx()
	}
	return rs.getRegularMatcher(pat)
}

func (rs *regexpSet) regularMatchForLikeOp(pat []byte, str []byte) (match bool, err error) {
	return rs.regularMatchForLikeOpWithEscape(pat, str, DefaultEscapeChar, true, false)
}

func (rs *regexpSet) regularMatchForLikeOpWithEscape(
	pat []byte,
	str []byte,
	escape rune,
	escapeEnabled bool,
	caseInsensitive bool,
) (match bool, err error) {
	replace := func(s string) string {
		isRegexMeta := func(r rune) bool {
			switch r {
			case '.', '+', '*', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\':
				return true
			default:
				return false
			}
		}
		appendLiteral := func(buf *bytes.Buffer, r rune) {
			if caseInsensitive {
				r = unicode.ToLower(r)
			}
			if isRegexMeta(r) {
				buf.WriteByte('\\')
			}
			buf.WriteRune(r)
		}

		var escaped bool
		var buf bytes.Buffer
		buf.Grow(len(s) * 2)
		for len(s) > 0 {
			r, size := utf8.DecodeRuneInString(s)
			s = s[size:]
			if escaped {
				appendLiteral(&buf, r)
				escaped = false
				continue
			}
			switch {
			case escapeEnabled && r == escape:
				escaped = true
			case r == '_':
				buf.WriteByte('.')
			case r == '%':
				buf.WriteString(".*")
			default:
				appendLiteral(&buf, r)
			}
		}
		if escaped {
			appendLiteral(&buf, escape)
		}
		return buf.String()
	}
	convert := func(expr []byte) string {
		return fmt.Sprintf("^(?s:%s)$", replace(util.UnsafeBytesToString(expr)))
	}

	realPat := convert(pat)
	reg, err := rs.getRegularMatcher(realPat)
	if err != nil {
		return false, nil
	}
	if caseInsensitive {
		str = []byte(strings.ToLower(util.UnsafeBytesToString(str)))
	}
	return reg.Match(str), nil
}

// if str[pos:] matched pat.
// return Nth (N = occurrence here) of match result
func (rs *regexpSet) regularSubstr(pat string, str string, pos, occurrence int64) (match bool, substr string, err error) {
	return rs.regularSubstrWithMatchType(pat, str, pos, occurrence, "c")
}

func (rs *regexpSet) regularSubstrWithMatchType(
	pat string, str string, pos, occurrence int64, matchType string,
) (match bool, substr string, err error) {
	reg, err := rs.getMySQLRegexp(pat, matchType, "regexp_substr")
	if err != nil {
		return false, "", err
	}
	runes := []rune(str)
	if pos < 1 || pos > int64(len(runes))+1 {
		return false, "", moerr.NewRegexpIndexOutOfBoundsNoCtx()
	}
	if pos == int64(len(runes))+1 {
		return false, "", nil
	}
	if occurrence < 1 {
		occurrence = 1
	}
	m, err := reg.FindRunesMatchStartingAt(runes, int(pos-1))
	for found := int64(1); m != nil && found < occurrence; found++ {
		m, err = reg.FindNextMatch(m)
		if err != nil {
			return false, "", mysqlRegexpExecError(err)
		}
	}
	if err != nil {
		return false, "", mysqlRegexpExecError(err)
	}
	if m == nil {
		return false, "", nil
	}
	return true, m.String(), nil
}

func (rs *regexpSet) regularReplace(pat string, str string, repl string, pos, occurrence int64) (r string, err error) {
	return rs.regularReplaceWithMatchType(pat, str, repl, pos, occurrence, "c")
}

func (rs *regexpSet) regularReplaceWithMatchType(
	pat string, str string, repl string, pos, occurrence int64, matchType string,
) (r string, err error) {
	reg, err := rs.getMySQLRegexp(pat, matchType, "regexp_replace")
	if err != nil {
		return "", err
	}
	replacement, err := parseMySQLRegexpReplacement(repl, reg)
	if err != nil {
		return "", err
	}
	runes := []rune(str)
	if pos < 1 || pos > int64(len(runes))+1 {
		return "", moerr.NewRegexpIndexOutOfBoundsNoCtx()
	}
	if pos == int64(len(runes))+1 {
		return str, nil
	}
	if occurrence < 0 {
		occurrence = 1
	}
	if literal, ok := replacement.literal(); ok && !regexpPatternHasUnescapedAnchor(pat) {
		mode, unixLines, ignoreCase, parseErr := parseMySQLRegexpMatchType(matchType, "regexp_replace")
		if parseErr != nil {
			return "", parseErr
		}
		goPattern := rewriteMySQLRegexpPattern(pat, mode, unixLines)
		if ignoreCase {
			goPattern = "(?i)" + goPattern
		}
		if goReg, compileErr := regexp.Compile(goPattern); compileErr == nil {
			prefix := string(runes[:pos-1])
			search := string(runes[pos-1:])
			if occurrence == 0 {
				return prefix + goReg.ReplaceAllLiteralString(search, literal), nil
			}
			matches := goReg.FindAllStringIndex(search, int(occurrence))
			if int64(len(matches)) < occurrence {
				return str, nil
			}
			selected := matches[occurrence-1]
			return prefix + search[:selected[0]] + literal + search[selected[1]:], nil
		}
	}

	m, err := reg.FindRunesMatchStartingAt(runes, int(pos-1))
	if err != nil {
		return "", mysqlRegexpExecError(err)
	}
	if m == nil {
		return str, nil
	}

	if occurrence > 0 {
		for found := int64(1); found < occurrence && m != nil; found++ {
			m, err = reg.FindNextMatch(m)
			if err != nil {
				return "", mysqlRegexpExecError(err)
			}
		}
		if m == nil {
			return str, nil
		}
		return string(runes[:m.Index]) + replacement.expand(m) + string(runes[m.Index+m.Length:]), nil
	}

	var result strings.Builder
	result.Grow(len(str))
	last := 0
	for m != nil {
		result.WriteString(string(runes[last:m.Index]))
		result.WriteString(replacement.expand(m))
		last = m.Index + m.Length
		m, err = reg.FindNextMatch(m)
		if err != nil {
			return "", mysqlRegexpExecError(err)
		}
	}
	result.WriteString(string(runes[last:]))
	return result.String(), nil
}

// regularInstr return an index indicating the starting or ending position of the match.
// it depends on the value of retOption, if 0 then return start, if 1 then return end.
// return 0 if match failed.
func (rs *regexpSet) regularInstr(pat string, str string, pos, occurrence int64, retOption int8) (index int64, err error) {
	return rs.regularInstrWithMatchType(pat, str, pos, occurrence, retOption, "c")
}

func (rs *regexpSet) regularInstrWithMatchType(
	pat string, str string, pos, occurrence int64, retOption int8, matchType string,
) (index int64, err error) {
	runes := []rune(str)
	if occurrence < 1 {
		occurrence = 1
	}
	if retOption < 0 || retOption > 1 {
		return 0, moerr.NewWrongArguments(moerr.Context(), "regexp_instr")
	}

	reg, err := rs.getMySQLRegexp(pat, matchType, "regexp_instr")
	if err != nil {
		return 0, err
	}
	if pos < 1 || pos > int64(len(runes)) {
		if len(runes) == 0 && pos == 1 {
			return 0, nil
		}
		return 0, moerr.NewRegexpIndexOutOfBoundsNoCtx()
	}

	search := runes[pos-1:]
	m, err := reg.FindRunesMatch(search)
	for found := int64(1); m != nil && found < occurrence; found++ {
		m, err = reg.FindNextMatch(m)
		if err != nil {
			return 0, mysqlRegexpExecError(err)
		}
	}
	if err != nil {
		return 0, mysqlRegexpExecError(err)
	}
	if m == nil {
		return 0, nil
	}
	if retOption == 1 {
		return int64(m.Index+m.Length) + pos, nil
	}
	return int64(m.Index) + pos, nil
}

func (rs *regexpSet) regularLike(pat string, str string, matchType string) (bool, error) {
	reg, err := rs.getMySQLRegexp(pat, matchType, "regexp_like")
	if err != nil {
		return false, err
	}
	matched, err := reg.MatchString(str)
	return matched, mysqlRegexpExecError(err)
}
