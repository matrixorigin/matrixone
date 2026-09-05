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
			mp: make(map[regexpCacheKey]*regexp.Regexp, mapSizeForRegexp),
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
	if n == 1 && pat[0] == '_' {
		for i := uint64(0); i < uint64(length); i++ {
			v1, null1 := p1.GetStrValue(i)
			v1 = specialFnForV(v1)
			_, runeSize := utf8.DecodeRune(v1)
			if err := rs.Append(runeSize > 0 && runeSize == len(v1), null1); err != nil {
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
					_, runeSize := utf8.DecodeRune(v1)
					if err := rs.Append(runeSize > 0 && len(v1) == len(literal)+runeSize && bytes.Equal(literal, v1[runeSize:]), null1); err != nil {
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
				// Rule 4.4, foobarzoo_, it turns into eq ignoring the last character.
				prefix := functionUtil.RemoveEscapeChar(pat[:n-1], DefaultEscapeChar)
				for i := uint64(0); i < uint64(length); i++ {
					v1, null1 := p1.GetStrValue(i)
					v1 = specialFnForV(v1)
					_, runeSize := utf8.DecodeLastRune(v1)
					if err := rs.Append(runeSize > 0 && len(v1) == len(prefix)+runeSize && bytes.Equal(prefix, v1[:len(prefix)]), null1); err != nil {
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
					_, runeSize := utf8.DecodeLastRune(v1)
					if err := rs.Append(runeSize > 0 && bytes.HasSuffix(v1[:len(v1)-runeSize], suffix), null1); err != nil {
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
					_, runeSize := utf8.DecodeRune(v1)
					if err := rs.Append(runeSize > 0 && bytes.HasPrefix(v1[runeSize:], prefix), null1); err != nil {
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

func (op *opBuiltInRegexp) builtInRegexpSubstr(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[types.Varlena](result)
	switch len(parameters) {
	case 2:
		for i := uint64(0); i < uint64(length); i++ {
			if regexpRowMasked(selectList, i) {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 || len(v2) == 0 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				subjectIsBinary := parameters[0].GetIsBinaryStringAt(int(i))
				match, res, err := op.regMap.regularSubstrWithMode(pat, expr, 1, 1, subjectIsBinary)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), !match); err != nil {
					return err
				}
				if match {
					if err = copyRegexpSubjectDomain(parameters[0], rs.GetResultVector(), int(i), proc); err != nil {
						return err
					}
				}
			}
		}

	case 3:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		for i := uint64(0); i < uint64(length); i++ {
			if regexpRowMasked(selectList, i) {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			if null1 || null2 || null3 || len(v2) == 0 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				subjectIsBinary := parameters[0].GetIsBinaryStringAt(int(i))
				match, res, err := op.regMap.regularSubstrWithMode(pat, expr, pos, 1, subjectIsBinary)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), !match); err != nil {
					return err
				}
				if match {
					if err = copyRegexpSubjectDomain(parameters[0], rs.GetResultVector(), int(i), proc); err != nil {
						return err
					}
				}
			}
		}

	case 4:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		occurrences := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		for i := uint64(0); i < uint64(length); i++ {
			if regexpRowMasked(selectList, i) {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
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
				subjectIsBinary := parameters[0].GetIsBinaryStringAt(int(i))
				match, res, err := op.regMap.regularSubstrWithMode(pat, expr, pos, ocur, subjectIsBinary)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), !match); err != nil {
					return err
				}
				if match {
					if err = copyRegexpSubjectDomain(parameters[0], rs.GetResultVector(), int(i), proc); err != nil {
						return err
					}
				}
			}
		}
		return nil

	}
	return nil
}

func (op *opBuiltInRegexp) builtInRegexpInstr(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[int64](result)
	switch len(parameters) {
	case 2:
		for i := uint64(0); i < uint64(length); i++ {
			if regexpRowMasked(selectList, i) {
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			if null1 || null2 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
			index, err := op.regMap.regularInstrWithMode(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), 1, 1, 0, parameters[0].GetIsBinaryStringAt(int(i)))
			if err != nil {
				return err
			}
			if err = rs.Append(index, false); err != nil {
				return err
			}
		}

	case 3:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		for i := uint64(0); i < uint64(length); i++ {
			if regexpRowMasked(selectList, i) {
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			pos, null3 := positions.GetValue(i)
			if null1 || null2 || null3 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				subjectIsBinary := parameters[0].GetIsBinaryStringAt(int(i))
				index, err := op.regMap.regularInstrWithMode(pat, expr, pos, 1, 0, subjectIsBinary)
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
			if regexpRowMasked(selectList, i) {
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
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
				subjectIsBinary := parameters[0].GetIsBinaryStringAt(int(i))
				index, err := op.regMap.regularInstrWithMode(pat, expr, pos, ocur, 0, subjectIsBinary)
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
			if regexpRowMasked(selectList, i) {
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
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
				subjectIsBinary := parameters[0].GetIsBinaryStringAt(int(i))
				index, err := op.regMap.regularInstrWithMode(pat, expr, pos, ocur, resOp, subjectIsBinary)
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

func (op *opBuiltInRegexp) builtInRegexpLike(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)

	if len(parameters) == 2 {
		return opBinaryStrStrToFixedWithErrorCheck[bool](parameters, result, proc, length, func(v1, v2 string) (bool, error) {
			match, err := op.regMap.regularLike(v2, v1, "c")
			return match, err
		}, selectList)
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

func (op *opBuiltInRegexp) builtInRegexpReplace(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
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
			if regexpRowMasked(selectList, i) {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			v3, null3 := p3.GetStrValue(i)
			if null1 || null2 || null3 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				subjectIsBinary := parameters[0].GetIsBinaryStringAt(int(i))
				val, err := op.regMap.regularReplaceWithMode(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v3), 1, 0, subjectIsBinary)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes([]byte(val), false); err != nil {
					return err
				}
				if err = copyRegexpSubjectDomain(parameters[0], rs.GetResultVector(), int(i), proc); err != nil {
					return err
				}
			}
		}

	case 4:
		p4 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		for i := uint64(0); i < uint64(length); i++ {
			if regexpRowMasked(selectList, i) {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
			v1, null1 := p1.GetStrValue(i)
			v2, null2 := p2.GetStrValue(i)
			v3, null3 := p3.GetStrValue(i)
			v4, null4 := p4.GetValue(i)
			if null1 || null2 || null3 || null4 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				subjectIsBinary := parameters[0].GetIsBinaryStringAt(int(i))
				val, err := op.regMap.regularReplaceWithMode(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v3), v4, 0, subjectIsBinary)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes([]byte(val), false); err != nil {
					return err
				}
				if err = copyRegexpSubjectDomain(parameters[0], rs.GetResultVector(), int(i), proc); err != nil {
					return err
				}
			}
		}

	case 5:
		p4 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		p5 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[4])
		for i := uint64(0); i < uint64(length); i++ {
			if regexpRowMasked(selectList, i) {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
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
				subjectIsBinary := parameters[0].GetIsBinaryStringAt(int(i))
				val, err := op.regMap.regularReplaceWithMode(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v3), v4, v5, subjectIsBinary)
				if err != nil {
					return err
				}
				if err = rs.AppendBytes([]byte(val), false); err != nil {
					return err
				}
				if err = copyRegexpSubjectDomain(parameters[0], rs.GetResultVector(), int(i), proc); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func copyRegexpSubjectDomain(subject, result *vector.Vector, row int, proc *process.Process) error {
	return result.SetRuntimeStringDomainAtWithMP(row, subject.GetRuntimeStringDomainAt(row), proc.Mp())
}

func regexpRowMasked(selectList *FunctionSelectList, row uint64) bool {
	return selectList != nil && selectList.Contains(row)
}

type regexpSet struct {
	mp map[regexpCacheKey]*regexp.Regexp
}

func (rs *regexpSet) getRegularMatcher(pat string) (*regexp.Regexp, error) {
	return rs.getRegularMatcherWithMode(pat, false)
}

type regexpCacheKey struct {
	pattern string
	binary  bool
}

func (rs *regexpSet) getRegularMatcherWithMode(pat string, binary bool) (*regexp.Regexp, error) {
	var err error

	key := regexpCacheKey{pattern: pat, binary: binary}
	reg, ok := rs.mp[key]
	if !ok {
		if len(rs.mp) == mapSizeForRegexp {
			for key := range rs.mp {
				delete(rs.mp, key)
				break
			}
		}

		// pat can be a zero-copy string backed by a reusable input vector. Both
		// map keys and regexp expressions must outlive the current data block.
		pat = strings.Clone(pat)
		key.pattern = pat
		expression := pat
		if binary {
			expression, _ = encodeBinaryRegexpBytes(pat, 0)
		}
		reg, err = regexp.Compile(expression)
		if err != nil {
			return nil, err
		}
		rs.mp[key] = reg
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
	return rs.regularSubstrWithMode(pat, str, pos, occurrence, false)
}

func (rs *regexpSet) regularSubstrWithMode(pat string, str string, pos, occurrence int64, subjectIsBinary bool) (match bool, substr string, err error) {
	// check position
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok {
		return false, "", moerr.NewInvalidInputNoCtxf("regexp_substr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	// check occurrence
	if occurrence < 1 {
		return false, "", moerr.NewInvalidInputNoCtxf("regexp_substr have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	reg, err := rs.getRegularMatcherWithMode(pat, subjectIsBinary)
	if err != nil {
		return false, "", err
	}

	selected, found, err := rs.regexpNthMatchAtOrAfter(
		reg, pat, str, startByte, subjectIsBinary, occurrence)
	if err != nil {
		return false, "", err
	}
	if !found {
		return false, "", nil
	}
	return true, str[selected[0]:selected[1]], nil
}

func (rs *regexpSet) regularReplace(pat string, str string, repl string, pos, occurrence int64) (r string, err error) {
	return rs.regularReplaceWithMode(pat, str, repl, pos, occurrence, false)
}

func (rs *regexpSet) regularReplaceWithMode(pat string, str string, repl string, pos, occurrence int64, subjectIsBinary bool) (r string, err error) {
	// check position
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok {
		return "", moerr.NewInvalidInputNoCtxf("regexp_replace: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	// check occurrence
	if occurrence < 0 {
		return "", moerr.NewInvalidInputNoCtxf("regexp_replace have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}

	reg, err := rs.getRegularMatcherWithMode(pat, subjectIsBinary)
	if err != nil {
		pat = "[" + pat + "]"
		return "", moerr.NewInvalidArgNoCtx("regexp_replace have invalid regexp pattern arg", pat)
	}
	if startByte == 0 && occurrence == 0 {
		if !subjectIsBinary {
			return reg.ReplaceAllLiteralString(str, repl), nil
		}
		encodedSubject, _ := encodeBinaryRegexpBytes(str, 0)
		encodedReplacement, _ := encodeBinaryRegexpBytes(repl, 0)
		return decodeBinaryRegexpBytes(reg.ReplaceAllLiteralString(encodedSubject, encodedReplacement)), nil
	}

	if occurrence == 0 {
		return rs.regexpReplaceAllAtOrAfter(reg, pat, str, repl, startByte, subjectIsBinary)
	}
	match, found, err := rs.regexpNthMatchAtOrAfter(
		reg, pat, str, startByte, subjectIsBinary, occurrence)
	if err != nil {
		return "", err
	}
	if !found {
		return str, nil
	}
	return str[:match[0]] + repl + str[match[1]:], nil
}

// regularInstr return an index indicating the starting or ending position of the match.
// it depends on the value of retOption, if 0 then return start, if 1 then return end.
// return 0 if match failed.
func (rs *regexpSet) regularInstr(pat string, str string, pos, occurrence int64, retOption int8) (index int64, err error) {
	return rs.regularInstrWithMode(pat, str, pos, occurrence, retOption, false)
}

func (rs *regexpSet) regularInstrWithMode(pat string, str string, pos, occurrence int64, retOption int8, subjectIsBinary bool) (index int64, err error) {
	// check position
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	// check occurrence
	if occurrence < 1 {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	// check retOption
	if retOption < 0 || retOption > 1 {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr have Index out of bounds in regular expression search, return option %d", retOption)
	}

	reg, err := rs.getRegularMatcherWithMode(pat, subjectIsBinary)
	if err != nil {
		pat = "[" + pat + "]"
		return 0, moerr.NewInvalidArgNoCtx("regexp_instr have invalid regexp pattern arg", pat)
	}

	match, found, err := rs.regexpNthMatchAtOrAfter(
		reg, pat, str, startByte, subjectIsBinary, occurrence)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	matchOffset := match[retOption]
	return regexpByteOffsetToPosition(str, matchOffset, subjectIsBinary), nil
}

// regexpSearchStartByte converts a one-based SQL position to the byte offset
// used by Go's regexp implementation. Binary positions are already byte based;
// text positions count UTF-8 code points. The common position-1 path is
// constant time. Invalid UTF-8 bytes in text are one-character units, matching
// utf8.RuneCountInString.
func regexpSearchStartByte(str string, pos int64, subjectIsBinary bool) (int, bool) {
	if pos < 1 {
		return 0, false
	}
	if pos <= 1 {
		return 0, true
	}
	if subjectIsBinary {
		if pos > int64(len(str)) {
			return 0, false
		}
		return int(pos - 1), true
	}
	target := pos - 1
	seen := int64(0)
	for offset := range str {
		if seen == target {
			return offset, true
		}
		seen++
	}
	return 0, false
}

// regexpByteOffsetToPosition converts a regexp byte offset to the one-based SQL
// position in the subject's unit. Text match boundaries are rune boundaries,
// so counting runes in the prefix preserves the end-position convention.
func regexpByteOffsetToPosition(str string, offset int, subjectIsBinary bool) int64 {
	if offset <= 0 {
		return 1
	}
	if offset > len(str) {
		offset = len(str)
	}
	if subjectIsBinary {
		return int64(offset) + 1
	}
	return int64(utf8.RuneCountInString(str[:offset])) + 1
}

func regexpSubjectLength(str string, subjectIsBinary bool) int64 {
	if subjectIsBinary {
		return int64(len(str))
	}
	return int64(utf8.RuneCountInString(str))
}

// regexpNthMatchAtOrAfter preserves full-subject anchor and boundary semantics
// while retaining only the requested match. Work remains proportional to the
// requested occurrence, but memory is constant and the discarded prefix is
// never materialized.
func (rs *regexpSet) regexpNthMatchAtOrAfter(
	reg *regexp.Regexp,
	pat, str string,
	startByte int,
	subjectIsBinary bool,
	occurrence int64,
) ([2]int, bool, error) {
	searchSubject := str
	searchStart := startByte
	if subjectIsBinary {
		searchSubject, searchStart = encodeBinaryRegexpBytes(str, startByte)
	}

	selected := [2]int{}
	visited := int64(0)
	err := rs.regexpVisitAtOrAfter(reg, pat, searchSubject, searchStart, subjectIsBinary, occurrence,
		func(start, end int) {
			selected = [2]int{start, end}
			visited++
		})
	if err != nil {
		return [2]int{}, false, err
	}
	if visited < occurrence {
		return [2]int{}, false, nil
	}
	if subjectIsBinary {
		selected = decodeBinaryRegexpMatch(searchSubject, selected)
	}
	return selected, true, nil
}

// regexpVisitAtOrAfter walks non-overlapping matches without retaining them.
// str and startByte are already in the regexp engine's representation: ordinary
// UTF-8 for text, or one encoded rune per source byte for binary strings.
func (rs *regexpSet) regexpVisitAtOrAfter(
	reg *regexp.Regexp,
	pat, str string,
	startByte int,
	subjectIsBinary bool,
	limit int64,
	visit func(start, end int),
) error {
	visited := int64(0)
	nextStart := startByte
	previousEnd := -1
	for nextStart <= len(str) {
		start, end, found, err := rs.regexpFindAtOrAfter(reg, pat, str, nextStart, subjectIsBinary)
		if err != nil {
			return err
		}
		if !found {
			break
		}
		if start == end && start == previousEnd {
			// Match Go's FindAll convention: ignore an empty match directly
			// abutting the preceding match, then make one unit of progress.
			nextStart = regexpAdvancePosition(str, start)
			continue
		}
		visit(start, end)
		visited++
		if limit > 0 && visited >= limit {
			break
		}
		previousEnd = end
		if start == end {
			nextStart = regexpAdvancePosition(str, end)
		} else {
			nextStart = end
		}
	}
	return nil
}

func (rs *regexpSet) regexpReplaceAllAtOrAfter(
	reg *regexp.Regexp,
	pat, str, repl string,
	startByte int,
	subjectIsBinary bool,
) (string, error) {
	searchSubject := str
	searchStart := startByte
	replacement := repl
	if subjectIsBinary {
		searchSubject, searchStart = encodeBinaryRegexpBytes(str, startByte)
		replacement, _ = encodeBinaryRegexpBytes(repl, 0)
	}

	var b strings.Builder
	b.Grow(len(searchSubject))
	last := 0
	matched := false
	err := rs.regexpVisitAtOrAfter(reg, pat, searchSubject, searchStart, subjectIsBinary, 0,
		func(start, end int) {
			matched = true
			b.WriteString(searchSubject[last:start])
			b.WriteString(replacement)
			last = end
		})
	if err != nil {
		return "", err
	}
	if !matched {
		return str, nil
	}
	b.WriteString(searchSubject[last:])
	result := b.String()
	if subjectIsBinary {
		result = decodeBinaryRegexpBytes(result)
	}
	return result, nil
}

// regexpFindAtOrAfter supplies the context that slicing at startByte would
// lose. The wrapper consumes exactly the preceding text unit, then lazily
// searches for the original pattern. This keeps ^, multiline ^, and word
// boundaries relative to the original subject while excluding matches before
// startByte. The wrapped matcher is cached with the ordinary pattern matchers.
func (rs *regexpSet) regexpFindAtOrAfter(reg *regexp.Regexp, pat, str string, startByte int, subjectIsBinary bool) (start, end int, found bool, err error) {
	if startByte <= 0 {
		indices := reg.FindStringIndex(str)
		if indices == nil {
			return 0, 0, false, nil
		}
		return indices[0], indices[1], true, nil
	}
	if startByte > len(str) {
		return 0, 0, false, nil
	}

	_, size := utf8.DecodeLastRuneInString(str[:startByte])
	contextStart := startByte - size
	if size < 1 {
		contextStart = startByte - 1
	}
	wrapped, err := rs.getRegularMatcherWithMode("^(?s:.)(?s:.*?)("+pat+")", subjectIsBinary)
	if err != nil {
		return 0, 0, false, err
	}
	indices := wrapped.FindStringSubmatchIndex(str[contextStart:])
	if len(indices) < 4 || indices[2] < 0 {
		return 0, 0, false, nil
	}
	return contextStart + indices[2], contextStart + indices[3], true, nil
}

func regexpAdvancePosition(str string, offset int) int {
	if offset >= len(str) {
		return len(str) + 1
	}
	_, size := utf8.DecodeRuneInString(str[offset:])
	if size < 1 {
		size = 1
	}
	return offset + size
}

// encodeBinaryRegexpBytes maps every non-ASCII byte to a distinct private-use
// rune while leaving ASCII regexp syntax untouched. RE2 can then apply its
// normal regexp grammar with one input rune per original byte, including for
// valid and invalid UTF-8. encodedStart is the equivalent offset of startByte.
func encodeBinaryRegexpBytes(value string, startByte int) (encoded string, encodedStart int) {
	if startByte < 0 {
		startByte = 0
	} else if startByte > len(value) {
		startByte = len(value)
	}
	if isASCIIBytes(value) {
		return value, startByte
	}

	var b strings.Builder
	b.Grow(len(value) * 2)
	for i := 0; i < len(value); i++ {
		if i == startByte {
			encodedStart = b.Len()
		}
		if value[i] < utf8.RuneSelf {
			b.WriteByte(value[i])
		} else {
			b.WriteRune(rune(0xE000) + rune(value[i]))
		}
	}
	if startByte == len(value) {
		encodedStart = b.Len()
	}
	return b.String(), encodedStart
}

func isASCIIBytes(value string) bool {
	for i := 0; i < len(value); i++ {
		if value[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

func decodeBinaryRegexpMatch(encoded string, match [2]int) [2]int {
	start := utf8.RuneCountInString(encoded[:match[0]])
	return [2]int{start, start + utf8.RuneCountInString(encoded[match[0]:match[1]])}
}

func decodeBinaryRegexpBytes(encoded string) string {
	if isASCIIBytes(encoded) {
		return encoded
	}
	var b strings.Builder
	b.Grow(len(encoded))
	for _, r := range encoded {
		if r >= 0xE080 && r <= 0xE0FF {
			b.WriteByte(byte(r - 0xE000))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (rs *regexpSet) regularLike(pat string, str string, matchType string) (bool, error) {
	mt, err := getPureMatchType(matchType)
	if err != nil {
		return false, err
	}
	if pat == "" {
		return false, moerr.NewRegexpIllegalArgumentNoCtx()
	}
	rule := fmt.Sprintf("(?%s)%s", mt, pat)

	reg, err := rs.getRegularMatcher(rule)
	if err != nil {
		return false, err
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
