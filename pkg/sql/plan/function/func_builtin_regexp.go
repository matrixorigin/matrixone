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
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
			mp: make(map[string]*regexp.Regexp, mapSizeForRegexp),
		},
	}
}

func (op *opBuiltInRegexp) likeFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(parameters) == 3 {
		return op.likeFnWithEscape(parameters, result, proc, length, selectList, false)
	}

	uniformBinary, perRow := stringDomainMode(parameters[0])
	if uniformBinary || perRow {
		return op.likeByStringDomain(
			parameters, result, proc, length, selectList, uniformBinary, perRow,
			[]byte{byte(DefaultEscapeChar)}, true)
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

	escapeEnabled := !escapeIsNull && len(escapeBytes) != 0
	if !escapeIsNull && len(escapeBytes) == 0 && likeNoBackslashEscapes(proc) {
		return moerr.NewInvalidInputNoCtx("Incorrect arguments to ESCAPE")
	}

	uniformBinary, perRow := stringDomainMode(parameters[0])
	if !caseInsensitive && (uniformBinary || perRow) {
		return op.likeByStringDomain(
			parameters[:2], result, proc, length, selectList, uniformBinary, perRow,
			escapeBytes, escapeEnabled)
	}
	if !escapeIsNull && (!utf8.Valid(escapeBytes) || utf8.RuneCount(escapeBytes) > 1) {
		return moerr.NewInvalidInputNoCtx("Incorrect arguments to ESCAPE")
	}
	var escape rune
	if escapeEnabled {
		escape, _ = utf8.DecodeRune(escapeBytes)
	}
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](parameters[:2], result, proc, length, func(value, pattern []byte) (bool, error) {
		return op.regMap.regularMatchForLikeOpWithEscape(pattern, value, escape, escapeEnabled, caseInsensitive)
	}, selectList)
}

func (op *opBuiltInRegexp) likeByStringDomain(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	uniformBinary, perRow bool,
	escapeBytes []byte,
	escapeEnabled bool,
) error {
	values := vector.GenerateFunctionStrParameter(parameters[0])
	patterns := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)
	constantPattern := parameters[1].IsConst()
	compiledPattern := &compiledByteLikePattern{mp: proc.Mp(), ctx: proc.Ctx}
	compiledPatternReady := false
	defer compiledPattern.free()
	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selectList, row) {
			if err := rs.Append(false, true); err != nil {
				return err
			}
			continue
		}
		value, valueNull := values.GetStrValue(row)
		pattern, patternNull := patterns.GetStrValue(row)
		if valueNull || patternNull {
			if err := rs.Append(false, true); err != nil {
				return err
			}
			continue
		}
		var matched bool
		var err error
		if binaryStringAt(parameters[0], int(row), uniformBinary, perRow) {
			if escapeEnabled && len(escapeBytes) != 1 {
				return moerr.NewInvalidInputNoCtx("Incorrect arguments to ESCAPE")
			}
			if !constantPattern || !compiledPatternReady {
				if err = compiledPattern.reset(pattern, escapeBytes, escapeEnabled); err != nil {
					return err
				}
				compiledPatternReady = true
			}
			matched, err = compiledPattern.match(value)
		} else {
			if !utf8.Valid(escapeBytes) || utf8.RuneCount(escapeBytes) > 1 {
				return moerr.NewInvalidInputNoCtx("Incorrect arguments to ESCAPE")
			}
			var escapeRune rune
			if escapeEnabled {
				escapeRune, _ = utf8.DecodeRune(escapeBytes)
			}
			matched, err = op.regMap.regularMatchForLikeOpWithEscape(
				pattern, value, escapeRune, escapeEnabled, false)
		}
		if err != nil {
			return err
		}
		if err = rs.Append(matched, false); err != nil {
			return err
		}
	}
	return nil
}

const (
	byteLikeLiteral byte = iota
	byteLikeOne
	byteLikeAny
)

type compiledByteLikePattern struct {
	storage                []byte
	kinds                  []byte
	literals               []byte
	convolutionScratch     []byte
	literalPositionScratch []byte
	mp                     *mpool.MPool
	ctx                    context.Context
}

type byteLikeDirectVerificationBudget struct {
	remaining uint64
}

func newByteLikeDirectVerificationBudget(valueLength, patternLength int) byteLikeDirectVerificationBudget {
	linearBaseline := uint64(valueLength) + uint64(patternLength)
	if linearBaseline > ^uint64(0)/byteLikeConvolutionRelativeWorkFactor {
		return byteLikeDirectVerificationBudget{remaining: ^uint64(0)}
	}
	return byteLikeDirectVerificationBudget{
		remaining: linearBaseline * byteLikeConvolutionRelativeWorkFactor,
	}
}

func (budget *byteLikeDirectVerificationBudget) consume(work uint64) bool {
	if budget == nil {
		return true
	}
	if work > budget.remaining {
		budget.remaining = 0
		return false
	}
	budget.remaining -= work
	return true
}

func compileByteLikePattern(
	pattern, escape []byte,
	escapeEnabled bool,
	mp *mpool.MPool,
) (*compiledByteLikePattern, error) {
	compiled := &compiledByteLikePattern{mp: mp}
	if err := compiled.reset(pattern, escape, escapeEnabled); err != nil {
		compiled.free()
		return nil, err
	}
	return compiled, nil
}

func (compiled *compiledByteLikePattern) reset(
	pattern, escape []byte,
	escapeEnabled bool,
) error {
	tokenCount := 0
	previousAny := false
	for at := 0; at < len(pattern); {
		if at&(byteLikeCancellationCheckInterval-1) == 0 {
			if err := compiled.byteLikeCancellationError(); err != nil {
				return err
			}
		}
		kind, literal, next := nextByteLikeToken(pattern, at, escape, escapeEnabled)
		if kind != byteLikeAny || !previousAny {
			if kind == byteLikeLiteral {
				tokenCount += len(literal)
			} else {
				tokenCount++
			}
		}
		previousAny = kind == byteLikeAny
		at = next
	}
	storageSize := tokenCount * 2
	if cap(compiled.storage) < storageSize {
		storage, err := compiled.mp.Grow(compiled.storage, storageSize, true)
		if err != nil {
			return err
		}
		compiled.storage = storage
	}
	compiled.storage = compiled.storage[:storageSize]
	compiled.kinds = compiled.storage[:tokenCount]
	compiled.literals = compiled.storage[tokenCount:]

	position := 0
	previousAny = false
	for at := 0; at < len(pattern); {
		if at&(byteLikeCancellationCheckInterval-1) == 0 {
			if err := compiled.byteLikeCancellationError(); err != nil {
				return err
			}
		}
		kind, literal, next := nextByteLikeToken(pattern, at, escape, escapeEnabled)
		if kind != byteLikeAny || !previousAny {
			if kind == byteLikeLiteral {
				for _, b := range literal {
					compiled.kinds[position] = byteLikeLiteral
					compiled.literals[position] = b
					position++
				}
			} else {
				compiled.kinds[position] = kind
				position++
			}
		}
		previousAny = kind == byteLikeAny
		at = next
	}
	return nil
}

func byteLike(
	pattern, value, escape []byte,
	escapeEnabled bool,
	mp *mpool.MPool,
) (bool, error) {
	compiled, err := compileByteLikePattern(pattern, escape, escapeEnabled, mp)
	if err != nil {
		return false, err
	}
	defer compiled.free()
	return compiled.match(value)
}

func (compiled *compiledByteLikePattern) free() {
	if compiled == nil {
		return
	}
	if compiled.storage != nil {
		compiled.mp.Free(compiled.storage)
		compiled.storage = nil
		compiled.kinds = nil
		compiled.literals = nil
	}
	if compiled.convolutionScratch != nil {
		compiled.mp.Free(compiled.convolutionScratch)
		compiled.convolutionScratch = nil
	}
	if compiled.literalPositionScratch != nil {
		compiled.mp.Free(compiled.literalPositionScratch)
		compiled.literalPositionScratch = nil
	}
}

func (compiled *compiledByteLikePattern) match(value []byte) (bool, error) {
	if len(compiled.kinds) == 0 {
		return len(value) == 0, nil
	}
	directBudget := newByteLikeDirectVerificationBudget(len(value), len(compiled.kinds))
	firstAny := slices.Index(compiled.kinds, byteLikeAny)
	if firstAny < 0 {
		if len(value) != len(compiled.kinds) {
			return false, nil
		}
		matched, _, err := compiled.matchSegmentAt(0, len(compiled.kinds), value, 0, &directBudget)
		return matched, err
	}

	cursor := 0
	segmentAt := 0
	if firstAny > 0 {
		if len(value) < firstAny {
			return false, nil
		}
		matched, _, err := compiled.matchSegmentAt(0, firstAny, value, 0, nil)
		if err != nil || !matched {
			return false, err
		}
		cursor = firstAny
		segmentAt = firstAny
	}
	for segmentAt < len(compiled.kinds) && compiled.kinds[segmentAt] == byteLikeAny {
		segmentAt++
	}

	lastAny := len(compiled.kinds) - 1
	for compiled.kinds[lastAny] != byteLikeAny {
		lastAny--
	}
	suffixAt := len(compiled.kinds)
	searchLimit := len(value)
	if lastAny < len(compiled.kinds)-1 {
		suffixAt = lastAny + 1
		suffixLength := len(compiled.kinds) - suffixAt
		if suffixLength > len(value)-cursor {
			return false, nil
		}
		searchLimit = len(value) - suffixLength
		matched, _, err := compiled.matchSegmentAt(suffixAt, len(compiled.kinds), value, searchLimit, nil)
		if err != nil || !matched {
			return false, err
		}
	}

	var literalFrequency [256]int
	for at, b := range value[cursor:searchLimit] {
		if at&(byteLikeCancellationCheckInterval-1) == 0 {
			if err := compiled.byteLikeCancellationError(); err != nil {
				return false, err
			}
		}
		literalFrequency[b]++
	}
	for segmentAt < suffixAt {
		segmentEnd := slices.Index(compiled.kinds[segmentAt:suffixAt], byteLikeAny)
		if segmentEnd < 0 {
			segmentEnd = suffixAt
		} else {
			segmentEnd += segmentAt
		}
		matchAt, err := compiled.findSegment(
			segmentAt, segmentEnd, value, cursor, searchLimit, &literalFrequency, &directBudget)
		if err != nil {
			return false, err
		}
		if matchAt < 0 {
			return false, nil
		}
		cursor = matchAt + segmentEnd - segmentAt
		segmentAt = segmentEnd
		for segmentAt < suffixAt && compiled.kinds[segmentAt] == byteLikeAny {
			segmentAt++
		}
	}
	return cursor <= searchLimit, nil
}

func (compiled *compiledByteLikePattern) matchSegmentAt(
	start, end int,
	value []byte,
	valueAt int,
	directBudget *byteLikeDirectVerificationBudget,
) (matched, budgetExhausted bool, err error) {
	if end-start > len(value)-valueAt {
		return false, false, nil
	}
	for left, right, iteration := start, end-1, 0; left <= right; left, right, iteration = left+1, right-1, iteration+1 {
		if iteration&(byteLikeCancellationCheckInterval-1) == 0 {
			if err = compiled.byteLikeCancellationError(); err != nil {
				return false, false, err
			}
		}
		work := uint64(2)
		if left == right {
			work = 1
		}
		if !directBudget.consume(work) {
			return false, true, nil
		}
		if compiled.kinds[left] == byteLikeLiteral &&
			compiled.literals[left] != value[valueAt+left-start] {
			return false, false, nil
		}
		if right != left && compiled.kinds[right] == byteLikeLiteral &&
			compiled.literals[right] != value[valueAt+right-start] {
			return false, false, nil
		}
	}
	return true, false, nil
}

func (compiled *compiledByteLikePattern) prepareByteLikeLiteralPositions(
	start, end, literalCount int,
) ([]uint32, error) {
	requiredBytes := literalCount * 4
	if cap(compiled.literalPositionScratch) < requiredBytes {
		storage, err := compiled.mp.Grow(compiled.literalPositionScratch, requiredBytes, true)
		if err != nil {
			return nil, err
		}
		compiled.literalPositionScratch = storage
	}
	compiled.literalPositionScratch = compiled.literalPositionScratch[:requiredBytes]
	positions := byteLikeUint32Scratch(compiled.literalPositionScratch, literalCount)
	positionAt := 0
	for patternAt := start; patternAt < end; patternAt++ {
		if (patternAt-start)&(byteLikeCancellationCheckInterval-1) == 0 {
			if err := compiled.byteLikeCancellationError(); err != nil {
				return nil, err
			}
		}
		if compiled.kinds[patternAt] == byteLikeLiteral {
			positions[positionAt] = uint32(patternAt - start)
			positionAt++
		}
	}
	return positions, nil
}

func (compiled *compiledByteLikePattern) matchLiteralPositionsAt(
	segmentStart int,
	value []byte,
	valueAt int,
	positions []uint32,
	directBudget *byteLikeDirectVerificationBudget,
) (matched, budgetExhausted bool, err error) {
	for positionAt, position := range positions {
		if positionAt&(byteLikeCancellationCheckInterval-1) == 0 {
			if err = compiled.byteLikeCancellationError(); err != nil {
				return false, false, err
			}
		}
		if !directBudget.consume(1) {
			return false, true, nil
		}
		patternAt := segmentStart + int(position)
		if compiled.literals[patternAt] != value[valueAt+int(position)] {
			return false, false, nil
		}
	}
	return true, false, nil
}

func (compiled *compiledByteLikePattern) findSegment(
	start, end int,
	value []byte,
	from, limit int,
	literalFrequency *[256]int,
	directBudget *byteLikeDirectVerificationBudget,
) (int, error) {
	segmentLength := end - start
	if segmentLength > limit-from {
		return -1, nil
	}
	anchorStart, anchorEnd, anchorFrequency, literalCount, err :=
		compiled.rarestLiteralRun(start, end, literalFrequency)
	if err != nil {
		return -1, err
	}
	if anchorStart == anchorEnd {
		return from, nil
	}
	valueLength := limit - from
	candidateCount := valueLength - segmentLength + 1
	segmentHasOne := slices.Contains(compiled.kinds[start:end], byteLikeOne)
	var literalPositions []uint32
	verificationWidth := segmentLength
	if segmentHasOne && candidateCount > 1 && literalCount <= segmentLength/8 {
		literalPositions, err = compiled.prepareByteLikeLiteralPositions(start, end, literalCount)
		if err != nil {
			return -1, err
		}
		verificationWidth = literalCount
	}
	if segmentHasOne &&
		byteLikeShouldUseConvolution(anchorFrequency, candidateCount, verificationWidth, valueLength) {
		matchAt, used, err := compiled.findSegmentByConvolution(start, end, value, from, limit)
		if used {
			return matchAt, err
		}
	}
	anchor := compiled.literals[anchorStart:anchorEnd]
	anchorOffset := anchorStart - start
	searchAt := from + anchorOffset
	lastAnchorAt := limit - segmentLength + anchorOffset
	for searchIteration := 0; searchAt <= lastAnchorAt; searchIteration++ {
		if searchIteration&(byteLikeCancellationCheckInterval-1) == 0 {
			if err := compiled.byteLikeCancellationError(); err != nil {
				return -1, err
			}
		}
		found := bytes.Index(value[searchAt:lastAnchorAt+len(anchor)], anchor)
		if found < 0 {
			return -1, nil
		}
		candidate := searchAt + found - anchorOffset
		if !segmentHasOne {
			return candidate, nil
		}
		var matched, budgetExhausted bool
		if literalPositions != nil {
			matched, budgetExhausted, err = compiled.matchLiteralPositionsAt(
				start, value, candidate, literalPositions, directBudget)
		} else {
			matched, budgetExhausted, err = compiled.matchSegmentAt(
				start, end, value, candidate, directBudget)
		}
		if err != nil {
			return -1, err
		}
		if budgetExhausted {
			matchAt, used, convolutionErr := compiled.findSegmentByConvolution(
				start, end, value, candidate, limit)
			if used {
				return matchAt, convolutionErr
			}
			if literalPositions != nil {
				matched, _, err = compiled.matchLiteralPositionsAt(start, value, candidate, literalPositions, nil)
			} else {
				matched, _, err = compiled.matchSegmentAt(start, end, value, candidate, nil)
			}
			if err != nil {
				return -1, err
			}
		}
		if matched {
			return candidate, nil
		}
		searchAt += found + 1
	}
	return -1, nil
}

func byteLikeShouldUseConvolution(
	anchorFrequency, candidateCount, segmentLength, valueLength int,
) bool {
	candidateUpperBound := min(anchorFrequency, candidateCount)
	if candidateUpperBound <= 0 || segmentLength <= 0 || valueLength <= 0 {
		return false
	}
	linearBaseline := uint64(valueLength) + uint64(segmentLength)
	if linearBaseline > ^uint64(0)/byteLikeConvolutionRelativeWorkFactor {
		return false
	}
	scaledLinearBaseline := linearBaseline * byteLikeConvolutionRelativeWorkFactor
	return uint64(candidateUpperBound) > scaledLinearBaseline/uint64(segmentLength)
}

func (compiled *compiledByteLikePattern) rarestLiteralRun(
	start, end int,
	literalFrequency *[256]int,
) (bestStart, bestEnd, bestFrequency, literalCount int, err error) {
	maxInt := int(^uint(0) >> 1)
	bestFrequency = maxInt
	for at := start; at < end; {
		if (at-start)&(byteLikeCancellationCheckInterval-1) == 0 {
			if err = compiled.byteLikeCancellationError(); err != nil {
				return 0, 0, 0, 0, err
			}
		}
		if compiled.kinds[at] != byteLikeLiteral {
			at++
			continue
		}
		runStart := at
		runFrequency := maxInt
		for at < end && compiled.kinds[at] == byteLikeLiteral {
			if (at-start)&(byteLikeCancellationCheckInterval-1) == 0 {
				if err = compiled.byteLikeCancellationError(); err != nil {
					return 0, 0, 0, 0, err
				}
			}
			if literalFrequency[compiled.literals[at]] < runFrequency {
				runFrequency = literalFrequency[compiled.literals[at]]
			}
			literalCount++
			at++
		}
		if runFrequency < bestFrequency ||
			(runFrequency == bestFrequency && at-runStart >= bestEnd-bestStart) {
			bestStart, bestEnd = runStart, at
			bestFrequency = runFrequency
		}
	}
	return bestStart, bestEnd, bestFrequency, literalCount, nil
}

func nextByteLikeToken(pattern []byte, at int, escape []byte, escapeEnabled bool) (kind byte, literal []byte, next int) {
	if at >= len(pattern) {
		return byteLikeLiteral, nil, at
	}
	if escapeEnabled && len(escape) > 0 && len(escape) <= len(pattern)-at &&
		bytes.Equal(pattern[at:at+len(escape)], escape) {
		next = at + len(escape)
		if next >= len(pattern) {
			return byteLikeLiteral, pattern[at:next], next
		}
		return byteLikeLiteral, pattern[next : next+1], next + 1
	}
	switch pattern[at] {
	case '_':
		return byteLikeOne, nil, at + 1
	case '%':
		return byteLikeAny, nil, at + 1
	default:
		return byteLikeLiteral, pattern[at : at+1], at + 1
	}
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

func (op *opBuiltInRegexp) builtInRegexpSubstr(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
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

func (op *opBuiltInRegexp) builtInRegexpInstr(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	rs := vector.MustFunctionResult[int64](result)
	switch len(parameters) {
	case 2:
		return opBinaryStrStrToFixedWithErrorCheck[int64](parameters, result, proc, length, func(v1, v2 string) (int64, error) {
			return op.regMap.regularInstr(v2, v1, 1, 1, 0)
		}, selectList)

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

func (op *opBuiltInRegexp) builtInRegexpReplace(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
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

type regexpSet struct {
	mp map[string]*regexp.Regexp
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

		// pat can be a zero-copy string backed by a reusable input vector. Both
		// map keys and regexp expressions must outlive the current data block.
		pat = strings.Clone(pat)
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
	// check position
	if regexpSearchPositionOutOfBounds(str, pos) {
		return false, "", moerr.NewInvalidInputNoCtxf("regexp_substr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, len(str))
	}
	// check occurrence
	if occurrence < 1 {
		return false, "", moerr.NewInvalidInputNoCtxf("regexp_substr have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	reg, err := rs.getRegularMatcher(pat)
	if err != nil {
		return false, "", err
	}

	// match and return
	matches := reg.FindAllString(str[pos-1:], -1)
	if l := int64(len(matches)); l < occurrence {
		return false, "", nil
	}
	return true, matches[occurrence-1], nil
}

func (rs *regexpSet) regularReplace(pat string, str string, repl string, pos, occurrence int64) (r string, err error) {
	// check position
	if pos < 1 || pos > int64(len(str)) {
		return "", moerr.NewInvalidInputNoCtxf("regexp_replace: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, len(str))
	}
	// check occurrence
	if occurrence < 0 {
		return "", moerr.NewInvalidInputNoCtxf("regexp_replace have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}

	reg, err := rs.getRegularMatcher(pat)
	if err != nil {
		pat = "[" + pat + "]"
		return "", moerr.NewInvalidArgNoCtx("regexp_replace have invalid regexp pattern arg", pat)
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
	// check position
	if regexpSearchPositionOutOfBounds(str, pos) {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, len(str))
	}
	// check occurrence
	if occurrence < 1 {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	// check retOption
	if retOption < 0 || retOption > 1 {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr have Index out of bounds in regular expression search, return option %d", retOption)
	}

	reg, err := rs.getRegularMatcher(pat)
	if err != nil {
		pat = "[" + pat + "]"
		return 0, moerr.NewInvalidArgNoCtx("regexp_instr have invalid regexp pattern arg", pat)
	}

	matches := reg.FindAllStringIndex(str[pos-1:], -1)
	if int64(len(matches)) < occurrence {
		return 0, nil
	}
	return int64(matches[occurrence-1][retOption]) + pos, nil
}

func regexpSearchPositionOutOfBounds(str string, pos int64) bool {
	// Position 1 is the sole valid search start for an empty subject.
	return pos < 1 || (pos > int64(len(str)) && !(len(str) == 0 && pos == 1))
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
