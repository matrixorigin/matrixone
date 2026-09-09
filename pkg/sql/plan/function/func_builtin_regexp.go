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
	"regexp/syntax"
	"slices"
	"strconv"
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
			mp: make(map[regexpCacheKey]*regexp.Regexp, mapSizeForRegexp),
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
		nextCursor := matchAt + segmentEnd - segmentAt
		segmentAt = segmentEnd
		for segmentAt < suffixAt && compiled.kinds[segmentAt] == byteLikeAny {
			segmentAt++
		}
		if segmentAt < suffixAt {
			// Keep frequencies exact for the next segment's [cursor, searchLimit) range.
			// Across the complete match each value byte is removed at most once.
			for at := cursor; at < nextCursor; at++ {
				if (at-cursor)&(byteLikeCancellationCheckInterval-1) == 0 {
					if err := compiled.byteLikeCancellationError(); err != nil {
						return false, err
					}
				}
				literalFrequency[value[at]]--
			}
		}
		cursor = nextCursor
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
	if anchorFrequency == 0 {
		return -1, nil
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
		if err := compiled.byteLikeCancellationError(); err != nil {
			return -1, err
		}
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
	return op.builtInRegexpPredicate(parameters, result, length, selectList, false, false)
}

func (op *opBuiltInRegexp) builtInNotRegMatch(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return op.builtInRegexpPredicate(parameters, result, length, selectList, false, true)
}

// builtInRegexpPredicate is shared by REGEXP/RLIKE, NOT REGEXP and
// REGEXP_LIKE. Each operand owns its input decoding; neither operand can
// reinterpret its peer's bytes merely by being binary.
func (op *opBuiltInRegexp) builtInRegexpPredicate(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	length int,
	selectList *FunctionSelectList,
	like, negate bool,
) error {
	if len(parameters) < 2 || len(parameters) > 3 || (!like && len(parameters) != 2) {
		return moerr.NewInvalidInputNoCtx("invalid regexp predicate arity")
	}
	if len(parameters) == 2 && !parameters[0].HasNull() {
		if binary, uniform := regexpMatchDomainUniform(parameters); uniform {
			return opBinaryStrStrToFixedWithErrorCheck[bool](
				parameters, result, nil, length,
				func(subject, pattern string) (bool, error) {
					if !binary {
						subject = regexpValidTextPrefix(subject)
						pattern = regexpValidTextPrefix(pattern)
					}
					var match bool
					var err error
					if like {
						match, err = op.regMap.regularLikeWithMode(pattern, subject, "c", binary)
					} else {
						match, err = op.regMap.regularMatchWithMode(pattern, subject, binary)
					}
					if negate {
						match = !match
					}
					return match, err
				}, selectList)
		}
	}
	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	var p3 vector.FunctionParameterWrapper[types.Varlena]
	if len(parameters) == 3 {
		p3 = vector.GenerateFunctionStrParameter(parameters[2])
	}
	rs := vector.MustFunctionResult[bool](result)

	for i := uint64(0); i < uint64(length); i++ {
		if regexpRowMasked(selectList, i) {
			if err := rs.Append(false, true); err != nil {
				return err
			}
			continue
		}
		subject, subjectNull := p1.GetStrValue(i)
		pattern, patternNull := p2.GetStrValue(i)
		matchType, matchTypeNull := []byte("c"), false
		if len(parameters) == 3 {
			matchType, matchTypeNull = p3.GetStrValue(i)
		}
		if matchTypeNull {
			if err := rs.Append(false, true); err != nil {
				return err
			}
			continue
		}
		matchTypeString := functionUtil.QuickBytesToStr(matchType)
		pureMatchType := ""
		if like {
			// MySQL validates a present match_type before a NULL pattern or
			// subject can determine the row result.
			var err error
			pureMatchType, err = getPureMatchType(matchTypeString)
			if err != nil {
				return err
			}
		}
		if patternNull {
			if err := rs.Append(false, true); err != nil {
				return err
			}
			continue
		}

		patternString := functionUtil.QuickBytesToStr(pattern)
		subjectString := functionUtil.QuickBytesToStr(subject)
		binary := regexpMatchUsesBinary(parameters, int(i))
		if !binary {
			if parameters[0].GetIsBinaryStringAt(int(i)) {
				subjectString = regexpBinaryBytesToText(subjectString)
			} else {
				subjectString = regexpValidTextPrefix(subjectString)
			}
			if parameters[1].GetIsBinaryStringAt(int(i)) {
				patternString = regexpBinaryBytesToText(patternString)
			} else {
				patternString = regexpValidTextPrefix(patternString)
			}
		}
		var reg *regexp.Regexp
		var err error
		if like {
			reg, err = op.regMap.getRegularLikeMatcherForPureMatchTypeWithMode(
				patternString, pureMatchType, binary)
		} else {
			reg, err = op.regMap.getRegularMatcherForMatchWithMode(patternString, binary)
		}
		if err != nil {
			return err
		}
		if subjectNull {
			if err := rs.Append(false, true); err != nil {
				return err
			}
			continue
		}

		match := regexpMatchCompiled(
			reg,
			subjectString,
			binary,
			binary && strings.ContainsRune(pureMatchType, 'i'),
		)
		if negate {
			match = !match
		}
		if err = rs.Append(match, false); err != nil {
			return err
		}
	}
	return nil
}

func (op *opBuiltInRegexp) builtInRegexpSubstr(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := newRegexpStringParameter(parameters, 0)
	p2 := newRegexpStringParameter(parameters, 1)

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
			matchingIsBinary := regexpMatchUsesBinary(parameters, int(i))
			if null2 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResult(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_substr", null1,
			); err != nil {
				return err
			} else if null1 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				match, res, err := op.regMap.regularSubstrWithMode(pat, expr, 1, 1, matchingIsBinary)
				res = regexpEncodeResult(res, matchingIsBinary, regexpResultUsesBinary(parameters, int(i)))
				matchingIsBinary = regexpResultUsesBinary(parameters, int(i))
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), !match); err != nil {
					return err
				}
				if match {
					if err = setRegexpResultDomain(rs.GetResultVector(), int(i), matchingIsBinary, proc); err != nil {
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
			matchingIsBinary := regexpMatchUsesBinary(parameters, int(i))
			if null2 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResult(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_substr", null1 || null3,
			); err != nil {
				return err
			} else if null1 || null3 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				match, res, err := op.regMap.regularSubstrWithMode(pat, expr, pos, 1, matchingIsBinary)
				res = regexpEncodeResult(res, matchingIsBinary, regexpResultUsesBinary(parameters, int(i)))
				matchingIsBinary = regexpResultUsesBinary(parameters, int(i))
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), !match); err != nil {
					return err
				}
				if match {
					if err = setRegexpResultDomain(rs.GetResultVector(), int(i), matchingIsBinary, proc); err != nil {
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
			matchingIsBinary := regexpMatchUsesBinary(parameters, int(i))
			if null2 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResult(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_substr", null1 || null3 || null4,
			); err != nil {
				return err
			} else if null1 || null3 || null4 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				match, res, err := op.regMap.regularSubstrWithMode(pat, expr, pos, ocur, matchingIsBinary)
				res = regexpEncodeResult(res, matchingIsBinary, regexpResultUsesBinary(parameters, int(i)))
				matchingIsBinary = regexpResultUsesBinary(parameters, int(i))
				if err != nil {
					return err
				}
				if err = rs.AppendBytes(functionUtil.QuickStrToBytes(res), !match); err != nil {
					return err
				}
				if match {
					if err = setRegexpResultDomain(rs.GetResultVector(), int(i), matchingIsBinary, proc); err != nil {
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
	p1 := newRegexpStringParameter(parameters, 0)
	p2 := newRegexpStringParameter(parameters, 1)

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
			matchingIsBinary := regexpMatchUsesBinary(parameters, int(i))
			if null2 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
			if err := op.regMap.validateRegexpBeforeNullableResult(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_instr", null1,
			); err != nil {
				return err
			}
			if null1 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
				continue
			}
			index, err := op.regMap.regularInstrWithMode(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), 1, 1, 0, matchingIsBinary)
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
			matchingIsBinary := regexpMatchUsesBinary(parameters, int(i))
			if null2 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResult(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_instr", null1 || null3,
			); err != nil {
				return err
			} else if null1 || null3 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				index, err := op.regMap.regularInstrWithMode(pat, expr, pos, 1, 0, matchingIsBinary)
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
			matchingIsBinary := regexpMatchUsesBinary(parameters, int(i))
			if null2 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResult(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_instr", null1 || null3 || null4,
			); err != nil {
				return err
			} else if null1 || null3 || null4 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				index, err := op.regMap.regularInstrWithMode(pat, expr, pos, ocur, 0, matchingIsBinary)
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
			matchingIsBinary := regexpMatchUsesBinary(parameters, int(i))
			if null2 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResult(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_instr", null1 || null3 || null4 || null5,
			); err != nil {
				return err
			} else if null1 || null3 || null4 || null5 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				index, err := op.regMap.regularInstrWithMode(pat, expr, pos, ocur, resOp, matchingIsBinary)
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
	return op.builtInRegexpPredicate(parameters, result, length, selectList, true, false)
}

func (op *opBuiltInRegexp) builtInRegexpReplace(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	p1 := newRegexpStringParameter(parameters, 0)
	p2 := newRegexpStringParameter(parameters, 1)
	p3 := newRegexpStringParameter(parameters, 2)
	rs := vector.MustFunctionResult[types.Varlena](result)

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
			matchingIsBinary := regexpMatchUsesBinary(parameters, int(i))
			if null2 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResult(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_replace", null1 || null3,
			); err != nil {
				return err
			} else if null1 || null3 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				replacement := functionUtil.QuickBytesToStr(v3)
				val, err := op.regMap.regularReplaceWithMode(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), replacement, 1, 0, matchingIsBinary)
				val = regexpEncodeResult(val, matchingIsBinary, regexpResultUsesBinary(parameters, int(i)))
				matchingIsBinary = regexpResultUsesBinary(parameters, int(i))
				if err != nil {
					return err
				}
				if err = rs.AppendBytes([]byte(val), false); err != nil {
					return err
				}
				if err = setRegexpResultDomain(rs.GetResultVector(), int(i), matchingIsBinary, proc); err != nil {
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
			matchingIsBinary := regexpMatchUsesBinary(parameters, int(i))
			if null2 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResult(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_replace", null1 || null3 || null4,
			); err != nil {
				return err
			} else if null1 || null3 || null4 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				replacement := functionUtil.QuickBytesToStr(v3)
				val, err := op.regMap.regularReplaceWithMode(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), replacement, v4, 0, matchingIsBinary)
				val = regexpEncodeResult(val, matchingIsBinary, regexpResultUsesBinary(parameters, int(i)))
				matchingIsBinary = regexpResultUsesBinary(parameters, int(i))
				if err != nil {
					return err
				}
				if err = rs.AppendBytes([]byte(val), false); err != nil {
					return err
				}
				if err = setRegexpResultDomain(rs.GetResultVector(), int(i), matchingIsBinary, proc); err != nil {
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
			matchingIsBinary := regexpMatchUsesBinary(parameters, int(i))
			if null2 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResult(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_replace", null1 || null3 || null4 || null5,
			); err != nil {
				return err
			} else if null1 || null3 || null4 || null5 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				replacement := functionUtil.QuickBytesToStr(v3)
				val, err := op.regMap.regularReplaceWithMode(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), replacement, v4, v5, matchingIsBinary)
				val = regexpEncodeResult(val, matchingIsBinary, regexpResultUsesBinary(parameters, int(i)))
				matchingIsBinary = regexpResultUsesBinary(parameters, int(i))
				if err != nil {
					return err
				}
				if err = rs.AppendBytes([]byte(val), false); err != nil {
					return err
				}
				if err = setRegexpResultDomain(rs.GetResultVector(), int(i), matchingIsBinary, proc); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Only a homogeneous binary pair can use the byte matcher. Mixed pairs
// require independent decoding, not a shared IsBin shortcut.
func regexpMatchUsesBinary(parameters []*vector.Vector, row int) bool {
	if len(parameters) < RegexpMatchStringOperandCount ||
		!parameters[0].GetIsBinaryStringAt(row) || !parameters[1].GetIsBinaryStringAt(row) {
		return false
	}
	for _, parameter := range parameters[:RegexpMatchStringOperandCount] {
		switch parameter.GetStringSourceAt(row) {
		case types.StringSourceSQLPrepare, types.StringSourceCOMStmt:
			// Markers keep a text result charset. Use Unicode matching so a
			// text replacement need not round-trip through a byte alphabet.
			return false
		}
	}
	return true
}

type regexpReplacementDomainConverter struct {
	parameter             *vector.Vector
	mayBeBinary           bool
	constant              bool
	constantText          string
	constantTextConverted bool
}

func newRegexpReplacementDomainConverter(parameter *vector.Vector) regexpReplacementDomainConverter {
	return regexpReplacementDomainConverter{
		parameter: parameter,
		mayBeBinary: types.StaticStringDomain(*parameter.GetType()) == types.StringDomainBinary ||
			parameter.HasBinaryStringMetadata(),
		constant: parameter.IsConst(),
	}
}

func (c *regexpReplacementDomainConverter) forMatchDomain(
	replacement string,
	row int,
	matchingIsBinary bool,
) string {
	if !c.mayBeBinary || matchingIsBinary || !c.parameter.GetIsBinaryStringAt(row) {
		return replacement
	}
	if !c.constant {
		return regexpBinaryBytesToText(replacement)
	}
	if !c.constantTextConverted {
		c.constantText = regexpBinaryBytesToText(replacement)
		c.constantTextConverted = true
	}
	return c.constantText
}

// MySQL presents binary strings to its regexp library as Windows-1252 so that
// each source byte has a stable character value. Replacement conversion uses
// this when a binary replacement enters a text result; REGEXP_LIKE also uses
// it while explicit case folding is active. Keep ASCII zero-copy.
func regexpBinaryBytesToText(value string) string {
	firstHighByte := -1
	for i := 0; i < len(value); i++ {
		if value[i] >= utf8.RuneSelf {
			firstHighByte = i
			break
		}
	}
	if firstHighByte == -1 {
		return value
	}

	var converted strings.Builder
	converted.Grow(len(value))
	converted.WriteString(value[:firstHighByte])
	for i := firstHighByte; i < len(value); i++ {
		current := value[i]
		if current < utf8.RuneSelf {
			converted.WriteByte(current)
		} else {
			converted.WriteRune(regexpWindows1252Rune(current))
		}
	}
	return converted.String()
}

func regexpWindows1252Rune(value byte) rune {
	switch value {
	case 0x80:
		return 0x20ac
	case 0x82:
		return 0x201a
	case 0x83:
		return 0x0192
	case 0x84:
		return 0x201e
	case 0x85:
		return 0x2026
	case 0x86:
		return 0x2020
	case 0x87:
		return 0x2021
	case 0x88:
		return 0x02c6
	case 0x89:
		return 0x2030
	case 0x8a:
		return 0x0160
	case 0x8b:
		return 0x2039
	case 0x8c:
		return 0x0152
	case 0x8e:
		return 0x017d
	case 0x91:
		return 0x2018
	case 0x92:
		return 0x2019
	case 0x93:
		return 0x201c
	case 0x94:
		return 0x201d
	case 0x95:
		return 0x2022
	case 0x96:
		return 0x2013
	case 0x97:
		return 0x2014
	case 0x98:
		return 0x02dc
	case 0x99:
		return 0x2122
	case 0x9a:
		return 0x0161
	case 0x9b:
		return 0x203a
	case 0x9c:
		return 0x0153
	case 0x9e:
		return 0x017e
	case 0x9f:
		return 0x0178
	default:
		// MySQL maps the five undefined Windows-1252 bytes to their
		// same-numbered C1 control characters.
		return rune(value)
	}
}

// regexpMatchDomainUniform identifies the common no-row-metadata case so
// boolean predicates retain the allocation-free vectorized executor. Mixed
// prepared/user-variable batches fall back to the row-aware loop above.
func regexpMatchDomainUniform(parameters []*vector.Vector) (binary, uniform bool) {
	if len(parameters) < RegexpMatchStringOperandCount {
		return false, false
	}
	for _, parameter := range parameters[:RegexpMatchStringOperandCount] {
		if parameter.HasBinaryStringRows() || parameter.GetStringSources() != nil {
			return false, false
		}
	}
	subjectBinary := parameters[0].GetIsBinaryStringAt(0)
	patternBinary := parameters[1].GetIsBinaryStringAt(0)
	return subjectBinary, subjectBinary == patternBinary &&
		(!subjectBinary || regexpMatchUsesBinary(parameters, 0))
}

func setRegexpResultDomain(result *vector.Vector, row int, matchingIsBinary bool, proc *process.Process) error {
	domain := types.RuntimeStringText
	if matchingIsBinary {
		domain = types.RuntimeStringBinary
	}
	if (domain == types.RuntimeStringBinary) ==
		(types.StaticStringDomain(*result.GetType()) == types.StringDomainBinary) {
		domain = types.RuntimeStringInherit
	}
	return result.SetRuntimeStringDomainAtWithMP(row, domain, proc.Mp())
}

func regexpRowMasked(selectList *FunctionSelectList, row uint64) bool {
	return selectList != nil && selectList.Contains(row)
}

type regexpSet struct {
	mp            map[regexpCacheKey]*regexp.Regexp
	mayMatchEmpty map[regexpCacheKey]bool
}

func (rs *regexpSet) getRegularMatcher(pat string) (*regexp.Regexp, error) {
	return rs.getRegularMatcherWithMode(pat, false)
}

type regexpCacheKey struct {
	pattern        string
	binary         bool
	binaryCaseFold bool
}

func (rs *regexpSet) getRegularMatcherWithMode(pat string, binary bool) (*regexp.Regexp, error) {
	reg, _, err := rs.getRegularMatcherInfoWithMode(pat, binary)
	return reg, err
}

func (rs *regexpSet) getRegularMatcherInfoWithMode(pat string, binary bool) (*regexp.Regexp, bool, error) {
	return rs.getRegularMatcherInfoWithBinaryCaseFold(pat, binary, false)
}

func (rs *regexpSet) getRegularMatcherInfoWithBinaryCaseFold(
	pat string, binary bool, binaryCaseFold bool,
) (*regexp.Regexp, bool, error) {
	var err error

	key := regexpCacheKey{pattern: pat, binary: binary, binaryCaseFold: binaryCaseFold}
	reg, ok := rs.mp[key]
	if !ok {
		if len(rs.mp) == mapSizeForRegexp {
			for key := range rs.mp {
				delete(rs.mp, key)
				delete(rs.mayMatchEmpty, key)
				break
			}
		}

		// pat can be a zero-copy string backed by a reusable input vector. Both
		// map keys and regexp expressions must outlive the current data block.
		pat = strings.Clone(pat)
		key.pattern = pat
		expression := pat
		if binary {
			expression, err = encodeBinaryRegexpPattern(pat, binaryCaseFold)
			if err != nil {
				return nil, false, err
			}
		}
		reg, err = regexp.Compile(expression)
		if err != nil {
			return nil, false, err
		}
		parsed, parseErr := syntax.Parse(expression, syntax.Perl)
		if parseErr != nil {
			return nil, false, parseErr
		}
		if rs.mayMatchEmpty == nil {
			rs.mayMatchEmpty = make(map[regexpCacheKey]bool, mapSizeForRegexp)
		}
		rs.mp[key] = reg
		rs.mayMatchEmpty[key] = regexpSyntaxMayMatchEmpty(parsed)
	}
	return reg, rs.mayMatchEmpty[key], nil
}

// regexpSyntaxMayMatchEmpty is a conservative, syntax-level property: true
// means some successful path can consume zero input units. It lets ordinary
// replace-all calls keep regexp.ReplaceAllLiteralString's optimized path while
// routing anchors, boundaries, and nullable repetitions through the iterator
// whose empty-match sequence follows MySQL/ICU semantics.
func regexpSyntaxMayMatchEmpty(expr *syntax.Regexp) bool {
	if expr == nil {
		return false
	}
	switch expr.Op {
	case syntax.OpEmptyMatch, syntax.OpBeginLine, syntax.OpEndLine,
		syntax.OpBeginText, syntax.OpEndText, syntax.OpWordBoundary,
		syntax.OpNoWordBoundary:
		return true
	case syntax.OpCapture, syntax.OpPlus:
		return len(expr.Sub) == 1 && regexpSyntaxMayMatchEmpty(expr.Sub[0])
	case syntax.OpStar, syntax.OpQuest:
		return true
	case syntax.OpRepeat:
		return expr.Min == 0 || (len(expr.Sub) == 1 && regexpSyntaxMayMatchEmpty(expr.Sub[0]))
	case syntax.OpConcat:
		for _, sub := range expr.Sub {
			if !regexpSyntaxMayMatchEmpty(sub) {
				return false
			}
		}
		return true
	case syntax.OpAlternate:
		for _, sub := range expr.Sub {
			if regexpSyntaxMayMatchEmpty(sub) {
				return true
			}
		}
	}
	return false
}

func (rs *regexpSet) getRegularMatcherForMatchWithMode(pat string, binary bool) (*regexp.Regexp, error) {
	return rs.getCompiledRegexpWithMode(pat, binary, "")
}

func validateRegexpPattern(pat string) error {
	if pat == "" {
		return moerr.NewRegexpIllegalArgumentNoCtx()
	}
	return nil
}

// getCompiledRegexpWithMode is the shared pattern-validation boundary for
// regexp functions that need only the compiled matcher. Compilation must
// happen before a later NULL or range shortcut can determine the row result.
func (rs *regexpSet) getCompiledRegexpWithMode(
	pat string, binary bool, functionName string,
) (*regexp.Regexp, error) {
	if err := validateRegexpPattern(pat); err != nil {
		return nil, err
	}
	reg, err := rs.getRegularMatcherWithMode(pat, binary)
	if err == nil {
		return reg, nil
	}
	return nil, regexpCompileError(functionName, pat, err)
}

func regexpCompileError(functionName, pat string, err error) error {
	if functionName == "regexp_instr" || functionName == "regexp_replace" {
		return moerr.NewInvalidArgNoCtx(
			functionName+" have invalid regexp pattern arg", "["+pat+"]")
	}
	return err
}

func (rs *regexpSet) validateCompiledRegexpWithMode(
	pat string, binary bool, functionName string,
) error {
	_, err := rs.getCompiledRegexpWithMode(pat, binary, functionName)
	return err
}

func (rs *regexpSet) validateRegexpBeforeNullableResult(
	pat string, binary bool, functionName string, laterArgumentIsNull bool,
) error {
	if laterArgumentIsNull {
		return rs.validateCompiledRegexpWithMode(pat, binary, functionName)
	}
	// The non-NULL execution path compiles exactly once in regular*WithMode.
	// Retain the cheap empty-pattern precedence check here.
	return validateRegexpPattern(pat)
}

func (rs *regexpSet) regularMatchWithMode(pat, str string, binary bool) (bool, error) {
	reg, err := rs.getRegularMatcherForMatchWithMode(pat, binary)
	if err != nil {
		return false, err
	}
	if binary {
		str, _ = encodeBinaryRegexpBytes(str, 0)
	}
	return reg.MatchString(str), nil
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
	reg, err := rs.getCompiledRegexpWithMode(pat, subjectIsBinary, "regexp_substr")
	if err != nil {
		return false, "", err
	}
	// check position
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok && pos == regexpSubjectLength(str, subjectIsBinary)+1 {
		// Value functions may search the terminal boundary; INSTR has a
		// separate policy and rejects this position for non-empty input.
		startByte, ok = len(str), true
	}
	if !ok {
		return false, "", moerr.NewInvalidInputNoCtxf("regexp_substr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	occurrence = max(1, occurrence)
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
	if err = validateRegexpPattern(pat); err != nil {
		return "", err
	}
	reg, mayMatchEmpty, err := rs.getRegularMatcherInfoWithMode(pat, subjectIsBinary)
	if err != nil {
		return "", regexpCompileError("regexp_replace", pat, err)
	}
	// check position
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok && pos == regexpSubjectLength(str, subjectIsBinary)+1 {
		startByte, ok = len(str), true
	}
	if !ok {
		return "", moerr.NewInvalidInputNoCtxf("regexp_replace: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	if occurrence < 0 {
		occurrence = 1
	}

	// MySQL returns an empty subject unchanged, even for a regexp such as ^$
	// that can match an empty string. Compile first so malformed patterns still
	// report their error instead of being hidden by this result shortcut.
	if len(str) == 0 {
		return str, nil
	}
	if startByte == 0 && occurrence == 0 && !mayMatchEmpty {
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
	reg, err := rs.getCompiledRegexpWithMode(pat, subjectIsBinary, "regexp_instr")
	if err != nil {
		return 0, err
	}
	// check position
	startByte, ok := 0, pos >= 1 && len(str) == 0
	if len(str) != 0 {
		startByte, ok = regexpSearchStartByte(str, pos, subjectIsBinary)
	}
	if !ok {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	occurrence = max(1, occurrence)
	// check retOption
	if retOption < 0 || retOption > 1 {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr have Index out of bounds in regular expression search, return option %d", retOption)
	}

	// MySQL REGEXP_INSTR rebases its matcher subject at pos. This is
	// deliberately different from SUBSTR and REPLACE: ^, multiline ^, and word
	// boundaries at pos > 1 are relative to this suffix, not to the original
	// subject. Searching only the suffix also avoids encoding or scanning a
	// discarded binary/text prefix.
	searchSubject := str[startByte:]
	match, found, err := rs.regexpNthMatchAtOrAfter(
		reg, pat, searchSubject, 0, subjectIsBinary, occurrence)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	matchOffset := match[retOption]
	return regexpSuffixByteOffsetToPosition(
		searchSubject, pos, matchOffset, subjectIsBinary), nil
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

// regexpSuffixByteOffsetToPosition translates a matcher offset relative to the
// INSTR suffix back to a one-based SQL position. pos already accounts for the
// discarded prefix, so text mode only counts runes inside the matched suffix;
// rescanning the original prefix would make a near-end search unnecessarily
// linear twice.
func regexpSuffixByteOffsetToPosition(suffix string, pos int64, offset int, subjectIsBinary bool) int64 {
	if offset <= 0 {
		return pos
	}
	if offset > len(suffix) {
		offset = len(suffix)
	}
	if subjectIsBinary {
		return pos + int64(offset)
	}
	return pos + int64(utf8.RuneCountInString(suffix[:offset]))
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
	for nextStart <= len(str) {
		start, end, found, err := rs.regexpFindAtOrAfter(reg, pat, str, nextStart, subjectIsBinary)
		if err != nil {
			return err
		}
		if !found {
			break
		}
		visit(start, end)
		visited++
		if limit > 0 && visited >= limit {
			break
		}
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
		writeBinaryRegexpByte(&b, value[i])
	}
	if startByte == len(value) {
		encodedStart = b.Len()
	}
	return b.String(), encodedStart
}

// encodeBinaryRegexpPattern preserves regexp syntax while mapping literal
// bytes to the subject alphabet. The ordinary binary path uses private-use
// runes for exact byte identity. REGEXP_LIKE with explicit i uses Windows-1252
// runes instead, allowing RE2's Unicode folding to implement MySQL's binary
// facade high-byte folding. Hexadecimal and octal byte escapes follow the same
// rule as literal bytes.
func encodeBinaryRegexpPattern(pattern string, caseFold bool) (string, error) {
	if err := validateBinaryRegexpPattern(pattern); err != nil {
		return "", err
	}
	var b strings.Builder
	b.Grow(len(pattern) * 2)
	quoted := false
	for i := 0; i < len(pattern); {
		if pattern[i] != '\\' {
			writeBinaryRegexpPatternByte(&b, pattern[i], caseFold)
			i++
			continue
		}

		if i+1 >= len(pattern) {
			b.WriteByte(pattern[i])
			break
		}
		if quoted {
			if pattern[i+1] == 'E' {
				b.WriteString(pattern[i : i+2])
				quoted = false
				i += 2
				continue
			}
			b.WriteByte(pattern[i])
			i++
			continue
		}
		if pattern[i+1] == 'Q' {
			b.WriteString(pattern[i : i+2])
			quoted = true
			i += 2
			continue
		}

		value, end, ok := binaryRegexpByteEscape(pattern, i)
		if ok {
			if value >= utf8.RuneSelf {
				writeBinaryRegexpPatternByte(&b, value, caseFold)
			} else {
				b.WriteString(pattern[i:end])
			}
			i = end
			continue
		}
		// Consume an ordinary escape as one token. Advancing only over the
		// backslash would misread the second slash in `\\xFF` as a byte escape.
		b.WriteString(pattern[i : i+2])
		i += 2
	}
	return b.String(), nil
}

func writeBinaryRegexpPatternByte(b *strings.Builder, value byte, caseFold bool) {
	if caseFold && value >= utf8.RuneSelf {
		b.WriteRune(regexpWindows1252Rune(value))
		return
	}
	writeBinaryRegexpByte(b, value)
}

// validateBinaryRegexpPattern defines the public grammar boundary around the
// private-use alphabet used internally for byte matching. Unicode code-point
// and property escapes cannot have byte semantics: accepting them would let a
// caller address the U+E080..U+E0FF implementation alphabet directly (for
// example, \x{E080} would alias byte 0x80 and \p{Co} would match every high
// byte). Reject them before encoding while preserving quoted or escaped text.
func validateBinaryRegexpPattern(pattern string) error {
	quoted := false
	for i := 0; i < len(pattern); {
		if pattern[i] != '\\' {
			i++
			continue
		}
		if i+1 >= len(pattern) {
			break
		}
		if quoted {
			if pattern[i+1] == 'E' {
				quoted = false
			}
			i += 2
			continue
		}
		switch pattern[i+1] {
		case 'Q':
			quoted = true
			i += 2
		case 'p', 'P':
			return moerr.NewInvalidInputNoCtx(
				"binary regular expressions do not support Unicode property escapes")
		case 'x':
			if i+2 >= len(pattern) || pattern[i+2] != '{' {
				i += 2
				continue
			}
			close := strings.IndexByte(pattern[i+3:], '}')
			if close < 0 {
				// Let regexp.Compile produce the ordinary malformed-pattern
				// diagnostic rather than inventing a second parser here.
				return nil
			}
			end := i + 3 + close
			value, err := strconv.ParseUint(pattern[i+3:end], 16, 32)
			if err == nil && value > 0xff {
				return moerr.NewInvalidInputNoCtx(
					"binary regular expressions only support byte escapes up to \\x{FF}")
			}
			i = end + 1
		default:
			// Consume the escaped token as a unit so \\p is treated as a
			// literal backslash followed by p, not a property escape.
			i += 2
		}
	}
	return nil
}

// binaryRegexpByteEscape recognizes the numeric escapes accepted by Go RE2
// when they denote one byte. Keeping recognition here deliberately narrow
// avoids reinterpreting escapes such as \\b, \\p, or quoted regexp text.
func binaryRegexpByteEscape(pattern string, start int) (byte, int, bool) {
	if start+1 >= len(pattern) || pattern[start] != '\\' {
		return 0, start, false
	}
	if pattern[start+1] == 'x' {
		if start+2 < len(pattern) && pattern[start+2] == '{' {
			close := strings.IndexByte(pattern[start+3:], '}')
			if close < 0 {
				return 0, start, false
			}
			end := start + 3 + close
			value, err := strconv.ParseUint(pattern[start+3:end], 16, 8)
			if err != nil {
				return 0, start, false
			}
			return byte(value), end + 1, true
		}
		if start+4 > len(pattern) {
			return 0, start, false
		}
		value, err := strconv.ParseUint(pattern[start+2:start+4], 16, 8)
		if err != nil {
			return 0, start, false
		}
		return byte(value), start + 4, true
	}
	if start+4 <= len(pattern) {
		value, err := strconv.ParseUint(pattern[start+1:start+4], 8, 8)
		if err == nil {
			return byte(value), start + 4, true
		}
	}
	return 0, start, false
}

func writeBinaryRegexpByte(b *strings.Builder, value byte) {
	if value < utf8.RuneSelf {
		b.WriteByte(value)
		return
	}
	b.WriteRune(rune(0xE000) + rune(value))
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
	return rs.regularLikeWithMode(pat, str, matchType, false)
}

func (rs *regexpSet) getRegularLikeMatcherForPureMatchTypeWithMode(
	pat string, pureMatchType string, binary bool,
) (*regexp.Regexp, error) {
	if err := validateRegexpPattern(pat); err != nil {
		return nil, err
	}
	rule := fmt.Sprintf("(?%s)%s", pureMatchType, pat)
	binaryCaseFold := binary && strings.ContainsRune(pureMatchType, 'i')
	reg, _, err := rs.getRegularMatcherInfoWithBinaryCaseFold(rule, binary, binaryCaseFold)
	return reg, err
}

func regexpMatchCompiled(reg *regexp.Regexp, str string, binary, binaryCaseFold bool) bool {
	if binary {
		if binaryCaseFold {
			str = regexpBinaryBytesToText(str)
		} else {
			str, _ = encodeBinaryRegexpBytes(str, 0)
		}
	}
	return reg.MatchString(str)
}

func (rs *regexpSet) regularLikeWithMode(pat string, str string, matchType string, binary bool) (bool, error) {
	pureMatchType, err := getPureMatchType(matchType)
	if err != nil {
		return false, err
	}
	reg, err := rs.getRegularLikeMatcherForPureMatchTypeWithMode(pat, pureMatchType, binary)
	if err != nil {
		return false, err
	}
	return regexpMatchCompiled(
		reg, str, binary, binary && strings.ContainsRune(pureMatchType, 'i')), nil
}

// Support four arguments:
// i: case insensitive.
// c: case sensitive.
// m: multiple line mode.
// n: '.' can match line terminator.
// Binary operands default to case-sensitive matching, but an explicit i or c
// still overrides that default.  The rightmost case flag wins in both domains;
// high bytes use MySQL's Windows-1252 binary facade while explicit i is active.
func getPureMatchType(input string) (string, error) {
	retstring := ""
	caseType := ""
	foundn := false
	foundm := false

	for _, c := range input {
		switch c {
		case 'i':
			caseType = "i"
		case 'c':
			caseType = ""
		case 'm':
			if !foundm {
				retstring += "m"
				foundm = true
			}
		case 'n':
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
