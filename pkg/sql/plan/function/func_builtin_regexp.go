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
	"errors"
	"fmt"
	"regexp/syntax"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/internal/regexp"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
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
	return &opBuiltInRegexp{}
}

func (op *opBuiltInRegexp) likeFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	// LIKE compares with the character equivalence of its resolved text
	// collation: utf8mb4_general_ci per-character weights, UCA 4.0.0 weight
	// streams for the unicode_ci family, and the byte matcher for binary
	// string domains (issue #31175).
	fold := likeFoldModeFor(parameters[0].GetType().Charset)
	if len(parameters) == 3 {
		return op.likeFnWithEscape(parameters, result, proc, length, selectList, fold, false)
	}

	// MySQL compares byte-wise when either LIKE operand is a binary
	// string, so the pattern domain participates in the routing exactly
	// as the subject domain does (issue #31175).
	uniformBinary, perRow := stringDomainMode(parameters[0])
	patternBinary, patternPerRow := stringDomainMode(parameters[1])
	if uniformBinary || perRow || patternBinary || patternPerRow {
		return op.likeByStringDomain(
			parameters, result, proc, length, selectList, uniformBinary, perRow,
			patternBinary, patternPerRow,
			[]byte{byte(DefaultEscapeChar)}, true, fold)
	}

	// UCA weights are multi-element streams per character (U+00DF weighs as
	// "ss"), which the 1:1 rune fold of the byte fast paths cannot
	// represent, so unicode_ci patterns use the per-character matcher.
	if fold == likeFoldUnicodeCI {
		return op.likeByStringDomain(
			parameters, result, proc, length, selectList, false, false, false, false,
			[]byte{byte(DefaultEscapeChar)}, true, fold)
	}

	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)

	// optimize rule for some special case.
	if parameters[1].IsConst() && !selectList.IgnoreAllRow() && selectList.ShouldEvalAllRow() {
		specialFnForV := func(i []byte) []byte {
			return i
		}
		if fold == likeFoldGeneralCI {
			// general_ci weights are single canonical runes, so the byte
			// fast paths see exactly the collation's weight stream.
			specialFnForV = generalCILikeFold
		}
		canOptimize, err := optimizeRuleForLike(proc.Ctx, p1, p2, rs, length, specialFnForV)
		if canOptimize {
			return err
		}
	}

	return opBinaryBytesBytesToFixedWithErrorCheck[bool](parameters, result, proc, length,
		op.regMap.likeMatchFn(proc.Ctx, parameters[1].IsConst(), DefaultEscapeChar, true, fold), selectList)
}

func (op *opBuiltInRegexp) iLikeFn(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	if len(parameters) == 3 {
		// ILIKE keeps its historical fold-on-every-domain behavior.
		return op.likeFnWithEscape(parameters, result, proc, length, selectList, likeFoldILike, true)
	}

	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)

	// optimize rule for some special case.
	if parameters[1].IsConst() && !selectList.IgnoreAllRow() && selectList.ShouldEvalAllRow() {
		canOptimize, err := optimizeRuleForLike(proc.Ctx, p1, p2, rs, length, ilikeFold)
		if canOptimize {
			return err
		}
	}

	return opBinaryBytesBytesToFixedWithErrorCheck[bool](parameters, result, proc, length,
		op.regMap.likeMatchFn(proc.Ctx, parameters[1].IsConst(), DefaultEscapeChar, true, likeFoldILike), selectList)
}

func (op *opBuiltInRegexp) likeFnWithEscape(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	fold likeFoldMode,
	foldBinaryDomain bool,
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
	patternBinary, patternPerRow := stringDomainMode(parameters[1])
	if !foldBinaryDomain && (uniformBinary || perRow || patternBinary || patternPerRow) {
		return op.likeByStringDomain(
			parameters[:2], result, proc, length, selectList, uniformBinary, perRow,
			patternBinary, patternPerRow,
			escapeBytes, escapeEnabled, fold)
	}
	if !escapeIsNull && (!utf8.Valid(escapeBytes) || utf8.RuneCount(escapeBytes) > 1) {
		return moerr.NewInvalidInputNoCtx("Incorrect arguments to ESCAPE")
	}
	if fold == likeFoldUnicodeCI {
		// The weight-stream matcher owns unicode_ci escapes as well.
		return op.likeByStringDomain(
			parameters[:2], result, proc, length, selectList, false, false, false, false,
			escapeBytes, escapeEnabled, fold)
	}
	var escape rune
	if escapeEnabled {
		escape, _ = utf8.DecodeRune(escapeBytes)
	}
	return opBinaryBytesBytesToFixedWithErrorCheck[bool](parameters[:2], result, proc, length,
		op.regMap.likeMatchFn(proc.Ctx, parameters[1].IsConst(), escape, escapeEnabled, fold), selectList)
}

func (op *opBuiltInRegexp) likeByStringDomain(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
	uniformBinary, perRow bool,
	patternBinary, patternPerRow bool,
	escapeBytes []byte,
	escapeEnabled bool,
	fold likeFoldMode,
) error {
	values := vector.GenerateFunctionStrParameter(parameters[0])
	patterns := vector.GenerateFunctionStrParameter(parameters[1])
	rs := vector.MustFunctionResult[bool](result)
	constantPattern := parameters[1].IsConst()
	var escapeRune rune
	if escapeEnabled {
		escapeRune, _ = utf8.DecodeRune(escapeBytes)
	}
	var matchText func([]byte, []byte) (bool, error)
	if fold == likeFoldUnicodeCI {
		matchText = unicodeLikeMatchFn(proc.Ctx, constantPattern, escapeRune, escapeEnabled)
	} else {
		matchText = op.regMap.likeMatchFn(proc.Ctx, constantPattern, escapeRune, escapeEnabled, fold)
	}
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
		if binaryStringAt(parameters[0], int(row), uniformBinary, perRow) ||
			binaryStringAt(parameters[1], int(row), patternBinary, patternPerRow) {
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
			matched, err = matchText(value, pattern)
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

// likeFoldMode selects the character equivalence LIKE applies beyond exact
// bytes (issue #31175).
type likeFoldMode uint8

const (
	// likeFoldNone keeps the byte matcher.
	likeFoldNone likeFoldMode = iota
	// likeFoldGeneralCI compares utf8mb4_general_ci per-character weights;
	// each weight is one canonical rune (á=A, ı=I, ß=S, Σ/σ/ς=Σ).
	likeFoldGeneralCI
	// likeFoldUnicodeCI compares UCA 4.0.0 primary weight streams one
	// character at a time (my_wildcmp_uca): á equals a, the dotless ı
	// stays distinct from I, and a multi-weight character such as ß
	// matches only itself, never s or ss.
	likeFoldUnicodeCI
	// likeFoldILike is ILIKE's complete Unicode simple-fold equivalence on
	// every string domain: U+212A folds with k and U+212B with å.
	likeFoldILike
)

// likeFoldModeFor selects the character equivalence a resolved collation
// gives LIKE (issue #31175).
func likeFoldModeFor(charset uint8) likeFoldMode {
	switch {
	case types.IsUnicodeCollation(charset):
		return likeFoldUnicodeCI
	case charset == types.CharsetUTF8:
		return likeFoldGeneralCI
	default:
		return likeFoldNone
	}
}

func likeAllASCII(value []byte) bool {
	for _, b := range value {
		if b >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// generalCILikeFold maps a value to its utf8mb4_general_ci weight stream.
// Every weight is one canonical rune, so the byte-oriented LIKE paths keep
// working on the folded form; the ASCII path matches bytes.ToUpper exactly.
func generalCILikeFold(value []byte) []byte {
	if likeAllASCII(value) {
		return bytes.ToUpper(value)
	}
	return collation.AppendGeneralCIFold(nil, value)
}

// ilikeFoldRune folds r to the smallest lowercase rune in its Unicode
// simple-fold orbit. The orbit keeps every equivalence ILIKE already had
// (U+212A with k, U+212B with å) and the sigma correction (U+03A3, U+03C3
// and U+03C2 share one form), while U+0131 stays distinct from i and U+00DF
// from s (issue #31175).
func ilikeFoldRune(r rune) rune {
	folded := unicode.ToLower(r)
	for s := unicode.SimpleFold(r); s != r; s = unicode.SimpleFold(s) {
		if f := unicode.ToLower(s); f < folded {
			folded = f
		}
	}
	return folded
}

// ilikeFold folds a value rune-wise with ilikeFoldRune, preserving invalid
// bytes so they can only match themselves.
func ilikeFold(value []byte) []byte {
	if likeAllASCII(value) {
		return bytes.ToLower(value)
	}
	folded := make([]byte, 0, len(value))
	for len(value) > 0 {
		r, size := utf8.DecodeRune(value)
		if r == utf8.RuneError && size == 1 {
			folded = append(folded, value[0])
			value = value[1:]
			continue
		}
		folded = utf8.AppendRune(folded, ilikeFoldRune(r))
		value = value[size:]
	}
	return folded
}

// likeFoldValue applies a fold mode to a LIKE subject. likeFoldNone and
// likeFoldUnicodeCI never reach the regexp translation: the former needs no
// fold and the latter is owned by the weight-stream matcher.
func likeFoldValue(fold likeFoldMode, value []byte) []byte {
	switch fold {
	case likeFoldGeneralCI:
		return generalCILikeFold(value)
	case likeFoldILike:
		return ilikeFold(value)
	default:
		return value
	}
}

// unicodeLikeMatchFn builds the per-character weight matcher for the
// unicode_ci family. A constant pattern is tokenized once per batch,
// mirroring the lazy regexp preparation in likeMatchFn.
func unicodeLikeMatchFn(ctx context.Context, constantPattern bool, escape rune, escapeEnabled bool) func([]byte, []byte) (bool, error) {
	var tokens []collation.LikeToken
	ready := false
	return func(value, pattern []byte) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		if !constantPattern || !ready {
			var err error
			tokens, err = collation.TokenizeLikePattern(ctx, pattern, escape, escapeEnabled, collation.UCA400CharWeight)
			if err != nil {
				return false, err
			}
			ready = true
		}
		matched, err := collation.MatchLikePattern(ctx, tokens, value, collation.UCA400CharWeight)
		if err != nil {
			// pkg/common/collation is stdlib-only and cannot raise moerr, so
			// the typed resource-limit error is converted here at the function
			// layer. The message keeps the stable SQL-visible prefix
			// "LIKE pattern resource limit exceeded: " (issue #31175 review).
			var limitErr *collation.LikeResourceLimitError
			if errors.As(err, &limitErr) {
				return false, moerr.NewInternalErrorNoCtx(err.Error())
			}
			return false, err
		}
		return matched, nil
	}
}

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

// Probe only once per 256 fast-path rows; leave error retrieval on the cold path.
func likeBatchCanceled(ctx context.Context, row uint64) bool {
	if row&255 != 0 {
		return false
	}
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func optimizeRuleForLike(ctx context.Context, p1, p2 vector.FunctionParameterWrapper[types.Varlena], rs *vector.FunctionResult[bool], length int,
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
			if likeBatchCanceled(ctx, i) {
				return true, ctx.Err()
			}
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
			if likeBatchCanceled(ctx, i) {
				return true, ctx.Err()
			}
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
			if likeBatchCanceled(ctx, i) {
				return true, ctx.Err()
			}
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
			if likeBatchCanceled(ctx, i) {
				return true, ctx.Err()
			}
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
			if c1 == '%' || c1 == '_' {
				escapeCount := 0
				for i := n - 2; i >= 0 && pat[i] == DefaultEscapeChar; i-- {
					escapeCount++
				}
				// Only an odd-length escape run makes the trailing wildcard literal.
				if escapeCount%2 != 0 {
					c1 = DefaultEscapeChar
				}
			}
			switch {
			case !(c0 == '%' || c0 == '_') && !(c1 == '%' || c1 == '_'):
				// Rule 4.1: no wild card, so it is a simple compare eq.
				literal := functionUtil.RemoveEscapeChar(pat, DefaultEscapeChar)
				for i := uint64(0); i < uint64(length); i++ {
					if likeBatchCanceled(ctx, i) {
						return true, ctx.Err()
					}
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
					if likeBatchCanceled(ctx, i) {
						return true, ctx.Err()
					}
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
					if likeBatchCanceled(ctx, i) {
						return true, ctx.Err()
					}
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
					if likeBatchCanceled(ctx, i) {
						return true, ctx.Err()
					}
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
					if likeBatchCanceled(ctx, i) {
						return true, ctx.Err()
					}
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
					if likeBatchCanceled(ctx, i) {
						return true, ctx.Err()
					}
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
					if likeBatchCanceled(ctx, i) {
						return true, ctx.Err()
					}
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
					if likeBatchCanceled(ctx, i) {
						return true, ctx.Err()
					}
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
				if likeBatchCanceled(ctx, i) {
					return true, ctx.Err()
				}
				v1, null1 := p1.GetStrValue(i)
				v1 = specialFnForV(v1)
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
	return optimizeAnchoredLikeSegments(ctx, p1, rs, pat, length, specialFnForV)
}

func optimizeAnchoredLikeSegments(ctx context.Context, p1 vector.FunctionParameterWrapper[types.Varlena], rs *vector.FunctionResult[bool], pat []byte, length int, specialFnForV func([]byte) []byte) (bool, error) {
	n := len(pat)
	// Extend the existing percent-segment optimization to anchored patterns.
	// Keep escapes, Unicode literals and '_' on the regexp path. Limiting each
	// literal bounds candidate verification work during substring searches.
	const maxLikeLiteralSegment = 64
	if n > 1 && (pat[0] != '%' || pat[n-1] != '%') && bytes.Contains(pat, []byte{'%'}) {
		for _, b := range pat {
			if b >= utf8.RuneSelf || b == '_' || b == DefaultEscapeChar {
				return false, nil
			}
		}
		segments := bytes.Split(pat, []byte{'%'})
		for _, segment := range segments {
			if len(segment) > maxLikeLiteralSegment {
				return false, nil
			}
		}
		prefix, suffix := segments[0], segments[len(segments)-1]
		for row := uint64(0); row < uint64(length); row++ {
			if likeBatchCanceled(ctx, row) {
				return true, ctx.Err()
			}
			value, isNull := p1.GetStrValue(row)
			value = specialFnForV(value)
			matched := false
			if !isNull && len(value) >= len(prefix)+len(suffix) &&
				bytes.HasPrefix(value, prefix) && bytes.HasSuffix(value, suffix) {
				remaining := value[len(prefix) : len(value)-len(suffix)]
				matched = true
				for _, segment := range segments[1 : len(segments)-1] {
					at := bytes.Index(remaining, segment)
					if at < 0 {
						matched = false
						break
					}
					remaining = remaining[at+len(segment):]
				}
			}
			if err := rs.Append(matched, isNull); err != nil {
				return true, err
			}
		}
		return true, nil
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
// REGEXP_LIKE. The subject and pattern jointly select the row's execution
// domain; this is essential for prepared markers because their concrete
// binary/text domain is intentionally not fixed by the binder.
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
	// REGEXP/RLIKE and REGEXP_LIKE take their default case sensitivity from
	// the subject's resolved text collation, exactly as MySQL and the
	// LOCATE/INSTR helpers do. An explicit match_type c/i flag and a binary
	// match domain still override this default (issue #31175).
	collationCaseInsensitive := types.IsCaseInsensitiveCollation(parameters[0].GetType().Charset)
	if len(parameters) == 2 && !parameters[0].HasNull() {
		if binary, uniform := regexpMatchDomainUniform(parameters); uniform {
			defaultMatchType := "c"
			pureDefaultMatchType := ""
			if collationCaseInsensitive && !binary {
				defaultMatchType = "i"
				pureDefaultMatchType = "i"
			}
			return opBinaryStrStrToFixedWithErrorCheck[bool](
				parameters, result, nil, length,
				func(subject, pattern string) (bool, error) {
					var match bool
					var err error
					if like {
						match, err = op.regMap.regularLikeWithMode(pattern, subject, defaultMatchType, binary)
					} else {
						match, err = op.regMap.regularMatchWithPureMatchType(pattern, subject, binary, pureDefaultMatchType)
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
		binary := regexpMatchUsesBinary(parameters, int(i))
		// Without an explicit match_type case flag, a case-insensitive text
		// collation supplies the default; a binary match domain stays
		// case-sensitive unless the caller asked for i (issue #31175).
		userCaseFlag := like && len(parameters) == 3 &&
			(strings.ContainsRune(matchTypeString, 'c') || strings.ContainsRune(matchTypeString, 'i'))
		// Non-case match_type flags (m/n/u) do not block the collation
		// default: MySQL applies the argument collation whenever neither
		// c nor i is present, so an explicit m mode still folds on a ci
		// collation (issue #31175).
		if collationCaseInsensitive && !binary && !userCaseFlag &&
			!strings.ContainsRune(pureMatchType, 'i') {
			pureMatchType += "i"
		}
		if op.regMap.requiresRegexp2(patternString, pureMatchType, binary) {
			matcher, err := op.regMap.getRegexp2MatcherWithMatchType(
				patternString, pureMatchType, binary)
			if err != nil {
				return err
			}
			if subjectNull {
				if err := rs.Append(false, true); err != nil {
					return err
				}
				continue
			}
			match, err := op.regMap.regexp2MatchWithMatchType(
				matcher, functionUtil.QuickBytesToStr(subject), binary, pureMatchType)
			if err != nil {
				return err
			}
			if negate {
				match = !match
			}
			if err = rs.Append(match, false); err != nil {
				return err
			}
			continue
		}
		var reg *regexp.Regexp
		var err error
		if like {
			reg, err = op.regMap.getRegularLikeMatcherForPureMatchTypeWithMode(
				patternString, pureMatchType, binary)
		} else {
			reg, err = op.regMap.getCompiledRegexpWithMatchType(patternString, pureMatchType, binary, "")
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
			functionUtil.QuickBytesToStr(subject),
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

	case 4, 5:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		occurrences := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		var matchTypes vector.FunctionParameterWrapper[types.Varlena]
		if len(parameters) == 5 {
			matchTypes = vector.GenerateFunctionStrParameter(parameters[4])
		}
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
			pureMatchType := ""
			if len(parameters) == 5 {
				matchType, matchTypeNull := matchTypes.GetStrValue(i)
				if matchTypeNull {
					if err := rs.AppendBytes(nil, true); err != nil {
						return err
					}
					continue
				}
				var err error
				pureMatchType, err = getPureMatchType(functionUtil.QuickBytesToStr(matchType))
				if err != nil {
					return err
				}
			}
			if null2 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResultWithMatchType(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_substr", null1 || null3 || null4,
				pureMatchType,
			); err != nil {
				return err
			} else if null1 || null3 || null4 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				match, res, err := op.regMap.regularSubstrWithMatchType(
					pat, expr, pos, ocur, matchingIsBinary, pureMatchType)
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

	case 5, 6:
		positions := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
		occurrences := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		resultOption := vector.GenerateFunctionFixedTypeParameter[int8](parameters[4])
		var matchTypes vector.FunctionParameterWrapper[types.Varlena]
		if len(parameters) == 6 {
			matchTypes = vector.GenerateFunctionStrParameter(parameters[5])
		}
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
			pureMatchType := ""
			if len(parameters) == 6 {
				matchType, matchTypeNull := matchTypes.GetStrValue(i)
				if matchTypeNull {
					if err := rs.Append(0, true); err != nil {
						return err
					}
					continue
				}
				var err error
				pureMatchType, err = getPureMatchType(functionUtil.QuickBytesToStr(matchType))
				if err != nil {
					return err
				}
			}
			if null2 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResultWithMatchType(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_instr", null1 || null3 || null4 || null5,
				pureMatchType,
			); err != nil {
				return err
			} else if null1 || null3 || null4 || null5 {
				if err := rs.Append(0, true); err != nil {
					return err
				}
			} else {
				expr, pat := functionUtil.QuickBytesToStr(v1), functionUtil.QuickBytesToStr(v2)
				index, err := op.regMap.regularInstrWithMatchType(
					pat, expr, pos, ocur, resOp, matchingIsBinary, pureMatchType)
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
	p1 := vector.GenerateFunctionStrParameter(parameters[0]) // expr
	p2 := vector.GenerateFunctionStrParameter(parameters[1]) // pat
	p3 := vector.GenerateFunctionStrParameter(parameters[2]) // repl
	rs := vector.MustFunctionResult[types.Varlena](result)
	replacementConverter := newRegexpReplacementDomainConverter(parameters[2])

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
				if replacementConverter.mayBeBinary {
					replacement = replacementConverter.forMatchDomain(replacement, int(i), matchingIsBinary)
				}
				val, err := op.regMap.regularReplaceWithMode(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), replacement, 1, 0, matchingIsBinary)
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
				if replacementConverter.mayBeBinary {
					replacement = replacementConverter.forMatchDomain(replacement, int(i), matchingIsBinary)
				}
				val, err := op.regMap.regularReplaceWithMode(functionUtil.QuickBytesToStr(v2), functionUtil.QuickBytesToStr(v1), replacement, v4, 0, matchingIsBinary)
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

	case 5, 6:
		p4 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[3])
		p5 := vector.GenerateFunctionFixedTypeParameter[int64](parameters[4])
		var matchTypes vector.FunctionParameterWrapper[types.Varlena]
		if len(parameters) == 6 {
			matchTypes = vector.GenerateFunctionStrParameter(parameters[5])
		}
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
			pureMatchType := ""
			if len(parameters) == 6 {
				matchType, matchTypeNull := matchTypes.GetStrValue(i)
				if matchTypeNull {
					if err := rs.AppendBytes(nil, true); err != nil {
						return err
					}
					continue
				}
				var err error
				pureMatchType, err = getPureMatchType(functionUtil.QuickBytesToStr(matchType))
				if err != nil {
					return err
				}
			}
			if null2 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else if err := op.regMap.validateRegexpBeforeNullableResultWithMatchType(
				functionUtil.QuickBytesToStr(v2),
				matchingIsBinary,
				"regexp_replace", null1 || null3 || null4 || null5,
				pureMatchType,
			); err != nil {
				return err
			} else if null1 || null3 || null4 || null5 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
			} else {
				replacement := functionUtil.QuickBytesToStr(v3)
				if replacementConverter.mayBeBinary {
					replacement = replacementConverter.forMatchDomain(replacement, int(i), matchingIsBinary)
				}
				val, err := op.regMap.regularReplaceWithMatchType(
					functionUtil.QuickBytesToStr(v2),
					functionUtil.QuickBytesToStr(v1),
					replacement,
					v4,
					v5,
					matchingIsBinary,
					pureMatchType,
				)
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

// regexpMatchUsesBinary owns the execution-domain rule shared by every REGEXP
// function: only the subject and pattern select text or byte matching. Other
// string arguments, such as REGEXP_REPLACE's replacement, may have separate
// compatibility rules but never change the matcher domain.
func regexpMatchUsesBinary(parameters []*vector.Vector, row int) bool {
	operandCount := min(RegexpMatchStringOperandCount, len(parameters))
	for i := 0; i < operandCount; i++ {
		if parameters[i].GetIsBinaryStringAt(row) {
			return true
		}
	}
	return false
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
// this when a binary replacement enters a text result; case-insensitive
// matching uses it to preserve one matcher character per source byte. Keep
// ASCII zero-copy.
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

// encodeBinaryRegexpText maps every binary byte to its Windows-1252 rune and
// returns the corresponding UTF-8 byte offset for startByte. Each source byte
// remains exactly one rune, so decoded match offsets still map to SQL byte
// positions.
func encodeBinaryRegexpText(value string, startByte int) (encoded string, encodedStart int) {
	if startByte < 0 {
		startByte = 0
	} else if startByte > len(value) {
		startByte = len(value)
	}
	if isASCIIBytes(value) {
		return value, startByte
	}

	var converted strings.Builder
	converted.Grow(len(value))
	for i := 0; i < len(value); i++ {
		if i == startByte {
			encodedStart = converted.Len()
		}
		if value[i] < utf8.RuneSelf {
			converted.WriteByte(value[i])
		} else {
			converted.WriteRune(regexpWindows1252Rune(value[i]))
		}
	}
	if startByte == len(value) {
		encodedStart = converted.Len()
	}
	return converted.String(), encodedStart
}

// decodeBinaryRegexpText is the inverse of regexpBinaryBytesToText for text
// created from a binary operand. Non-Windows-1252 runes are preserved.
func decodeBinaryRegexpText(encoded string) string {
	if isASCIIBytes(encoded) {
		return encoded
	}

	var decoded strings.Builder
	decoded.Grow(len(encoded))
	for _, r := range encoded {
		if value, ok := regexpWindows1252Byte(r); ok {
			decoded.WriteByte(value)
		} else {
			decoded.WriteRune(r)
		}
	}
	return decoded.String()
}

func regexpWindows1252Byte(value rune) (byte, bool) {
	switch value {
	case 0x20ac:
		return 0x80, true
	case 0x201a:
		return 0x82, true
	case 0x0192:
		return 0x83, true
	case 0x201e:
		return 0x84, true
	case 0x2026:
		return 0x85, true
	case 0x2020:
		return 0x86, true
	case 0x2021:
		return 0x87, true
	case 0x02c6:
		return 0x88, true
	case 0x2030:
		return 0x89, true
	case 0x0160:
		return 0x8a, true
	case 0x2039:
		return 0x8b, true
	case 0x0152:
		return 0x8c, true
	case 0x017d:
		return 0x8e, true
	case 0x2018:
		return 0x91, true
	case 0x2019:
		return 0x92, true
	case 0x201c:
		return 0x93, true
	case 0x201d:
		return 0x94, true
	case 0x2022:
		return 0x95, true
	case 0x2013:
		return 0x96, true
	case 0x2014:
		return 0x97, true
	case 0x02dc:
		return 0x98, true
	case 0x2122:
		return 0x99, true
	case 0x0161:
		return 0x9a, true
	case 0x203a:
		return 0x9b, true
	case 0x0153:
		return 0x9c, true
	case 0x017e:
		return 0x9e, true
	case 0x0178:
		return 0x9f, true
	default:
		if value >= 0x80 && value <= 0xff {
			return byte(value), true
		}
		return 0, false
	}
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
	operandCount := min(RegexpMatchStringOperandCount, len(parameters))
	for i := 0; i < operandCount; i++ {
		parameter := parameters[i]
		if parameter.HasBinaryStringRows() {
			return false, false
		}
		if types.StaticStringDomain(*parameter.GetType()) == types.StringDomainBinary ||
			parameter.GetIsBinaryString() {
			binary = true
		}
	}
	return binary, true
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
	icu           map[regexpCacheKey]*regexp2Matcher
	icuBytes      int
}

func (rs *regexpSet) requiresRegexp2(pat, matchType string, binary bool) bool {
	key := regexpCacheKey{pattern: pat, matchType: matchType, binary: binary, binaryCaseFold: binary && strings.ContainsRune(matchType, 'i')}
	if _, ok := rs.mp[key]; ok {
		return false
	}
	if _, ok := rs.icu[key]; ok {
		return true
	}
	return requiresRegexp2Pattern(pat)
}

func (rs *regexpSet) getRegularMatcher(pat string) (*regexp.Regexp, error) {
	return rs.getRegularMatcherWithMode(pat, false)
}

type regexpCacheKey struct {
	matchType      string
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
	return rs.getRegularMatcherInfoForKey(regexpCacheKey{pattern: pat, binary: binary, binaryCaseFold: binaryCaseFold})
}

func (rs *regexpSet) getRegularMatcherInfoForKey(key regexpCacheKey) (*regexp.Regexp, bool, error) {
	reg, ok := rs.mp[key]
	if !ok {
		if len(rs.mp)+len(rs.icu) >= mapSizeForRegexp {
			rs.evictRegexpCacheEntry()
		}

		// pat can be a zero-copy string backed by a reusable input vector. Both
		// map keys and regexp expressions must outlive the current data block.
		key.pattern = strings.Clone(key.pattern)
		key.matchType = strings.Clone(key.matchType)
		// The case-fold expansion (issue #31175) can push a pattern the
		// unexpanded admission accepted over RE2's parse-tree height bound
		// (for example deeply nested capture groups around a foldable
		// literal run), or exhaust a rewrite budget. The source pattern's
		// own verdict stands when it has one (malformed stays malformed);
		// a well-formed, source-admitted pattern raises the bounded
		// resource-limit error, because its faithful full-fold expansion
		// cannot be evaluated within bounds and falling back to the
		// engine's per-character caseless orbit would silently narrow the
		// promised match set (issue #31175 design, ownership item 4).
		var (
			parsed   *syntax.Regexp
			expanded bool
			err      error
		)
		reg, parsed, expanded, err = buildRE2MatcherForKey(key, true)
		if err != nil && expanded {
			foldErr := err
			reg, parsed, _, err = buildRE2MatcherForKey(key, false)
			if err == nil {
				// The source pattern is admissible: only its faithful
				// expansion exceeds a bound. Keep the rewrite's own
				// resource-limit error when it has one; otherwise name
				// the engine admission bound the expanded form crossed.
				if moerr.IsMoErrCode(foldErr, moerr.ErrInternal) {
					err = foldErr
				} else {
					err = regexpCaseFoldResourceLimitf("expanded-pattern admission: %v", foldErr)
				}
				reg, parsed = nil, nil
			}
		}
		if err != nil {
			return nil, false, err
		}
		if rs.mp == nil {
			rs.mp = make(map[regexpCacheKey]*regexp.Regexp, mapSizeForRegexp)
		}
		if rs.mayMatchEmpty == nil {
			rs.mayMatchEmpty = make(map[regexpCacheKey]bool, mapSizeForRegexp)
		}
		rs.mp[key] = reg
		rs.mayMatchEmpty[key] = regexpSyntaxMayMatchEmpty(parsed)
	}
	return reg, rs.mayMatchEmpty[key], nil
}

// buildRE2MatcherForKey runs the RE2 admission chain for one prepared form
// of key.pattern: with expand set the case-fold literal expansion is applied
// first, and the returned expanded reports whether that rewrite was applied
// (changed bytes or a resource-limit failure). The caller checks the source
// pattern's own admission verdict when the expanded form fails. The returned
// parsed tree describes the compiled expression and feeds the mayMatchEmpty
// property.
func buildRE2MatcherForKey(key regexpCacheKey, expand bool) (*regexp.Regexp, *syntax.Regexp, bool, error) {
	pattern := key.pattern
	expanded := false
	if expand && regexpCaseFoldApplies(pattern, key.matchType, key.binary) {
		// Case-insensitive literal runs match by full case folding, the
		// way MySQL's ICU engine evaluates them (issue #31175 review).
		// Exhaustion of a rewrite budget fails with the bounded
		// resource-limit error; the caller decides whether the source
		// pattern has its own verdict that stands instead.
		rewritten, err := expandRegexpCaseFoldRuns(pattern, strings.ContainsRune(key.matchType, 'i'))
		if err != nil {
			return nil, nil, true, err
		}
		expanded = rewritten != pattern
		pattern = rewritten
	}
	expression := regexpPatternWithPureMatchTypeRE2(pattern, key.matchType)
	if key.binary {
		var err error
		expression, err = encodeBinaryRegexpPattern(expression, key.binaryCaseFold)
		if err != nil {
			return nil, nil, expanded, err
		}
	}
	lineMode := regexp.UnixLines
	if regexpMatchTypeWideMultiline(key.matchType) {
		lineMode = regexp.WideTextLines
		if key.binary {
			lineMode = regexp.WideBinaryLines
			if key.binaryCaseFold {
				lineMode = regexp.WideBinaryFoldLines
			}
		}
	}
	reg, err := regexp.CompileWithLineMode(expression, lineMode)
	if err != nil {
		return nil, nil, expanded, err
	}
	parsed, err := syntax.Parse(expression, syntax.Perl)
	if err != nil {
		return nil, nil, expanded, err
	}
	return reg, parsed, expanded, nil
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
	if requiresRegexp2Pattern(pat) {
		if err := validateRegexpPattern(pat); err != nil {
			return err
		}
		_, err := rs.getRegexp2MatcherWithMatchType(pat, "", binary)
		if err != nil {
			return regexpCompileError(functionName, pat, err)
		}
		return nil
	}
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

func (rs *regexpSet) validateRegexpBeforeNullableResultWithMatchType(
	pat string,
	binary bool,
	functionName string,
	laterArgumentIsNull bool,
	pureMatchType string,
) error {
	if laterArgumentIsNull {
		if rs.requiresRegexp2(pat, pureMatchType, binary) {
			if err := validateRegexpPattern(pat); err != nil {
				return err
			}
			_, err := rs.getRegexp2MatcherWithMatchType(pat, pureMatchType, binary)
			if err != nil {
				return regexpCompileError(functionName, pat, err)
			}
			return nil
		}
		_, err := rs.getCompiledRegexpWithMatchType(pat, pureMatchType, binary, functionName)
		return err
	}
	return validateRegexpPattern(pat)
}

func (rs *regexpSet) regularMatchWithMode(pat, str string, binary bool) (bool, error) {
	return rs.regularMatchWithPureMatchType(pat, str, binary, "")
}

func (rs *regexpSet) regularMatchWithPureMatchType(pat, str string, binary bool, pureMatchType string) (bool, error) {
	if rs.requiresRegexp2(pat, pureMatchType, binary) {
		if err := validateRegexpPattern(pat); err != nil {
			return false, err
		}
		matcher, err := rs.getRegexp2MatcherWithMatchType(pat, pureMatchType, binary)
		if err != nil {
			return false, err
		}
		return rs.regexp2MatchWithMatchType(matcher, str, binary, pureMatchType)
	}
	reg, err := rs.getCompiledRegexpWithMatchType(pat, pureMatchType, binary, "")
	if err != nil {
		return false, err
	}
	return regexpMatchCompiled(reg, str, binary, binary && strings.ContainsRune(pureMatchType, 'i')), nil
}

func (rs *regexpSet) regularMatchForLikeOp(pat []byte, str []byte) (match bool, err error) {
	return rs.regularMatchForLikeOpWithEscape(pat, str, DefaultEscapeChar, true, likeFoldNone)
}

func (rs *regexpSet) regularMatchForLikeOpWithEscape(
	pat []byte,
	str []byte,
	escape rune,
	escapeEnabled bool,
	fold likeFoldMode,
) (match bool, err error) {
	reg, err := rs.prepareLikeRegexp(pat, escape, escapeEnabled, fold)
	if err != nil {
		return false, nil
	}
	str = likeFoldValue(fold, str)
	return reg.Match(str), nil
}

// A constant pattern is prepared lazily, once per vector evaluation. The
// vector wrapper remains responsible for NULLs and selection, so unevaluated
// rows never prepare a pattern. No state survives into the next batch.
func (rs *regexpSet) likeMatchFn(ctx context.Context, constantPattern bool, escape rune, escapeEnabled bool, fold likeFoldMode) func([]byte, []byte) (bool, error) {
	var reg *regexp.Regexp
	ready := false
	return func(value, pattern []byte) (bool, error) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}
		if !constantPattern {
			return rs.regularMatchForLikeOpWithEscape(pattern, value, escape, escapeEnabled, fold)
		}
		if !ready {
			reg, _ = rs.prepareLikeRegexp(pattern, escape, escapeEnabled, fold)
			ready = true
		}
		// Preserve the generic LIKE path's false result on regexp compile failure.
		if reg == nil {
			return false, nil
		}
		return reg.Match(likeFoldValue(fold, value)), nil
	}
}

func (rs *regexpSet) prepareLikeRegexp(pat []byte, escape rune, escapeEnabled bool, fold likeFoldMode) (*regexp.Regexp, error) {
	replace := func(s string) string {
		isRegexMeta := func(r rune) bool {
			switch r {
			case '.', '+', '*', '?', '^', '$', '(', ')', '[', ']', '{', '}', '|', '\\':
				return true
			default:
				return false
			}
		}
		appendLiteral := func(buf *bytes.Buffer, r rune, size int) {
			switch fold {
			case likeFoldGeneralCI:
				// Compare the collation's per-character weights; each is a
				// single canonical rune. A malformed byte folds to the same
				// shared weight rune the subject fold produces, mirroring
				// the U+FFFD decoding of the raw regex matcher (issue
				// #31175).
				if r == utf8.RuneError && size == 1 {
					r = rune(collation.LikeMalformedByteWeight)
				} else {
					r = rune(collation.GeneralCIWeightRune(r))
				}
			case likeFoldILike:
				r = ilikeFoldRune(r)
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
				appendLiteral(&buf, r, size)
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
				appendLiteral(&buf, r, size)
			}
		}
		if escaped {
			appendLiteral(&buf, escape, utf8.RuneLen(escape))
		}
		return buf.String()
	}
	convert := func(expr []byte) string {
		return fmt.Sprintf("^(?s:%s)$", replace(util.UnsafeBytesToString(expr)))
	}

	return rs.getRegularMatcher(convert(pat))
}

// if str[pos:] matched pat.
// return Nth (N = occurrence here) of match result
func (rs *regexpSet) regularSubstr(pat string, str string, pos, occurrence int64) (match bool, substr string, err error) {
	return rs.regularSubstrWithMode(pat, str, pos, occurrence, false)
}

func (rs *regexpSet) regularSubstrWithMode(pat string, str string, pos, occurrence int64, subjectIsBinary bool) (match bool, substr string, err error) {
	return rs.regularSubstrWithMatchType(pat, str, pos, occurrence, subjectIsBinary, "")
}

func (rs *regexpSet) regularSubstrWithMatchType(
	pat string,
	str string,
	pos, occurrence int64,
	subjectIsBinary bool,
	pureMatchType string,
) (match bool, substr string, err error) {
	if rs.requiresRegexp2(pat, pureMatchType, subjectIsBinary) {
		return rs.regexp2SubstrWithMatchType(
			pat, str, pos, occurrence, subjectIsBinary, pureMatchType)
	}
	reg, err := rs.getCompiledRegexpWithMatchType(
		pat, pureMatchType, subjectIsBinary, "regexp_substr")
	if err != nil {
		return false, "", err
	}
	// check position
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok {
		return false, "", moerr.NewInvalidInputNoCtxf("regexp_substr: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	// check occurrence
	if occurrence < 1 {
		return false, "", moerr.NewInvalidInputNoCtxf("regexp_substr have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
	selected, found, err := rs.regexpNthMatchAtOrAfterWithMatchType(
		reg, pat, str, startByte, subjectIsBinary, occurrence, pureMatchType)
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
	return rs.regularReplaceWithMatchType(pat, str, repl, pos, occurrence, subjectIsBinary, "")
}

func (rs *regexpSet) regularReplaceWithMatchType(
	pat string,
	str string,
	repl string,
	pos, occurrence int64,
	subjectIsBinary bool,
	pureMatchType string,
) (r string, err error) {
	if err = validateRegexpPattern(pat); err != nil {
		return "", err
	}
	if rs.requiresRegexp2(pat, pureMatchType, subjectIsBinary) {
		return rs.regexp2ReplaceWithMatchType(
			pat, str, repl, pos, occurrence, subjectIsBinary, pureMatchType)
	}
	reg, mayMatchEmpty, err := rs.getRegularMatcherInfoWithMatchType(
		pat, pureMatchType, subjectIsBinary)
	if err != nil {
		return "", regexpCompileError("regexp_replace", pat, err)
	}
	// check position
	startByte, ok := regexpSearchStartByte(str, pos, subjectIsBinary)
	if !ok {
		return "", moerr.NewInvalidInputNoCtxf("regexp_replace: Index out of bounds in regular expression search. Search start position: %d, Search string length: %d", pos, regexpSubjectLength(str, subjectIsBinary))
	}
	// check occurrence
	if occurrence < 0 {
		return "", moerr.NewInvalidInputNoCtxf("regexp_replace have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}

	// MySQL returns an empty subject unchanged, even for a regexp such as ^$
	// that can match an empty string. Compile first so malformed patterns still
	// report their error instead of being hidden by this result shortcut.
	if len(str) == 0 {
		return str, nil
	}
	if regexpReplacementNeedsExpansion(repl) {
		return rs.regularReplaceWithTemplate(
			reg, pat, str, repl, startByte, occurrence, subjectIsBinary, pureMatchType,
			regexpReplaceMaxResultBytes,
		)
	}
	if len(repl) > 0 {
		if upperBound, ok := regexpReplaceOutputUpperBound(len(str), len(repl)); !ok ||
			upperBound > uint64(regexpReplaceMaxResultBytes/2) {
			return rs.regexpReplaceLiteralWithLimit(
				reg, pat, str, repl, startByte, occurrence, subjectIsBinary, pureMatchType,
				regexpReplaceMaxResultBytes,
			)
		}
	}
	return rs.regularReplaceLiteralWithMatchType(
		reg, pat, str, repl, startByte, occurrence, subjectIsBinary, pureMatchType, mayMatchEmpty)
}

func (rs *regexpSet) regularReplaceLiteralWithMatchType(
	reg *regexp.Regexp,
	pat, str, repl string,
	startByte int,
	occurrence int64,
	subjectIsBinary bool,
	pureMatchType string,
	mayMatchEmpty bool,
) (r string, err error) {
	if startByte == 0 && occurrence == 0 && !mayMatchEmpty {
		if !subjectIsBinary {
			return reg.ReplaceAllLiteralString(str, repl), nil
		}
		if strings.ContainsRune(pureMatchType, 'i') {
			encodedSubject := regexpBinaryBytesToText(str)
			encodedReplacement := regexpBinaryBytesToText(repl)
			return decodeBinaryRegexpText(reg.ReplaceAllLiteralString(encodedSubject, encodedReplacement)), nil
		}
		encodedSubject, _ := encodeBinaryRegexpBytes(str, 0)
		encodedReplacement, _ := encodeBinaryRegexpBytes(repl, 0)
		return decodeBinaryRegexpBytes(reg.ReplaceAllLiteralString(encodedSubject, encodedReplacement)), nil
	}

	if occurrence == 0 {
		return rs.regexpReplaceAllAtOrAfterWithMatchType(
			reg, pat, str, repl, startByte, subjectIsBinary, pureMatchType)
	}
	match, found, err := rs.regexpNthMatchAtOrAfterWithMatchType(
		reg, pat, str, startByte, subjectIsBinary, occurrence, pureMatchType)
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
	return rs.regularInstrWithMatchType(
		pat, str, pos, occurrence, retOption, subjectIsBinary, "")
}

func (rs *regexpSet) regularInstrWithMatchType(
	pat string,
	str string,
	pos, occurrence int64,
	retOption int8,
	subjectIsBinary bool,
	pureMatchType string,
) (index int64, err error) {
	if rs.requiresRegexp2(pat, pureMatchType, subjectIsBinary) {
		return rs.regexp2InstrWithMatchType(
			pat, str, pos, occurrence, retOption, subjectIsBinary, pureMatchType)
	}
	reg, err := rs.getCompiledRegexpWithMatchType(
		pat, pureMatchType, subjectIsBinary, "regexp_instr")
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
	// check occurrence
	if occurrence < 1 {
		return 0, moerr.NewInvalidInputNoCtxf("regexp_instr have Index out of bounds in regular expression search, return occurrence %d", occurrence)
	}
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
	match, found, err := rs.regexpNthMatchAtOrAfterWithMatchType(
		reg, pat, searchSubject, 0, subjectIsBinary, occurrence, pureMatchType)
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
	return rs.regexpNthMatchAtOrAfterWithMatchType(
		reg, pat, str, startByte, subjectIsBinary, occurrence, "")
}

func (rs *regexpSet) regexpNthMatchAtOrAfterWithMatchType(
	reg *regexp.Regexp,
	pat, str string,
	startByte int,
	subjectIsBinary bool,
	occurrence int64,
	pureMatchType string,
) ([2]int, bool, error) {
	searchSubject := str
	searchStart := startByte
	if subjectIsBinary {
		if strings.ContainsRune(pureMatchType, 'i') {
			searchSubject, searchStart = encodeBinaryRegexpText(str, startByte)
		} else {
			searchSubject, searchStart = encodeBinaryRegexpBytes(str, startByte)
		}
	}

	selected := [2]int{}
	visited := int64(0)
	err := rs.regexpVisitAtOrAfterWithMatchType(
		reg, pat, searchSubject, searchStart, subjectIsBinary, occurrence, pureMatchType,
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
	return rs.regexpVisitAtOrAfterWithMatchType(
		reg, pat, str, startByte, subjectIsBinary, limit, "", visit)
}

func (rs *regexpSet) regexpVisitAtOrAfterWithMatchType(
	reg *regexp.Regexp,
	pat, str string,
	startByte int,
	subjectIsBinary bool,
	limit int64,
	pureMatchType string,
	visit func(start, end int),
) error {
	visited := int64(0)
	nextStart := startByte
	for nextStart <= len(str) {
		indices := reg.FindStringIndexAt(str, nextStart)
		if indices == nil {
			break
		}
		start, end := indices[0], indices[1]
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

func (rs *regexpSet) regexpReplaceAllAtOrAfterWithMatchType(
	reg *regexp.Regexp,
	pat, str, repl string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
) (string, error) {
	searchSubject := str
	searchStart := startByte
	replacement := repl
	if subjectIsBinary {
		if strings.ContainsRune(pureMatchType, 'i') {
			searchSubject, searchStart = encodeBinaryRegexpText(str, startByte)
			replacement = regexpBinaryBytesToText(repl)
		} else {
			searchSubject, searchStart = encodeBinaryRegexpBytes(str, startByte)
			replacement, _ = encodeBinaryRegexpBytes(repl, 0)
		}
	}

	var b strings.Builder
	b.Grow(len(searchSubject))
	last := 0
	matched := false
	err := rs.regexpVisitAtOrAfterWithMatchType(
		reg, pat, searchSubject, searchStart, subjectIsBinary, 0, pureMatchType,
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
		if strings.ContainsRune(pureMatchType, 'i') {
			result = decodeBinaryRegexpText(result)
		} else {
			result = decodeBinaryRegexpBytes(result)
		}
	}
	return result, nil
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
	class := false
	freeSpacing := false
	freeSpacingStack := make([]bool, 0, 8)
	for i := 0; i < len(pattern); {
		if quoted {
			if pattern[i] == '\\' && i+1 < len(pattern) && pattern[i+1] == 'E' {
				quoted = false
				i += 2
				continue
			}
			i++
			continue
		}
		if class {
			if pattern[i] == ']' {
				class = false
				i++
				continue
			}
			if pattern[i] == '\\' && i+1 < len(pattern) {
				switch pattern[i+1] {
				case 'p', 'P':
					return moerr.NewInvalidInputNoCtx(
						"binary regular expressions do not support Unicode property escapes")
				case 'u', 'U':
					return moerr.NewInvalidInputNoCtx(
						"binary regular expressions do not support Unicode code-point escapes")
				case 'x':
					if i+2 < len(pattern) && pattern[i+2] == '{' {
						close := strings.IndexByte(pattern[i+3:], '}')
						if close < 0 {
							return nil
						}
						end := i + 3 + close
						value, err := strconv.ParseUint(pattern[i+3:end], 16, 32)
						if err == nil && value > 0xff {
							return moerr.NewInvalidInputNoCtx(
								"binary regular expressions only support byte escapes up to \\x{FF}")
						}
						i = end + 1
						continue
					}
				}
				i += regexpEscapeTokenLen(pattern, i)
				continue
			}
			i++
			continue
		}
		if freeSpacing {
			if isRegexpPatternWhitespace(pattern[i]) {
				i++
				continue
			}
			if pattern[i] == '#' {
				for i < len(pattern) && pattern[i] != '\n' {
					i++
				}
				continue
			}
		}
		if pattern[i] == '(' {
			if end, enabled, scoped, ok := regexpInlineFreeSpacing(pattern, i, freeSpacing); ok {
				if scoped {
					freeSpacingStack = append(freeSpacingStack, freeSpacing)
					freeSpacing = enabled
				} else {
					freeSpacing = enabled
				}
				i = end
				continue
			}
			freeSpacingStack = append(freeSpacingStack, freeSpacing)
		}
		if pattern[i] == ')' {
			if n := len(freeSpacingStack); n > 0 {
				freeSpacing = freeSpacingStack[n-1]
				freeSpacingStack = freeSpacingStack[:n-1]
			}
			i++
			continue
		}
		if pattern[i] == '[' {
			class = true
			i++
			continue
		}
		if pattern[i] != '\\' {
			i++
			continue
		}
		if i+1 >= len(pattern) {
			break
		}
		switch pattern[i+1] {
		case 'Q':
			quoted = true
			i += 2
		case 'p', 'P':
			return moerr.NewInvalidInputNoCtx(
				"binary regular expressions do not support Unicode property escapes")
		case 'u', 'U':
			return moerr.NewInvalidInputNoCtx(
				"binary regular expressions do not support Unicode code-point escapes")
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
			// Consume the escaped token as a unit so a backslash in quoted or
			// escaped text cannot be mistaken for a byte escape.
			i += regexpEscapeTokenLen(pattern, i)
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
	reg, _, err := rs.getRegularMatcherInfoWithMatchType(pat, pureMatchType, binary)
	return reg, err
}

// regexpMatchTypeUnixLines reports whether the (pure) match_type marker
// requests MySQL's Unix-only line-ending multiline behavior (u).
func regexpMatchTypeUnixLines(pureMatchType string) bool {
	return strings.ContainsRune(pureMatchType, 'u')
}

// regexpMatchTypeWideMultiline reports whether multiline matching must
// accept MySQL's wider line-terminator set (m without u): CR, VT, FF, NEL,
// LINE SEPARATOR, PARAGRAPH SEPARATOR, plus LF (issue #30210). Both engines
// apply this policy natively to assertions through their line modes.
func regexpMatchTypeWideMultiline(pureMatchType string) bool {
	return strings.ContainsRune(pureMatchType, 'm') && !regexpMatchTypeUnixLines(pureMatchType)
}

// regexpPatternWithPureMatchType is the ICU pattern-size admission owner.
// Bound raw input and every generated append before allocating an expansion.
func regexpPatternWithPureMatchType(pat, pureMatchType string, binary bool) (string, error) {
	rule, _, err := regexpPatternWithPureMatchTypeExpanded(pat, pureMatchType, binary, true)
	return rule, err
}

// regexpPatternWithPureMatchTypeExpanded folds match_type into the regexp2
// flag prefix. With expand set, a pattern the case-fold rewrite applies to
// first gets its full-fold literal expansion (issue #31175), and the
// returned expanded reports whether the rewrite was applied (changed bytes
// or a resource-limit failure); the regexp2 admission checks the source
// pattern's own verdict when the expanded form fails.
func regexpPatternWithPureMatchTypeExpanded(pat, pureMatchType string, binary, expand bool) (string, bool, error) {
	if len(pat) > regexp2MaxPatternBytes {
		return "", false, regexp2InvalidInputf("regular expression pattern exceeds the maximum supported size of %d bytes", regexp2MaxPatternBytes)
	}
	expanded := false
	if expand && regexpCaseFoldApplies(pat, pureMatchType, binary) {
		// Same full-fold literal expansion as the RE2 path (issue #31175
		// review); the prefix-size admission below bounds the result.
		// Exhaustion of a rewrite budget fails with the bounded
		// resource-limit error; the caller decides whether the source
		// pattern has its own verdict that stands instead.
		rewritten, err := expandRegexpCaseFoldRuns(pat, strings.ContainsRune(pureMatchType, 'i'))
		if err != nil {
			return "", true, err
		}
		expanded = rewritten != pat
		pat = rewritten
	}
	engineFlags := strings.ReplaceAll(pureMatchType, "u", "")
	prefix := ""
	if engineFlags != "" {
		prefix = "(?" + engineFlags + ")"
	}
	if len(pat) > regexp2MaxPatternBytes-len(prefix) {
		return "", expanded, regexp2InvalidInputf("regular expression pattern exceeds the maximum supported size of %d bytes", regexp2MaxPatternBytes)
	}
	return prefix + pat, expanded, nil
}

// regexpPatternWithPureMatchTypeRE2 folds match_type into the RE2 flag
// prefix without applying the wide-multiline anchor rewrite: the rewrite is
// regexp2-only syntax (lookarounds). The regular engine applies its immutable
// line mode directly when evaluating empty-width assertions.
func regexpPatternWithPureMatchTypeRE2(pat, pureMatchType string) string {
	engineFlags := strings.ReplaceAll(pureMatchType, "u", "")
	if engineFlags == "" {
		return pat
	}
	return "(?" + engineFlags + ")" + pat
}

func (rs *regexpSet) getRegularMatcherInfoWithMatchType(
	pat, pureMatchType string,
	binary bool,
) (*regexp.Regexp, bool, error) {
	return rs.getRegularMatcherInfoForKey(regexpCacheKey{
		pattern: pat, matchType: pureMatchType, binary: binary,
		binaryCaseFold: binary && strings.ContainsRune(pureMatchType, 'i'),
	})
}

func (rs *regexpSet) getRegularMatcherWithMatchType(
	pat, pureMatchType string,
	binary bool,
) (*regexp.Regexp, error) {
	reg, _, err := rs.getRegularMatcherInfoWithMatchType(pat, pureMatchType, binary)
	return reg, err
}

func (rs *regexpSet) getCompiledRegexpWithMatchType(
	pat, pureMatchType string,
	binary bool,
	functionName string,
) (*regexp.Regexp, error) {
	if err := validateRegexpPattern(pat); err != nil {
		return nil, err
	}
	reg, err := rs.getRegularMatcherWithMatchType(pat, pureMatchType, binary)
	if err == nil {
		return reg, nil
	}
	return nil, regexpCompileError(functionName, pat, err)
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
	if rs.requiresRegexp2(pat, pureMatchType, binary) {
		if err := validateRegexpPattern(pat); err != nil {
			return false, err
		}
		matcher, err := rs.getRegexp2MatcherWithMatchType(pat, pureMatchType, binary)
		if err != nil {
			return false, err
		}
		return rs.regexp2MatchWithMatchType(matcher, str, binary, pureMatchType)
	}
	reg, err := rs.getRegularLikeMatcherForPureMatchTypeWithMode(pat, pureMatchType, binary)
	if err != nil {
		return false, err
	}
	return regexpMatchCompiled(
		reg, str, binary, binary && strings.ContainsRune(pureMatchType, 'i')), nil
}

// Parse MySQL match_type flags:
// i: case insensitive.
// c: case sensitive.
// m: multiple line mode.
// n: '.' can match line terminator.
// u: Unix-only line endings. Go regexp already treats LF as its only line
// terminator, so this flag does not require an additional engine option.
// Binary operands default to case-sensitive matching, but an explicit i or c
// still overrides that default.  The rightmost case flag wins in both domains;
// high bytes use MySQL's Windows-1252 binary facade while explicit i is active.
func getPureMatchType(input string) (string, error) {
	retstring := ""
	caseType := ""
	foundn := false
	foundm := false
	foundu := false

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
		case 'u':
			// Go regexp uses LF as its only line terminator, which is the
			// Unix-lines behavior requested by MySQL's explicit u option.
			// Keep the marker: m must widen its line-terminator set to
			// MySQL's (CR, NEL, U+2028, U+2029) unless u pins it to LF
			// (issue #30210).
			if !foundu {
				retstring += "u"
				foundu = true
			}
		default:
			return "", moerr.NewInvalidInputNoCtx("regexp_like got invalid match_type input!")
		}
	}

	retstring += caseType

	return retstring, nil
}
