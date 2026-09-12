// Copyright 2026 Matrix Origin
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
	"encoding/binary"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const regexpReplaceMaxResultBytes = int64(types.MaxBlobLen)

type regexpReplacementTemplate struct {
	program   []byte
	groupMask []uint64
	groupRefs []byte
	hasGroups bool
}

func regexpReplacementNeedsExpansion(replacement string) bool {
	return strings.ContainsAny(replacement, "$\\")
}

// regexpReplaceOutputUpperBound matches the conservative return-type bound:
// source + (source+1)*replacement. False means the arithmetic overflowed.
func regexpReplaceOutputUpperBound(sourceBytes, replacementBytes int) (uint64, bool) {
	if sourceBytes < 0 || replacementBytes < 0 {
		return 0, false
	}
	source := uint64(sourceBytes)
	replacement := uint64(replacementBytes)
	maxUint := ^uint64(0)
	if source == maxUint {
		return 0, false
	}
	matchSlots := source + 1
	if replacement != 0 && matchSlots > (maxUint-source)/replacement {
		return 0, false
	}
	return source + matchSlots*replacement, true
}

func parseRegexpReplacementTemplate(replacement string, reg *regexp.Regexp) (regexpReplacementTemplate, error) {
	template := regexpReplacementTemplate{
		program: make([]byte, 0, min(len(replacement), 64<<10)),
	}
	for i := 0; i < len(replacement); {
		switch replacement[i] {
		case '\\':
			if i+1 == len(replacement) {
				// ICU treats a trailing quote character as having no following
				// character to quote; it contributes no output byte.
				i = len(replacement)
				continue
			}
			template.program = append(template.program, replacement[i+1])
			i += 2
		case '$':
			if i+1 >= len(replacement) {
				return regexpReplacementTemplate{}, invalidRegexpReplacementTemplate()
			}
			group, next, err := parseRegexpReplacementGroup(replacement, i+1, reg)
			if err != nil {
				return regexpReplacementTemplate{}, invalidRegexpReplacementTemplate()
			}
			template.appendGroup(group)
			i = next
		default:
			template.program = append(template.program, replacement[i])
			i++
		}
	}
	return template, nil
}

func invalidRegexpReplacementTemplate() error {
	return moerr.NewInvalidInputNoCtx("regexp_replace: invalid replacement template")
}

func (t *regexpReplacementTemplate) appendGroup(group int) {
	position := len(t.program)
	t.program = append(t.program, 0)
	word := position / 64
	for len(t.groupMask) <= word {
		t.groupMask = append(t.groupMask, 0)
	}
	t.groupMask[word] |= uint64(1) << uint(position%64)
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], uint64(group))
	t.groupRefs = append(t.groupRefs, encoded[:n]...)
	t.hasGroups = true
}

func (t *regexpReplacementTemplate) isGroup(position int) bool {
	return position/64 < len(t.groupMask) &&
		t.groupMask[position/64]&(uint64(1)<<uint(position%64)) != 0
}

func (t *regexpReplacementTemplate) visit(visit func(group int, literal []byte) error) error {
	groupOffset := 0
	literalStart := 0
	for position := 0; position < len(t.program); position++ {
		if !t.isGroup(position) {
			continue
		}
		if literalStart < position {
			if err := visit(-1, t.program[literalStart:position]); err != nil {
				return err
			}
		}
		if groupOffset >= len(t.groupRefs) {
			return moerr.NewInternalErrorNoCtx("regexp_replace: invalid compiled replacement template")
		}
		group, n := binary.Uvarint(t.groupRefs[groupOffset:])
		if n <= 0 {
			return moerr.NewInternalErrorNoCtx("regexp_replace: invalid compiled replacement template")
		}
		groupOffset += n
		if err := visit(int(group), nil); err != nil {
			return err
		}
		literalStart = position + 1
	}
	if literalStart < len(t.program) {
		return visit(-1, t.program[literalStart:])
	}
	return nil
}

func parseRegexpReplacementGroup(replacement string, start int, reg *regexp.Regexp) (group, next int, err error) {
	if start >= len(replacement) {
		return 0, start, moerr.NewInvalidInputNoCtx("regexp_replace: invalid replacement template")
	}
	if replacement[start] == '{' {
		closeRelative := strings.IndexByte(replacement[start+1:], '}')
		if closeRelative < 0 {
			return 0, start, moerr.NewInvalidInputNoCtx("regexp_replace: invalid replacement template")
		}
		closeAt := start + 1 + closeRelative
		name := replacement[start+1 : closeAt]
		group := reg.SubexpIndex(name)
		if name == "" || group < 0 {
			return 0, start, moerr.NewInvalidInputNoCtx("regexp_replace: invalid replacement template")
		}
		return group, closeAt + 1, nil
	}
	if replacement[start] < '0' || replacement[start] > '9' {
		return 0, start, moerr.NewInvalidInputNoCtx("regexp_replace: invalid replacement template")
	}

	maxGroup := reg.NumSubexp()
	group := int(replacement[start] - '0')
	if group > maxGroup {
		return 0, start, moerr.NewInvalidInputNoCtx("regexp_replace: invalid replacement template")
	}
	next := start + 1
	for next < len(replacement) && replacement[next] >= '0' && replacement[next] <= '9' {
		digit := int(replacement[next] - '0')
		if digit > maxGroup || group > (maxGroup-digit)/10 {
			break
		}
		group = group*10 + digit
		next++
	}
	return group, next, nil
}

func regexpReplacementSizeError(maxBytes int64) error {
	return moerr.NewInvalidInputNoCtxf(
		"regexp_replace result exceeds the maximum supported size of %d bytes", maxBytes)
}

func regexpAddReplacementSize(current uint64, size int, maxBytes int64) (uint64, error) {
	if size < 0 || maxBytes < 0 || current > uint64(maxBytes) ||
		uint64(size) > uint64(maxBytes)-current {
		return current, regexpReplacementSizeError(maxBytes)
	}
	return current + uint64(size), nil
}

func regexpWriteReplacementString(b *strings.Builder, value string, maxBytes int64) error {
	if maxBytes < 0 || int64(b.Len()) > maxBytes || int64(len(value)) > maxBytes-int64(b.Len()) {
		return regexpReplacementSizeError(maxBytes)
	}
	b.WriteString(value)
	return nil
}

func regexpWriteReplacementBytes(b *strings.Builder, value []byte, maxBytes int64) error {
	if maxBytes < 0 || int64(b.Len()) > maxBytes || int64(len(value)) > maxBytes-int64(b.Len()) {
		return regexpReplacementSizeError(maxBytes)
	}
	b.Write(value)
	return nil
}

func regexpAddTemplateSize(
	current uint64,
	str string,
	indices []int,
	template regexpReplacementTemplate,
	maxBytes int64,
) (uint64, error) {
	err := template.visit(func(group int, literal []byte) error {
		if group < 0 {
			var err error
			current, err = regexpAddReplacementSize(current, len(literal), maxBytes)
			return err
		}
		pair := group * 2
		if pair+1 >= len(indices) {
			return moerr.NewInternalErrorNoCtx("regexp_replace: capture index is out of range")
		}
		start, end := indices[pair], indices[pair+1]
		if start < 0 || end < 0 {
			return nil
		}
		var err error
		current, err = regexpAddReplacementSize(current, end-start, maxBytes)
		return err
	})
	if err != nil {
		return current, err
	}
	return current, nil
}

func regexpWriteTemplate(
	b *strings.Builder,
	str string,
	indices []int,
	template regexpReplacementTemplate,
	maxBytes int64,
) error {
	return template.visit(func(group int, literal []byte) error {
		if group < 0 {
			return regexpWriteReplacementBytes(b, literal, maxBytes)
		}
		pair := group * 2
		if pair+1 >= len(indices) {
			return moerr.NewInternalErrorNoCtx("regexp_replace: capture index is out of range")
		}
		start, end := indices[pair], indices[pair+1]
		if start < 0 || end < 0 {
			return nil
		}
		return regexpWriteReplacementString(b, str[start:end], maxBytes)
	})
}

func regexpSearchSubjectAndStart(
	str string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
) (string, int) {
	if !subjectIsBinary {
		return str, startByte
	}
	if strings.ContainsRune(pureMatchType, 'i') {
		return encodeBinaryRegexpText(str, startByte)
	}
	return encodeBinaryRegexpBytes(str, startByte)
}

type regexpOffsetPosition struct {
	offset int
	index  int
}

// regexpBinaryOffsetMapper converts monotonically visited offsets in the
// one-rune-per-source-byte matcher representation back to source byte offsets.
type regexpBinaryOffsetMapper struct {
	encoded       string
	encodedOffset int
	sourceOffset  int
	positions     []regexpOffsetPosition
}

func (m *regexpBinaryOffsetMapper) byteOffset(encodedOffset int) int {
	if encodedOffset > m.encodedOffset {
		m.sourceOffset += utf8.RuneCountInString(m.encoded[m.encodedOffset:encodedOffset])
		m.encodedOffset = encodedOffset
	}
	return m.sourceOffset
}

func (m *regexpBinaryOffsetMapper) mapSubmatches(indices []int) {
	positions := m.positions[:0]
	for i, offset := range indices {
		if offset >= 0 {
			positions = append(positions, regexpOffsetPosition{offset: offset, index: i})
		}
	}
	sort.Slice(positions, func(i, j int) bool {
		return positions[i].offset < positions[j].offset
	})
	for _, position := range positions {
		indices[position.index] = m.byteOffset(position.offset)
	}
	m.positions = positions
}

func (rs *regexpSet) regexpFindSubmatchesAtOrAfterWithMatchType(
	reg *regexp.Regexp,
	pat, str string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
) ([]int, bool, error) {
	if startByte <= 0 {
		indices := reg.FindStringSubmatchIndex(str)
		return indices, indices != nil, nil
	}
	if startByte > len(str) {
		return nil, false, nil
	}

	_, size := utf8.DecodeLastRuneInString(str[:startByte])
	contextStart := startByte - size
	if size < 1 {
		contextStart = startByte - 1
	}
	wrappedPattern := "^(?s:.)(?s:.*?)(" + regexpPatternWithPureMatchType(pat, pureMatchType) + ")"
	wrapped, _, err := rs.getRegularMatcherInfoWithBinaryCaseFold(
		wrappedPattern,
		subjectIsBinary,
		subjectIsBinary && strings.ContainsRune(pureMatchType, 'i'),
	)
	if err != nil {
		return nil, false, err
	}
	indices := wrapped.FindStringSubmatchIndex(str[contextStart:])
	if len(indices) < 4 || indices[2] < 0 {
		return nil, false, nil
	}
	for i := 2; i < len(indices); i++ {
		if indices[i] >= 0 {
			indices[i] += contextStart
		}
	}
	return indices[2:], true, nil
}

func (rs *regexpSet) regexpVisitSubmatchesAtOrAfterWithMatchType(
	reg *regexp.Regexp,
	pat, str string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
	limit int64,
	visit func([]int) error,
) (int64, error) {
	searchSubject, searchStart := regexpSearchSubjectAndStart(str, startByte, subjectIsBinary, pureMatchType)
	mapper := regexpBinaryOffsetMapper{encoded: searchSubject}
	visited := int64(0)
	nextStart := searchStart
	for nextStart <= len(searchSubject) {
		indices, found, err := rs.regexpFindSubmatchesAtOrAfterWithMatchType(
			reg, pat, searchSubject, nextStart, subjectIsBinary, pureMatchType)
		if err != nil {
			return visited, err
		}
		if !found {
			break
		}
		start, end := indices[0], indices[1]
		if subjectIsBinary {
			mapper.mapSubmatches(indices)
		}
		if err := visit(indices); err != nil {
			return visited, err
		}
		visited++
		if limit > 0 && visited >= limit {
			break
		}
		if start == end {
			nextStart = regexpAdvancePosition(searchSubject, end)
		} else {
			nextStart = end
		}
	}
	return visited, nil
}

func (rs *regexpSet) regexpNthSubmatchesAtOrAfterWithMatchType(
	reg *regexp.Regexp,
	pat, str string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
	occurrence int64,
) ([]int, bool, error) {
	if occurrence < 1 {
		return nil, false, nil
	}
	var selected []int
	visited, err := rs.regexpVisitSubmatchesAtOrAfterWithMatchType(
		reg, pat, str, startByte, subjectIsBinary, pureMatchType, occurrence,
		func(indices []int) error {
			selected = append(selected[:0], indices...)
			return nil
		},
	)
	if err != nil {
		return nil, false, err
	}
	return selected, visited >= occurrence, nil
}

func (rs *regexpSet) regexpVisitMatchesAtOrAfterWithMatchType(
	reg *regexp.Regexp,
	pat, str string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
	limit int64,
	visit func(start, end int) error,
) (int64, error) {
	searchSubject, searchStart := regexpSearchSubjectAndStart(str, startByte, subjectIsBinary, pureMatchType)
	mapper := regexpBinaryOffsetMapper{encoded: searchSubject}
	visited := int64(0)
	nextStart := searchStart
	for nextStart <= len(searchSubject) {
		start, end, found, err := rs.regexpFindAtOrAfterWithMatchType(
			reg, pat, searchSubject, nextStart, subjectIsBinary, pureMatchType)
		if err != nil {
			return visited, err
		}
		if !found {
			break
		}
		encodedStart, encodedEnd := start, end
		if subjectIsBinary {
			start = mapper.byteOffset(start)
			end = mapper.byteOffset(end)
		}
		if err := visit(start, end); err != nil {
			return visited, err
		}
		visited++
		if limit > 0 && visited >= limit {
			break
		}
		if encodedStart == encodedEnd {
			nextStart = regexpAdvancePosition(searchSubject, encodedEnd)
		} else {
			nextStart = encodedEnd
		}
	}
	return visited, nil
}

func (rs *regexpSet) regexpReplaceLiteralWithLimit(
	reg *regexp.Regexp,
	pat, str, repl string,
	startByte int,
	occurrence int64,
	subjectIsBinary bool,
	pureMatchType string,
	maxBytes int64,
) (string, error) {
	if maxBytes < 0 {
		return "", regexpReplacementSizeError(maxBytes)
	}
	if len(str) == 0 {
		return str, nil
	}
	if occurrence > 0 {
		match, found, err := rs.regexpNthMatchAtOrAfterWithMatchType(
			reg, pat, str, startByte, subjectIsBinary, occurrence, pureMatchType)
		if err != nil {
			return "", err
		}
		if !found {
			return str, nil
		}
		size, err := regexpAddReplacementSize(0, match[0], maxBytes)
		if err != nil {
			return "", err
		}
		size, err = regexpAddReplacementSize(size, len(repl), maxBytes)
		if err != nil {
			return "", err
		}
		size, err = regexpAddReplacementSize(size, len(str)-match[1], maxBytes)
		if err != nil {
			return "", err
		}
		var result strings.Builder
		result.Grow(int(size))
		if err := regexpWriteReplacementString(&result, str[:match[0]], maxBytes); err != nil {
			return "", err
		}
		if err := regexpWriteReplacementString(&result, repl, maxBytes); err != nil {
			return "", err
		}
		if err := regexpWriteReplacementString(&result, str[match[1]:], maxBytes); err != nil {
			return "", err
		}
		return result.String(), nil
	}

	var size uint64
	last := 0
	matched := false
	_, err := rs.regexpVisitMatchesAtOrAfterWithMatchType(
		reg, pat, str, startByte, subjectIsBinary, pureMatchType, 0,
		func(start, end int) error {
			var writeErr error
			size, writeErr = regexpAddReplacementSize(size, start-last, maxBytes)
			if writeErr != nil {
				return writeErr
			}
			size, writeErr = regexpAddReplacementSize(size, len(repl), maxBytes)
			if writeErr != nil {
				return writeErr
			}
			last = end
			matched = true
			return nil
		},
	)
	if err != nil {
		return "", err
	}
	if !matched {
		return str, nil
	}
	size, err = regexpAddReplacementSize(size, len(str)-last, maxBytes)
	if err != nil {
		return "", err
	}

	var result strings.Builder
	result.Grow(int(size))
	last = 0
	_, err = rs.regexpVisitMatchesAtOrAfterWithMatchType(
		reg, pat, str, startByte, subjectIsBinary, pureMatchType, 0,
		func(start, end int) error {
			if writeErr := regexpWriteReplacementString(&result, str[last:start], maxBytes); writeErr != nil {
				return writeErr
			}
			if writeErr := regexpWriteReplacementString(&result, repl, maxBytes); writeErr != nil {
				return writeErr
			}
			last = end
			return nil
		},
	)
	if err != nil {
		return "", err
	}
	if err := regexpWriteReplacementString(&result, str[last:], maxBytes); err != nil {
		return "", err
	}
	if uint64(result.Len()) != size {
		return "", moerr.NewInternalErrorNoCtx("regexp_replace: output size changed during replacement")
	}
	return result.String(), nil
}

func (rs *regexpSet) regularReplaceWithTemplate(
	reg *regexp.Regexp,
	pat, str, replacement string,
	startByte int,
	occurrence int64,
	subjectIsBinary bool,
	pureMatchType string,
	maxBytes int64,
) (string, error) {
	if maxBytes < 0 {
		return "", regexpReplacementSizeError(maxBytes)
	}
	if occurrence > 0 {
		indices, found, err := rs.regexpNthSubmatchesAtOrAfterWithMatchType(
			reg, pat, str, startByte, subjectIsBinary, pureMatchType, occurrence)
		if err != nil {
			return "", err
		}
		if !found {
			return str, nil
		}
		template, err := parseRegexpReplacementTemplate(replacement, reg)
		if err != nil {
			return "", err
		}
		if !template.hasGroups {
			return rs.regexpReplaceLiteralWithLimit(
				reg, pat, str, regexpReplacementLiteral(template), startByte,
				occurrence, subjectIsBinary, pureMatchType, maxBytes)
		}
		size, err := regexpAddReplacementSize(0, indices[0], maxBytes)
		if err != nil {
			return "", err
		}
		size, err = regexpAddTemplateSize(size, str, indices, template, maxBytes)
		if err != nil {
			return "", err
		}
		size, err = regexpAddReplacementSize(size, len(str)-indices[1], maxBytes)
		if err != nil {
			return "", err
		}
		var result strings.Builder
		result.Grow(int(size))
		if err := regexpWriteReplacementString(&result, str[:indices[0]], maxBytes); err != nil {
			return "", err
		}
		if err := regexpWriteTemplate(&result, str, indices, template, maxBytes); err != nil {
			return "", err
		}
		if err := regexpWriteReplacementString(&result, str[indices[1]:], maxBytes); err != nil {
			return "", err
		}
		return result.String(), nil
	}

	upperBound, bounded := regexpReplaceOutputUpperBound(len(str), len(replacement))
	if bounded && upperBound <= uint64(maxBytes/2) {
		var result strings.Builder
		last := 0
		parsed := false
		var template regexpReplacementTemplate
		_, err := rs.regexpVisitSubmatchesAtOrAfterWithMatchType(
			reg, pat, str, startByte, subjectIsBinary, pureMatchType, 0,
			func(indices []int) error {
				if !parsed {
					var parseErr error
					template, parseErr = parseRegexpReplacementTemplate(replacement, reg)
					if parseErr != nil {
						return parseErr
					}
					result.Grow(min(len(str), 64<<10))
					parsed = true
				}
				if err := regexpWriteReplacementString(&result, str[last:indices[0]], maxBytes); err != nil {
					return err
				}
				if err := regexpWriteTemplate(&result, str, indices, template, maxBytes); err != nil {
					return err
				}
				last = indices[1]
				return nil
			},
		)
		if err != nil {
			return "", err
		}
		if !parsed {
			return str, nil
		}
		if err := regexpWriteReplacementString(&result, str[last:], maxBytes); err != nil {
			return "", err
		}
		return result.String(), nil
	}

	template, size, found, err := rs.regexpMeasureAllTemplateOutput(
		reg, pat, str, replacement, startByte, subjectIsBinary, pureMatchType, maxBytes)
	if err != nil {
		return "", err
	}
	if !found {
		return str, nil
	}
	var result strings.Builder
	result.Grow(int(size))
	last := 0
	_, err = rs.regexpVisitSubmatchesAtOrAfterWithMatchType(
		reg, pat, str, startByte, subjectIsBinary, pureMatchType, 0,
		func(indices []int) error {
			if writeErr := regexpWriteReplacementString(&result, str[last:indices[0]], maxBytes); writeErr != nil {
				return writeErr
			}
			if writeErr := regexpWriteTemplate(&result, str, indices, template, maxBytes); writeErr != nil {
				return writeErr
			}
			last = indices[1]
			return nil
		},
	)
	if err != nil {
		return "", err
	}
	if err := regexpWriteReplacementString(&result, str[last:], maxBytes); err != nil {
		return "", err
	}
	if uint64(result.Len()) != size {
		return "", moerr.NewInternalErrorNoCtx("regexp_replace: output size changed during replacement")
	}
	return result.String(), nil
}

func regexpReplacementLiteral(template regexpReplacementTemplate) string {
	return string(template.program)
}

func (rs *regexpSet) regexpMeasureAllTemplateOutput(
	reg *regexp.Regexp,
	pat, str, replacement string,
	startByte int,
	subjectIsBinary bool,
	pureMatchType string,
	maxBytes int64,
) (regexpReplacementTemplate, uint64, bool, error) {
	var template regexpReplacementTemplate
	var size uint64
	last := 0
	parsed := false
	matched := false
	_, err := rs.regexpVisitSubmatchesAtOrAfterWithMatchType(
		reg, pat, str, startByte, subjectIsBinary, pureMatchType, 0,
		func(indices []int) error {
			if !parsed {
				var parseErr error
				template, parseErr = parseRegexpReplacementTemplate(replacement, reg)
				if parseErr != nil {
					return parseErr
				}
				parsed = true
			}
			var addErr error
			size, addErr = regexpAddReplacementSize(size, indices[0]-last, maxBytes)
			if addErr != nil {
				return addErr
			}
			size, addErr = regexpAddTemplateSize(size, str, indices, template, maxBytes)
			if addErr != nil {
				return addErr
			}
			last = indices[1]
			matched = true
			return nil
		},
	)
	if err != nil {
		return regexpReplacementTemplate{}, 0, matched, err
	}
	if !matched {
		return regexpReplacementTemplate{}, 0, false, nil
	}
	size, err = regexpAddReplacementSize(size, len(str)-last, maxBytes)
	if err != nil {
		return regexpReplacementTemplate{}, 0, true, err
	}
	return template, size, true, nil
}
