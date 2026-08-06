// Copyright 2021 - 2026 Matrix Origin
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

/*
#cgo linux LDFLAGS: -ldl
#include <stdint.h>
#include <stdlib.h>

typedef struct mo_icu_regex mo_icu_regex;

mo_icu_regex *mo_icu_regex_open(
    const uint16_t *, int32_t, uint32_t, int32_t, int32_t,
    int32_t *, int32_t *, int32_t *);
void mo_icu_regex_close(mo_icu_regex *);
int mo_icu_regex_set_text(mo_icu_regex *, const uint16_t *, int32_t, int32_t *);
int mo_icu_regex_find(mo_icu_regex *, int32_t, int32_t, int32_t *, int32_t *, int32_t *);
int mo_icu_regex_replace(
    mo_icu_regex *, const uint16_t *, int32_t, int32_t, int32_t,
    uint16_t **, int32_t *, int32_t *);
void mo_icu_regex_free(void *);
*/
import "C"

import (
	"unicode/utf16"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	icuRegexpTimeLimit  = 32
	icuRegexpStackLimit = 8_000_000

	icuCaseInsensitive = 2
	icuMultiline       = 8
	icuDotAll          = 32
	icuUnixLines       = 1

	icuRegexErrorStart = 0x10300
)

type mysqlRegexp struct {
	ptr    *C.mo_icu_regex
	binary bool
}

type mysqlRegexpExecutionCache struct {
	key string
	reg *mysqlRegexp
}

func (cache *mysqlRegexpExecutionCache) close() {
	if cache != nil && cache.reg != nil {
		cache.reg.close()
		cache.reg = nil
	}
}

func (cache *mysqlRegexpExecutionCache) get(
	pattern []byte, matchType, functionName string, binary bool,
) (*mysqlRegexp, error) {
	key := functionName + "\x00" + matchType + "\x00" + string(pattern)
	if binary {
		key = "b\x00" + key
	}
	if cache.reg != nil && cache.key == key {
		return cache.reg, nil
	}
	cache.close()
	reg, err := newMySQLRegexp(pattern, matchType, functionName, binary)
	if err != nil {
		return nil, err
	}
	cache.key = key
	cache.reg = reg
	return reg, nil
}

func mysqlRegexpForExecution(
	cache *mysqlRegexpExecutionCache, pattern []byte, matchType, functionName string, binary bool,
) (*mysqlRegexp, bool, error) {
	if cache != nil {
		reg, err := cache.get(pattern, matchType, functionName, binary)
		return reg, false, err
	}
	reg, err := newMySQLRegexp(pattern, matchType, functionName, binary)
	return reg, true, err
}

func parseMySQLRegexpMatchType(input, functionName string) (uint32, error) {
	var flags uint32
	for _, flag := range input {
		switch flag {
		case 'i':
			flags |= icuCaseInsensitive
		case 'c':
			flags &^= icuCaseInsensitive
		case 'm':
			flags |= icuMultiline
		case 'n':
			flags |= icuDotAll
		case 'u':
			flags |= icuUnixLines
		default:
			return 0, moerr.NewWrongArguments(moerr.Context(), functionName)
		}
	}
	return flags, nil
}

func utf16Pointer(value []uint16) *C.uint16_t {
	if len(value) == 0 {
		return nil
	}
	return (*C.uint16_t)(unsafe.Pointer(&value[0]))
}

// MySQL maps binary strings one byte to one ICU code unit. This is deliberately
// not UTF-8 decoding: invalid bytes remain distinct and positions remain byte
// positions.
func regexpToUTF16(value []byte, binary bool) []uint16 {
	if binary {
		result := make([]uint16, len(value))
		for i, b := range value {
			result[i] = mysqlLatin1CodePoint(b)
		}
		return result
	}
	return utf16.Encode([]rune(string(value)))
}

func regexpFromUTF16(value []uint16, binary bool) []byte {
	if binary {
		result := make([]byte, len(value))
		for i, unit := range value {
			result[i] = mysqlLatin1Byte(unit)
		}
		return result
	}
	return []byte(string(utf16.Decode(value)))
}

var mysqlLatin1HighCodePoints = [...]uint16{
	0x20ac, 0x0081, 0x201a, 0x0192, 0x201e, 0x2026, 0x2020, 0x2021,
	0x02c6, 0x2030, 0x0160, 0x2039, 0x0152, 0x008d, 0x017d, 0x008f,
	0x0090, 0x2018, 0x2019, 0x201c, 0x201d, 0x2022, 0x2013, 0x2014,
	0x02dc, 0x2122, 0x0161, 0x203a, 0x0153, 0x009d, 0x017e, 0x0178,
}

func mysqlLatin1CodePoint(value byte) uint16 {
	if value >= 0x80 && value <= 0x9f {
		return mysqlLatin1HighCodePoints[value-0x80]
	}
	return uint16(value)
}

func mysqlLatin1Byte(value uint16) byte {
	if value < 0x80 || value > 0x9f && value <= 0xff {
		return byte(value)
	}
	for i, codePoint := range mysqlLatin1HighCodePoints {
		if value == codePoint {
			return byte(i + 0x80)
		}
	}
	return byte(value)
}

func regexpCharacterCount(value []uint16, binary bool) int64 {
	if binary {
		return int64(len(value))
	}
	return int64(len(utf16.Decode(value)))
}

func regexpCharacterToUTF16(value []uint16, character int64, binary bool) int32 {
	if binary {
		return int32(character)
	}
	if character == 0 {
		return 0
	}
	runes := utf16.Decode(value)
	return int32(len(utf16.Encode(runes[:character])))
}

func regexpUTF16ToCharacter(value []uint16, offset int32, binary bool) int64 {
	if binary {
		return int64(offset)
	}
	return int64(len(utf16.Decode(value[:offset])))
}

func newMySQLRegexp(pattern []byte, matchType, functionName string, binary bool) (*mysqlRegexp, error) {
	if len(pattern) == 0 {
		return nil, moerr.NewRegexpIllegalArgumentNoCtx()
	}
	flags, err := parseMySQLRegexpMatchType(matchType, functionName)
	if err != nil {
		return nil, err
	}
	encoded := regexpToUTF16(pattern, binary)
	var status, line, offset C.int32_t
	ptr := C.mo_icu_regex_open(
		utf16Pointer(encoded), C.int32_t(len(encoded)), C.uint32_t(flags),
		icuRegexpTimeLimit, icuRegexpStackLimit, &status, &line, &offset)
	if status > 0 || ptr == nil {
		return nil, mysqlRegexpError(int32(status), int32(line), int32(offset))
	}
	return &mysqlRegexp{ptr: ptr, binary: binary}, nil
}

func (rs *regexpSet) validateMySQLRegexp(pattern, matchType, functionName string) error {
	return rs.validateMySQLRegexpBytes([]byte(pattern), matchType, functionName, false)
}

func (rs *regexpSet) validateMySQLRegexpBytes(pattern []byte, matchType, functionName string, binary bool) error {
	reg, err := newMySQLRegexp(pattern, matchType, functionName, binary)
	if reg != nil {
		reg.close()
	}
	return err
}

func (rs *regexpSet) validateMySQLRegexpReplacement(pattern, _ string, matchType string) error {
	return rs.validateMySQLRegexp(pattern, matchType, "regexp_replace")
}

func (reg *mysqlRegexp) close() {
	if reg != nil && reg.ptr != nil {
		C.mo_icu_regex_close(reg.ptr)
		reg.ptr = nil
	}
}

func (reg *mysqlRegexp) setText(subject []uint16) error {
	var status C.int32_t
	if C.mo_icu_regex_set_text(reg.ptr, utf16Pointer(subject), C.int32_t(len(subject)), &status) == 0 {
		return mysqlRegexpError(int32(status), 0, 0)
	}
	return nil
}

func (reg *mysqlRegexp) find(subject []uint16, start int32, occurrence int64) (bool, int32, int32, error) {
	if err := reg.setText(subject); err != nil {
		return false, 0, 0, err
	}
	var matchStart, matchEnd, status C.int32_t
	found := C.mo_icu_regex_find(
		reg.ptr, C.int32_t(start), C.int32_t(occurrence), &matchStart, &matchEnd, &status)
	if status > 0 {
		return false, 0, 0, mysqlRegexpError(int32(status), 0, 0)
	}
	return found != 0, int32(matchStart), int32(matchEnd), nil
}

func (reg *mysqlRegexp) replace(
	subject, replacement []uint16, start int32, occurrence int64,
) ([]uint16, error) {
	if err := reg.setText(subject); err != nil {
		return nil, err
	}
	var output *C.uint16_t
	var outputLen, status C.int32_t
	ok := C.mo_icu_regex_replace(
		reg.ptr, utf16Pointer(replacement), C.int32_t(len(replacement)), C.int32_t(start),
		C.int32_t(occurrence), &output, &outputLen, &status)
	if output != nil {
		defer C.mo_icu_regex_free(unsafe.Pointer(output))
	}
	if ok == 0 || status > 0 {
		return nil, mysqlRegexpError(int32(status), 0, 0)
	}
	if outputLen == 0 {
		return []uint16{}, nil
	}
	return append([]uint16(nil), unsafe.Slice((*uint16)(unsafe.Pointer(output)), int(outputLen))...), nil
}

func mysqlRegexpError(status, line, offset int32) error {
	switch status {
	case -1:
		return moerr.NewInternalErrorNoCtx("ICU regular expression library is unavailable")
	case 1, icuRegexErrorStart + 4:
		return moerr.NewRegexpIllegalArgumentNoCtx()
	case 8:
		return moerr.NewRegexpIndexOutOfBoundsNoCtx()
	case 15:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpBufferOverflow)
	case icuRegexErrorStart:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpInternal)
	case icuRegexErrorStart + 1:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpRuleSyntax, line, offset)
	case icuRegexErrorStart + 3:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpBadEscape)
	case icuRegexErrorStart + 5:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpUnimplemented)
	case icuRegexErrorStart + 6:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpMismatchedParen)
	case icuRegexErrorStart + 7:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpNumberTooBig)
	case icuRegexErrorStart + 8:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpBadInterval)
	case icuRegexErrorStart + 9:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpMaxLessThanMin)
	case icuRegexErrorStart + 10:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpInvalidBackRef)
	case icuRegexErrorStart + 11:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpInvalidFlag)
	case icuRegexErrorStart + 12:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpLookBehindLimit)
	case icuRegexErrorStart + 15:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpMissingCloseBracket)
	case icuRegexErrorStart + 16:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpInvalidRange)
	case icuRegexErrorStart + 17:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpStackOverflow)
	case icuRegexErrorStart + 18:
		return moerr.NewRegexpTimeoutNoCtx()
	case icuRegexErrorStart + 20:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpPatternTooBig)
	case icuRegexErrorStart + 21:
		return moerr.NewRegexpInvalidCaptureGroupNoCtx()
	case 7:
		return moerr.NewOOMNoCtx()
	default:
		return moerr.NewRegexpErrorNoCtx(moerr.ErrRegexpInternal)
	}
}
