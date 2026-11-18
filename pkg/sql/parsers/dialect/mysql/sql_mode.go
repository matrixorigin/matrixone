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

package mysql

import (
	"slices"
	"strings"
)

// SystemSQLModes is the ordered set accepted by the sql_mode system variable.
// Its order defines the corresponding SET bit values.
func SystemSQLModes() []string {
	return []string{
		"ANSI", "TRADITIONAL", "ALLOW_INVALID_DATES", "ANSI_QUOTES",
		"ERROR_FOR_DIVISION_BY_ZERO", "HIGH_NOT_PRECEDENCE", "IGNORE_SPACE",
		"MATRIXONE_NATIVE", "NO_AUTO_VALUE_ON_ZERO", "NO_BACKSLASH_ESCAPES",
		"NO_DIR_IN_CREATE", "NO_ENGINE_SUBSTITUTION", "NO_UNSIGNED_SUBTRACTION",
		"NO_ZERO_DATE", "NO_ZERO_IN_DATE", "ONLY_FULL_GROUP_BY",
		"PAD_CHAR_TO_FULL_LENGTH", "PIPES_AS_CONCAT", "REAL_AS_FLOAT",
		"STRICT_ALL_TABLES", "STRICT_TRANS_TABLES", "TIME_TRUNCATE_FRACTIONAL",
	}
}

// IsLegacySQLMode reports only strings the old SET formatter could have
// emitted when ANSI was selected. Preserve composite values verbatim: some
// consumers used substring checks while the parser ignored the glued token.
func IsLegacySQLMode(mode string) bool {
	if mode == "ANS" {
		return true
	}
	if len(mode) > 4096 || !strings.HasPrefix(mode, "ANSI") {
		return false
	}
	parts := strings.Split(mode, ",")
	modes := SystemSQLModes()
	previous := slices.Index(modes, strings.TrimPrefix(parts[0], "ANSI"))
	if previous <= 0 {
		return false
	}
	for _, part := range parts[1:] {
		index := slices.Index(modes, part)
		if index <= previous {
			return false
		}
		previous = index
	}
	return true
}

// NormalizeLegacySQLMode repairs the old SQL-mode SET formatter's standalone ANSI
// value. The stored "ANS" was unrecognized by the parser and had no effect.
func NormalizeLegacySQLMode(mode string) string {
	if mode == "ANS" {
		return ""
	}
	return mode
}

type SQLModeFlag uint8

const SQLModeMatrixOneNative = "MATRIXONE_NATIVE"

const (
	sqlModeANSIQuotes         = "ANSI_QUOTES"
	sqlModePipesAsConcat      = "PIPES_AS_CONCAT"
	sqlModeNoBackslashEscapes = "NO_BACKSLASH_ESCAPES"
	sqlModeRealAsFloat        = "REAL_AS_FLOAT"
)

var parserSQLModeTokens = []string{
	sqlModeANSIQuotes,
	sqlModePipesAsConcat,
	sqlModeNoBackslashEscapes,
	sqlModeRealAsFloat,
}

const (
	SQLModeANSIQuotes SQLModeFlag = 1 << iota
	SQLModePipesAsConcat
	SQLModeNoBackslashEscapes
	SQLModeRealAsFloat
)

type SQLModeFlags uint8

func ParseSQLModeFlags(mode string) SQLModeFlags {
	var flags SQLModeFlags
	for _, part := range strings.Split(mode, ",") {
		switch strings.ToUpper(strings.TrimSpace(part)) {
		case "ANSI":
			flags |= SQLModeFlags(SQLModeANSIQuotes | SQLModePipesAsConcat | SQLModeRealAsFloat)
		case sqlModeANSIQuotes:
			flags |= SQLModeFlags(SQLModeANSIQuotes)
		case sqlModePipesAsConcat:
			flags |= SQLModeFlags(SQLModePipesAsConcat)
		case sqlModeNoBackslashEscapes:
			flags |= SQLModeFlags(SQLModeNoBackslashEscapes)
		case sqlModeRealAsFloat:
			flags |= SQLModeFlags(SQLModeRealAsFloat)
		}
	}
	return flags
}

func SessionSQLModeForParser(mode string) string {
	return mode
}

// ParserSQLModeCombinations returns every distinct combination of SQL modes
// that can change parser output. Callers that recover syntax without the
// original session mode must consider the complete set rather than assuming a
// single default interpretation.
func ParserSQLModeCombinations() []string {
	modes := make([]string, 0, 1<<len(parserSQLModeTokens))
	for mask := 0; mask < 1<<len(parserSQLModeTokens); mask++ {
		parts := make([]string, 0, len(parserSQLModeTokens))
		for bit, token := range parserSQLModeTokens {
			if mask&(1<<bit) != 0 {
				parts = append(parts, token)
			}
		}
		modes = append(modes, strings.Join(parts, ","))
	}
	return modes
}

func HasSQLMode(mode string, token string) bool {
	if token == "" {
		return false
	}
	for _, part := range strings.Split(mode, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}

func HasMatrixOneNativeSQLMode(mode string) bool {
	return HasSQLMode(mode, SQLModeMatrixOneNative)
}

func (flags SQLModeFlags) Has(flag SQLModeFlag) bool {
	return flags&SQLModeFlags(flag) != 0
}
