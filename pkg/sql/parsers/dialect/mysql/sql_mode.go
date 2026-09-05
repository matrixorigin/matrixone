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

import "strings"

type SQLModeFlag uint8

const SQLModeMatrixOneNative = "MATRIXONE_NATIVE"

// SQLModeEnableBoolSumAvg selects MySQL's reading of SUM/AVG over a predicate.
// MySQL has no BOOL type, so a predicate is an integer 0/1 there and
// aggregating one is ordinary numeric aggregation; MO types it as BOOL and
// rejects it when this token is absent.
const SQLModeEnableBoolSumAvg = "ENABLE_BOOL_SUMAVG"

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

func HasEnableBoolSumAvgSQLMode(mode string) bool {
	return HasSQLMode(mode, SQLModeEnableBoolSumAvg)
}

func (flags SQLModeFlags) Has(flag SQLModeFlag) bool {
	return flags&SQLModeFlags(flag) != 0
}
