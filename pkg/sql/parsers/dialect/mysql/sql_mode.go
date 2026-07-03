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

const moDefaultSQLMode = "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES"

type SQLModeFlag uint8

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
		case "ANSI_QUOTES":
			flags |= SQLModeFlags(SQLModeANSIQuotes)
		case "PIPES_AS_CONCAT":
			flags |= SQLModeFlags(SQLModePipesAsConcat)
		case "NO_BACKSLASH_ESCAPES":
			flags |= SQLModeFlags(SQLModeNoBackslashEscapes)
		case "REAL_AS_FLOAT":
			flags |= SQLModeFlags(SQLModeRealAsFloat)
		}
	}
	return flags
}

func SessionSQLModeForParser(mode string) string {
	if isMODefaultSQLMode(mode) {
		return mode + ",PIPES_AS_CONCAT"
	}
	return mode
}

func (flags SQLModeFlags) Has(flag SQLModeFlag) bool {
	return flags&SQLModeFlags(flag) != 0
}

func isMODefaultSQLMode(mode string) bool {
	modeParts := strings.Split(mode, ",")
	defaultParts := strings.Split(moDefaultSQLMode, ",")
	if len(modeParts) != len(defaultParts) {
		return false
	}

	seen := make(map[string]struct{}, len(defaultParts))
	for _, part := range defaultParts {
		seen[strings.ToUpper(strings.TrimSpace(part))] = struct{}{}
	}
	for _, part := range modeParts {
		normalized := strings.ToUpper(strings.TrimSpace(part))
		if _, ok := seen[normalized]; !ok {
			return false
		}
		delete(seen, normalized)
	}
	return len(seen) == 0
}
