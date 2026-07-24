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

package process

import "strings"

func parseStrictSQLMode(mode any) (strict, noZeroDate bool) {
	modeStr, ok := mode.(string)
	if !ok {
		return false, false
	}

	for token := range strings.SplitSeq(modeStr, ",") {
		switch strings.ToUpper(strings.TrimSpace(token)) {
		case "TRADITIONAL":
			strict = true
			noZeroDate = true
		case "STRICT_TRANS_TABLES", "STRICT_ALL_TABLES":
			strict = true
		case "NO_ZERO_DATE":
			noZeroDate = true
		}
	}
	return strict, noZeroDate
}

func IsStrictMode(mode any) bool {
	strict, _ := parseStrictSQLMode(mode)
	return strict
}

func IsStrictNoZeroDateMode(mode any) bool {
	strict, noZeroDate := parseStrictSQLMode(mode)
	return strict && noZeroDate
}

func ResolveExplicitZeroTemporalCastReturnsNull(proc *Process) (bool, error) {
	if proc == nil {
		return false, nil
	}
	if proc.GetSessionInfo().ExplicitZeroTemporalCastReturnsNull {
		return true, nil
	}
	resolveFunc := proc.GetResolveVariableFunc()
	if resolveFunc == nil {
		return false, nil
	}

	mode, err := resolveFunc("sql_mode", true, false)
	if err != nil {
		return false, err
	}
	return IsStrictNoZeroDateMode(mode), nil
}
