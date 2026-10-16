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

func parseStrictSQLMode(mode any) (strict, noZeroDate, errorForDivisionByZero bool) {
	modeStr, ok := mode.(string)
	if !ok {
		return false, false, false
	}

	for token := range strings.SplitSeq(modeStr, ",") {
		switch strings.ToUpper(strings.TrimSpace(token)) {
		case "TRADITIONAL":
			strict = true
			noZeroDate = true
			errorForDivisionByZero = true
		case "STRICT_TRANS_TABLES", "STRICT_ALL_TABLES":
			strict = true
		case "ERROR_FOR_DIVISION_BY_ZERO":
			errorForDivisionByZero = true
		case "NO_ZERO_DATE":
			noZeroDate = true
		}
	}
	return strict, noZeroDate, errorForDivisionByZero
}

func IsStrictMode(mode any) bool {
	strict, _, _ := parseStrictSQLMode(mode)
	return strict
}

func IsStrictNoZeroDateMode(mode any) bool {
	strict, noZeroDate, _ := parseStrictSQLMode(mode)
	return strict && noZeroDate
}

// IsNoZeroDateMode is the SELECT-expression rule. Data-changing statements
// retain their separate strict-mode check at the assignment boundary.
func IsNoZeroDateMode(mode any) bool {
	_, noZeroDate, _ := parseStrictSQLMode(mode)
	return noZeroDate
}

// IsStrictDivisionByZeroMode reports whether sql_mode requires division by zero
// to error in data-changing statements without IGNORE. TRADITIONAL enables both
// strict mode and ERROR_FOR_DIVISION_BY_ZERO.
func IsStrictDivisionByZeroMode(mode any) bool {
	strict, _, errorForDivisionByZero := parseStrictSQLMode(mode)
	return strict && errorForDivisionByZero
}

// IsDivisionByZeroMode is the SELECT warning rule, independent of strict mode.
func IsDivisionByZeroMode(mode any) bool {
	_, _, enabled := parseStrictSQLMode(mode)
	return enabled
}

func IsPadCharToFullLengthMode(mode any) bool {
	modeStr, ok := mode.(string)
	if !ok {
		return false
	}

	for token := range strings.SplitSeq(modeStr, ",") {
		if strings.EqualFold(strings.TrimSpace(token), "PAD_CHAR_TO_FULL_LENGTH") {
			return true
		}
	}
	return false
}

// IsTimeTruncateFractionalMode reports whether temporal casts must discard
// fractional digits instead of applying the normal half-up rounding rule.
func IsTimeTruncateFractionalMode(mode any) bool {
	modeStr, ok := mode.(string)
	if !ok {
		return false
	}
	for token := range strings.SplitSeq(modeStr, ",") {
		if strings.EqualFold(strings.TrimSpace(token), "TIME_TRUNCATE_FRACTIONAL") {
			return true
		}
	}
	return false
}

// ResolveTimeTruncateFractional reads the live session mode when available
// and otherwise uses the mode snapshot carried to a remote process.
func ResolveTimeTruncateFractional(proc *Process) (bool, error) {
	if proc == nil {
		return false, nil
	}
	if resolveFunc := proc.GetResolveVariableFunc(); resolveFunc != nil {
		mode, err := resolveFunc("sql_mode", true, false)
		if err != nil {
			return false, err
		}
		return IsTimeTruncateFractionalMode(mode), nil
	}
	if proc.GetSessionInfo() == nil {
		return false, nil
	}
	return IsTimeTruncateFractionalMode(proc.GetSessionInfo().SqlMode), nil
}

// ResolvePadCharToFullLength reports the current PAD_CHAR_TO_FULL_LENGTH mode.
// A local process resolves the live session variable, while a remote process
// uses the sql_mode snapshot carried in SessionInfo by the pipeline codec.
func ResolvePadCharToFullLength(proc *Process) (bool, error) {
	if proc == nil {
		return false, nil
	}
	if resolveFunc := proc.GetResolveVariableFunc(); resolveFunc != nil {
		mode, err := resolveFunc("sql_mode", true, false)
		if err != nil {
			return false, err
		}
		return IsPadCharToFullLengthMode(mode), nil
	}
	return IsPadCharToFullLengthMode(proc.GetSessionInfo().SqlMode), nil
}

func ResolveExplicitZeroTemporalCastReturnsNull(proc *Process) (bool, error) {
	if proc == nil {
		return false, nil
	}
	resolveFunc := proc.GetResolveVariableFunc()
	if resolveFunc != nil {
		mode, err := resolveFunc("sql_mode", true, false)
		if err != nil {
			return false, err
		}
		return IsNoZeroDateMode(mode), nil
	}
	if proc.GetSessionInfo().SqlMode != "" {
		return IsNoZeroDateMode(proc.GetSessionInfo().SqlMode), nil
	}
	return proc.GetSessionInfo().ExplicitZeroTemporalCastReturnsNull, nil
}

// IsSelectStatement requires positive classification of the outer statement.
// Remote processes receive this classification in ProcessInfo; legacy payloads
// stay unclassified, so a DML scan cannot accidentally acquire SELECT warnings.
func (proc *Process) IsSelectStatement() bool {
	if proc == nil || proc.GetStmtProfile() == nil {
		return false
	}
	statement, query, _ := proc.GetStmtProfile().GetStatementRuntimeProfile()
	return strings.EqualFold(strings.TrimSpace(statement), "Select") && strings.EqualFold(strings.TrimSpace(query), "DQL")
}

// DivisionByZeroWarningsEnabled resolves one execution's SELECT warning policy.
// A live empty value overrides the remote snapshot. Failure to resolve diagnostic
// policy does not turn a successful arithmetic result into a new SELECT error.
func (proc *Process) DivisionByZeroWarningsEnabled() bool {
	if !proc.IsSelectStatement() {
		return false
	}
	if resolve := proc.GetResolveVariableFunc(); resolve != nil {
		mode, err := resolve("sql_mode", true, false)
		return err == nil && IsDivisionByZeroMode(mode)
	}
	return proc.GetSessionInfo() != nil && IsDivisionByZeroMode(proc.GetSessionInfo().SqlMode)
}
