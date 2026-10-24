// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// MaskSensitiveSqlLiterals replaces credential-bearing literals in a raw SQL
// fragment with `******`. It covers:
//
//   - `identified by|with '<password>'` (users/accounts)
//   - `access_key_id = '<key>'` / `secret_access_key = '<key>'` (external
//     storage options)
//   - every value inside a `CREDENTIALS = {...}` option map (#30747: stage
//     CREATE/ALTER otherwise persist cloud keys, secrets and tokens verbatim;
//     and the parse-error echo of a malformed such statement must not leak
//     them either, #30746)
//
// The replacement is textual and quote-preserving so retained diagnostic
// shapes (e.g. "identified by '******'") stay familiar. It works on raw token
// scans, therefore incomplete, unterminated or truncated fragments (the
// parse-error "near" text) are tolerated. The function is idempotent.
func MaskSensitiveSqlLiterals(sql string) string {
	return maskSensitiveSqlLiterals(sql, false)
}

// MaskSensitiveSqlLiteralsWithInitialState is MaskSensitiveSqlLiterals for a
// fragment that is known to begin INSIDE a CREDENTIALS = {...} option map
// (e.g. a parse-error "near" tail captured mid-map): the keyword context is
// no longer visible, so the scanner starts directly in map state.
func MaskSensitiveSqlLiteralsWithInitialState(sql string, inCredentialMap bool) string {
	return maskSensitiveSqlLiterals(sql, inCredentialMap)
}

func maskSensitiveSqlLiterals(sql string, inCredentialMap bool) string {
	scanner := NewScanner(dialect.MYSQL, sql)
	defer PutScanner(scanner)
	previousToken := 0
	indexes := []int{-1}
	eq := int('=')
	lbrace := int('{')
	rbrace := int('}')
	// CREDENTIALS option-map state machine:
	//
	//   none --[CREDENTIALS]--> afterKeyword --['=']--> afterEq --['{']--> inMap
	//   inMap --['}']--> none
	//
	// inside the map every token following an '=' is a credential value.
	const (
		credNone = iota
		credAfterKeyword
		credAfterEq
		credInMap
	)
	credState := credNone
	if inCredentialMap {
		credState = credInMap
	}
	maskNextValue := false
	for scanner.Pos < len(sql) {
		typ, s := scanSensitiveToken(scanner, &previousToken)
		if credState == credAfterKeyword {
			if typ == eq {
				credState = credAfterEq
				continue
			}
			credState = credNone
		}
		if credState == credAfterEq {
			if typ == lbrace {
				credState = credInMap
				continue
			}
			credState = credNone
		}
		if credState == credInMap {
			if typ == rbrace {
				credState = credNone
				continue
			}
			if maskNextValue {
				indexes = append(indexes, scanner.Pos-len(s)-1, scanner.Pos-2)
				maskNextValue = false
				continue
			}
			if typ == eq {
				maskNextValue = true
			}
			continue
		}
		if typ == CREDENTIALS {
			credState = credAfterKeyword
			continue
		}
		if typ == IDENTIFIED {
			typ, _ = scanSensitiveToken(scanner, &previousToken)
			if typ == BY || typ == WITH {
				typ, s = scanSensitiveToken(scanner, &previousToken)
				if typ != RANDOM {
					indexes = append(indexes, scanner.Pos-len(s)-1, scanner.Pos-2)
				}
			}
		} else if strings.ToLower(s) == "access_key_id" || strings.ToLower(s) == "secret_access_key" {
			typ, _ = scanSensitiveToken(scanner, &previousToken)
			if typ == eq {
				_, s = scanSensitiveToken(scanner, &previousToken)
				indexes = append(indexes, scanner.Pos-len(s)-1, scanner.Pos-2)
			}
		}
	}
	indexes = append(indexes, len(sql))

	if len(indexes) > 2 {
		var builder strings.Builder
		for j := 0; j < len(indexes); j += 2 {
			builder.WriteString(sql[indexes[j]+1 : indexes[j+1]])
			if j < len(indexes)-2 {
				builder.WriteString("******")
			}
		}
		sql = builder.String()
	}
	return sql
}

// scanSensitiveToken mirrors parsers.scanSQLToken: it preserves qualified
// identifier context across raw token scans. Duplicated here because the
// dialect package must not import the parser layer.
func scanSensitiveToken(scanner *Scanner, previousToken *int) (int, string) {
	var token int
	var value string
	if *previousToken == int('.') {
		token, value = scanner.ScanAfterDot()
	} else {
		token, value = scanner.Scan()
	}
	*previousToken = token
	return token, value
}
