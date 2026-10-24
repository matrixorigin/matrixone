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

// CredentialMapState names the scanner-visible progress through a
// `CREDENTIALS = { 'key' = 'value', ... }` option map. It is used to mask a
// fragment (typically the parse-error "near" tail) that begins MID-map: the
// keyword context is no longer visible, so the caller passes where the map
// stood when the fragment started.
type CredentialMapState int

const (
	// CredentialStateNone: not inside, and not heading into, a credential map.
	CredentialStateNone CredentialMapState = iota
	// CredentialStateAfterKeyword: just consumed the CREDENTIALS keyword.
	CredentialStateAfterKeyword
	// CredentialStateAfterMapEq: consumed `CREDENTIALS =`.
	CredentialStateAfterMapEq
	// CredentialStateInMap: inside the braces, expecting a key or `}`.
	CredentialStateInMap
	// CredentialStateInMapAfterEq: inside the braces immediately after an
	// `=`; the very next token is a credential value.
	CredentialStateInMapAfterEq
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
// Replacement happens on the RAW source span of each value token — the
// scanner-decoded token length is never trusted, so escaped content,
// dollar-quoted strings and unterminated values are masked precisely.
// Quoted values keep their quotes (`'******'`). The function tolerates
// incomplete, unterminated or truncated fragments and is idempotent.
func MaskSensitiveSqlLiterals(sql string) string {
	return maskSensitiveSqlLiteralsScoped(sql, CredentialStateNone, true)
}

// MaskSensitiveSqlLiteralsWithInitialState is MaskSensitiveSqlLiterals for a
// fragment that is known to begin at a specific point of a CREDENTIALS option
// map (e.g. a parse-error "near" tail captured mid-map, possibly right before
// a value): the keyword context is no longer visible, so the scanner starts
// in the given state.
func MaskSensitiveSqlLiteralsWithInitialState(sql string, initial CredentialMapState) string {
	return maskSensitiveSqlLiteralsScoped(sql, initial, true)
}

// MaskStageCredentialLiteralsWithInitialState masks ONLY the values inside a
// CREDENTIALS = {...} option map (stage credentials, #30747) and leaves every
// other literal visible. It is used for the parse-error "near" echo, where
// the pre-existing diagnostics convention keeps e.g. `identified by
// '<password>'` readable (zz_accesscontrol goldens), while stage credential
// maps must still be scrubbed even when the echo begins mid-map or right at
// a value token.
func MaskStageCredentialLiteralsWithInitialState(sql string, initial CredentialMapState) string {
	return maskSensitiveSqlLiteralsScoped(sql, initial, false)
}

// maskableSecretKeywords is the fast-path filter: without any of these
// substrings the full masker can produce no edit, so the common statement
// shape (plain DML) costs a single lowercasing pass instead of a token scan.
var maskableSecretKeywords = []string{"identified", "credentials", "access_key_id", "secret_access_key"}

// maskSensitiveSqlLiteralsScoped is the shared scanner. maskAll controls the
// identified-by/with and access-key rules; the CREDENTIALS map rule always
// runs.
func maskSensitiveSqlLiteralsScoped(sql string, initial CredentialMapState, maskAll bool) string {
	if initial == CredentialStateNone && maskAll {
		lower := strings.ToLower(sql)
		found := false
		for _, kw := range maskableSecretKeywords {
			if strings.Contains(lower, kw) {
				found = true
				break
			}
		}
		if !found {
			return sql
		}
	}

	scanner := NewScanner(dialect.MYSQL, sql)
	defer PutScanner(scanner)
	previousToken := 0
	eq := int('=')
	lbrace := int('{')
	rbrace := int('}')
	credState := initial
	var spans [][2]int

	for scanner.Pos < len(sql) {
		tokStart := scanner.Pos
		typ, s := scanSensitiveToken(scanner, &previousToken)
		if credState != CredentialStateNone {
			switch credState {
			case CredentialStateAfterKeyword:
				if typ == eq {
					credState = CredentialStateAfterMapEq
				} else if typ != CREDENTIALS {
					// a CREDENTIALS token here is the tail replaying the very
					// keyword that armed this state (parse failed AT it);
					// keep arming instead of dropping the map context.
					credState = CredentialStateNone
				}
				continue
			case CredentialStateAfterMapEq:
				if typ == lbrace {
					credState = CredentialStateInMap
				} else {
					credState = CredentialStateNone
				}
				continue
			case CredentialStateInMap:
				if typ == rbrace {
					credState = CredentialStateNone
					continue
				}
				if typ == eq {
					credState = CredentialStateInMapAfterEq
				}
				continue
			case CredentialStateInMapAfterEq:
				if typ == eq {
					// a doubled '=' inside the map still precedes the value
					continue
				}
				if typ == lbrace {
					// the tail still contains the map opening (it started
					// left of `credentials =`): open the map instead of
					// treating the brace as a value.
					credState = CredentialStateInMap
					continue
				}
				spans = appendValueMaskSpan(spans, sql, tokStart, scanner.Pos)
				credState = CredentialStateInMap
				continue
			}
		}
		if typ == CREDENTIALS {
			credState = CredentialStateAfterKeyword
			continue
		}
		if !maskAll {
			continue
		}
		if typ == IDENTIFIED {
			typ, _ = scanSensitiveToken(scanner, &previousToken)
			if typ == BY || typ == WITH {
				valueStart := scanner.Pos
				typ, _ = scanSensitiveToken(scanner, &previousToken)
				if typ != RANDOM {
					spans = appendValueMaskSpan(spans, sql, valueStart, scanner.Pos)
				}
			}
		} else if strings.ToLower(s) == "access_key_id" || strings.ToLower(s) == "secret_access_key" {
			typ, _ = scanSensitiveToken(scanner, &previousToken)
			if typ == eq {
				valueStart := scanner.Pos
				scanSensitiveToken(scanner, &previousToken)
				spans = appendValueMaskSpan(spans, sql, valueStart, scanner.Pos)
			}
		}
	}

	if len(spans) == 0 {
		return sql
	}
	var builder strings.Builder
	builder.Grow(len(sql))
	prev := 0
	for _, sp := range spans {
		if sp[1] <= sp[0] || sp[0] < prev {
			continue
		}
		builder.WriteString(sql[prev:sp[0]])
		builder.WriteString("******")
		prev = sp[1]
	}
	builder.WriteString(sql[prev:])
	return builder.String()
}

// appendValueMaskSpan records the RAW source span of a value token scanned
// between tokStart and tokEnd (tokEnd = scanner.Pos right after the token).
// Using the raw span — never the decoded token length — is what makes the
// replacement exact for escapes, dollar-quoted strings and unterminated
// fragments. Quoted and dollar-quoted values keep their delimiters.
func appendValueMaskSpan(spans [][2]int, sql string, tokStart, tokEnd int) [][2]int {
	for tokStart < tokEnd && (sql[tokStart] == ' ' || sql[tokStart] == '\t' ||
		sql[tokStart] == '\n' || sql[tokStart] == '\r') {
		tokStart++
	}
	if tokStart >= tokEnd {
		return spans
	}
	switch ch := sql[tokStart]; {
	case ch == '\'' || ch == '"':
		// closed quote: keep both delimiters; unterminated: mask everything
		// after the opening one.
		if tokEnd-tokStart >= 2 && sql[tokEnd-1] == ch {
			return append(spans, [2]int{tokStart + 1, tokEnd - 1})
		}
		return append(spans, [2]int{tokStart + 1, tokEnd})
	case ch == '$':
		// dollar-quoted string: $tag$content$tag$ (possibly unterminated).
		if i := strings.IndexByte(sql[tokStart+1:tokEnd], '$'); i >= 0 {
			tag := sql[tokStart+1 : tokStart+1+i]
			if isDollarQuoteTag(tag) {
				delim := sql[tokStart : tokStart+2+i]
				if tokEnd-tokStart >= 2*len(delim) &&
					strings.HasSuffix(sql[tokStart:tokEnd], delim) {
					return append(spans, [2]int{tokStart + len(delim), tokEnd - len(delim)})
				}
				return append(spans, [2]int{tokStart + len(delim), tokEnd})
			}
		}
		return append(spans, [2]int{tokStart, tokEnd})
	default:
		// unquoted (number, identifier or partial token): mask it whole.
		return append(spans, [2]int{tokStart, tokEnd})
	}
}

// isDollarQuoteTag accepts the tag characters the scanner allows between the
// two dollar signs of a dollar-quoted string.
func isDollarQuoteTag(tag string) bool {
	for i := 0; i < len(tag); i++ {
		c := tag[i]
		if !(c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
			return false
		}
	}
	return true
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
