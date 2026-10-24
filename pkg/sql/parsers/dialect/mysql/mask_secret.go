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
//   - the same rules applied INSIDE the string literal of a leading
//     `PREPARE ... FROM '<sql>'`: that literal is SQL of its own whose
//     credential text is invisible to an outer token scan (#30747 review)
//
// Replacement happens on the RAW source span of each value token — the
// scanner-decoded token length is never trusted, so escaped content,
// dollar-quoted strings and unterminated values are masked precisely.
// Quoted values keep their quotes (`'******'`). The function tolerates
// incomplete, unterminated or truncated fragments and is idempotent.
func MaskSensitiveSqlLiterals(sql string) string {
	return MaskSensitiveSqlLiteralsWithSQLMode(sql, 0)
}

// MaskSensitiveSqlLiteralsWithSQLMode is MaskSensitiveSqlLiterals evaluated
// under the effective lexical SQL mode: whether a backslash escapes a quote
// changes where string tokens — and therefore credential values — begin and
// end, so diagnostics must mask what the session parser actually read
// (#30747 review).
func MaskSensitiveSqlLiteralsWithSQLMode(sql string, mode SQLModeFlags) string {
	return maskSensitiveSqlLiteralsScoped(sql, CredentialStateNone, true, mode)
}

// MaskSensitiveSqlLiteralsWithInitialState is MaskSensitiveSqlLiterals for a
// fragment that is known to begin at a specific point of a CREDENTIALS option
// map (e.g. a parse-error "near" tail captured mid-map, possibly right before
// a value): the keyword context is no longer visible, so the scanner starts
// in the given state.
func MaskSensitiveSqlLiteralsWithInitialState(sql string, initial CredentialMapState) string {
	return maskSensitiveSqlLiteralsScoped(sql, initial, true, 0)
}

// MaskStageCredentialLiteralsWithInitialState masks ONLY the values inside a
// CREDENTIALS = {...} option map (stage credentials, #30747) and leaves every
// other literal visible. It is used for the parse-error "near" echo, where
// the pre-existing diagnostics convention keeps e.g. `identified by
// '<password>'` readable (zz_accesscontrol goldens), while stage credential
// maps must still be scrubbed even when the echo begins mid-map or right at
// a value token.
func MaskStageCredentialLiteralsWithInitialState(sql string, initial CredentialMapState) string {
	return maskSensitiveSqlLiteralsScoped(sql, initial, false, 0)
}

// MaskStageCredentialLiteralsWithInitialStateAndSQLMode is
// MaskStageCredentialLiteralsWithInitialState under the effective lexical
// SQL mode (see MaskSensitiveSqlLiteralsWithSQLMode).
func MaskStageCredentialLiteralsWithInitialStateAndSQLMode(sql string, initial CredentialMapState, mode SQLModeFlags) string {
	return maskSensitiveSqlLiteralsScoped(sql, initial, false, mode)
}

// maskableSecretKeywords is the fast-path filter: without any of these
// substrings the full masker can produce no edit, so the common statement
// shape (plain DML) costs a single lowercasing pass instead of a token scan.
var maskableSecretKeywords = []string{"identified", "credentials", "access_key_id", "secret_access_key"}

// maskSensitiveSqlLiteralsScoped is the shared scanner. maskAll controls the
// identified-by/with and access-key rules; the CREDENTIALS map rule always
// runs. Effective token boundaries follow the scanner's own trivia skipping
// exactly, so whitespace or comments before a token never desynchronize the
// map separator and value detection (#30747 review).
func maskSensitiveSqlLiteralsScoped(sql string, initial CredentialMapState, maskAll bool, mode SQLModeFlags) string {
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

	scanner := NewScannerWithSQLMode(dialect.MYSQL, sql, mode)
	defer PutScanner(scanner)
	previousToken := 0
	eq := int('=')
	// The stage map grammar takes '=', but users commonly write JSON-style
	// `{'key':'value'}` instead — which then fails parsing mid-map with the
	// remainder echoed in the diagnostics (#30746). A bare ':' reaches the
	// scanner as LEX_ERROR; accept it as an in-map separator too, so that
	// malformed input still gets scrubbed.
	isMapSep := func(typ int, rawPos int) bool {
		return typ == eq || (typ == LEX_ERROR && rawPos < len(sql) && sql[rawPos] == ':')
	}
	lbrace := int('{')
	rbrace := int('}')
	credState := initial
	var spans [][2]int

	for scanner.Pos < len(sql) {
		tokStart := scanner.Pos
		typ, s := scanSensitiveToken(scanner, &previousToken)
		// Scan() consumes leading whitespace and comments itself, so the
		// token's byte identity lives at the first post-trivia position, not
		// at tokStart (#30747 review: `{'key' : 'v'}` or `=/*c*/'v'`).
		effStart := scanner.skipBlankAndCommentsFrom(tokStart)
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
					continue
				}
				if isMapSep(typ, effStart) {
					// a doubled separator (CREDENTIALS = = {...}) must not
					// abandon the sensitive map context (#30747 review).
					continue
				}
				credState = CredentialStateNone
				continue
			case CredentialStateInMap:
				if typ == rbrace {
					credState = CredentialStateNone
					continue
				}
				if isMapSep(typ, effStart) {
					credState = CredentialStateInMapAfterEq
				}
				continue
			case CredentialStateInMapAfterEq:
				if isMapSep(typ, effStart) {
					// a doubled separator inside the map still precedes the
					// value
					continue
				}
				if typ == lbrace {
					// the tail still contains the map opening (it started
					// left of `credentials =`): open the map instead of
					// treating the brace as a value.
					credState = CredentialStateInMap
					continue
				}
				if effStart < scanner.Pos && (sql[effStart] == '\'' || sql[effStart] == '"' ||
					(sql[effStart] == '$' && len(dollarQuoteDelimiter(sql, effStart)) > 0)) {
					spans = appendValueMaskSpan(spans, sql, effStart, scanner.Pos)
				} else {
					// An UNQUOTED value is already outside the stage map
					// grammar: a single token is not a trustworthy boundary
					// and punctuation between tokens (/, +, ...) would leave
					// the tail of the secret visible (#30747 review). Redact
					// everything up to the next map-level separator, where
					// the next key or the map end reliably resumes.
					spans = append(spans, maskUnquotedValueRun(sql, effStart))
				}
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
					spans = appendValueMaskSpan(spans, sql,
						scanner.skipBlankAndCommentsFrom(valueStart), scanner.Pos)
				}
			}
		} else if strings.ToLower(s) == "access_key_id" || strings.ToLower(s) == "secret_access_key" {
			typ, _ = scanSensitiveToken(scanner, &previousToken)
			if typ == eq {
				valueStart := scanner.Pos
				scanSensitiveToken(scanner, &previousToken)
				spans = appendValueMaskSpan(spans, sql,
					scanner.skipBlankAndCommentsFrom(valueStart), scanner.Pos)
			}
		}
	}

	out := sql
	if len(spans) > 0 {
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
		out = builder.String()
	}
	if initial == CredentialStateNone {
		// A leading PREPARE wraps its statement in one string token; the
		// outer scan cannot see credentials inside it (#30747 review).
		out = maskPreparedSqlLiterals(out, maskAll, mode)
	}
	return out
}

// maskUnquotedValueRun covers an unquoted (malformed) credential value: from
// the value's first byte through the next `,` or `}` — the nearest positions
// where map structure reliably resumes. Idempotent: the replacement `******`
// re-derives the identical span.
func maskUnquotedValueRun(sql string, start int) [2]int {
	end := start
	for end < len(sql) && sql[end] != ',' && sql[end] != '}' {
		end++
	}
	return [2]int{start, end}
}

// dollarQuoteDelimiter reports the opening delimiter of a well-formed
// dollar-quoted string starting at pos, or "" when pos does not start one.
func dollarQuoteDelimiter(sql string, pos int) string {
	if pos >= len(sql) || sql[pos] != '$' {
		return ""
	}
	i := strings.IndexByte(sql[pos+1:], '$')
	if i < 0 {
		return ""
	}
	if !isDollarQuoteTag(sql[pos+1 : pos+1+i]) {
		return ""
	}
	return sql[pos : pos+2+i]
}

// maskPreparedSqlLiterals descends into the string literal of a leading
// `PREPARE <name> FROM '<sql>'`: that literal is a statement of its own, so
// its credential literals must be masked by the same rules before the text
// reaches statement records, diagnostics, or debug traces (#30747 review).
// Text without the PREPARE/FROM string shape is returned unchanged. The
// descent is depth-1: prepared content is masked in the base state without
// re-entering this wrapper.
func maskPreparedSqlLiterals(sql string, maskAll bool, mode SQLModeFlags) string {
	scanner := NewScannerWithSQLMode(dialect.MYSQL, sql, mode)
	defer PutScanner(scanner)
	previousToken := 0
	typ, _ := scanSensitiveToken(scanner, &previousToken)
	if typ != PREPARE {
		return sql
	}
	typ, _ = scanSensitiveToken(scanner, &previousToken)
	if typ == EofChar() || typ == LEX_ERROR {
		return sql
	}
	typ, _ = scanSensitiveToken(scanner, &previousToken)
	if typ != FROM {
		return sql
	}
	tokStart := scanner.Pos
	typ, _ = scanSensitiveToken(scanner, &previousToken)
	if typ == EofChar() {
		return sql
	}
	effStart := scanner.skipBlankAndCommentsFrom(tokStart)
	if effStart >= scanner.Pos || (sql[effStart] != '\'' && sql[effStart] != '"') {
		return sql
	}
	quote := sql[effStart]
	contentEnd := scanner.Pos - 1 // closing delimiter when terminated
	if effStart+1 > contentEnd || sql[contentEnd] != quote {
		// unterminated literal: everything after the opening quote is content
		contentEnd = scanner.Pos
		if contentEnd > len(sql) {
			contentEnd = len(sql)
		}
	}
	content := sql[effStart+1 : contentEnd]
	masked := maskSensitiveSqlLiteralsScoped(content, CredentialStateNone, maskAll, mode)
	if masked == content {
		return sql
	}
	return sql[:effStart+1] + masked + sql[contentEnd:]
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
