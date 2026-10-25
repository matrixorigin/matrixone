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
	"sort"
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

// maskSensitiveSqlLiteralsScoped is the shared scanner. maskAll controls the
// identified-by/with and access-key rules; the CREDENTIALS map rule always
// runs. Effective token boundaries follow the scanner's own trivia skipping
// exactly, so whitespace or comments before a token never desynchronize the
// map separator and value detection (#30747 review).
func maskSensitiveSqlLiteralsScoped(sql string, initial CredentialMapState, maskAll bool, mode SQLModeFlags) string {
	// Byte-level prefilter (#30747 review: keyword-bearing ordinary SQL must
	// not pay a token scan): without an ACTIONABLE keyword occurrence the
	// masker can produce no edit. This is a lexical scan like the gate the
	// token loop applies — bare words only for CREDENTIALS/IDENTIFIED/PREPARE
	// (a quoted 'credentials' stays an inert STRING token for every rule),
	// plus the two access-key names inside string/identifier bodies (a quoted
	// 'access_key_id' key DOES arm the access-key rule) — so values of a
	// giant INSERT no longer drag the scanner over every row.
	if initial == CredentialStateNone && !hasCredentialActionableKeyword(sql, maskAll, mode) {
		return sql
	}
	spans, rawEdits, malformedMap := collectCredentialMasks(sql, initial, maskAll, mode, true)
	if malformedMap {
		// A broken quoted value can turn the following key/separator into
		// literal data before the scanner reports an error. Keep the original
		// bytes and reuse conservative span union rather than publish that tail.
		return maskPieceUnderAllLexicalVariantsScoped(sql, mode, maskAll, initial, rawEdits, false)
	}
	return spliceCredentialMasks(sql, spans, rawEdits)
}

// credentialRawEdit is one raw-range replacement produced by the PREPARE
// descent (the credential spans all replace with the same `******`, so only
// prepare rewrites need to carry their own text).
type credentialRawEdit struct {
	start, end int
	text       string
}

// collectCredentialMasks runs the shared token pass and returns the mask
// spans (replaced by `******`) and prepare-descent edits (replaced by their
// own text), all in coordinates of the ORIGINAL sql. Exported internally as
// a separate step so the multi-variant conservative path can UNION the
// regions several lexical modes see in the very same bytes — a chained
// re-mask would destroy structural separators one mode hides inside a
// string token of the other (#30747 review).
func collectCredentialMasks(sql string, initial CredentialMapState, maskAll bool, mode SQLModeFlags, descendPrepare bool) ([][2]int, []credentialRawEdit, bool) {
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
	coveredUntil := -1
	var spans [][2]int
	// A statement-initial PREPARE wraps its statement in one string token;
	// the credential rules cannot see inside it, so the descent into the
	// decoded literal runs INLINE in this same pass (#30747 review: one
	// shared scan instead of a full second one for every keyword-bearing
	// statement). Descended edits replace raw byte ranges, so they splice
	// alongside the credential spans below.
	var rawEdits []credentialRawEdit
	statementStart := true
	malformedMap := false

	for scanner.Pos < len(sql) {
		tokStart := scanner.Pos
		typ, s := scanSensitiveToken(scanner, &previousToken)
		// Scan() consumes leading whitespace and comments itself, so the
		// token's byte identity lives at the first post-trivia position, not
		// at tokStart (#30747 review: `{'key' : 'v'}` or `=/*c*/'v'`).
		// Executable comments are NOT pure trivia: the scanner yields their
		// contents as tokens, so maskTokenStart descends into them
		// (#30747 review: `identified by /*! 'x' */`).
		effStart := maskTokenStart(sql, tokStart)
		if typ == LEX_ERROR && credState != CredentialStateNone && !isMapSep(typ, effStart) {
			malformedMap = true
		}
		if typ == int(';') {
			statementStart = true
		} else {
			if descendPrepare && initial == CredentialStateNone && typ == PREPARE && statementStart && tokStart >= coveredUntil {
				// Consumes the `<name> FROM '<sql>'` tail; the decoded
				// payload is masked by a recursive scoped pass.
				if rs, re, text, ok := maskOnePrepareLiteral(sql, scanner, &previousToken, maskAll, mode); ok {
					rawEdits = append(rawEdits, credentialRawEdit{rs, re, text})
					if coveredUntil < re {
						coveredUntil = re
					}
				}
			}
			statementStart = false
		}
		if tokStart < coveredUntil {
			// Already redacted by an unquoted value run below; skip this
			// token entirely instead of re-scanning the same suffix once per
			// token, which is quadratic (#30747 review). The scanner still
			// advances linearly, so the whole pass stays O(input).
			continue
		}
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
				if effStart >= scanner.Pos {
					// Comment-close / version-marker tokens deliver no value
					// byte: an `=` assignment inside `/*! = */` must keep
					// pending until the REAL value token arrives, instead of
					// computing an unquoted boundary that stops at the first
					// embedded comma (#30747 review).
					continue
				}
				if typ == lbrace {
					// the tail still contains the map opening (it started
					// left of `credentials =`): open the map instead of
					// treating the brace as a value.
					credState = CredentialStateInMap
					continue
				}
				if typ == STRING || sql[effStart] == '\'' || sql[effStart] == '"' ||
					(sql[effStart] == '_' && valueDelimiterPos(sql, effStart, scanner.Pos) > effStart) ||
					(sql[effStart] == '$' && len(dollarQuoteDelimiter(sql, effStart)) > 0) {
					// Quoted values keep their delimiters — including when a
					// charset introducer prefixes the literal (`_utf8mb4'...'`):
					// the token type is authoritative, NOT the first raw byte
					// (an introducer's `_` must not reroute the value to the
					// unquoted run that stops at an embedded comma, #30747
					// review).
					spans = appendValueMaskSpan(spans, sql, effStart, scanner.Pos)
				} else {
					// An UNQUOTED value is already outside the stage map
					// grammar: a single token is not a trustworthy boundary
					// and punctuation between tokens (/, +, ...) would leave
					// the tail of the secret visible (#30747 review). Redact
					// everything up to the next map-level separator, where
					// the next key or the map end reliably resumes, consuming
					// each byte ONCE (coveredUntil keeps the pass linear —
					// including unclosed runs at end-of-input and runs inside
					// executable comments, #30747 review).
					run := maskUnquotedValueRun(sql, effStart)
					spans = append(spans, run)
					if coveredUntil < run[1] {
						coveredUntil = run[1]
					}
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
						maskTokenStart(sql, valueStart), scanner.Pos)
				}
			}
		} else if accessKeyOptionNameFold(s) {
			typ, _ = scanSensitiveToken(scanner, &previousToken)
			if typ == eq {
				valueStart := scanner.Pos
				scanSensitiveToken(scanner, &previousToken)
				spans = appendValueMaskSpan(spans, sql,
					maskTokenStart(sql, valueStart), scanner.Pos)
			}
		}
	}

	return spans, rawEdits, malformedMap
}

// spliceCredentialMasks applies the collected spans (as `******`) and
// prepare-descent edits (as their own text) to sql in one ordered pass.
func spliceCredentialMasks(sql string, spans [][2]int, rawEdits []credentialRawEdit) string {
	if len(spans) == 0 && len(rawEdits) == 0 {
		return sql
	}
	var builder strings.Builder
	builder.Grow(len(sql))
	prev := 0
	si, ri := 0, 0
	for si < len(spans) || ri < len(rawEdits) {
		var start, end int
		var text string
		if si < len(spans) && (ri >= len(rawEdits) || spans[si][0] <= rawEdits[ri].start) {
			start, end, text = spans[si][0], spans[si][1], "******"
			si++
		} else {
			start, end, text = rawEdits[ri].start, rawEdits[ri].end, rawEdits[ri].text
			ri++
		}
		if end <= start || start < prev {
			continue
		}
		builder.WriteString(sql[prev:start])
		builder.WriteString(text)
		prev = end
	}
	builder.WriteString(sql[prev:])
	return builder.String()
}

// MaskSensitiveSqlLiteralsPerStatement masks a WHOLE multi-statement
// COM_QUERY request statement by statement, tracking the effective lexical
// SQL mode across `SET sql_mode = ...` transitions: the debug trace receives
// the entire request text, and masking every statement under the request's
// INITIAL mode mis-masks what the session actually parses after a mode
// switch (`set sql_mode='NO_BACKSLASH_ESCAPES'; <stage>` would leave the
// credential visible, #30747 review). Each statement is tokenized and masked
// under the mode in effect at its start, and only SESSION-scope assignments
// retune that tracking (`@sql_mode`, GLOBAL/PERSIST and @@global forms leave
// the session alone). When a sql_mode assignment cannot be evaluated
// faithfully (expression, DEFAULT, list values), the REST of the request is
// kept whole and masked under EVERY lexical variant of the last known mode:
// after an unknown transition even `;` splitting would be context-losing —
// and masking is idempotent, so over-masking is the conservative direction.
func MaskSensitiveSqlLiteralsPerStatement(sql string, initial SQLModeFlags) string {
	// Byte-level gates: a mode transition needs a top-level statement
	// separator AND a sql_mode assignment. Without a raw ';' there is no
	// following statement at all; without the bytes "sql_mode" no statement
	// can set it. Both checks are substring-fast, so the common shapes
	// (single statements, bulk DML, multi-statement batches that never touch
	// sql_mode) keep the single-pass behavior untouched.
	if strings.IndexByte(sql, ';') < 0 || !containsFoldASCII(sql, "sql_mode") {
		return maskSensitiveSqlLiteralsScoped(sql, CredentialStateNone, true, initial)
	}
	// Probe credential rules and executable SET together. If neither can
	// occur, the request is already safe: do not tokenize/retain inert bulk
	// data, or scan it a second time just because it mentions sql_mode.
	if !hasMaskingKeyword(sql, true, true, initial) {
		return sql
	}

	type stmtSpan struct {
		start, end int
		mode       SQLModeFlags
		known      bool
	}
	var spans []stmtSpan
	mode := initial
	known := true
	stmtStart := 0
	for stmtStart < len(sql) {
		scanner := NewScannerWithSQLMode(dialect.MYSQL, sql, mode)
		scanner.Pos = stmtStart
		previousToken := 0
		stmtEnd := len(sql)
		var typList []int
		var valList []string
		firstToken := true
		collectTokens := false
		for scanner.Pos < len(sql) {
			typ, s := scanSensitiveToken(scanner, &previousToken)
			if typ == EofChar() {
				break
			}
			if firstToken {
				firstToken = false
				collectTokens = typ == SET
			}
			// Only a SET-initial statement can assign the session mode. Other
			// statements need boundary discovery, not a retained token inventory.
			if collectTokens {
				typList = append(typList, typ)
				valList = append(valList, s)
			}
			if typ == int(';') {
				stmtEnd = scanner.Pos
				break
			}
		}
		PutScanner(scanner)
		spans = append(spans, stmtSpan{stmtStart, stmtEnd, mode, known})
		if newMode, ok, touched := setStatementSQLModeChange(typList, valList); touched {
			// A SET in this statement governs the FOLLOWING statements.
			if ok {
				mode, known = newMode, true
			} else {
				// The effective mode for the REST of the request cannot be
				// determined (expression RHS, DEFAULT, a session-rejected
				// value). Then even SPLITTING further under the stale mode is
				// unsound: a semicolon inside a credential literal of the
				// unknown mode would fake a statement boundary whose context
				// no per-variant mask can recover (#30747 review). Stop
				// splitting and keep the remainder as ONE span masked under
				// every lexical variant of the last known mode.
				if stmtEnd < len(sql) {
					spans = append(spans, stmtSpan{stmtEnd, len(sql), mode, false})
				}
				break
			}
		}
		if stmtEnd <= stmtStart {
			break
		}
		stmtStart = stmtEnd
	}

	var builder strings.Builder
	builder.Grow(len(sql))
	for _, sp := range spans {
		piece := sql[sp.start:sp.end]
		if sp.known {
			piece = maskSensitiveSqlLiteralsScoped(piece, CredentialStateNone, true, sp.mode)
		} else {
			piece = maskPieceUnderAllLexicalVariants(piece, sp.mode)
		}
		builder.WriteString(piece)
	}
	return builder.String()
}

// maskPieceUnderAllLexicalVariants is the conservative fallback for a
// statement whose effective lexical mode cannot be determined (a sql_mode
// assignment with an unevaluable value, #30747 review): the byte regions
// EVERY lexical variant of the last known mode marks as credentials are
// UNIONED in the original coordinates and masked in one splice. Chaining
// per-variant re-masks instead would lose separators that one mode hides
// inside a string token of another (`{'k'='a\', 'k2'='X'}`: mode 0 masks
// through the comma, so a second pass can no longer see the k2 value).
// Only NO_BACKSLASH_ESCAPES and ANSI_QUOTES change where string tokens
// begin and end; over-masking is the safe direction here.
func maskPieceUnderAllLexicalVariants(piece string, lastKnown SQLModeFlags) string {
	return maskPieceUnderAllLexicalVariantsScoped(piece, lastKnown, true, CredentialStateNone, nil, true)
}

func maskPieceUnderAllLexicalVariantsScoped(piece string, lastKnown SQLModeFlags, maskAll bool, initial CredentialMapState, preparedEdits []credentialRawEdit, descendPrepare bool) string {
	base := lastKnown &^ SQLModeFlags(SQLModeNoBackslashEscapes|SQLModeANSIQuotes)
	type maskItem struct {
		start, end int
		text       string
		isRaw      bool
	}
	var items []maskItem
	// A known-mode malformed-map fallback already masked each PREPARE child
	// during its first collection. Reuse those edits instead of expanding the
	// same subtree four times at every nesting level. Unknown-mode requests
	// still need the separate per-variant descents (preparedEdits=nil).
	for _, r := range preparedEdits {
		items = append(items, maskItem{r.start, r.end, r.text, true})
	}
	for variant := 0; variant < 4; variant++ {
		m := base
		if variant&1 != 0 {
			m |= SQLModeFlags(SQLModeNoBackslashEscapes)
		}
		if variant&2 != 0 {
			m |= SQLModeFlags(SQLModeANSIQuotes)
		}
		if initial == CredentialStateNone && !hasCredentialActionableKeyword(piece, maskAll, m) {
			continue
		}
		spans, rawEdits, _ := collectCredentialMasks(piece, initial, maskAll, m, descendPrepare)
		for _, s := range spans {
			items = append(items, maskItem{s[0], s[1], "******", false})
		}
		for _, r := range rawEdits {
			items = append(items, maskItem{r.start, r.end, r.text, true})
		}
	}
	if len(items) == 0 {
		return piece
	}
	sort.Slice(items, func(i, j int) bool { return items[i].start < items[j].start })
	var builder strings.Builder
	builder.Grow(len(piece))
	prev := 0
	for i := 0; i < len(items); {
		// Merge every interval overlapping the current cluster.
		clusterStart, clusterEnd := items[i].start, items[i].end
		singleText := items[i].text
		single := true
		j := i + 1
		for j < len(items) && items[j].start < clusterEnd {
			if items[j].end > clusterEnd {
				clusterEnd = items[j].end
			}
			single = false
			j++
		}
		if clusterEnd <= clusterStart || clusterStart < prev {
			i = j
			continue
		}
		builder.WriteString(piece[prev:clusterStart])
		if single {
			builder.WriteString(singleText)
		} else {
			builder.WriteString("******")
		}
		prev = clusterEnd
		i = j
	}
	builder.WriteString(piece[prev:])
	return builder.String()
}

// setStatementSQLModeChange inspects one statement's token sequence for a
// SESSION-scope `SET ... sql_mode = <value>` assignment. Only statements
// that BEGIN with SET are considered, so DML with `SET`-clause looks-alikes
// (`UPDATE ...`, `INSERT ... SET`) is never misread as a mode switch; and
// only SYSTEM-variable assignment targets that retune THIS session's parsing
// count (#30747 review): user variables (`SET @sql_mode=...`, and even
// `@session.sql_mode` — both AT_ID tokens, System=false), `SET GLOBAL/PERSIST
// sql_mode`, the `@@global.sql_mode` form, and quoted strings that merely
// CONTAIN the name all leave the current session mode untouched and must not
// trigger a transition, while the `:=` assignment operator (accepted by the
// parser like `=`) must. touched
// reports that a session-scope assignment was present; ok reports that its
// value was a single literal or word that parses into flags (an expression,
// DEFAULT, or a multi-token list yields ok=false so the caller can fall back
// to conservative masking).
func setStatementSQLModeChange(typList []int, valList []string) (SQLModeFlags, bool, bool) {
	if len(typList) == 0 || typList[0] != SET {
		return 0, false, false
	}
	var mode SQLModeFlags
	touched := false
	ok := true
	for start := 1; start < len(typList); {
		// Assignment-list commas are separators only outside expressions.
		// In particular, a system variable inside IF/COALESCE is an operand,
		// never the target of a new SET item.
		end, depth := start, 0
		for end < len(typList) {
			typ := typList[end]
			if typ == int(';') || typ == int(',') && depth == 0 {
				break
			}
			switch typ {
			case int('('):
				depth++
			case int(')'):
				if depth > 0 {
					depth--
				}
			}
			end++
		}
		i := start
		start = end + 1
		if i == end {
			continue
		}
		switch typList[i] {
		case GLOBAL, PERSIST:
			continue
		case SESSION, LOCAL:
			i++
		}
		if i+2 >= end || (typList[i+1] != int('=') && typList[i+1] != ASSIGNMENT) {
			continue
		}
		// Only genuine SYSTEM-variable targets count (isSessionSQLModeSet
		// semantics, #30747 review): a `@session.sql_mode` is a USER variable
		// (an AT_ID token, System=false) however it is spelled, and a quoted
		// string naming sql_mode is an assignment VALUE, not a target.
		fold := strings.ToLower(valList[i])
		var isSysTarget bool
		if typList[i] == AT_AT_ID {
			// `@@sql_mode` / `@@session.sql_mode` / `@@local.sql_mode`:
			// system variable, session scope (@@global. never matches: its
			// value reads `global.sql_mode`).
			isSysTarget = fold == "sql_mode" || fold == "session.sql_mode" ||
				fold == "local.sql_mode"
		} else if typList[i] != AT_ID && typList[i] != STRING && fold == "sql_mode" {
			// bare identifier path: `set sql_mode` / `set session sql_mode`
			isSysTarget = true
		}
		if !isSysTarget {
			continue
		}
		touched = true
		if end == i+3 && (typList[i+2] == STRING || valList[i+2] != "") {
			// A single literal or bare mode name is evaluable. Never turn a
			// previous unknown assignment back into known: it could fail before
			// a later literal assignment runs, leaving the original mode intact.
			var valueOK bool
			mode, valueOK = sqlModeValueFlags(valList[i+2])
			ok = ok && valueOK
		} else {
			// DEFAULT, empty list, or an expression (concat, function call):
			// we cannot evaluate it the way the session would.
			ok = false
		}
	}
	return mode, ok, touched
}

// sessionSQLModeNames is the whitelist of sql_mode values the session layer
// ACCEPTS (mirrors the system-variable definition): a SET with any other
// value fails and leaves the session mode unchanged, and combination aliases
// like ORACLE/POSTGRESQL are not among them — so for every value in this set,
// ParseSQLModeFlags yields exactly the lexical bits the session ends up with.
var sessionSQLModeNames = map[string]struct{}{
	"ANSI": {}, "TRADITIONAL": {}, "ALLOW_INVALID_DATES": {}, "ANSI_QUOTES": {},
	"ERROR_FOR_DIVISION_BY_ZERO": {}, "HIGH_NOT_PRECEDENCE": {}, "IGNORE_SPACE": {},
	"MATRIXONE_NATIVE": {}, "NO_AUTO_VALUE_ON_ZERO": {}, "NO_BACKSLASH_ESCAPES": {},
	"NO_DIR_IN_CREATE": {}, "NO_ENGINE_SUBSTITUTION": {}, "NO_UNSIGNED_SUBTRACTION": {},
	"NO_ZERO_DATE": {}, "NO_ZERO_IN_DATE": {}, "ONLY_FULL_GROUP_BY": {},
	"PAD_CHAR_TO_FULL_LENGTH": {}, "PIPES_AS_CONCAT": {}, "REAL_AS_FLOAT": {},
	"STRICT_ALL_TABLES": {}, "STRICT_TRANS_TABLES": {}, "TIME_TRUNCATE_FRACTIONAL": {},
	"ENABLE_BOOL_SUMAVG": {},
}

// sqlModeValueFlags evaluates a sql_mode assignment value: every nonempty
// comma-separated part must be a name the session accepts, otherwise the SET
// would fail (mode unchanged) or carries unparseable concatenations — both
// reported as ok=false so the caller falls back to conservative masking.
func sqlModeValueFlags(value string) (SQLModeFlags, bool) {
	for _, part := range strings.Split(value, ",") {
		name := strings.ToUpper(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		if _, exists := sessionSQLModeNames[name]; !exists {
			return 0, false
		}
	}
	return ParseSQLModeFlags(value), true
}

// containsFoldASCII reports whether sql contains the ASCII needle under
// simple case folding, without allocating a lowercased copy of sql. The
// needle must be lowercase ASCII (a-z, 0-9, punctuation).
func containsFoldASCII(sql string, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(sql) < len(needle) {
		return false
	}
	first := needle[0]
	for i := 0; i+len(needle) <= len(sql); i++ {
		c := sql[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != first {
			continue
		}
		match := true
		for j := 1; j < len(needle); j++ {
			d := sql[i+j]
			if d >= 'A' && d <= 'Z' {
				d += 'a' - 'A'
			}
			if d != needle[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// accessKeyOptionNameFold reports whether a candidate key token value names
// one of the external-storage credential keys. Besides the plain decoded
// name it accepts the HEX literal spelling: the scanner renders X'...' as
// `0x...` (#30747 review: `X'6163636573735f6b65795f6964'` decodes to
// access_key_id and still arms the rule).
func accessKeyOptionNameFold(s string) bool {
	if len(s) > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		if decoded, ok := hexDecodeBytes(s[2:]); ok {
			s = decoded
		}
	}
	return strings.EqualFold(s, "access_key_id") || strings.EqualFold(s, "secret_access_key")
}

func isIdentifierByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func hexDigitByte(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// hexDecodeBytes decodes an even-length hex string; ok=false for odd length
// or non-hex bytes.
func hexDecodeBytes(s string) (string, bool) {
	if len(s) == 0 || len(s)%2 != 0 {
		return "", false
	}
	buf := make([]byte, len(s)/2)
	for i := 0; i < len(buf); i++ {
		hi, ok1 := hexNibble(s[2*i])
		lo, ok2 := hexNibble(s[2*i+1])
		if !ok1 || !ok2 {
			return "", false
		}
		buf[i] = hi<<4 | lo
	}
	return string(buf), true
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// hasFoldWordAtEscapeTolerant is hasFoldWordAt for a DECODED-body compare:
// when escapes are active it skips backslash bytes before comparing, since
// the scanner collapses `\x` escape sequences to the escaped character when
// it builds the token value the credential rules match on (#30747 review:
// the prefilter must never be stricter than the scanner). Skipping admits a
// superset of the true matches; the token scan decides for real.
func hasFoldWordAtEscapeTolerant(sql string, pos int, needle string, escapes bool) bool {
	if !escapes {
		return hasFoldWordAt(sql, pos, needle)
	}
	i := pos
	for k := 0; k < len(needle); k++ {
		for i < len(sql) && sql[i] == '\\' {
			i++
		}
		if i >= len(sql) {
			return false
		}
		c := sql[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != needle[k] {
			return false
		}
		i++
	}
	return true
}

// hasFoldWordAt reports whether the bytes at sql[pos:pos+len(needle)] equal
// needle under ASCII case folding (needle must be lowercase).
func hasFoldWordAt(sql string, pos int, needle string) bool {
	if pos+len(needle) > len(sql) {
		return false
	}
	for k := 0; k < len(needle); k++ {
		c := sql[pos+k]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != needle[k] {
			return false
		}
	}
	return true
}

// hasCredentialActionableKeyword is the byte-level prefilter for the scoped
// masker: it reports whether sql can contain any occurrence that arms a
// credential rule, without allocating and without a token scan (#30747
// review: keyword-bearing ordinary SQL must not pay for the full masker).
// Bare words (outside strings, quoted identifiers and comments) are compared
// against credentials / identified / prepare — prepare arms the DESCENT, not
// a credential rule, but the descent is where an ENCODED credential keyword
// becomes visible (#30747 review). The two access-key names are ALSO
// searched inside string, quoted-identifier and dollar-quoted bodies,
// because a QUOTED 'access_key_id' key arms the access-key rule while a
// quoted 'credentials' stays an inert STRING token. Ordinary comments are
// skipped; executable `/*! ... */` comments are scanned through, since the
// scanner yields their contents as tokens.
func hasCredentialActionableKeyword(sql string, maskAll bool, mode SQLModeFlags) bool {
	return hasMaskingKeyword(sql, maskAll, false, mode)
}

// hasMaskingKeyword shares scanner-compatible, allocation-free lexical
// skipping between credential admission and the request's SET-mode probe.
// includeSQLModeSet additionally admits executable SET words;
// SET/sql_mode inside inert string data must not trigger token retention.
func hasMaskingKeyword(sql string, maskAll bool, includeSQLModeSet bool, mode SQLModeFlags) bool {
	i := 0
	n := len(sql)
	for i < n {
		c := sql[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			// String literal (a double quote is an identifier quote under
			// ANSI_QUOTES and a backtick always quotes an identifier; the
			// keyword-armed rules treat every such body the same way: only
			// the access-key names matter inside).
			delim := c
			// The access-key RULE matches the DECODED string value
			// (`'\access_key_id'` and `'access_k\ey_id'` both decode to the
			// key name and DO arm it, #30747 review), so the admission check
			// inside a string body is backslash-tolerant — including at the
			// very first body byte — whenever the current mode escapes.
			// Quoted identifiers have NO escapes: under ANSI_QUOTES a double
			// quote is an identifier quote where a backslash is a literal
			// byte, so an ident-following quote must not be treated as
			// escape-swallowed or the whole rest of the statement (including
			// the credentials clause) is skipped (#30747 review).
			bodyEscapes := delim != '`' && !mode.Has(SQLModeNoBackslashEscapes) &&
				!(delim == '"' && mode.Has(SQLModeANSIQuotes))
			// HEX literal (X'...' / x'...', optionally introducer-prefixed):
			// its decoded bytes may BE an access-key name that arms the rule
			// (#30747 review), so admit it when the short body decodes to one.
			if maskAll && delim != '`' && i > 0 && (sql[i-1] == 'X' || sql[i-1] == 'x') &&
				(i < 2 || !isIdentifierByte(sql[i-2])) {
				hexEnd := i + 1
				for hexEnd < n && hexDigitByte(sql[hexEnd]) {
					hexEnd++
				}
				// bounded: key names decode to <= 17 bytes (34 hex digits)
				if hexEnd < n && sql[hexEnd] == delim && hexEnd-i-1 <= 64 &&
					(hexEnd-i-1)%2 == 0 {
					if decoded, ok := hexDecodeBytes(sql[i+1 : hexEnd]); ok &&
						(strings.EqualFold(decoded, "access_key_id") ||
							strings.EqualFold(decoded, "secret_access_key")) {
						return true
					}
				}
			}
			j := i + 1
			for j < n {
				if b := sql[j] | 0x20; maskAll && (b == 'a' || b == 's' ||
					(bodyEscapes && sql[j] == '\\')) {
					if hasFoldWordAtEscapeTolerant(sql, j, "access_key_id", bodyEscapes) ||
						hasFoldWordAtEscapeTolerant(sql, j, "secret_access_key", bodyEscapes) {
						return true
					}
				}
				if bodyEscapes && sql[j] == '\\' {
					j += 2
					continue
				}
				if sql[j] == delim {
					if j+1 < n && sql[j+1] == delim {
						j += 2
						continue
					}
					break
				}
				j++
			}
			if j < n {
				j++
			}
			i = j
		case c == '$':
			delim := dollarQuoteDelimiter(sql, i)
			if delim == "" {
				i++
				continue
			}
			bodyStart := i + len(delim)
			end := n
			if idx := strings.Index(sql[bodyStart:], delim); idx >= 0 {
				end = bodyStart + idx
			}
			if maskAll && (containsFoldASCII(sql[bodyStart:end], "access_key_id") ||
				containsFoldASCII(sql[bodyStart:end], "secret_access_key")) {
				return true
			}
			i = end + len(delim)
			if i > n {
				i = n
			}
		case c == '/' && i+2 < n && sql[i+1] == '*' && sql[i+2] == '!':
			// Executable comment: its contents reach the scanner as tokens,
			// so they are scanned through like ordinary code. The predicate
			// must match all three bytes of `/*!`: a weaker one lets the
			// block-comment case below swallow the comment body, and
			// credentials inside it go completely unseen (#30747 review).
			i += 3
			for i < n && sql[i] >= '0' && sql[i] <= '9' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				return false
			}
			i += 2 + end + 2
		case c == '/':
			if i+1 < n && sql[i+1] == '/' {
				i = skipLineCommentFrom(sql, i+2)
			} else {
				i++
			}
		case c == '#':
			i = skipLineCommentFrom(sql, i+1)
		case c == '-' && i+1 < n && sql[i+1] == '-' && (i+2 == n || isMySQLDashCommentBlank(sql[i+2])):
			i = skipLineCommentFrom(sql, i+2)
		case c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
			j := i + 1
			for j < n {
				d := sql[j]
				if d == '_' || (d >= 'a' && d <= 'z') || (d >= 'A' && d <= 'Z') || (d >= '0' && d <= '9') {
					j++
				} else {
					break
				}
			}
			word := sql[i:j]
			if includeSQLModeSet && len(word) == len("set") && hasFoldWordAt(word, 0, "set") {
				return true
			}
			if hasFoldWordAt(word, 0, "credentials") && len(word) == len("credentials") ||
				maskAll && len(word) == len("identified") && hasFoldWordAt(word, 0, "identified") ||
				len(word) == len("prepare") && hasFoldWordAt(word, 0, "prepare") ||
				maskAll && (len(word) == len("access_key_id") && hasFoldWordAt(word, 0, "access_key_id") ||
					len(word) == len("secret_access_key") && hasFoldWordAt(word, 0, "secret_access_key")) {
				return true
			}
			i = j
		default:
			i++
		}
	}
	return false
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

// maskOnePrepareLiteral is invoked right after a PREPARE token: it consumes
// the `<name> FROM '<sql>'` tail and, when the literal carries credential
// text, returns the literal's byte span and the masked replacement. The
// literal is DECODED first — the payload of a prepare body is SQL of its
// own, and leaving it encoded (doubled quotes, backslash escapes,
// dollar-quoting) hides credentials from the inner scan and can even split
// the payload into misleading tokens (#30747 review) — and the masked
// payload is re-encoded into a valid literal for the same delimiter.
func maskOnePrepareLiteral(sql string, scanner *Scanner, previousToken *int,
	maskAll bool, mode SQLModeFlags) (int, int, string, bool) {
	fail := func() (int, int, string, bool) { return 0, 0, "", false }
	typ, _ := scanSensitiveToken(scanner, previousToken)
	if typ == EofChar() || typ == LEX_ERROR {
		return fail()
	}
	typ, _ = scanSensitiveToken(scanner, previousToken)
	if typ != FROM {
		return fail()
	}
	tokStart := scanner.Pos
	typ, payload := scanSensitiveToken(scanner, previousToken)
	if typ == EofChar() {
		return fail()
	}
	// The AUTHORITATIVE payload of a quoted literal is the scanner's own
	// decoded value (#30747 review): MySQL folds doubled quotes, backslash
	// quote/backslash escapes, multi-byte escape sequences and — inside a
	// prepare body — even backslash-escaped letters (`cred\entials` decodes
	// to `credentials`); a custom decoder split differently hides credentials
	// from the inner scan and splits payload SQL into misleading tokens. The
	// literal token also may arrive with a charset introducer
	// (`_utf8mb4'...'`): the scanner consumes it as ONE string token, so
	// gating on the first raw byte would wrongly skip the descent. An
	// UNTERMINATED literal surfaces as LEX_ERROR but still carries the
	// decoded tail, and an unterminated dollar-quoted body carries an empty
	// value — both are still credential-bearing payloads.
	if typ != STRING && typ != LEX_ERROR {
		return fail()
	}
	effStart := maskTokenStart(sql, tokStart)
	if effStart >= scanner.Pos {
		return fail()
	}
	// Locate the opening delimiter (and any introducer prefix) inside the raw
	// span; the prefix and the closing delimiter are carried over verbatim.
	openEnd := effStart
	var quote byte
	var delim string
	for openEnd < scanner.Pos && openEnd < len(sql) {
		if c := sql[openEnd]; c == '\'' || c == '"' {
			quote = c
			openEnd++
			break
		} else if c == '$' && len(dollarQuoteDelimiter(sql, openEnd)) > 0 {
			delim = dollarQuoteDelimiter(sql, openEnd)
			openEnd += len(delim)
			break
		}
		openEnd++
	}
	if quote == 0 && delim == "" {
		return fail()
	}
	// Determine the closing delimiter within the raw span. For a terminated
	// quoted literal it is the last byte; for dollar-quoted, the trailing tag.
	closer := ""
	if quote != 0 {
		if scanner.Pos-effStart > 1 && sql[scanner.Pos-1] == quote && typ == STRING {
			closer = string(quote)
		}
	} else if n := len(delim); scanner.Pos-effStart > n && strings.HasSuffix(sql[effStart:scanner.Pos], delim) {
		closer = sql[scanner.Pos-n : scanner.Pos]
	}
	if delim != "" {
		// Dollar-quoted bodies have no escapes: the raw bytes between the
		// delimiters ARE the payload, including when a malformed tail left
		// the token value empty.
		payload = sql[openEnd : scanner.Pos-len(closer)]
	}
	masked := maskSensitiveSqlLiteralsScoped(payload, CredentialStateNone, maskAll, mode)
	if masked == payload {
		return 0, 0, "", false
	}
	var body string
	if quote != 0 {
		body = sql[effStart:openEnd] + encodeSqlLiteralContent(masked, quote, mode) + closer
	} else {
		// Dollar-quoted payloads are re-emitted raw: they hold any byte
		// sequence and need no escaping.
		body = sql[effStart:openEnd] + masked + closer
	}
	return effStart, scanner.Pos, body, true
}

// encodeSqlLiteralContent re-encodes a masked payload for a quoted literal:
// delimiter bytes double (valid under any SQL mode) and, when backslash
// escapes apply, backslash bytes double as well, so the result reads back as
// exactly the masked payload.
func encodeSqlLiteralContent(payload string, quote byte, mode SQLModeFlags) string {
	backslash := !mode.Has(SQLModeNoBackslashEscapes)
	var b strings.Builder
	b.Grow(len(payload) + 4)
	for i := 0; i < len(payload); i++ {
		c := payload[i]
		if backslash && c == '\\' {
			b.WriteString(`\\`)
			continue
		}
		if c == quote {
			b.WriteByte(quote)
			b.WriteByte(quote)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// maskTokenStart resolves the first byte of the token that begins at or
// after pos. Whitespace and NON-executable comments are trivia and are
// skipped, but executable `/*! ... */` comments are NOT: the scanner yields
// their contents as real tokens, so the token's byte identity starts INSIDE
// the comment wrapper (#30747 review: `identified by /*! 'CANARY' */` and
// `{'aws_secret' = /*! 'CANARY' */}` would otherwise keep the secret
// visible in statement records and debug traces).
func maskTokenStart(sql string, pos int) int {
	for pos < len(sql) {
		switch sql[pos] {
		case ' ', '\n', '\r', '\t':
			pos++
			continue
		}
		if strings.HasPrefix(sql[pos:], "/*!") {
			pos += 3
			// MySQL version prefix (`/*!12345 ... */`): the digits are part
			// of the comment wrapper, not of the delivered token.
			for pos < len(sql) && sql[pos] >= '0' && sql[pos] <= '9' {
				pos++
			}
			continue
		}
		if strings.HasPrefix(sql[pos:], "*/") {
			// A comment terminator closed by the executable-comment subscan:
			// the scanner has already passed it, so it is trivia here too —
			// otherwise a token preceded by `*/` (e.g. an `=` assignment
			// inside `/*! = */` before a quoted value) computes an unquoted
			// boundary and stops masking at an embedded comma (#30747 review).
			pos += 2
			continue
		}
		if strings.HasPrefix(sql[pos:], "/*") {
			end := strings.Index(sql[pos+2:], "*/")
			if end < 0 {
				return pos
			}
			pos += 2 + end + 2
			continue
		}
		if strings.HasPrefix(sql[pos:], "//") {
			pos = skipLineCommentFrom(sql, pos+2)
			continue
		}
		if sql[pos] == '#' {
			pos = skipLineCommentFrom(sql, pos+1)
			continue
		}
		if strings.HasPrefix(sql[pos:], "--") &&
			(pos+2 == len(sql) || isMySQLDashCommentBlank(sql[pos+2])) {
			pos = skipLineCommentFrom(sql, pos+2)
			continue
		}
		return pos
	}
	return pos
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
	// A charset introducer (`_utf8mb4'...'`) prefixes the delimiter with
	// identifier bytes; the introducer stays visible and only the literal
	// content is masked (#30747 review).
	vstart := valueDelimiterPos(sql, tokStart, tokEnd)
	if vstart < 0 {
		// unquoted (number, identifier or partial token): mask it whole.
		return append(spans, [2]int{tokStart, tokEnd})
	}
	switch ch := sql[vstart]; {
	case ch == '\'' || ch == '"':
		// An executable-comment subscan can split a literal that ENDS in
		// operator bytes (the inserted `******` itself re-scans as `*` math
		// tokens), truncating the span mid-string and breaking idempotency
		// (#30747 review). Extend the end to the real closing delimiter
		// whenever the reported token stopped short of it.
		if closeRel := strings.IndexByte(sql[vstart+1:], ch); closeRel >= 0 {
			if rc := vstart + 1 + closeRel + 1; rc > tokEnd && rc <= len(sql) {
				tokEnd = rc
			}
		}
		// closed quote: keep both delimiters; unterminated: mask everything
		// after the opening one.
		if tokEnd-vstart >= 2 && sql[tokEnd-1] == ch {
			return append(spans, [2]int{vstart + 1, tokEnd - 1})
		}
		return append(spans, [2]int{vstart + 1, tokEnd})
	case ch == '$':
		// dollar-quoted string: $tag$content$tag$ (possibly unterminated).
		if i := strings.IndexByte(sql[vstart+1:tokEnd], '$'); i >= 0 {
			tag := sql[vstart+1 : vstart+1+i]
			if isDollarQuoteTag(tag) {
				delim := sql[vstart : vstart+2+i]
				if tokEnd-vstart >= 2*len(delim) &&
					strings.HasSuffix(sql[vstart:tokEnd], delim) {
					return append(spans, [2]int{vstart + len(delim), tokEnd - len(delim)})
				}
				return append(spans, [2]int{vstart + len(delim), tokEnd})
			}
		}
		return append(spans, [2]int{tokStart, tokEnd})
	default:
		// unreachable: valueDelimiterPos only returns a quote or dollar
		// start; kept whole for safety.
		return append(spans, [2]int{tokStart, tokEnd})
	}
}

// valueDelimiterPos returns where the value's opening delimiter starts: at
// tokStart for a plain literal, or after a charset introducer prefix
// (`_utf8mb4` / `_latin1` / `_binary` ..., i.e. '_' plus identifier bytes)
// directly followed by a quote or dollar-quote delimiter (#30747 review).
// Returns -1 when the token carries no literal delimiter.
func valueDelimiterPos(sql string, tokStart, tokEnd int) int {
	if tokStart >= tokEnd {
		return -1
	}
	if sql[tokStart] == '_' {
		j := tokStart + 1
		for j < tokEnd && (sql[j] == '_' || (sql[j] >= 'a' && sql[j] <= 'z') ||
			(sql[j] >= 'A' && sql[j] <= 'Z') || (sql[j] >= '0' && sql[j] <= '9')) {
			j++
		}
		if j > tokStart+1 && j < tokEnd &&
			(sql[j] == '\'' || sql[j] == '"' ||
				(sql[j] == '$' && len(dollarQuoteDelimiter(sql, j)) > 0)) {
			return j
		}
		return -1
	}
	if c := sql[tokStart]; c == '\'' || c == '"' ||
		(c == '$' && len(dollarQuoteDelimiter(sql, tokStart)) > 0) {
		return tokStart
	}
	return -1
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
