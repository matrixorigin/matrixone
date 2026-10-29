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
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// A parse-error tail starting mid-map carries no CREDENTIALS keyword context,
// so the scanner must start directly in map state. (#30747/#30746)
func TestMaskSensitiveSqlLiteralsWithInitialState(t *testing.T) {
	frag := `{'aws_secret_key'='XYZCANARY', 'aws_region'='r1'} more text`
	got := MaskSensitiveSqlLiteralsWithInitialState(frag, CredentialStateInMap)
	require.NotContains(t, got, "XYZCANARY")
	require.Contains(t, got, "'aws_secret_key'='******'")
	require.Equal(t, " more text", got[len(got)-len(" more text"):])

	// without the map context the fragment is left alone
	got = MaskSensitiveSqlLiteralsWithInitialState(frag, CredentialStateNone)
	require.Contains(t, got, "XYZCANARY")
}

// End-to-end: malformed credential-bearing DDL must not echo its secrets in
// the parse error, whichever side of the map the failure sits on.
func TestParseErrorMasksStageCredentials(t *testing.T) {
	cases := []string{
		// unterminated map: tail begins at the credentials keyword
		"create stage bad URL = 'file:///x' CREDENTIALS = {'aws_secret_key'='XYZCANARY', 'aws_region'='r1'",
		// trailing comma before EOF: tail begins mid-map
		"create stage bad credentials={'aws_secret_key'='XYZCANARY',",
		// garbage inside the map
		"create stage bad credentials={'aws_secret_key'='XYZCANARY', !!!}",
	}
	for _, sql := range cases {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err, sql)
		require.NotContains(t, err.Error(), "XYZCANARY", sql)
	}
}

// The error tail can begin AT the secret value itself (right after '='), so
// the lexer additionally tracks the pending-value state. An unterminated
// value must never reach the client error. (#30747 review)
func TestParseErrorMasksUnterminatedCredentialValue(t *testing.T) {
	cases := []string{
		// missing closing quote inside the map: the echo starts at the value
		"create stage s url='file:///x' credentials={'aws_secret_key'='CANARYNOEND",
		// numeric value shape
		"create stage s url='file:///x' credentials={'aws_port'=CANARY12345",
	}
	for _, sql := range cases {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err, sql)
		require.NotContains(t, err.Error(), "CANARY", sql)
	}
}

// The client-side parse-error "near" echo deliberately keeps identified-by
// literals readable: that diagnostic shape is part of the existing behavior
// (zz_accesscontrol goldens). Secrets of that kind are masked on the
// statement-record and diagnostic-error path instead (see frontend
// redactStatementErrorForLogging); the lexer echo narrows itself to stage
// credential maps only.
func TestParseErrorKeepsIdentifiedByInNearEcho(t *testing.T) {
	// misspelled CREATE keyword: the error position is at the start, so the
	// whole statement (including the identified-by literal) echoes into the
	// near text and must stay readable (zz_accesscontrol convention).
	//
	// Scope note (#30747 review round 16): this pins the PARSER's raw
	// serialization only — it is the raw material the frontend redacts
	// from (the near-fragment redactor locates fragments verbatim), not
	// the client-visible contract. On the client path, credential-bearing
	// malformed statements receive the redacted serialization instead
	// (TestParseErrorClientEchoRedactsCredentials, battery cell
	// r13-identified-by-unterminated): tests asserting plaintext AT THE
	// CLIENT are gone.
	_, err := ParseOne(context.Background(), "create accout a1 ADMIN_NAME 'admin' IDENTIFIED BY 'PWN123456';", 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "IDENTIFIED BY 'PWN123456'")
}

// Masking offsets must follow the RAW source span of each value token, so
// escapes and dollar-quoted values never leave a residue of the original
// secret in the masked text. (#30747 review)
func TestMaskSensitiveSqlLiteralsRawSpans(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		// dollar-quoted credential value
		{
			`create stage s url='file:///x' credentials={'aws_secret_key'=$credential$CANARY$credential$}`,
			`create stage s url='file:///x' credentials={'aws_secret_key'=$credential$******$credential$}`,
		},
		// single-quote escape inside the value
		{
			`create stage s url='file:///x' credentials={'aws_secret_key'='CAN''ARY'}`,
			`create stage s url='file:///x' credentials={'aws_secret_key'='******'}`,
		},
		// unquoted numeric-looking value
		{
			`credentials={'aws_port'=123456789}`,
			`credentials={'aws_port'=******}`,
		},
	}
	for _, c := range cases {
		got := MaskSensitiveSqlLiterals(c.sql)
		require.Equal(t, c.want, got, c.sql)
		require.NotContains(t, got, "CAN", c.sql)
	}
}

func TestMaskStageCredentialLiteralsWithInitialState(t *testing.T) {
	cases := []struct {
		sql  string
		in   CredentialMapState
		want string
	}{
		{
			`create stage s credentials={'aws_key'='AK1', "aws_secret"='SEC1'}`,
			CredentialStateNone,
			`create stage s credentials={'aws_key'='******', "aws_secret"='******'}`,
		},
		{ // mid-map tail: keyword context cut off
			`{'aws_secret_key'='SEC1',`,
			CredentialStateInMap,
			`{'aws_secret_key'='******',`,
		},
		{ // tail starts AT the value (pending-value state)
			`'SEC1'`,
			CredentialStateInMapAfterEq,
			`'******'`,
		},
		{ // unterminated tail starting at the value
			`'SEC1`,
			CredentialStateInMapAfterEq,
			`'******`,
		},
		{ // idempotent
			`credentials={'aws_key'='******'}`,
			CredentialStateNone,
			`credentials={'aws_key'='******'}`,
		},
		{ // out of scope for this masker: identified-by stays visible
			`create user u identified by 'pw1'`,
			CredentialStateNone,
			`create user u identified by 'pw1'`,
		},
		{ // out of scope: s3 access keys stay visible
			`url s3option{'access_key_id'='123', 'secret_access_key'='456'}`,
			CredentialStateNone,
			`url s3option{'access_key_id'='123', 'secret_access_key'='456'}`,
		},
		{ // JSON-style colon maps (invalid grammar, parse fails mid-map) scrub too (#30746)
			`create stage s credentials={'access_key_id':'AK1','secret_key':'ts'}`,
			CredentialStateNone,
			`create stage s credentials={'access_key_id':'******','secret_key':'******'}`,
		},
		{ // colon-map tail starting right AT the separator
			`:'AK1','secret_key':'ts'}`,
			CredentialStateInMapAfterEq,
			`:'******','secret_key':'******'}`,
		},
		{ // colon-map tail starting at a key: the key token is taken as the
			// pending value (diagnostic-only over-mask), later values scrubbed
			`'secret_key':'ts'}`,
			CredentialStateInMapAfterEq,
			`'******':'******'}`,
		},
	}
	for _, c := range cases {
		require.Equal(t, c.want, MaskStageCredentialLiteralsWithInitialState(c.sql, c.in), c.sql)
	}
}

// #30747 review: the scanner skips whitespace and comments before a token,
// so separator/value detection must use post-trivia positions; malformed
// maps (doubled separators, unquoted punctuated values) keep conservative
// masking instead of abandoning the sensitive context.
func TestMaskCredentialMapTriviaAndMalformedShapes(t *testing.T) {
	cases := []struct {
		sql  string
		want string
	}{
		// whitespace before a JSON-style separator
		{`credentials={'aws_key' : 'CANARY_ALPHA'}`, `credentials={'aws_key' : '******'}`},
		// comment before the separator
		{`credentials={'aws_key' /*c*/ : 'CANARY_ALPHA'}`, `credentials={'aws_key' /*c*/ : '******'}`},
		// comment before the value: quote delimiters survive, masking idempotent
		{`credentials={'aws_key'=/*a*/'CANARY_ALPHA'}`, `credentials={'aws_key'=/*a*/'******'}`},
		// doubled map separator keeps the pending map context
		{`CREDENTIALS = = {'aws_key'='CANARY_ALPHA'}`, `CREDENTIALS = = {'aws_key'='******'}`},
		// malformed unquoted value: the punctuated tail is secret too
		{`credentials={'aws_key'=prefix/CANARY_BETA+suffix}`, `credentials={'aws_key'=******}`},
	}
	for _, c := range cases {
		got := MaskSensitiveSqlLiterals(c.sql)
		require.Equal(t, c.want, got, c.sql)
		require.NotContains(t, got, "CANARY", c.sql)
		require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent: "+c.sql)
	}
}

// #30747 review: under NO_BACKSLASH_ESCAPES a backslash does not escape the
// closing quote; masking with the effective lexical mode must not scan across
// the first value's terminator into the second credential.
func TestMaskCredentialMapNoBackslashEscapes(t *testing.T) {
	sql := `create stage s credentials={'aws_key'='foo\','secret_key'='CANARY_BETA'}`
	got := MaskSensitiveSqlLiteralsWithSQLMode(sql, ParseSQLModeFlags("NO_BACKSLASH_ESCAPES"))
	require.NotContains(t, got, "CANARY")
	require.Contains(t, got, `'aws_key'='******'`)
	require.Contains(t, got, `'secret_key'='******'`)
	require.Equal(t, got, MaskSensitiveSqlLiteralsWithSQLMode(got, ParseSQLModeFlags("NO_BACKSLASH_ESCAPES")))
}

// #30747 review: PREPARE wraps its statement in one string literal, so an
// outer token scan cannot see credential text inside it; the masker descends
// into that literal at its owning boundary.
func TestMaskPreparedSqlLiterals(t *testing.T) {
	sql := `prepare p from "create stage s url='file:///x' credentials={'aws_key'='PREPARE_CANARY'}"`
	got := MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "PREPARE_CANARY")
	require.Contains(t, got, `prepare p from "create stage s url='file:///x' credentials={'aws_key'='******'}"`)
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

	// single-quoted prepare with an identified-by inside
	got = MaskSensitiveSqlLiterals(`prepare q from 'create user u2 identified by "PWSECRET"'`)
	require.NotContains(t, got, "PWSECRET")

	// an ordinary string literal that is not a PREPARE body stays untouched
	plain := `select "credentials={'aws_key'='VISIBLE_LITERAL'}"`
	require.Equal(t, plain, MaskSensitiveSqlLiterals(plain))

	// PREPARE over an unterminated literal: content still scrubbed
	got = MaskSensitiveSqlLiterals(`prepare p from "create stage s credentials={'aws_key'='OPEN_END`)
	require.NotContains(t, got, "OPEN_END")

	// stage-only narrowing descends too (near-echo prefetch path)
	got = MaskStageCredentialLiteralsWithInitialState(
		`prepare p from "create stage s url='file:///x' credentials={'aws_key'='PREPARE_CANARY'}"`,
		CredentialStateNone)
	require.NotContains(t, got, "PREPARE_CANARY")
}

// #30747 review: executable comments deliver their CONTENTS as tokens, so
// trivia resolution must descend into them; otherwise a credential wrapped
// in /*! ... */ survives in statement records and debug traces.
func TestMaskExecutableCommentSecrets(t *testing.T) {
	cases := []struct {
		sql    string
		canary string
	}{
		{`create user u identified by /*! 'EXEC_CANARY_A' */`, "EXEC_CANARY_A"},
		{`create stage s credentials={'aws_key'='AK1', 'aws_secret'=/*! 'EXEC_CANARY_B' */}`, "EXEC_CANARY_B"},
		{`create stage s credentials={'aws_key'='AK1', 'aws_secret'=/*!12345 'EXEC_CANARY_C' */}`, "EXEC_CANARY_C"},
		{`select access_key_id = /*! 'EXEC_CANARY_D' */`, "EXEC_CANARY_D"},
	}
	for _, c := range cases {
		got := MaskSensitiveSqlLiterals(c.sql)
		require.NotContains(t, got, c.canary, c.sql)
		require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent: "+c.sql)
	}
	// the comment wrapper and quote delimiters are preserved around the mask
	require.Contains(t,
		MaskSensitiveSqlLiterals(`create stage s credentials={'aws_key'=/*! 'EXEC_CANARY_E' */}`),
		`/*! '******' */`)
}

// #30747 review: a PREPARE body is SQL of its own — mask its DECODED payload
// (doubled quotes, backslash escapes, dollar-quoting) and rebuild a valid
// literal, instead of scanning the encoded source text.
func TestMaskPreparedSqlLiteralsEncodedBodies(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{"doubled quotes",
			`prepare p from 'create stage s url=''file:///x'' credentials={''aws_key''=''ENC_CANARY_A''}'`,
			`prepare p from 'create stage s url=''file:///x'' credentials={''aws_key''=''******''}'`},
		{"double-quoted body",
			`prepare p from "create stage s url='file:///x' credentials={'aws_key'='ENC_CANARY_B'}"`,
			`prepare p from "create stage s url='file:///x' credentials={'aws_key'='******'}"`},
		{"backslash-escaped body",
			`prepare p from 'create stage s url=\'file:///x\' credentials={\'aws_key\'=\'ENC_CANARY_C\'}'`,
			`prepare p from 'create stage s url=''file:///x'' credentials={''aws_key''=''******''}'`},
		{"dollar-quoted body",
			`prepare p from $pq$create stage s url='file:///x' credentials={'aws_key'='ENC_CANARY_D'}$pq$`,
			`prepare p from $pq$create stage s url='file:///x' credentials={'aws_key'='******'}$pq$`},
	}
	for _, c := range cases {
		got := MaskSensitiveSqlLiterals(c.sql)
		require.Equal(t, c.want, got, c.name)
		require.NotContains(t, got, "ENC_CANARY", c.name)
		require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent: "+c.name)
	}
	// the re-encoded literal must still parse as a prepare statement
	_, err := ParseOne(context.Background(),
		`prepare p from 'create stage s url=''file:///x'' credentials={''aws_key''=''******''}'`, 1)
	require.NoError(t, err)
}

// #30747 review: the COM_QUERY debug trace carries the whole multi-statement
// request; every statement-initial PREPARE must be descended, not only the
// leading one.
func TestMaskPreparedSqlLiteralsMultiStatement(t *testing.T) {
	sql := `select 1; prepare p from 'create stage s url=''file:///x'' credentials={''aws_key''=''MULTI_CANARY_A''}'; select 2`
	got := MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "MULTI_CANARY_A")
	require.Contains(t, got, `credentials={''aws_key''=''******''}'`)
	require.Contains(t, got, "select 2")

	// two prepare statements in one request: both descended
	sql = `prepare p1 from 'create stage s credentials={''aws_key''=''MULTI_CANARY_B''}'; prepare p2 from 'create stage s credentials={''aws_key''=''MULTI_CANARY_C''}'`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "MULTI_CANARY_B")
	require.NotContains(t, got, "MULTI_CANARY_C")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

	// a PREPARE keyword that is NOT at a statement start is left alone:
	// EXECUTE names the earlier prepared statement
	sql = `select 1; execute p1`
	require.Equal(t, sql, MaskSensitiveSqlLiterals(sql))
}

// #30747 review (nested PREPARE boundary): the FIRST statement inside a
// compound block or control-flow body sits at statement-initial position
// even though no ';' precedes it — re-arming statement start only at ';'
// left an interior PREPARE's payload opaque, so its credentials survived
// (review probe, verbatim: `begin prepare p from "..."; execute p; end`
// and the CREATE SQL TASK wrapper). Nested statement starts re-arm the
// descent without double-masking later statements of the same body.
func TestMaskPrepareAtNestedStatementBoundary(t *testing.T) {
	payload := `"create stage s url='file:///x' credentials={'aws_secret_key'='NESTED_REVIEW_CANARY'}"`
	maskedPayload := `"create stage s url='file:///x' credentials={'aws_secret_key'='******'}"`

	t.Run("first prepare inside begin end", func(t *testing.T) {
		sql := `begin prepare p from ` + payload + `; execute p; end`
		got := MaskSensitiveSqlLiterals(sql)
		require.NotContains(t, got, "NESTED_REVIEW_CANARY")
		require.Contains(t, got, `begin prepare p from `+maskedPayload)
		require.Contains(t, got, "; execute p; end")
		require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

		// per-statement walker keeps the same masking
		perStmt := MaskSensitiveSqlLiteralsPerStatement(sql, 0)
		require.NotContains(t, perStmt, "NESTED_REVIEW_CANARY")
		require.Contains(t, perStmt, maskedPayload)

		// task wrapper hits the same first-in-body position
		sql = `create task t as begin prepare p from ` + payload + `; execute p; end`
		got = MaskSensitiveSqlLiterals(sql)
		require.NotContains(t, got, "NESTED_REVIEW_CANARY")
		require.Contains(t, got, maskedPayload)
	})

	t.Run("nesting depth and case variants", func(t *testing.T) {
		// inner block's first prepare at depth 2
		sql := `begin Begin prepare p from ` + payload + `; end; end`
		got := MaskSensitiveSqlLiterals(sql)
		require.NotContains(t, got, "NESTED_REVIEW_CANARY")
		require.Contains(t, got, maskedPayload)
		require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")
	})

	t.Run("second prepare in one block masks once via the semicolon path", func(t *testing.T) {
		sql := `begin prepare p from ` + payload + `; prepare q from ` + payload + `; execute p; execute q; end`
		got := MaskSensitiveSqlLiterals(sql)
		require.NotContains(t, got, "NESTED_REVIEW_CANARY")
		require.Contains(t, got, `begin prepare p from `+maskedPayload)
		require.Contains(t, got, `; prepare q from `+maskedPayload)
		require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent, no double masking")
	})

	t.Run("control-flow body openers", func(t *testing.T) {
		for _, tc := range []struct{ name, sql string }{
			{"then", `begin if a then prepare p from ` + payload + `; end if; end`},
			{"else", `begin if a then select 1; else prepare p from ` + payload + `; end if; end`},
			{"while do", `begin while a do prepare p from ` + payload + `; end while; end`},
			{"repeat", `begin repeat prepare p from ` + payload + ` until b end repeat; end`},
			{"loop", `begin lp: loop prepare p from ` + payload + `; end loop; end`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got := MaskSensitiveSqlLiterals(tc.sql)
				require.NotContains(t, got, "NESTED_REVIEW_CANARY")
				require.Contains(t, got, maskedPayload)
				require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")
			})
		}
	})

	t.Run("lex error tail recovery", func(t *testing.T) {
		// #30747 (BVT lane): the empty quoted identifier ` `` ` poisons
		// the scanner into reporting the remainder as one LEX_ERROR tail;
		// without recovery the credential keyword inside stayed invisible
		// and `create account `` ADMIN_NAME 'admin' IDENTIFIED BY 'v'`
		// shipped its secret verbatim in records, diagnostics and the
		// client echo.
		poison := "create account `` ADMIN_NAME 'admin' IDENTIFIED BY '123456';"
		got := MaskSensitiveSqlLiterals(poison)
		require.Equal(t, "create account `` ADMIN_NAME 'admin' IDENTIFIED BY '******';", got)
		got = MaskSensitiveSqlLiteralsPerStatement(poison, 0)
		require.Equal(t, "create account `` ADMIN_NAME 'admin' IDENTIFIED BY '******';", got)
		require.NotEmpty(t, SensitiveLiteralSpansForDiagnostic(poison, 0), "gate span present")
		require.Equal(t, got, MaskSensitiveSqlLiteralsPerStatement(got, 0), "idempotent")

		// a credential map inside the same swallowed shape
		poison = "create account `` ADMIN_NAME 'a' credentials={'aws_key'='TAIL_POISON_CANARY'};"
		got = MaskSensitiveSqlLiterals(poison)
		require.NotContains(t, got, "TAIL_POISON_CANARY")
		require.Contains(t, got, "credentials={'aws_key'='******'}")
		require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

		// controls: empty quoted identifier alone is untouched; stacked
		// poisons still mask through the conservative union path
		require.Equal(t, "select ``;", MaskSensitiveSqlLiterals("select ``;"))
		require.Empty(t, SensitiveLiteralSpansForDiagnostic("select ``;", 0))
		require.NotContains(t,
			MaskSensitiveSqlLiteralsPerStatement("create account `` x `` ADMIN_NAME 'a' IDENTIFIED BY 'STACKED_CANARY';", 0),
			"STACKED_CANARY")
		for _, depth := range []int{3, 4, 16} {
			poison := "create account " + strings.Repeat("`` x ", depth) + "ADMIN_NAME 'a' IDENTIFIED BY 'RECOVERY_BUDGET_CANARY';"
			for _, mode := range []SQLModeFlags{0, SQLModeFlags(SQLModeNoBackslashEscapes), SQLModeFlags(SQLModeANSIQuotes | SQLModeNoBackslashEscapes)} {
				got := MaskSensitiveSqlLiteralsPerStatement(poison, mode)
				require.NotContains(t, got, "RECOVERY_BUDGET_CANARY", "depth=%d mode=%d", depth, mode)
				require.NotEmpty(t, SensitiveLiteralSpansForDiagnostic(poison, mode))
				require.Equal(t, got, MaskSensitiveSqlLiteralsPerStatement(got, mode), "idempotent")
			}
		}
	})

	t.Run("controls", func(t *testing.T) {
		// unwrapped PREPARE still masks exactly as before
		sql := `prepare p from "create stage s url='file:///x' credentials={'aws_key'='CTRL_CANARY'}"`
		got := MaskSensitiveSqlLiterals(sql)
		require.NotContains(t, got, "CTRL_CANARY")
		require.Contains(t, got, `credentials={'aws_key'='******'}`)

		// the r14 4-statement NBE batch keeps its 4 effective-mode spans
		batch := `SET sql_mode='NO_BACKSLASH_ESCAPES'; SELECT 'access_key_id'='foo\'; SET sql_mode=''; create stage s2 url="file:///x\"z" credentials={'k'='REVIEW_NEW_CANARY'}`
		require.Len(t, StatementSQLModes(batch, 0), 4)
		require.NotContains(t, MaskSensitiveSqlLiteralsPerStatement(batch, 0), "REVIEW_NEW_CANARY")
	})
}

// #30747 review: each malformed unquoted value run is consumed once; a long
// run of repeated a= segments must stay linear instead of re-scanning the
// suffix once per token. Assert the raw ranges, not scheduler-sensitive wall
// time; large-input scaling is covered by BenchmarkMaskMalformedValueRunScale.
func TestMaskUnquotedCredentialValueRunLinear(t *testing.T) {
	const prefix = `create stage s credentials={'aws_key'=`
	value := strings.Repeat("a=", 16) + `'LINEAR_CANARY'`
	for _, tc := range []struct{ name, tail, maskedTail string }{
		{"closing brace", `}; select 'VISIBLE'`, `}; select 'VISIBLE'`},
		{"next value", `,'secret_key'='NEXT_CANARY'}; select 'VISIBLE'`, `,'secret_key'='******'}; select 'VISIBLE'`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := prefix + value + tc.tail
			wantSpans := [][2]int{{len(prefix), len(prefix) + len(value)}}
			if next := strings.Index(sql, "NEXT_CANARY"); next >= 0 {
				wantSpans = append(wantSpans, [2]int{next, next + len("NEXT_CANARY")})
			}
			spans, edits, _ := collectCredentialMasks(sql, CredentialStateNone, true, 0, true)
			require.Equal(t, wantSpans, spans, "one original range per value, without overlapping suffix rescans")
			require.Empty(t, edits)
			got := MaskSensitiveSqlLiterals(sql)
			require.Equal(t, prefix+"******"+tc.maskedTail, got)
			require.NotContains(t, got, "LINEAR_CANARY")
			require.NotContains(t, got, "NEXT_CANARY")
			require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")
		})
	}
}

// #30747 review: parser reuse must not inherit the previous statement's
// credential-map context; an unrelated later statement would otherwise see
// its literals redacted by position.
func TestSetScannerResetsCredentialContext(t *testing.T) {
	lex := NewLexer(dialect.MYSQL, "create stage s credentials={'aws_key'='x'", 1)
	lex.credentialsState = 4
	lex.credentialsValuePos = 42
	lex.setScanner(NewScanner(dialect.MYSQL, "badquery where value = 'VISIBLE'"), 1, 0)
	require.Zero(t, lex.credentialsState)
	require.Zero(t, lex.credentialsValuePos)
}

// #30747 review round 4, finding 1: a prepare body can encode the very
// bytes that name the credential keyword. The descent must decode the
// literal with the scanner's OWN rules (authoritative payload), so
// newline-escape sequences, backslash-escaped letters and charset
// introducers cannot hide credentials from the inner scan, while the raw
// prefix and closing delimiter are carried over verbatim.
func TestMaskPreparedSqlLiteralsEdgeEncodings(t *testing.T) {
	// `\n` decodes to a newline: `credentials\n=` IS a credentials map.
	sql := `prepare p from 'create stage s credentials\n={''aws_key''=''EDGE_CANARY_A''}'`
	got := MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "EDGE_CANARY_A")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")
	_, err := ParseOne(context.Background(), got, 1)
	require.NoError(t, err, "masked prepare must re-parse")

	// `\e` decodes to `e`: `cred\entials` decodes to the keyword itself.
	sql = `prepare p from 'create stage s cred\entials={''aws_key''=''EDGE_CANARY_B''}'`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "EDGE_CANARY_B")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")
	_, err = ParseOne(context.Background(), got, 1)
	require.NoError(t, err, "masked prepare must re-parse")

	// a charset introducer prefixes the literal but is one token.
	sql = `prepare p from _utf8mb4'create stage s credentials={''aws_key''=''EDGE_CANARY_C''}'`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "EDGE_CANARY_C")
	require.Contains(t, got, "_utf8mb4", "the introducer prefix is preserved")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")
	_, err = ParseOne(context.Background(), got, 1)
	require.NoError(t, err, "masked prepare must re-parse")

	// unterminated prepare bodies: quote and dollar-quoted tails are still
	// credential-bearing payloads.
	sql = `prepare p from 'create stage s credentials={''aws_key''=''EDGE_CANARY_D`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "EDGE_CANARY_D")
	sql = `prepare p from $$create stage s credentials={'aws_key'='EDGE_CANARY_E`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "EDGE_CANARY_E")
}

// #30747 review round 4, finding 2: an executable comment WITH a version
// prefix before a quoted map value made the masker treat the value as
// unquoted (the version digits masqueraded as the value start, and masking
// stopped at an embedded comma); an `=` assignment INSIDE `/*! = */` before
// a comma-bearing quoted value did the same. The version prefix and the
// comment terminator are trivia, and a value-less close token must not
// consume the pending assignment.
func TestMaskExecutableCommentVersionPrefixValue(t *testing.T) {
	sql := `create stage s url='file:///x' credentials={'aws_key'=/*!12345 'prefix,CANARY_VERSION4' */}`
	got := MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "CANARY_VERSION4")
	require.Contains(t, got, "******")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

	sql = `create stage s credentials={'aws_key' /*! = */ 'prefix,CANARY_ASSIGN4' */`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "CANARY_ASSIGN4")
	require.Contains(t, got, "******")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")
}

// #30747 review round 4, finding 3: the quadratic guard covered only runs
// closed by a separator; unclosed runs at END-OF-INPUT and runs inside
// executable comments re-scanned the tail once per token. coveredUntil now
// consumes every byte of a run exactly once for those shapes too. A single
// original mask range proves the guard without a wall-clock threshold.
func TestMaskMalformedRunsAtEOFAndInCommentStayLinear(t *testing.T) {
	const prefix = `create stage s credentials={'aws_key'=`
	value := strings.Repeat("a=", 16) + "RUN_CANARY"
	for _, tc := range []struct {
		name, start, tail, maskedTail string
	}{
		{"EOF", prefix, "", ""},
		{"executable comment", prefix + "/*! ", " */}", "}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := tc.start + value + tc.tail
			end := len(sql) - len(tc.maskedTail)
			spans, edits, _ := collectCredentialMasks(sql, CredentialStateNone, true, 0, true)
			require.Equal(t, [][2]int{{len(tc.start), end}}, spans, "unquoted run produces exactly one full mask range")
			require.Empty(t, edits)
			got := MaskSensitiveSqlLiterals(sql)
			require.Equal(t, tc.start+"******"+tc.maskedTail, got)
			require.NotContains(t, got, "a=a=")
			require.NotContains(t, got, "RUN_CANARY")
			require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")
		})
	}
}

// #30747 review round 4, finding 4: the COM_QUERY debug trace receives the
// WHOLE multi-statement request, so a leading SET sql_mode must re-tune the
// lexical mode for the statements after it; masking everything under the
// request's INITIAL mode leaves backslash-bearing secrets visible.
func TestMaskPerStatementSQLMode(t *testing.T) {
	stage := ` create stage s credentials={'aws_key'='a\', url='STAGED_CANARY_A'}`

	// The wrong mode now conservatively masks the malformed map instead of
	// leaking its tail. Accurate mode tracking still matters: the valid map
	// must preserve its key structure rather than use that lossy fallback.
	wrongMode := maskSensitiveSqlLiteralsScoped(stage, CredentialStateNone, true, 0)
	require.NotContains(t, wrongMode, "STAGED_CANARY_A")
	require.NotEqual(t, wrongMode, maskSensitiveSqlLiteralsScoped(stage, CredentialStateNone, true,
		SQLModeFlags(SQLModeNoBackslashEscapes)))
	sql := `set sql_mode='NO_BACKSLASH_ESCAPES';` + stage
	got := MaskSensitiveSqlLiteralsPerStatement(sql, 0)
	require.NotContains(t, got, "STAGED_CANARY_A")
	require.Contains(t, got, "url='******'")

	// qualified assignment forms
	got = MaskSensitiveSqlLiteralsPerStatement(
		`SET @@session.sql_mode='NO_BACKSLASH_ESCAPES';`+stage, 0)
	require.NotContains(t, got, "STAGED_CANARY_A")

	// unevaluable values (expression, DEFAULT, a mode the session would
	// reject) fall back to conservative every-variant masking
	for _, prefix := range []string{
		`set sql_mode=concat('NO_BACKSLASH');`,
		`set sql_mode=DEFAULT;`,
		`set sql_mode='ORACLE';`,
	} {
		got = MaskSensitiveSqlLiteralsPerStatement(prefix+stage, 0)
		require.NotContains(t, got, "STAGED_CANARY_A", "prefix %q", prefix)
	}

	// the INITIAL mode still applies before the transition
	got = MaskSensitiveSqlLiteralsPerStatement(
		stage+`; set sql_mode='NO_BACKSLASH_ESCAPES'`,
		SQLModeFlags(SQLModeNoBackslashEscapes))
	require.NotContains(t, got, "STAGED_CANARY_A")

	// DML SET-clauses are not mode switches
	sql = `update t set sql_mode='x';` + stage[1:]
	got = MaskSensitiveSqlLiteralsPerStatement(sql, SQLModeFlags(SQLModeNoBackslashEscapes))
	require.Contains(t, got, "sql_mode='x'")
	require.NotContains(t, got, "STAGED_CANARY_A")

	// fast paths stay equivalent to the single-pass scoped form
	for _, s := range []string{
		`select 1`,
		`select 'sql_mode'`,
		`select 1; select 'no mode word'`,
		`create stage s credentials={'k'='v1'}`,
		`prepare p from 'create stage s credentials={''k''=''v2''}'`,
	} {
		require.Equal(t, maskSensitiveSqlLiteralsScoped(s, CredentialStateNone, true, 0),
			MaskSensitiveSqlLiteralsPerStatement(s, 0), "no-transition equivalence: %q", s)
	}

	// idempotent on a transitioned request
	got = MaskSensitiveSqlLiteralsPerStatement(`set sql_mode='NO_BACKSLASH_ESCAPES';`+stage, 0)
	require.Equal(t, got, MaskSensitiveSqlLiteralsPerStatement(got, 0), "idempotent")
}

// #30747 review round 5, finding 1: the byte-level prefilter must admit
// every occurrence the TOKEN rules can arm — executable-comment contents
// reach the scanner as tokens (comment `identified by` / `credentials={...}`
// must not be skipped as ordinary comment text), and the access-key rule
// matches the DECODED string value (`'access_k\ey_id'` arms the rule, so the
// prefilter's in-body check must be backslash-tolerant). Both shapes were
// masked before the prefilter landed; this restores parity.
func TestMaskPrefilterExecCommentAndDecodedKeyAdmission(t *testing.T) {
	sql := `create user u /*! identified by 'R5_PASSWORD_CANARY' */`
	got := MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R5_PASSWORD_CANARY")
	require.Contains(t, got, `/*! identified by '******' */`)
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

	sql = `create stage s url='file:///x' /*! credentials={'aws_key'='R5_COMMENT_CANARY'} */`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R5_COMMENT_CANARY")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

	// decoded key names: the escaped spelling decodes to the access-key name
	sql = `select 'access_k\ey_id'='R5_ENCODED_CANARY_A'`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R5_ENCODED_CANARY_A")
	sql = `select 'secret_ac\cess_key'='R5_ENCODED_CANARY_B'`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R5_ENCODED_CANARY_B")

	// under NO_BACKSLASH_ESCAPES the same raw text does NOT decode to the
	// key name: the rule does not arm and the prefilter must agree
	for _, s := range []string{
		`select 'access_k\ey_id'='VISIBLE_NBE'`,
		`select 'secret_ac\cess_key'='VISIBLE_NBE'`,
	} {
		require.Equal(t, s,
			MaskSensitiveSqlLiteralsWithSQLMode(s, SQLModeFlags(SQLModeNoBackslashEscapes)),
			"no decoded key under NBE")
	}
}

// #30747 review round 5, finding 2: a charset introducer prefixes the
// literal of a credential value; the first raw byte (`_`) is not a quote, so
// the value must be classified by the token type, and the span must keep the
// introducer AND the quotes while masking the content. An unquoted run that
// classifies by raw byte stops at a comma INSIDE the literal and leaks the
// suffix.
func TestMaskIntroducerPrefixedCredentialValue(t *testing.T) {
	sql := `create stage s credentials={'aws_key'=_utf8mb4'prefix,R5_INTRO_CANARY'}`
	got := MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R5_INTRO_CANARY")
	require.Contains(t, got, "_utf8mb4'******'")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

	sql = `create user u identified by _utf8mb4'pw,R5_INTRO_USER'`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R5_INTRO_USER")
	require.Contains(t, got, "_utf8mb4'******'")

	sql = `select access_key_id=_utf8mb4'x,R5_INTRO_KEY'`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R5_INTRO_KEY")
	require.Contains(t, got, "_utf8mb4'******'")

	// unterminated introducer literal: content through end-of-input
	sql = `create stage s credentials={'aws_key'=_utf8mb4'prefix,R5_INTRO_OPEN`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R5_INTRO_OPEN")
}

// #30747 review round 5, finding 3: only SESSION-scope sql_mode assignments
// retune the current session's lexing — a user variable (`@sql_mode`),
// GLOBAL/PERSIST scope, and the @@global form must NOT trigger a transition,
// while the `:=` operator assigns exactly like `=`.
func TestMaskPerStatementSQLModeScope(t *testing.T) {
	nbe := SQLModeFlags(SQLModeNoBackslashEscapes)
	stage := ` create stage s credentials={'aws_key'='foo\','secret_key'='R5_SCOPE_CANARY'}`

	// session-untouched forms: initial NBE keeps applying, and the whole
	// statement masks cleanly under it
	for _, prefix := range []string{
		`set @sql_mode='';`,
		`set global sql_mode='';`,
		`set persist sql_mode='';`,
		`set @@global.sql_mode='';`,
	} {
		got := MaskSensitiveSqlLiteralsPerStatement(prefix+stage, nbe)
		require.NotContains(t, got, "R5_SCOPE_CANARY", "prefix %q", prefix)
		require.Contains(t, got, `'aws_key'='******','secret_key'='******'`,
			"NBE-shape masking preserved for %q", prefix)
	}

	// session forms DO transition (mode cleared to default)
	for _, prefix := range []string{
		`set sql_mode='';`,
		`set session sql_mode='';`,
		`set @@session.sql_mode='';`,
		`set @@sql_mode='';`,
	} {
		got := MaskSensitiveSqlLiteralsPerStatement(prefix+stage, nbe)
		require.Contains(t, got, `'aws_key'='******'secret_key'`,
			"transition applied for %q", prefix)
	}

	// := is accepted by the parser and must be honored by inference
	got := MaskSensitiveSqlLiteralsPerStatement(
		`set sql_mode:='NO_BACKSLASH_ESCAPES'; create stage s credentials={'aws_key'='a\', url='R5_ASSIGN_CANARY'}`, 0)
	require.NotContains(t, got, "R5_ASSIGN_CANARY")
	require.Contains(t, got, "url='******'", ":= transition must re-lex the next statement")
}

// #30747 review round 5, finding 4: after an unevaluable sql_mode
// assignment the effective mode is unknown; then even `;` SPLITTING under
// the stale mode is unsound — a semicolon inside a credential literal fakes
// a boundary and the second piece loses the map context entirely. The
// remainder of the request stays whole and is masked under every lexical
// variant of the last known mode.
func TestMaskUnknownModeTransitionKeepsRemainderWhole(t *testing.T) {
	// reviewer repro
	sql := `set sql_mode=concat('NO_BACKSLASH','_ESCAPES'); create stage s url='x' credentials={'aws_key'='foo\','secret_key'='prefix;R5_UNKNOWN_CANARY'}`
	got := MaskSensitiveSqlLiteralsPerStatement(sql, SQLModeFlags(SQLModeNoBackslashEscapes))
	require.NotContains(t, got, "R5_UNKNOWN_CANARY")

	// adversarial: under the last-known mode the credential's semicolon IS
	// a top-level boundary; an unsplit read keeps the map context and masks
	// both values
	sql = `set sql_mode=concat('A'); create stage s credentials={'aws_key'='foo';'bar','secret_key'='prefix;R5_SPLIT_CANARY'}`
	got = MaskSensitiveSqlLiteralsPerStatement(sql, 0)
	require.NotContains(t, got, "R5_SPLIT_CANARY")
}

// #30747 review round 6, finding 1: an ESCAPED INITIAL letter of an S3
// option name decodes to the recognized credential key (`'\access_key_id'`
// → access_key_id); the access-key rule matches the decoded value and masks,
// so the prefilter's in-body check must consult the decoded name starting at
// the very first body byte — otherwise the statement bypasses masking
// COMPLETELY (base implementation masked both; parity restored here).
func TestMaskPrefilterEscapedInitialAccessKey(t *testing.T) {
	sql := `select '\access_key_id'='R6_ESCAPED_A'`
	got := MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R6_ESCAPED_A")
	require.Contains(t, got, `'\access_key_id'='******'`)

	sql = `select '\secret_access_key'='R6_ESCAPED_S'`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R6_ESCAPED_S")
	require.Contains(t, got, `'\secret_access_key'='******'`)

	// inside a credential map too
	sql = `create stage s credentials={'\access_key_id'='R6_ESCAPED_M'}`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R6_ESCAPED_M")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

	// NBE parity: `\a` stays two literal bytes, so the decoded value is not
	// the key name and nothing arms
	require.Equal(t, `select '\access_key_id'='VISIBLE_NBE'`,
		MaskSensitiveSqlLiteralsWithSQLMode(`select '\access_key_id'='VISIBLE_NBE'`,
			SQLModeFlags(SQLModeNoBackslashEscapes)))
}

// #30747 review round 6, finding 2: under ANSI_QUOTES a double quote is an
// IDENTIFIER quote with NO backslash escapes — `"s\"` ends right after the
// backslash. Treating that closing quote as escape-swallowed makes the
// prefilter skip the entire remainder (including the credentials clause) and
// return the original SQL; the secret survives recording and debug masking.
func TestMaskPrefilterAnsiQuotesIdentifierBackslash(t *testing.T) {
	ansi := SQLModeFlags(SQLModeANSIQuotes)
	sql := `create stage "s\" url='file:///x' credentials={'k'='R6_ANSI_CANARY'}`
	got := MaskSensitiveSqlLiteralsWithSQLMode(sql, ansi)
	require.NotContains(t, got, "R6_ANSI_CANARY")
	require.Contains(t, got, `'k'='******'`)
	require.Equal(t, got, MaskSensitiveSqlLiteralsWithSQLMode(got, ansi), "idempotent")

	// the same identifier shape must not swallow the clause in map scans
	sql = `create stage "s\" url='u' credentials={'access_key_id'='R6_ANSI_MAP', 'r'='R6_ANSI_MAP2'}`
	got = MaskSensitiveSqlLiteralsWithSQLMode(sql, ansi)
	require.NotContains(t, got, "R6_ANSI_MAP")
	require.NotContains(t, got, "R6_ANSI_MAP2")

	// consistent no-op under the default lexical mode: there the whole tail
	// IS one unterminated double-quoted string — scanner and prefilter agree
	require.Equal(t,
		`create stage "s\" url='file:///x' credentials={'k'='VISIBLE_MODE0'}`,
		MaskSensitiveSqlLiterals(`create stage "s\" url='file:///x' credentials={'k'='VISIBLE_MODE0'}`))
}

// #30747 review round 7, finding 1: a HEX literal spelling of an access-key
// name decodes to the recognized key (`X'6163636573735f6b65795f6964'` →
// access_key_id), and introducer-prefixed forms decode the same way; the
// rule must arm on the HEXNUM token's decoded bytes and the prefilter must
// admit such literals, otherwise the statement bypasses masking COMPLETELY
// (base masked both; parity restored here).
func TestMaskHexEncodedAccessKey(t *testing.T) {
	// X'6163636573735f6b65795f6964' == "access_key_id"
	sql := `select X'6163636573735f6b65795f6964'='R7_HEX_A'`
	got := MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R7_HEX_A")
	require.Contains(t, got, "******")

	// X'7365637265745f6163636573735f6b6579' == "secret_access_key",
	// introducer-prefixed
	sql = `select _utf8mb4 X'7365637265745f6163636573735f6b6579'='R7_HEX_S'`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R7_HEX_S")

	// inside a credential map
	sql = `create stage s credentials={X'6163636573735f6b65795f6964'='R7_HEX_M'}`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R7_HEX_M")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

	// non-key decodes stay untouched
	sql = `select X'6162' from t`
	require.Equal(t, sql, MaskSensitiveSqlLiterals(sql))
	// odd/invalid hex never crashes nor arms
	sql = `select X'61626' from t`
	require.Equal(t, sql, MaskSensitiveSqlLiterals(sql))
}

// #30747 review round 7, finding 2: a QUALIFIED user variable is still a
// user variable — `@session.sql_mode` / `@local.sql_mode` scan as AT_ID
// (System=false) and never retune the session, and a quoted string that
// contains the name is an assignment VALUE, not a target; only genuine
// system-variable targets transition (isSessionSQLModeSet semantics).
func TestMaskPerStatementSQLModeSQLModeUserVarTargets(t *testing.T) {
	nbe := SQLModeFlags(SQLModeNoBackslashEscapes)
	stage := ` create stage s credentials={'aws_key'='foo\','secret_key'='R7_SCOPE_CANARY'}`

	for _, prefix := range []string{
		`set @session.sql_mode='';`,
		`set @local.sql_mode='';`,
		`set @x='sql_mode'='';`,
		`set @x:='sql_mode';`,
	} {
		got := MaskSensitiveSqlLiteralsPerStatement(prefix+stage, nbe)
		require.NotContains(t, got, "R7_SCOPE_CANARY", "prefix %q", prefix)
		require.Contains(t, got, `'aws_key'='******','secret_key'='******'`,
			"session mode untouched for %q", prefix)
	}

	// genuine system targets still transition (mode cleared → mode-0 lexing)
	for _, prefix := range []string{
		`set @@sql_mode='';`,
		`set @@session.sql_mode='';`,
		`set @@local.sql_mode='';`,
		`set sql_mode='';`,
		`set session sql_mode='';`,
		`set local sql_mode='';`,
	} {
		got := MaskSensitiveSqlLiteralsPerStatement(prefix+stage, nbe)
		require.Contains(t, got, `'aws_key'='******'secret_key'`,
			"transition applied for %q", prefix)
	}
}

func TestMaskPerStatementSQLModeAssignmentTargets(t *testing.T) {
	stage := ` create stage s url='file:///x' credentials={'k'='foo\','secret_key'='SCOPE_COMPARISON_CANARY'}`
	for _, test := range []struct {
		name, prefix string
		transition   bool
	}{
		{"session operand", `set @x=@@session.sql_mode='';`, false},
		{"unqualified operand", `set @x=@@sql_mode='';`, false},
		{"local operand", `set @x=@@local.sql_mode='';`, false},
		{"colon assignment", `set @x:=@@session.sql_mode='';`, false},
		{"nested comma", `set @x=if(1,@@session.sql_mode='',0);`, false},
		{"nested coalesce", `set @x=coalesce(NULL,@@local.sql_mode='');`, false},
		{"later user assignment", `set @x=1,@y=@@sql_mode='';`, false},
		{"real target after nested comma", `set @x=if(1,@@sql_mode='',0),sql_mode='';`, true},
		{"real target before operand", `set sql_mode='',@x=@@session.sql_mode='';`, true},
		{"quoted real target", "set `sql_mode`='';", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseOneWithSQLMode(t.Context(), test.prefix, 1, "NO_BACKSLASH_ESCAPES")
			require.NoError(t, err)
			defer stmt.Free()
			set, ok := stmt.(*tree.SetVar)
			require.True(t, ok)
			actualTransition := false
			for _, assignment := range set.Assignments {
				actualTransition = actualTransition || assignment.System && !assignment.Global && strings.EqualFold(assignment.Name, "sql_mode")
			}
			require.Equal(t, test.transition, actualTransition, "independent parser assignment oracle")
			mode := SQLModeFlags(SQLModeNoBackslashEscapes)
			if actualTransition {
				mode = 0
			}
			want := test.prefix + MaskSensitiveSqlLiteralsWithSQLMode(stage, mode)
			got := MaskSensitiveSqlLiteralsPerStatement(test.prefix+stage, SQLModeFlags(SQLModeNoBackslashEscapes))
			require.Equal(t, want, got)
			require.NotContains(t, got, "SCOPE_COMPARISON_CANARY")
			require.Equal(t, got, MaskSensitiveSqlLiteralsPerStatement(got, SQLModeFlags(SQLModeNoBackslashEscapes)))
		})
	}
	// A later literal must not turn an earlier unevaluable assignment into a
	// known transition: that earlier assignment can fail before it is applied.
	for _, prefix := range []string{
		`set sql_mode=concat('NO_BACKSLASH','_ESCAPES'),sql_mode='';`,
		`set sql_mode=concat('NO_BACKSLASH','_ESCAPES'),@x=if(1,@@sql_mode='',0);`,
	} {
		got := MaskSensitiveSqlLiteralsPerStatement(prefix+stage, SQLModeFlags(SQLModeNoBackslashEscapes))
		require.NotContains(t, got, "SCOPE_COMPARISON_CANARY")
	}
}

func TestMaskPerStatementSQLModeInertData(t *testing.T) {
	for _, value := range []string{"ordinary note", "sql_mode note", "sql_mode settings", "SET sql_mode note"} {
		sql := "insert into docs(id,note) values " + strings.TrimSuffix(strings.Repeat("(1,'"+value+"'),", 1000), ",") + ";"
		t.Run(value, func(t *testing.T) {
			require.Equal(t, sql, MaskSensitiveSqlLiteralsPerStatement(sql, 0))
			require.Zero(t, testing.AllocsPerRun(10, func() {
				MaskSensitiveSqlLiteralsPerStatement(sql, 0)
			}), "inert data must not retain a request-wide token inventory")
		})
	}
	for _, sql := range []string{
		`/*! set sql_mode='NO_BACKSLASH_ESCAPES' */; create stage s credentials={'k'='foo\','secret_key'='EXEC_SET_CANARY'}`,
		`select 'SET sql_mode'; set sql_mode='NO_BACKSLASH_ESCAPES'; create stage s credentials={'k'='foo\','secret_key'='EXEC_SET_CANARY'}`,
	} {
		require.NotContains(t, MaskSensitiveSqlLiteralsPerStatement(sql, 0), "EXEC_SET_CANARY", "real SET statements still reach mode inference")
	}
}

func TestMaskSensitiveMalformedQuotedMap(t *testing.T) {
	const stage = `create stage s url='file:///x' credentials={'k'='foo\','secret_key'='MALFORMED_QUOTE_CANARY'}`
	sql := stage
	got := MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "MALFORMED_QUOTE_CANARY")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got))
	// The parser's stage-only echo policy remains separate from record
	// masking, even when a broken map requires conservative span union.
	sql = `create user u identified by 'VISIBLE_PASSWORD'; ` + sql
	got = MaskStageCredentialLiteralsWithInitialState(sql, CredentialStateNone)
	require.NotContains(t, got, "MALFORMED_QUOTE_CANARY")
	require.Contains(t, got, "VISIBLE_PASSWORD")
	require.Equal(t, got, MaskStageCredentialLiteralsWithInitialState(got, CredentialStateNone))
	fragment := `'foo\','secret_key'='MALFORMED_QUOTE_CANARY'}`
	got = MaskStageCredentialLiteralsWithInitialState(fragment, CredentialStateInMapAfterEq)
	require.NotContains(t, got, "MALFORMED_QUOTE_CANARY", "a diagnostic tail must retain its initial map context in fallback")
	// Each level needs conservative map masking, but must reuse its already
	// redacted PREPARE child rather than recursively expand four variants.
	// Use escaped ordinary literals: the scanner does not support a dollar
	// delimiter inside another dollar-quoted literal, even with distinct tags.
	nested := stage
	for depth := 0; depth < 8; depth++ {
		encoded := strings.ReplaceAll(strings.ReplaceAll(nested, `\`, `\\`), `'`, `''`)
		nested = "prepare p from '" + encoded + "'; " + stage
	}
	_, edits, malformed := collectCredentialMasks(nested, CredentialStateNone, true, 0, true)
	require.True(t, malformed)
	require.NotEmpty(t, edits, "normal collection must descend into PREPARE")
	_, edits, malformed = collectCredentialMasks(nested, CredentialStateNone, true, 0, false)
	require.True(t, malformed)
	require.Empty(t, edits, "fallback collection must not re-expand an already masked subtree")
	got = MaskSensitiveSqlLiterals(nested)
	require.NotContains(t, got, "MALFORMED_QUOTE_CANARY")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got))
}

// #30747 review round 9, F2: a malformed credential map can be built from
// individually valid tokens — the closing brace lands INSIDE a mispaired
// string (`('foo\','secret_key'='X')} -- '`), so no LEX_ERROR ever fires and
// the old fallback never ran. The scan ending INSIDE the credential state
// machine (map never structurally closed) is itself the taint signal and
// must trigger the conservative union fallback over the whole fragment.
func TestMaskMalformedUnclosedMapTaintsStatement(t *testing.T) {
	sql := `create stage s url='file:///x' credentials={'k'='foo\','secret_key'='R9_MALFORMED_CANARY'} -- '`
	got := MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R9_MALFORMED_CANARY")
	require.Contains(t, got, "******")
	require.Equal(t, got, MaskSensitiveSqlLiterals(got), "idempotent")

	// variant: brace inside a dashed-comment string tail
	sql = `create stage s url='file:///x' credentials={'k'='foo\','secret_key'='R9_TAINT2'} # nonsense`
	got = MaskSensitiveSqlLiterals(sql)
	require.NotContains(t, got, "R9_TAINT2")

	// no taint for structurally complete maps + trailing comment: `}` is a
	// real token there, so the credential state resolves cleanly (the value
	// itself is always masked by the normal map rule)
	sql = `create stage s url='file:///x' credentials={'k'='v'} -- trailing note`
	got = MaskSensitiveSqlLiterals(sql)
	require.Contains(t, got, "-- trailing note")
	require.Contains(t, got, `'k'='******'}`)
}

// #30747 review round 9, F3: after an UNEVALUABLE sql_mode assignment the
// tail is processed per statement under conservative masking, and a LATER
// evaluable transition re-pins the effective mode — a constant-mode union
// over the remainder cannot see that re-pinning and leaks the third
// statement's credential.
func TestMaskUnknownTailTransitionRePins(t *testing.T) {
	sql := `set sql_mode=concat('NO_BACKSLASH','_ESCAPES'); ` +
		`create stage s1 url='file:///x' credentials={'k'='foo\','k2'='first'}; ` +
		`set sql_mode=''; ` +
		`create stage s2 url='file:///x' credentials={'k'='prefix\'R9_TRACE_TRANSITION_CANARY'}`
	got := MaskSensitiveSqlLiteralsPerStatement(sql, 0)
	require.NotContains(t, got, "R9_TRACE_TRANSITION_CANARY")
	require.Contains(t, got, `credentials={'k'='******'}`, "re-pinned mode-0 masking for s2")
}

// #30747 review round 10, P2 (the review's TestReviewUnknownModeBraceBoundary
// probe, verbatim): without a known effective mode, keep conservative
// boundaries outside every variant's credential interval. Known-mode spans
// instead preserve real boundaries and inherit crossing redaction context.
func TestMaskUnknownModeBraceBoundaryPreservesCredential(t *testing.T) {
	sql := `set sql_mode=concat('NO_BACKSLASH','_ESCAPES'); ` +
		`create stage s url='file:///x' credentials={'k'='foo\','secret_key'='prefix};REVIEW_BRACE_CANARY'}`
	got := MaskSensitiveSqlLiteralsPerStatement(sql, 0)
	require.NotContains(t, got, "REVIEW_BRACE_CANARY")
	require.Contains(t, got, "credentials=", "credential context must survive boundary selection")
	require.Contains(t, got, `secret_key`, "structural text outside spans is untouched")
	spans := StatementSQLModes(sql, 0)
	require.Len(t, spans, 2, "the ambiguous unknown tail stays conservative")
	require.True(t, spans[0].Known)
	require.False(t, spans[1].Known)
	require.False(t, spans[1].SemiTerminated)
	require.Equal(t, len(sql), spans[1].End)
	// With a known effective mode the same apparent terminator is allowed,
	// but its keyword-free suffix must inherit the original secret interval.
	known := strings.TrimPrefix(sql, `set sql_mode=concat('NO_BACKSLASH','_ESCAPES'); `)
	knownSpans := StatementSQLModes(known, 0)
	require.Len(t, knownSpans, 2)
	require.Equal(t, "******'}", knownSpans[1].MaskSensitiveLiterals(known))
	// A `;` genuinely OUTSIDE every variant's credential spans still
	// terminates: the credential map closes cleanly in all four modes, so
	// the boundary after it survives and the following statement is split.
	balanced := `set sql_mode=concat('NO_BACKSLASH','_ESCAPES'); ` +
		`create stage s url='file:///x' credentials={'aws_secret_key'='plain'}; select 1`
	balSpans := StatementSQLModes(balanced, 0)
	require.Len(t, balSpans, 3)
	require.True(t, balSpans[1].SemiTerminated)
	require.NotContains(t, MaskSensitiveSqlLiteralsPerStatement(balanced, 0), "plain")
	require.Contains(t, MaskSensitiveSqlLiteralsPerStatement(balanced, 0), "aws_secret_key")
}

// #30747 review round 12, P1: transition admission must tokenize trivia the
// way the scanner does. A leading-comment SET is still SET-initial — with
// an early exit on the first byte the `SET sql_mode='ANSI_QUOTES'` was
// invisible and the following stage was masked under the wrong mode.
func TestCommentTriviaModeTransitionAdmission(t *testing.T) {
	for _, prefix := range []string{
		"/* application */ ",
		"-- application\n",
		"# application\n",
		"/* x */ /* y */\n",
	} {
		sql := prefix + `SET sql_mode='ANSI_QUOTES'; create stage "s\" url='file:///x' credentials={'k'='REVIEW_COMMENT_CANARY'}`
		spans := StatementSQLModes(sql, 0)
		require.Len(t, spans, 2, prefix)
		require.True(t, spans[0].SemiTerminated, prefix)
		require.Equal(t, SQLModeFlags(SQLModeANSIQuotes), spans[1].Mode&SQLModeFlags(SQLModeANSIQuotes), prefix)
		require.True(t, spans[1].Known, prefix)
		out := MaskSensitiveSqlLiteralsPerStatement(sql, 0)
		require.NotContains(t, out, "REVIEW_COMMENT_CANARY", prefix)
		require.Contains(t, out, "credentials=", prefix)
	}
	// Executable comments deliver their CONTENT as tokens: a SET inside
	// /*! ... */ is itself the first statement and still re-pins the mode.
	exec := `/*! SET sql_mode='ANSI_QUOTES' */; create stage "s\" url='file:///x' credentials={'k'='REVIEW_COMMENT_CANARY'}`
	execSpans := StatementSQLModes(exec, 0)
	require.Len(t, execSpans, 2)
	require.True(t, execSpans[0].SemiTerminated)
	require.NotZero(t, execSpans[1].Mode&SQLModeFlags(SQLModeANSIQuotes), "SET inside an executable comment re-pins the mode")
	require.NotContains(t, MaskSensitiveSqlLiteralsPerStatement(exec, 0), "REVIEW_COMMENT_CANARY")
}

// #30747 review round 12, P1: variant cursors must synchronize with the
// statement walk, not reinterpret consumed prefix bytes. From an
// ANSI_QUOTES start the production parser consumes all five statements of
// this chain; the walker must produce the same shape, per-statement modes
// included. (The same chain read from default mode is mode-inconsistent —
// production fails at the very first alias — so no shape is asserted there.)
func TestStatementSQLModesKeepsEffectiveModeChain(t *testing.T) {
	sql := `SELECT 1 AS "a\"; SET sql_mode=''; SELECT 1; SET sql_mode='ANSI_QUOTES'; ` +
		`create stage "s\" url='file:///x' credentials={'k'='REVIEW_CURSOR_CANARY'}`
	spans := StatementSQLModes(sql, SQLModeFlags(SQLModeANSIQuotes))
	require.Len(t, spans, 5, "production consumes five statements; the walker must agree")
	require.True(t, spans[1].SemiTerminated)
	require.True(t, spans[2].SemiTerminated)
	require.True(t, spans[3].SemiTerminated)
	require.False(t, spans[4].SemiTerminated)
	require.True(t, spans[4].Known)
	require.NotZero(t, spans[4].Mode&SQLModeFlags(SQLModeANSIQuotes), "stage statement runs under the re-pinned ANSI_QUOTES")
	out := MaskSensitiveSqlLiteralsPerStatement(sql, SQLModeFlags(SQLModeANSIQuotes))
	require.NotContains(t, out, "REVIEW_CURSOR_CANARY")
	require.Contains(t, out, `credentials={'k'='******'}`)

	// An alternative-mode credential interval must not veto a real
	// boundary under the effective mode, especially a subsequent reset.
	t.Run("credential interval cannot hide reset", func(t *testing.T) {
		sql := `SET sql_mode='NO_BACKSLASH_ESCAPES'; SELECT 'access_key_id'='foo\'; SET sql_mode=''; create stage s2 url="file:///x\"z" credentials={'k'='REVIEW_NEW_CANARY'}`
		spans := StatementSQLModes(sql, 0)
		require.Len(t, spans, 4)
		for i, span := range spans {
			require.True(t, span.Known)
			require.Equal(t, i < 3, span.SemiTerminated)
		}
		require.Zero(t, spans[0].Mode)
		require.Equal(t, SQLModeFlags(SQLModeNoBackslashEscapes), spans[1].Mode)
		require.Equal(t, SQLModeFlags(SQLModeNoBackslashEscapes), spans[2].Mode)
		require.Zero(t, spans[3].Mode)
		masked := MaskSensitiveSqlLiteralsPerStatement(sql, 0)
		require.NotContains(t, masked, "REVIEW_NEW_CANARY")
		require.Equal(t, masked, MaskSensitiveSqlLiteralsPerStatement(masked, 0), "inherited redaction is idempotent")

		// The same interval cannot hide re-pinning after an unevaluable SET.
		unknown := `SET sql_mode=concat('',''); ` + sql
		unknownSpans := StatementSQLModes(unknown, 0)
		require.Len(t, unknownSpans, 5)
		require.False(t, unknownSpans[1].Known)
		require.True(t, unknownSpans[2].Known)
		require.Zero(t, unknownSpans[4].Mode)
		require.NotContains(t, MaskSensitiveSqlLiteralsPerStatement(unknown, 0), "REVIEW_NEW_CANARY")
	})
	t.Run("known mode discards speculative lookahead", func(t *testing.T) {
		sql := `SET sql_mode=concat('',''); SELECT 'foo\'; SET sql_mode=''; SET sql_mode='NO_BACKSLASH_ESCAPES'; create stage s credentials={'k'='foo\','secret_key'='LOOKAHEAD_CANARY'}`
		spans := StatementSQLModes(sql, 0)
		require.Len(t, spans, 5)
		require.True(t, spans[3].Known)
		require.Zero(t, spans[3].Mode)
		require.True(t, spans[4].Known)
		require.Equal(t, SQLModeFlags(SQLModeNoBackslashEscapes), spans[4].Mode)
		require.NotContains(t, MaskSensitiveSqlLiteralsPerStatement(sql, 0), "LOOKAHEAD_CANARY")
	})
}
