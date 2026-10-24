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
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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

// #30747 review: each malformed unquoted value run is consumed once; a long
// run of repeated a= segments must stay linear instead of re-scanning the
// suffix once per token. (Bound is generous: the previous quadratic form
// needed ~seconds here, the linear form is sub-millisecond.)
func TestMaskUnquotedCredentialValueRunLinear(t *testing.T) {
	repeat := strings.Repeat("a=", 1<<18)
	sql := `create stage s credentials={'aws_key'=` + repeat + `'LINEAR_CANARY'}`
	start := time.Now()
	got := MaskSensitiveSqlLiterals(sql)
	elapsed := time.Since(start)
	require.NotContains(t, got, "LINEAR_CANARY")
	require.Less(t, elapsed, 2*time.Second, "malformed unquoted run must be consumed once")
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
