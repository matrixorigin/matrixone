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
	"testing"

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
	}
	for _, c := range cases {
		require.Equal(t, c.want, MaskStageCredentialLiteralsWithInitialState(c.sql, c.in), c.sql)
	}
}
