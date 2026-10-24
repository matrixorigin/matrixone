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
	got := MaskSensitiveSqlLiteralsWithInitialState(frag, true)
	require.NotContains(t, got, "XYZCANARY")
	require.Contains(t, got, "'aws_secret_key'='******'")
	require.Equal(t, " more text", got[len(got)-len(" more text"):])

	// without the map context the fragment is left alone
	got = MaskSensitiveSqlLiteralsWithInitialState(frag, false)
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

func TestMaskStageCredentialLiteralsWithInitialState(t *testing.T) {
	cases := []struct {
		sql  string
		in   bool
		want string
	}{
		{
			`create stage s credentials={'aws_key'='AK1', "aws_secret"='SEC1'}`,
			false,
			`create stage s credentials={'aws_key'='******', "aws_secret"='******'}`,
		},
		{ // mid-map tail: keyword context cut off
			`{'aws_secret_key'='SEC1',`,
			true,
			`{'aws_secret_key'='******',`,
		},
		{ // idempotent
			`credentials={'aws_key'='******'}`,
			false,
			`credentials={'aws_key'='******'}`,
		},
		{ // out of scope for this masker: identified-by stays visible
			`create user u identified by 'pw1'`,
			false,
			`create user u identified by 'pw1'`,
		},
		{ // out of scope: s3 access keys stay visible
			`url s3option{'access_key_id'='123', 'secret_access_key'='456'}`,
			false,
			`url s3option{'access_key_id'='123', 'secret_access_key'='456'}`,
		},
	}
	for _, c := range cases {
		require.Equal(t, c.want, MaskStageCredentialLiteralsWithInitialState(c.sql, c.in), c.sql)
	}
}
