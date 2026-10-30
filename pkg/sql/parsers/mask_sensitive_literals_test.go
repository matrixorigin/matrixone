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

package parsers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// The masker keeps statement records, query traces and parse diagnostics free
// of credential literals. (#30747)
func TestMaskSensitiveLiteralsForLogging(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "create user identified by stays masked (historical rule)",
			in:   "create user u identified by 'pwn-secret'",
			want: "create user u identified by '******'",
		},
		{
			name: "create account identified with stays masked (historical rule)",
			in:   "create account nihao admin_name 'admin' identified with '123'",
			want: "create account nihao admin_name 'admin' identified with '******'",
		},
		{
			name: "s3 access key options stay masked (historical rule)",
			in:   "select * from t where access_key_id='AKIA123' and secret_access_key='hidden'",
			want: "select * from t where access_key_id='******' and secret_access_key='******'",
		},
		{
			name: "create stage credentials map fully masked, double-quoted keys",
			in:   "create stage s URL = 'file:///x' CREDENTIALS = {\"AWS_KEY_ID\"='AKIACANARYX9', \"AWS_SECRET_KEY\"='SECRETCANARYX9', \"AWS_REGION\"='us-east-1'}",
			want: "create stage s URL = 'file:///x' CREDENTIALS = {\"AWS_KEY_ID\"='******', \"AWS_SECRET_KEY\"='******', \"AWS_REGION\"='******'}",
		},
		{
			name: "alter stage credentials map masked, single-quoted keys",
			in:   "alter stage s set credentials={'aws_key'='a', 'aws_secret'='b'}",
			want: "alter stage s set credentials={'aws_key'='******', 'aws_secret'='******'}",
		},
		{
			name: "credentials map without space before brace",
			in:   "create stage bad credentials={'aws_secret_key'='XYZ',",
			want: "create stage bad credentials={'aws_secret_key'='******',",
		},
		{
			name: "unassigned credentials word is not consumed",
			in:   "select credentials from t",
			want: "select credentials from t",
		},
		{
			name: "credentials table alias left alone",
			in:   "select * from credentials t where access_key_id='z'",
			want: "select * from credentials t where access_key_id='******'",
		},
		{
			name: "stage statement without credentials unchanged",
			in:   "create stage s URL = 'file:///x'",
			want: "create stage s URL = 'file:///x'",
		},
		{
			name: "empty string",
			in:   "",
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MaskSensitiveLiteralsForLogging(tc.in)
			require.Equal(t, tc.want, got)
			// idempotent: masking masked text is a no-op
			require.Equal(t, got, MaskSensitiveLiteralsForLogging(got))
		})
	}
}

// A parse-error fragment captured mid-map carries no CREDENTIALS keyword; the
// dialect masker covers it through its initial-state variant. The serialized
// diagnostic is masked earlier still, at the lexer source. (#30747/#30746)
func TestMaskSensitiveLiteralsForLoggingInParseErrorEcho(t *testing.T) {
	in := `You have an error in your SQL syntax; ... near "creditials={'aws_secret_key'='******', more"`
	got := MaskSensitiveLiteralsForLogging(in)
	require.Equal(t, in, got, "already-masked diagnostics stay untouched")
}

// #30747 review: the record-text preprocessor (HandleSqlForRecord) must mask
// with the session's effective lexical mode. Under NO_BACKSLASH_ESCAPES the
// backslash is literal, so the first credential value terminates before the
// comma; with the default mode the same text is a single escaped string. The
// mode-less pass used to mangle the record and leave the second credential
// visible — an exact live-row shape reproduced here in both directions.
func TestHandleSqlForRecordWithSQLMode(t *testing.T) {
	in := "create stage s9 url='file:///x' credentials={'aws_key'='foo\\','secret_key'='R3ONLY_CANARY'};"

	parts := HandleSqlForRecordWithSQLMode(in, "NO_BACKSLASH_ESCAPES")
	require.Len(t, parts, 1)
	require.NotContains(t, parts[0], "R3ONLY_CANARY")
	require.Contains(t, parts[0], `'aws_key'='******'`)
	require.Contains(t, parts[0], `'secret_key'='******'`)

	// default mode keeps the historical single-string reading
	parts = HandleSqlForRecordWithSQLMode(in, "")
	require.NotContains(t, parts[0], "RATHER", "sanity: shape unchanged")
	require.NotContains(t, parts[0], "'foo\\'")

	// by-statement helper carries the mode down to the fragment pass
	records, err := HandleSqlForRecordByStatementWithSQLMode(context.Background(), in, "NO_BACKSLASH_ESCAPES")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.NotContains(t, records[0], "R3ONLY_CANARY")

	t.Run("cross-mode interval cannot hide reset", func(t *testing.T) {
		sql := `SET sql_mode='NO_BACKSLASH_ESCAPES'; SELECT 'access_key_id'='foo\'; SET sql_mode=''; create stage s2 url="file:///x\"z" credentials={'k'='REVIEW_NEW_CANARY'}`
		records, spans := HandleSqlForRecordSpansWithSQLMode(sql, "")
		require.Len(t, records, 4)
		require.Len(t, spans, len(records))
		for _, record := range records {
			require.NotContains(t, record, "REVIEW_NEW_CANARY")
		}
	})

	// #30747 review (nested PREPARE boundary): the first PREPARE inside a
	// compound `begin ... end` block — or a CREATE SQL TASK body — is
	// statement-initial for redaction even without a preceding ';'; its
	// payload must be descended into for records too.
	t.Run("first prepare inside compound block", func(t *testing.T) {
		sql := `begin prepare p from "create stage s url='file:///x' credentials={'aws_secret_key'='NESTED_RECORD_CANARY'}"; execute p; end`
		records, spans := HandleSqlForRecordSpansWithSQLMode(sql, "")
		require.Len(t, spans, len(records))
		for _, record := range records {
			require.NotContains(t, record, "NESTED_RECORD_CANARY")
		}
		require.Contains(t, records[0], `credentials={'aws_secret_key'='******'}`)

		// the by-statement helper masks the same nested payload
		byStmt, err := HandleSqlForRecordByStatementWithSQLMode(context.Background(), sql, "")
		require.NoError(t, err)
		require.NotEmpty(t, byStmt)
		for _, record := range byStmt {
			require.NotContains(t, record, "NESTED_RECORD_CANARY")
		}
	})
}
