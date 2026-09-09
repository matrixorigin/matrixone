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

package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type frontendJSONMergeWarningSink struct {
	codes []uint16
	msgs  []string
}

func (s *frontendJSONMergeWarningSink) AppendWarningDiagnostic(code uint16, msg string) {
	s.codes = append(s.codes, code)
	s.msgs = append(s.msgs, msg)
}

func TestContainsJSONMergeCall(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want bool
	}{
		{name: "direct", sql: "select json_merge('[1]', '[2]')", want: true},
		{name: "case and whitespace", sql: "select JSON_MERGE\n /* gap */ ( '[1]', '[2]' )", want: true},
		{name: "line comment gap", sql: "select json_merge -- gap\n ('[1]', '[2]')", want: true},
		{name: "quoted literal", sql: "select 'json_merge(\"x\")'", want: false},
		{name: "quoted identifier", sql: "select `json_merge`", want: false},
		{name: "block comment", sql: "select /* json_merge('[1]','[2]') */ 1", want: false},
		{name: "line comment", sql: "select 1 -- json_merge('[1]','[2]')\n", want: false},
		{name: "longer identifier", sql: "select json_merge_patch('[1]', '[2]')", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, containsJSONMergeCall(tc.sql))
		})
	}
}

func TestBuildPlanWithPrepareModeReusesJSONMergeWarningLifecycle(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL,
		"select json_merge('{\"a\":1}', '{\"b\":2}')", 1)
	require.NoError(t, err)
	defer stmt.Free()

	sink := new(frontendJSONMergeWarningSink)
	compilerCtx := plan.NewEmptyCompilerContext()
	compilerCtx.SetContext(plan.WithJSONMergeWarningContext(
		ctx, sink, plan.JSONMergeWarningUser))

	_, err = buildPlanWithPrepareMode(ctx, nil, compilerCtx, stmt, false)
	require.NoError(t, err)
	_, err = buildPlanForCompileRetry(ctx, nil, compilerCtx, stmt, false, nil)
	require.NoError(t, err)

	require.Equal(t, []uint16{moerr.ER_WARN_DEPRECATED_SYNTAX}, sink.codes)
	require.Equal(t, []string{plan.JSONMergeDeprecatedWarning}, sink.msgs)
}

func TestBeginJSONMergeWarningStatementRecognizesAllPrepareForms(t *testing.T) {
	for _, tc := range []struct {
		name string
		stmt tree.Statement
	}{
		{name: "stmt", stmt: &tree.PrepareStmt{}},
		{name: "string", stmt: &tree.PrepareString{}},
		{name: "var", stmt: &tree.PrepareVar{}},
	} {
		execCtx := &ExecCtx{reqCtx: context.Background()}
		beginJSONMergeWarningStatement(&Session{}, execCtx, &UserInput{}, tc.stmt)
		_, ok := plan.JSONMergeWarningOriginFromContext(execCtx.reqCtx)
		require.True(t, ok, tc.name)
	}
}
