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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

type jsonMergeWarningTestSink struct {
	codes    []uint16
	messages []string
}

func (s *jsonMergeWarningTestSink) AppendWarningDiagnostic(code uint16, msg string) {
	s.codes = append(s.codes, code)
	s.messages = append(s.messages, msg)
}

func buildJSONMergeWarningTestPlan(
	t *testing.T,
	ctx *MockCompilerContext,
	origin JSONMergeWarningOrigin,
	sink JSONMergeWarningSink,
) {
	stmt, err := mysql.ParseOne(context.Background(),
		"select json_merge('{\"a\":1}', '{\"b\":2}'), json_merge('[1]', '[2]')", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ctx.SetContext(WithJSONMergeWarningContext(context.Background(), sink, origin))
	_, err = BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
}

func TestJSONMergeWarningLifecycle(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	sink := new(jsonMergeWarningTestSink)

	buildJSONMergeWarningTestPlan(t, ctx, JSONMergeWarningUser, sink)
	require.Equal(t, []uint16{moerr.ER_WARN_DEPRECATED_SYNTAX, moerr.ER_WARN_DEPRECATED_SYNTAX}, sink.codes)
	require.Equal(t, []string{JSONMergeDeprecatedWarning, JSONMergeDeprecatedWarning}, sink.messages)

	// An internal EXECUTE reprepare rebuilds the AST and plan, but it must not
	// replay the warning emitted while the user first prepared the statement.
	buildJSONMergeWarningTestPlan(t, ctx, JSONMergeWarningInternalReprepare, sink)
	require.Len(t, sink.codes, 2)

	// A later user PREPARE starts a new warning lifecycle and warns again.
	buildJSONMergeWarningTestPlan(t, ctx, JSONMergeWarningUser, sink)
	require.Len(t, sink.codes, 4)

	// Expanding a stored view is an internal bind and must remain silent.
	buildJSONMergeWarningTestPlan(t, ctx, JSONMergeWarningStoredView, sink)
	require.Len(t, sink.codes, 4)
}

func TestJSONMergeWarningLifecycleDedupesReboundAST(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	sink := new(jsonMergeWarningTestSink)
	const sql = "select json_merge('{\"a\":1}', '{\"b\":2}'), json_merge('[1]', '[2]')"

	ctx.SetContext(WithJSONMergeWarningContext(
		context.Background(), sink, JSONMergeWarningUser))
	for i := 0; i < 2; i++ {
		if i > 0 {
			ctx.SetContext(AttachJSONMergeWarningContext(
				ctx.GetContext(), sink, JSONMergeWarningUser))
		}
		stmt, err := mysql.ParseOne(context.Background(), sql, 1)
		require.NoError(t, err)
		_, err = BuildPlan(ctx, stmt, false)
		stmt.Free()
		require.NoError(t, err)
	}

	require.Len(t, sink.codes, 2)
}

func TestJSONMergeWarningWithoutSinkIsSafe(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	buildJSONMergeWarningTestPlan(t, ctx, JSONMergeWarningUser, nil)
}
