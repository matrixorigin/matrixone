// Copyright 2021 - 2026 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// queryResultMetaStub records saved-result metadata lookups so tests can
// assert the internal executor forwards them to the owning session compiler
// context instead of resolving them against the wrong identity.
type queryResultMetaStub struct {
	sqlplan.CompilerContext

	requested []string
	cols      []*pbplan.ColDef
	path      string
	err       error
}

func (s *queryResultMetaStub) GetQueryResultMeta(uuid string) ([]*pbplan.ColDef, string, error) {
	s.requested = append(s.requested, uuid)
	return s.cols, s.path, s.err
}

// An internal-executor query that reaches a saved-result source without a
// frontend session delegate must fail with a clean typed error, never panic.
func TestGetQueryResultMetaWithoutSessionDelegateFailsCleanly(t *testing.T) {
	c := &compilerContext{ctx: context.Background()}

	cols, path, err := c.GetQueryResultMeta("7f0e9d51-8bd9-4e0f-8d1e-3cddc1234567")
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal),
		"want typed internal error, got %v", err)
	require.Nil(t, cols)
	require.Empty(t, path)

	// The message must identify the missing session plumbing so operators can
	// tell an authorization/session-state gap from a missing result file.
	require.Contains(t, err.Error(), "session compiler context")
}

// With the owning frontend session compiler context installed (as CTAS
// populate does), a saved-result metadata lookup must be forwarded to that
// delegate verbatim, with its columns and result path passed through.
func TestGetQueryResultMetaDelegatesToSessionCompilerContext(t *testing.T) {
	delegate := &queryResultMetaStub{
		cols: []*pbplan.ColDef{{Name: "id"}},
		path: "result-path",
	}
	ctx := attachInternalExecutorCompilerContext(context.Background(), delegate)
	c := &compilerContext{ctx: ctx}

	cols, path, err := c.GetQueryResultMeta("qid-1")
	require.NoError(t, err)
	require.Equal(t, []string{"qid-1"}, delegate.requested)
	require.Len(t, cols, 1)
	require.Equal(t, "id", cols[0].Name)
	require.Equal(t, "result-path", path)
}

// View-description child contexts must forward saved-result metadata lookups
// to their isolated delegate as well.
func TestGetQueryResultMetaViewChildForwardsToViewDelegate(t *testing.T) {
	delegate := &queryResultMetaStub{
		cols: []*pbplan.ColDef{{Name: "v"}},
		path: "view-path",
	}
	c := &compilerContext{ctx: context.Background(), viewChild: true, viewDelegate: delegate}

	cols, path, err := c.GetQueryResultMeta("qid-2")
	require.NoError(t, err)
	require.Equal(t, []string{"qid-2"}, delegate.requested)
	require.Len(t, cols, 1)
	require.Equal(t, "view-path", path)
}

// A delegate error must be propagated verbatim - never wrapped or swallowed -
// so the frontend session's typed result-file errors reach the caller intact.
func TestGetQueryResultMetaDelegatesErrorVerbatim(t *testing.T) {
	sentinel := moerr.NewResultFileNotFound(context.Background(), "deadbeef-qid")
	delegate := &queryResultMetaStub{err: sentinel}
	ctx := attachInternalExecutorCompilerContext(context.Background(), delegate)
	c := &compilerContext{ctx: ctx}

	cols, path, err := c.GetQueryResultMeta("deadbeef-qid")
	require.ErrorIs(t, err, sentinel)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrResultFileNotFound))
	require.Nil(t, cols)
	require.Empty(t, path)
}
