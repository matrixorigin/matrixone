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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func mysqlStatementDigestTextProtocolTestScope(t *testing.T, c *Compile, name string) *Scope {
	t.Helper()
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{{
		Typ:  planpb.Type{Id: int32(types.T_varchar)},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}})
	require.NoError(t, err)
	op := projection.NewArgument()
	t.Cleanup(op.Release)
	op.ProjectList = []*planpb.Expr{expr}
	return &Scope{
		Magic:    Remote,
		Proc:     c.proc,
		NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"},
		RootOp:   op,
	}
}

func TestStatementDigestTextRejectsUnsupportedWorker(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	scope := mysqlStatementDigestTextProtocolTestScope(t, c, "statement_digest_text")
	client.version = defines.MORPCVersion93
	data, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
	require.Empty(t, data)
	require.Equal(t, 1, client.calls)
	require.Equal(t, client.calls, client.releases)
}

func TestStatementDigestTextRechecksAfterWorkerDowngrade(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	scope := mysqlStatementDigestTextProtocolTestScope(t, c, "statement_digest_text")
	client.version = defines.MORPCVersion94
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Equal(t, 1, client.calls)

	// A replacement worker must not inherit the previous capability result.
	client.version = defines.MORPCVersion93
	data, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination")
	require.Empty(t, data)
	require.Equal(t, 2, client.calls)
	require.Equal(t, client.calls, client.releases)
}

func TestStatementDigestDestinationDoesNotProbeOrdinaryExpression(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion93
	scope := mysqlStatementDigestTextProtocolTestScope(t, c, "lower")
	data, err := encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	require.Zero(t, client.calls)
}
