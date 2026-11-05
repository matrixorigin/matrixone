// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestInternalExecutorInsertIDUsesCurrentPlanOwner(t *testing.T) {
	c := NewMockCompile(t)
	c.stmt = &tree.Insert{Rows: &tree.Select{Select: &tree.ValuesClause{}}, OnDuplicateUpdate: tree.UpdateExprs{&tree.UpdateExpr{}}}
	c.proc.SetLastInsertID(700)
	c.proc.SetStatementLastInsertIDIfEarlier(1)
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{
		NodeType: plan.Node_PRE_INSERT, PreInsertCtx: &plan.PreInsertCtx{},
	}}}}}
	require.Equal(t, uint64(1), c.statementLastInsertID(), "no-key legacy ODKU owns a statement ID")

	// A rebuilt tracked plan owns the summary even when it is empty. Never
	// fall back based on nonzero result facts or a stale legacy statement ID.
	c.pn.GetQuery().Nodes[0].PreInsertCtx.TrackOdkuResult = true
	require.Zero(t, c.statementLastInsertID())
	c.proc.RecordODKUAction(0, 90, true)
	require.Equal(t, uint64(90), c.statementLastInsertID())
	c.stmt.(*tree.Insert).Rows.Select = &tree.SelectClause{}
	require.Zero(t, c.statementLastInsertID(), "INSERT SELECT retains its own fallback rule")
	c.proc.RecordODKUGenerated(1, 3)
	require.Equal(t, uint64(3), c.statementLastInsertID())
	c.pn.GetQuery().Nodes[0].PreInsertCtx.TrackOdkuResult = false
	require.Equal(t, uint64(1), c.statementLastInsertID(), "an untracked final plan ignores stale summaries")
}

// A retry restores the LAST_INSERT_ID the statement started with, not the value
// captured when the COM_QUERY began: an earlier statement of the same COM_QUERY
// may have generated one since (#29903).
func TestRetryRestoresStatementStartLastInsertID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	_, txnOp := newTestTxnClientAndOp(ctrl)

	c := NewMockCompile(t)
	c.proc.Base.TxnOperator = txnOp
	c.proc.Base.SessionInfo.LastInsertID = 0 // the COM_QUERY began with no value
	c.proc.SetLastInsertID(7)                // an earlier statement generated 7
	c.lastInsertIDBaseline = c.proc.GetLastInsertID()

	// the failed attempt generated 42
	c.proc.SetLastInsertID(42)
	c.proc.SetStatementLastInsertID(42)

	var wait time.Duration
	require.NoError(t, c.prepareRetryTransition(&wait))
	require.Equal(t, uint64(7), c.proc.GetLastInsertID())
	require.Zero(t, c.proc.GetStatementLastInsertID())
}
