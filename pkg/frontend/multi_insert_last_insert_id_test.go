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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// autoIncrPreInsertPlan builds a plan with one PRE_INSERT node per target,
// each target owning an ordinary AUTO_INCREMENT column.
func autoIncrPreInsertPlan(targets int) *plan.Plan {
	nodes := make([]*plan.Node, 0, targets)
	for i := 0; i < targets; i++ {
		nodes = append(nodes, &plan.Node{
			NodeType: plan.Node_PRE_INSERT,
			PreInsertCtx: &plan.PreInsertCtx{
				TableDef: &plan.TableDef{
					Cols: []*plan.ColDef{
						{Name: "id", Typ: plan.Type{AutoIncr: true}},
					},
				},
			},
		})
	}
	return &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: nodes}}}
}

func newLastInsertIDHarness(t *testing.T, targets int) (
	*Session, *process.Process, *countingMysqlWriter, *MysqlResp, *ExecCtx,
) {
	t.Helper()
	ses := &Session{
		seqLastValue:  new(string),
		feSessionImpl: feSessionImpl{txnHandler: &TxnHandler{}},
	}
	proc := &process.Process{Base: &process.BaseProcess{
		LastInsertID:          new(uint64),
		StatementLastInsertID: new(uint64),
	}}
	proc.InitSeq()
	writer := &countingMysqlWriter{
		testMysqlWriter: &testMysqlWriter{},
		responses:       make([]*Response, 0, 2),
	}
	execCtx := &ExecCtx{
		reqCtx:    context.Background(),
		stmt:      &tree.MultiInsert{},
		proc:      proc,
		runResult: &util.RunResult{AffectRows: 2},
		cw:        &TxnComputationWrapper{plan: autoIncrPreInsertPlan(targets)},
	}
	return ses, proc, writer, NewMysqlResp(writer), execCtx
}

// TestMultiInsertAmbiguousInsertIDLeavesNoProcessState is the regression for
// the leak: with several AUTO_INCREMENT targets the statement reports no
// insert id, and it must not leave the suppressed value on the process either.
// The process is reused for the next statement of the same COM_QUERY, which
// resets only the statement value, so a suppressed value left in LastInsertID
// would answer that statement's LAST_INSERT_ID().
func TestMultiInsertAmbiguousInsertIDLeavesNoProcessState(t *testing.T) {
	ses, proc, writer, resper, execCtx := newLastInsertIDHarness(t, 2)

	// what the session last made visible, before this statement
	ses.SetLastInsertID(51)
	proc.SetLastInsertID(51)

	// two targets publish through the shared statement coordinator; it keeps
	// the smallest non-zero value and writes the session-visible field too
	proc.SetStatementLastInsertIDIfEarlier(101)
	proc.SetStatementLastInsertIDIfEarlier(1)
	require.Equal(t, uint64(1), proc.GetStatementLastInsertID())
	require.Equal(t, uint64(1), proc.GetLastInsertID(), "the coordinator wrote the session-visible field")

	require.NoError(t, resper.respStatus(ses, execCtx))

	require.Len(t, writer.responses, 1)
	require.Zero(t, writer.responses[0].lastInsertId, "OK packet reports no insert id")
	require.Equal(t, uint64(51), ses.GetLastInsertID(), "session keeps its previous value")
	require.Equal(t, uint64(51), proc.GetLastInsertID(), "process must not keep the suppressed value")

	// the same-COM_QUERY control: doComQuery clears only the statement value
	// before the next statement, which then reads LAST_INSERT_ID() from the
	// process
	proc.SetStatementLastInsertID(0)
	require.Equal(t, uint64(51), proc.GetLastInsertID(),
		"the next statement of this COM_QUERY sees the session value, not the suppressed one")
}

// TestMultiInsertSingleAutoIncrTargetReportsInsertID: one target can generate,
// so the value has its single-insert meaning and reaches all three states.
func TestMultiInsertSingleAutoIncrTargetReportsInsertID(t *testing.T) {
	ses, proc, writer, resper, execCtx := newLastInsertIDHarness(t, 1)
	ses.SetLastInsertID(51)
	proc.SetLastInsertID(51)
	proc.SetStatementLastInsertIDIfEarlier(101)

	require.NoError(t, resper.respStatus(ses, execCtx))

	require.Len(t, writer.responses, 1)
	require.Equal(t, uint64(101), writer.responses[0].lastInsertId)
	require.Equal(t, uint64(101), ses.GetLastInsertID())
	require.Equal(t, uint64(101), proc.GetLastInsertID())
}

// TestMultiInsertNoAutoIncrTargetKeepsPreviousValue: no target generates
// anything, so every state keeps what the session already had.
func TestMultiInsertNoAutoIncrTargetKeepsPreviousValue(t *testing.T) {
	ses, proc, writer, resper, execCtx := newLastInsertIDHarness(t, 0)
	ses.SetLastInsertID(51)
	proc.SetLastInsertID(51)

	require.NoError(t, resper.respStatus(ses, execCtx))

	require.Len(t, writer.responses, 1)
	require.Zero(t, writer.responses[0].lastInsertId)
	require.Equal(t, uint64(51), ses.GetLastInsertID())
	require.Equal(t, uint64(51), proc.GetLastInsertID())
}
