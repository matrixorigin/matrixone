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
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestLASTInsertIDBaselineUsesActualSerializedOwner(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	op := projection.NewArgument()
	defer op.Release()
	expr := &plan.Expr{Typ: plan.Type{Id: 28}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: int64(function.LAST_INSERT_ID) << 32}}}}
	op.ProjectList = []*plan.Expr{expr}
	actual := c.proc.NewNoContextChildProc(0)
	actual.Ctx = defines.AttachAccountId(context.Background(), 0)
	actual.Base.TxnOperator = fakeTxnOperator{}
	actual.Base.SessionInfo.TimeZone = time.UTC
	actual.SetResolveVariableFunc(nil)
	actual.Base.SessionInfo.LastInsertIDSet = false
	scope := &Scope{Magic: Remote, Proc: actual, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, baseline := range []uint64{0, 700} {
		actual.SetLastInsertID(baseline)
		_, _, data, _, err := prepareRemoteRunSendingData("", scope, c.proc, nil, uuid.Nil)
		require.NoError(t, err, "a real owned default0 is capturable without resolver/wire presence")
		info := new(pipeline.ProcessInfo)
		require.NoError(t, info.Unmarshal(data))
		require.True(t, info.SessionInfo.LastInsertIdSet)
		require.Equal(t, baseline, info.SessionInfo.LastInsertId)
	}
	// The capability/root Process must not lend an owner to the unrelated
	// Process actually serialized by prepareRemoteRunSendingData.
	other := NewMockCompile(t).proc
	other.Ctx = defines.AttachAccountId(context.Background(), 0)
	other.Base.LastInsertID = nil
	scope.Proc = other
	_, _, _, _, err := prepareRemoteRunSendingData("", scope, c.proc, nil, uuid.Nil)
	require.ErrorContains(t, err, "baseline owner")
	wire := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*plan.Expr{expr}}}}
	actual.Base.SessionInfo.LastInsertIDSet = false
	require.ErrorContains(t, validateRemoteExpressionPipeline(actual, wire), "explicit session baseline")
	actual.Base.SessionInfo.LastInsertIDSet = true
	actual.SetLastInsertID(0)
	require.NoError(t, validateRemoteExpressionPipeline(actual, wire))
}

// A decoded remote process owns a bounded ODKU range even for a SELECT.
// Ordinary child sends must not consume it or prevent a sibling from running.
func TestRemoteReadChildrenDoNotConsumeODKUOrdinals(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	c.proc.Ctx = defines.AttachAccountId(context.Background(), 0)
	c.proc.Base.TxnOperator = fakeTxnOperator{}
	c.proc.Base.SessionInfo.TimeZone = time.UTC
	require.NoError(t, c.proc.SetODKUInputOrdinalBase(0))
	op := projection.NewArgument()
	defer op.Release()
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for child := 0; child < 2; child++ {
		_, _, data, _, err := prepareRemoteRunSendingData("select", scope, c.proc, nil, uuid.Nil)
		require.NoError(t, err, "ordinary remote child %d must not exhaust ODKU range", child)
		info := new(pipeline.ProcessInfo)
		require.NoError(t, info.Unmarshal(data))
		require.Zero(t, info.OdkuOrdinalBase)
	}
	ordinal, err := c.proc.NextODKUInputOrdinal(1)
	require.NoError(t, err)
	require.Zero(t, ordinal, "ordinary sends do not advance the producer's input ordinal")
}

func TestRemoteODKUProducerReservationIncludesForwardedSubtrees(t *testing.T) {
	for _, shape := range []string{"root", "operator-child", "remote-descendant"} {
		t.Run(shape, func(t *testing.T) {
			c, _ := expressionProtocolTestCompile(t)
			c.proc.Ctx = defines.AttachAccountId(context.Background(), 0)
			c.proc.Base.TxnOperator = fakeTxnOperator{}
			c.proc.Base.SessionInfo.TimeZone = time.UTC
			require.NoError(t, c.proc.SetODKUInputOrdinalBase(0))
			producer := preinsert.NewArgument()
			defer producer.Release()
			producer.TrackODKUResult = true
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: producer}
			if shape != "root" {
				parent := projection.NewArgument()
				defer parent.Release()
				scope.RootOp = parent
				if shape == "operator-child" {
					parent.AppendChild(producer)
				} else {
					scope.PreScopes = []*Scope{{Magic: Remote, Proc: c.proc, NodeInfo: scope.NodeInfo, RootOp: producer}}
				}
			}
			_, _, data, _, err := prepareRemoteRunSendingData("insert", scope, c.proc, nil, uuid.Nil)
			require.NoError(t, err)
			info := new(pipeline.ProcessInfo)
			require.NoError(t, info.Unmarshal(data))
			require.Zero(t, info.OdkuOrdinalBase)
			_, err = c.proc.NextODKUInputOrdinal(1)
			require.ErrorContains(t, err, "range exhausted", "the genuine producer reserved the entire bounded range")
			before := *c.proc.Base.ODKUInputOrdinal
			_, _, _, _, err = prepareRemoteRunSendingData("insert", scope, c.proc, nil, uuid.Nil)
			require.ErrorContains(t, err, "range exhausted")
			require.Equal(t, before, *c.proc.Base.ODKUInputOrdinal, "a rejected producer cannot wrap or advance its range")
		})
	}
}
