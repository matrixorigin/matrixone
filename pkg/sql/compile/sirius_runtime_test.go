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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

func TestSiriusCompileFastRejections(t *testing.T) {
	require.True(t, siriusStatementEligible(&tree.Select{}))
	require.False(t, siriusStatementEligible(&tree.Select{IsPerform: true}))
	require.False(t, siriusStatementEligible(&tree.Select{Ep: &tree.ExportParam{}}))
	require.False(t, siriusStatementEligible(sqlCalcFoundRowsTestStatement()))
	require.False(t, siriusStatementEligible(nil))
	require.False(t, siriusPlanEligible(&planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		UnresolvedIndexHints: []*planpb.UnresolvedIndexHint{{IndexName: "idx_new"}},
	}}}))
	require.True(t, siriusPlanEligible(&planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{}}}))

	requested := WithSiriusOffload(context.Background())
	for _, c := range []*Compile{
		nil,
		{},
		{isPrepare: true, stmt: &tree.Select{}},
		{isInternal: true, stmt: &tree.Select{}},
		{stmt: &tree.Select{IsPerform: true}},
	} {
		offloaded, err := c.tryCompileSiriusRead(requested, nil)
		require.NoError(t, err)
		require.False(t, offloaded)
	}
	offloaded, err := (&Compile{}).tryCompileSiriusRead(context.Background(), nil)
	require.NoError(t, err)
	require.False(t, offloaded)

	require.NoError(t, (*siriusReadOwner)(nil).finish(context.Background(), false))
	err = (&Compile{}).runSiriusRead(context.Background())
	require.ErrorContains(t, err, "missing Sirius execution owner")
}

type siriusAdmissionBackend struct {
	SiriusEngine
	accepting bool
}

func (b *siriusAdmissionBackend) Accepting() bool { return b.accepting }

func (*siriusAdmissionBackend) Capabilities() uint64 { return 0 }

func TestEmbeddedSiriusAdmissionNeverSilentlyFallsBack(t *testing.T) {
	proc := testutil.NewProcess(t)
	runtime := moruntime.ServiceRuntime(proc.GetService())
	previous, existed := runtime.GetGlobalVariables(SiriusRuntimeKey)
	backend := &siriusAdmissionBackend{accepting: true}
	configured := &SiriusRuntime{Engine: backend, CleanupTimeout: time.Second}
	runtime.SetGlobalVariables(SiriusRuntimeKey, configured)
	t.Cleanup(func() {
		if existed {
			runtime.SetGlobalVariables(SiriusRuntimeKey, previous)
		} else {
			runtime.CompareAndDeleteGlobalVariables(SiriusRuntimeKey, configured)
		}
	})
	c := &Compile{proc: proc, stmt: &tree.Select{}}
	ctx := WithSiriusOffload(context.Background())
	for _, query := range []*planpb.Plan{nil, {Plan: &planpb.Plan_Query{Query: &planpb.Query{}}}} {
		offloaded, err := c.tryCompileSiriusRead(ctx, query)
		require.False(t, offloaded)
		require.Error(t, err)
		backend.accepting = false
		offloaded, err = c.tryCompileSiriusRead(ctx, query)
		require.False(t, offloaded)
		require.ErrorContains(t, err, "admission is sealed")
		backend.accepting = true
	}
	offloaded, err := c.tryCompileSiriusRead(context.Background(), nil)
	require.False(t, offloaded)
	require.NoError(t, err)
}

func TestSQLSelectLimitIsMaterializedBeforeSiriusExport(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.ApplySQLSelectLimit = true
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == plan2.SQLSelectLimitVariable {
			return uint64(3), nil
		}
		return nil, nil
	})
	t.Cleanup(proc.Free)

	query := &planpb.Query{
		StmtType: planpb.Query_SELECT, Steps: []int32{0}, Headings: []string{"a"},
		ApplySqlSelectLimit: true,
		Nodes: []*planpb.Node{{
			NodeId: 0, NodeType: planpb.Node_TABLE_SCAN,
			ObjRef: &planpb.ObjectRef{Obj: 42, ObjName: "t"},
			TableDef: &planpb.TableDef{
				DbId: 7, TblId: 42, Version: 3, Name: "t", TableType: "r",
				Cols: []*planpb.ColDef{{
					Name: "a", ColId: 11, Seqnum: 5,
					Typ: planpb.Type{Id: int32(types.T_int64)},
				}},
			},
		}},
	}
	queryPlan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}
	c := &Compile{proc: proc}
	materialization, err := c.materializeSQLSelectLimit(queryPlan)
	require.NoError(t, err)
	require.False(t, query.ApplySqlSelectLimit)
	require.Equal(t, uint64(3), query.Nodes[0].Limit.GetLit().GetU64Val())

	candidate, err := substrait.ExportEmbeddedMO(query, substrait.EmbeddedExportProfile{})
	require.NoError(t, err)
	wire, err := candidate.BuildEmbedded(map[int32]substrait.EmbeddedReadBinding{0: {BindingID: 1, Source: substrait.EmbeddedReadMO}})
	require.NoError(t, err)
	offloadedPlan := new(spb.Plan)
	require.NoError(t, proto.Unmarshal(wire, offloadedPlan))
	// The complete MO scan, including its session limit, feeds this binding.
	require.NotEmpty(t, offloadedPlan.Relations[0].GetRoot().Input.GetRead().GetNamedTable().Names)
	reads, err := candidate.EmbeddedMOReads()
	require.NoError(t, err)
	require.Len(t, reads, 1)
	require.Equal(t, int32(0), reads[0].NodeID)

	materialization.restore()
	require.True(t, query.ApplySqlSelectLimit)
	require.Nil(t, query.Nodes[0].Limit)
}
