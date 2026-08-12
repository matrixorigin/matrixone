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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/stretchr/testify/require"
)

type siriusJournalStub struct{}

func (siriusJournalStub) StoreIfCapacity(context.Context, []*substrait.Lease, int) (int, error) {
	return 0, nil
}
func (siriusJournalStub) Active(context.Context, *substrait.Lease) (bool, error)   { return false, nil }
func (siriusJournalStub) MarkReleased(context.Context, []byte) error               { return nil }
func (siriusJournalStub) Delete(context.Context, []byte) error                     { return nil }
func (siriusJournalStub) Load(context.Context, func(*substrait.Lease) error) error { return nil }

func TestSiriusReadPlanRelease(t *testing.T) {
	ctx := context.Background()
	require.NoError(t, (*SiriusReadPlan)(nil).Release(ctx, nil))
	plan := &SiriusReadPlan{ReadRefs: [][]byte{[]byte("one"), []byte("two")}}
	require.NoError(t, plan.Release(ctx, substrait.NewLeaseManager(1, nil)))

	notReplayed := substrait.NewPersistentLeaseManager(1, nil, siriusJournalStub{})
	require.ErrorContains(t, plan.Release(ctx, notReplayed), "not been replayed")
}

func TestCompileSiriusReadRejectsMissingPlan(t *testing.T) {
	ctx := context.Background()
	var c *Compile
	_, err := c.CompileSiriusRead(ctx, nil, 0, nil, nil, "", 0, nil)
	require.ErrorContains(t, err, "no query plan")
	c = &Compile{}
	_, err = c.CompileSiriusRead(ctx, &planpb.Plan{}, 0, nil, nil, "", 0, nil)
	require.True(t, substrait.IsNotEligible(err))

	invalid := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{StmtType: planpb.Query_SELECT}}}
	_, err = c.CompileSiriusRead(ctx, invalid, 0, nil, nil, "", 0, nil)
	require.Error(t, err)
	require.True(t, substrait.IsNotEligible(err))

	unsupported := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes:    []*planpb.Node{{NodeId: 0, NodeType: planpb.Node_JOIN}},
	}}}
	_, err = c.CompileSiriusRead(ctx, unsupported, 0, nil, nil, "", 0, nil)
	require.Error(t, err)
	require.True(t, substrait.IsNotEligible(err), "normal plan ineligibility must reach the compile caller")
}

func TestSiriusOffloadContextIsExplicit(t *testing.T) {
	require.False(t, siriusOffloadRequested(context.Background()))
	require.True(t, siriusOffloadRequested(WithSiriusOffload(context.Background())))
	require.True(t, siriusStatementEligible(&tree.Select{}))
	require.False(t, siriusStatementEligible(&tree.Select{IsPerform: true}))
	require.False(t, siriusStatementEligible(&tree.Select{Ep: &tree.ExportParam{}}))
	require.False(t, siriusStatementEligible(&tree.ExplainAnalyze{}))
}

func TestBuildSiriusReadPlanReturnsAdmittedOwnerOnBuildFailure(t *testing.T) {
	query := &planpb.Query{
		StmtType: planpb.Query_SELECT, Steps: []int32{0}, Headings: []string{strings.Repeat("h", substrait.MaxPlanBytes)},
		Nodes: []*planpb.Node{{
			NodeId: 0, NodeType: planpb.Node_TABLE_SCAN,
			ObjRef: &planpb.ObjectRef{Db: 7, Obj: 42, ObjName: "t"},
			TableDef: &planpb.TableDef{TblId: 42, Version: 3, Name: "t", TableType: "r", Cols: []*planpb.ColDef{{
				Name: "a", ColId: 11, Seqnum: 5, Typ: planpb.Type{Id: int32(types.T_int64)},
			}}},
		}},
	}
	candidate, err := substrait.Export(query)
	require.NoError(t, err)
	readRef := []byte("admitted-read-ref")
	expires := time.Now().Add(time.Minute)
	plan, err := buildSiriusReadPlan(context.Background(), candidate, query.Headings, &substrait.AdmittedReads{
		Wires: map[int32][]byte{0: {1}}, ReadRefs: [][]byte{readRef}, ExpiresAt: expires,
	})
	require.ErrorContains(t, err, "build admitted plan")
	require.NotNil(t, plan)
	require.Empty(t, plan.Plan)
	require.Equal(t, [][]byte{readRef}, plan.ReadRefs)
	require.Equal(t, expires, plan.LeaseExpiresAt)
	readRef[0] = 'x'
	require.Equal(t, byte('a'), plan.ReadRefs[0][0])
}
