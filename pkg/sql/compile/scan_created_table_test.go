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
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type scanCreatedTableRelation struct {
	*stubRelation
	created bool
	err     error
}

func (r *scanCreatedTableRelation) CreatedInCurrentTxn(context.Context) (bool, error) {
	return r.created, r.err
}

func TestGenerateNodesTransactionCreatedTable(t *testing.T) {
	oldForce := plan2.GetForceScanOnMultiCN()
	plan2.SetForceScanOnMultiCN(true)
	t.Cleanup(func() { plan2.SetForceScanOnMultiCN(oldForce) })
	lookupErr := errors.New("created-in-transaction lookup failed")
	for _, tc := range []struct {
		name       string
		created    bool
		noStats    bool
		remoteOnly bool
		noWorkers  bool
		lookupErr  error
	}{
		{name: "created", created: true},
		{name: "created_without_stats", created: true, noStats: true},
		{name: "created_without_workers", created: true, noWorkers: true},
		{name: "committed_stays_distributed"},
		{name: "created_outside_pool", created: true, remoteOnly: true},
		{name: "committed_remote_only", remoteOnly: true},
		{name: "lookup_error", lookupErr: lookupErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewMockCompile(t)
			c.addr = "cn-local:6001"
			c.cnList = engine.Nodes{
				{Id: "cn-remote", Addr: "cn-remote:6001", Mcpu: 8},
				{Id: "cn-local", Addr: c.addr, Mcpu: 8},
			}
			if tc.remoteOnly {
				c.cnList = c.cnList[:1]
			} else if tc.noWorkers {
				c.cnList = nil
			}
			rel := &scanCreatedTableRelation{stubRelation: newStubRelation("t"), created: tc.created, err: tc.lookupErr}
			ctrl := gomock.NewController(t)
			db := mock_frontend.NewMockDatabase(ctrl)
			db.EXPECT().Relation(gomock.Any(), "t", gomock.Any()).Return(rel, nil)
			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().Database(gomock.Any(), "testdb", gomock.Any()).Return(db, nil)
			c.e = eng
			node := &plan.Node{
				NodeType: plan.Node_TABLE_SCAN,
				ObjRef:   &plan.ObjectRef{SchemaName: "testdb", ObjName: "t"},
				TableDef: &plan.TableDef{Name: "t"},
				Stats:    &plan.Stats{BlockNum: int32(plan2.BlockThresholdForOneCN + 1), Dop: 6},
			}
			if tc.noStats {
				node.Stats = nil
			}
			nodes, err := c.generateNodes(node)
			if tc.lookupErr != nil {
				require.ErrorIs(t, err, lookupErr)
				require.Nil(t, nodes)
				return
			}
			if tc.created && tc.remoteOnly {
				require.ErrorContains(t, err, "requires the coordinator CN")
				require.Nil(t, nodes)
				return
			}
			require.NoError(t, err)
			if tc.created {
				require.Len(t, nodes, 1)
				require.Equal(t, c.addr, nodes[0].Addr)
				require.Equal(t, int32(1), nodes[0].CNCNT)
				require.Equal(t, int32(0), nodes[0].CNIDX)
				require.Nil(t, nodes[0].Data)
				require.Zero(t, rel.collectTombstonesCall)
			} else {
				require.Len(t, nodes, len(c.cnList))
				require.Equal(t, "cn-remote:6001", nodes[0].Addr)
				require.Equal(t, 1, rel.collectTombstonesCall)
			}
			if tc.noStats {
				require.Equal(t, 1, nodes[0].Mcpu)
				require.Nil(t, node.Stats)
			} else {
				require.Equal(t, 6, nodes[0].Mcpu, "retain intra-CN parallelism")
				require.False(t, node.Stats.ForceOneCN, "do not mutate reusable plan stats")
			}
		})
	}
}
