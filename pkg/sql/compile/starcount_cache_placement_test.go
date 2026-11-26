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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestIsCacheRoutableStarCount(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*plan.Query, *plan.Node)
		want bool
	}{
		{name: "single table", want: true},
		{name: "found rows having", edit: func(query *plan.Query, _ *plan.Node) {
			query.Nodes[1].FilterList = []*plan.Expr{zmAggFuncExpr("found_rows", zmAggConstExpr(), function.EncodeOverloadID(function.FOUND_ROWS, 0))}
		}},
		{name: "advisory lock having", edit: func(query *plan.Query, _ *plan.Node) {
			query.Nodes[1].FilterList = []*plan.Expr{zmAggFuncExpr("is_free_lock", zmAggConstExpr(), function.EncodeOverloadID(function.IS_FREE_LOCK, 0))}
		}},
		{name: "ordinary having", edit: func(query *plan.Query, _ *plan.Node) {
			query.Nodes[1].FilterList = []*plan.Expr{zmAggLitExpr(1)}
		}},
		{name: "coordinator projection above aggregate", edit: func(query *plan.Query, _ *plan.Node) {
			query.Nodes = append(query.Nodes, &plan.Node{
				NodeType:    plan.Node_PROJECT,
				Children:    []int32{1},
				ProjectList: []*plan.Expr{zmAggFuncExpr("found_rows", zmAggConstExpr(), function.EncodeOverloadID(function.FOUND_ROWS, 0))},
			})
			query.Steps[0] = 2
		}},
		{name: "coordinator projection on scan", edit: func(_ *plan.Query, scan *plan.Node) {
			scan.ProjectList = []*plan.Expr{zmAggFuncExpr("is_free_lock", zmAggConstExpr(), function.EncodeOverloadID(function.IS_FREE_LOCK, 0))}
		}},
		{name: "ordinary projection above aggregate", want: true, edit: func(query *plan.Query, _ *plan.Node) {
			query.Nodes = append(query.Nodes, &plan.Node{
				NodeType:    plan.Node_PROJECT,
				Children:    []int32{1},
				ProjectList: []*plan.Expr{zmAggColExpr(0)},
			})
			query.Steps[0] = 2
		}},
		{name: "published table", edit: func(_ *plan.Query, scan *plan.Node) { scan.ObjRef.PubInfo = &plan.PubInfo{} }},
		{name: "historical", edit: func(_ *plan.Query, scan *plan.Node) { scan.ScanSnapshot = &plan.Snapshot{} }},
		{name: "force local", edit: func(_ *plan.Query, scan *plan.Node) { scan.Stats = &plan.Stats{ForceOneCN: true} }},
		{name: "filtered", edit: func(_ *plan.Query, scan *plan.Node) { scan.FilterList = []*plan.Expr{{}} }},
		{name: "writer", edit: func(query *plan.Query, _ *plan.Node) { query.StmtType = plan.Query_INSERT }},
		{name: "join", edit: func(query *plan.Query, _ *plan.Node) { query.Nodes[1].NodeType = plan.Node_JOIN }},
		{name: "locked", edit: func(query *plan.Query, _ *plan.Node) { query.SelectLockReferences = []*plan.SelectLockReference{{}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scan := &plan.Node{
				NodeType: plan.Node_TABLE_SCAN,
				TableDef: &plan.TableDef{TableType: catalog.SystemOrdinaryRel},
				ObjRef:   &plan.ObjectRef{},
				AggList:  []*plan.Expr{zmAggFuncExpr("starcount", zmAggConstExpr(), 0)},
			}
			query := &plan.Query{
				StmtType: plan.Query_SELECT,
				Steps:    []int32{1},
				Nodes:    []*plan.Node{scan, {NodeType: plan.Node_AGG, Children: []int32{0}, AggList: scan.AggList}},
			}
			if tc.edit != nil {
				tc.edit(query, scan)
			}
			require.Equal(t, tc.want, isCacheRoutableStarCount(query, scan))
		})
	}
}

func TestBestStarCountCacheWorker(t *testing.T) {
	targets := schedule.Workers{{ID: "one"}, {ID: "two"}}
	for _, tc := range []struct {
		name string
		one  int
		two  int
		want string
	}{
		{name: "strict advantage", one: 3, two: 4, want: "two"},
		{name: "cold peers", one: 0, two: 1},
		{name: "ties local", one: 1, two: 2},
		{name: "ties another peer", one: 3, two: 3},
		{name: "failed probe", one: -1, two: 4},
		{name: "invalid response", one: 5, two: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker, ok := bestStarCountCacheWorker(t.Context(), targets, 2, 4,
				func(_ context.Context, worker schedule.Worker) (int, bool) {
					if worker.ID == "one" {
						return tc.one, tc.one >= 0
					}
					return tc.two, true
				})
			if tc.want == "" {
				require.False(t, ok)
				return
			}
			require.True(t, ok)
			require.Equal(t, tc.want, worker.ID)
		})
	}

	for _, tc := range []struct {
		name string
		ctx  func(context.Context) (context.Context, context.CancelFunc)
	}{
		{name: "cancelled", ctx: func(ctx context.Context) (context.Context, context.CancelFunc) {
			child, cancel := context.WithCancel(ctx)
			cancel()
			return child, cancel
		}},
		{name: "timed out", ctx: func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithDeadline(ctx, time.Now().Add(-time.Second))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.ctx(t.Context())
			defer cancel()
			_, ok := bestStarCountCacheWorker(ctx, targets, 0, 4, func(ctx context.Context, _ schedule.Worker) (int, bool) {
				<-ctx.Done()
				return 0, false
			})
			require.False(t, ok)
		})
	}
}

func TestStarCountCachePlacementFallback(t *testing.T) {
	scan := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{TableType: catalog.SystemOrdinaryRel},
		ObjRef:   &plan.ObjectRef{},
		AggList:  []*plan.Expr{zmAggFuncExpr("starcount", zmAggConstExpr(), 0)},
	}
	qry := &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{1},
		Nodes:    []*plan.Node{scan, {NodeType: plan.Node_AGG, Children: []int32{0}, AggList: scan.AggList}},
	}
	current := schedule.Worker{ID: "local", Addr: "local:6001", Route: schedule.WorkerRouteLocal}
	fallback := schedule.ScanDecision{
		Workers: schedule.Workers{current}, Reason: schedule.ReasonScanForceSingle, LocalOnly: true,
	}
	req := schedule.ScanRequest{
		CurrentCN:    current,
		QueryWorkers: schedule.Workers{current, {ID: "remote", Addr: "remote:6001", Route: schedule.WorkerRouteRemote}},
	}
	c := NewMockCompile(t)
	t.Cleanup(c.proc.Free)
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}
	c.addr = current.Addr
	c.execType = plan2.ExecTypeAP_MULTICN
	c.proc.Base.QueryClient = fakeQueryClient{}

	// A cancelled context reaches the probe boundary for a read-only query.
	// A writable workspace must return before candidate or cache probing.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, txn := newTestTxnClientAndOp(gomock.NewController(t), &Ws{})
	c.proc.Base.TxnOperator = txn
	actual, err := c.placeStarCountByCache(ctx, scan, nil, req, fallback)
	require.NoError(t, err)
	require.Equal(t, fallback, actual)

	_, txn = newTestTxnClientAndOp(gomock.NewController(t), &readonlyWorkspaceForScheduling{Ws: &Ws{}})
	c.proc.Base.TxnOperator = txn
	actual, err = c.placeStarCountByCache(ctx, scan, nil, req, fallback)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, fallback, actual)
}

type starCountSamplingRelation struct {
	engine.Relation
	tombstoneRows int
	samples       int
}

func (r *starCountSamplingRelation) SampleCountTombstoneObjects(_ context.Context, threshold int) ([][]byte, error) {
	r.samples++
	if r.tombstoneRows <= threshold {
		return nil, nil
	}
	id := objectio.NewObjectid()
	return [][]byte{id[:]}, nil
}

func TestStarCountCacheDiscoversCandidatesOnlyForHeavyCounts(t *testing.T) {
	scan := &plan.Node{
		NodeType: plan.Node_TABLE_SCAN,
		TableDef: &plan.TableDef{TableType: catalog.SystemOrdinaryRel},
		ObjRef:   &plan.ObjectRef{},
		AggList:  []*plan.Expr{zmAggFuncExpr("starcount", zmAggConstExpr(), 0)},
	}
	qry := &plan.Query{
		StmtType: plan.Query_SELECT,
		Steps:    []int32{1},
		Nodes:    []*plan.Node{scan, {NodeType: plan.Node_AGG, Children: []int32{0}, AggList: scan.AggList}},
	}
	current := schedule.Worker{ID: "local", Addr: "local:6001", Route: schedule.WorkerRouteLocal}
	fallback := schedule.ScanDecision{
		Workers: schedule.Workers{current}, Reason: schedule.ReasonScanForceSingle, LocalOnly: true,
	}
	req := schedule.ScanRequest{CurrentCN: current, QueryWorkers: fallback.Workers}
	c := NewMockCompile(t)
	t.Cleanup(c.proc.Free)
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}
	c.addr = current.Addr
	c.execType = plan2.ExecTypeAP_ONECN
	c.proc.Base.QueryClient = fakeQueryClient{}
	fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(t.Context()) })
	c.proc.Base.FileService = fs
	_, txn := newTestTxnClientAndOp(gomock.NewController(t), &readonlyWorkspaceForScheduling{Ws: &Ws{}})
	c.proc.Base.TxnOperator = txn
	discoverer := &schedulerProviderTestEngine{
		schedulerTestEngine: &schedulerTestEngine{},
		discoveryErr:        errors.New("discovery deliberately unavailable"),
	}
	c.e = discoverer

	for _, tc := range []struct {
		name           string
		rows           int
		discoveryCalls int
	}{
		{name: "empty"},
		{name: "below threshold", rows: defaultStarCountTombstoneThreshold - 1},
		{name: "at threshold", rows: defaultStarCountTombstoneThreshold},
		{name: "heavy", rows: defaultStarCountTombstoneThreshold + 1, discoveryCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			discoverer.discoveryCalls = 0
			rel := &starCountSamplingRelation{tombstoneRows: tc.rows}
			actual, err := c.placeStarCountByCache(t.Context(), scan, rel, req, fallback)
			require.NoError(t, err)
			require.Equal(t, fallback, actual)
			require.Equal(t, 1, rel.samples)
			require.Equal(t, tc.discoveryCalls, discoverer.discoveryCalls)
		})
	}
}
