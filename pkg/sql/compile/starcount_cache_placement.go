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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	starCountCacheProbeTimeout = 100 * time.Millisecond
	starCountCacheProbeWorkers = 4
)

func isCacheRoutableStarCount(q *plan.Query, scan *plan.Node) bool {
	if !isSingleStarCountNoFilterNoGroupBy(scan) || scan.NodeType != plan.Node_TABLE_SCAN ||
		scan.ScanSnapshot != nil || scan.Limit != nil || scan.Offset != nil ||
		len(scan.OrderBy) != 0 || len(scan.RuntimeFilterProbeList) != 0 ||
		len(scan.LockTargets) != 0 || scan.Stats.GetForceOneCN() ||
		scan.TableDef.GetTableType() != catalog.SystemOrdinaryRel ||
		scan.ObjRef == nil || scan.ObjRef.PubInfo != nil ||
		scan.ValuesWarningRow() != 0 || starCountRequiresCoordinator(scan) {
		return false
	}
	if q == nil || q.StmtType != plan.Query_SELECT || len(q.Steps) != 1 ||
		len(q.SelectLockReferences) != 0 || q.Correlation != nil {
		return false
	}
	for id := q.Steps[0]; id >= 0 && int(id) < len(q.Nodes); {
		node := q.Nodes[id]
		if node == nil || len(node.Children) != 1 || starCountRequiresCoordinator(node) {
			return false
		}
		child := node.Children[0]
		switch node.NodeType {
		case plan.Node_PROJECT, plan.Node_SORT:
			id = child
		case plan.Node_AGG:
			return child >= 0 && int(child) < len(q.Nodes) && q.Nodes[child] == scan &&
				len(node.GroupingFlag) == 0 && !plan2.IsSortRollupOption(node.ExtraOptions) &&
				isSingleStarCountNoFilterNoGroupBy(node)
		default:
			return false
		}
	}
	return false
}

func starCountRequiresCoordinator(node *plan.Node) bool {
	return nodeHasUserLevelLockFunction(node) || nodeHasCoordinatorSessionFunction(node)
}

func (c *Compile) placeStarCountByCache(
	ctx context.Context,
	node *plan.Node,
	rel engine.Relation,
	req schedule.ScanRequest,
	fallback schedule.ScanDecision,
) (schedule.ScanDecision, error) {
	if !isCacheRoutableStarCount(c.pn.GetQuery(), node) ||
		c.isPrepare || c.isInternal || c.getHaveDDL() ||
		executor.RequiresLocalExecution(ctx) || executor.RequiresLocalExecution(c.proc.Ctx) ||
		c.queryPlacement.CurrentCNPolicy == schedule.CurrentCNRequired ||
		len(fallback.Workers) != 1 || !c.canUseLocalExecutionRoute(fallback.Workers[0]) {
		return fallback, nil
	}
	txn := c.proc.GetTxnOperator()
	if txn == nil || txn.GetWorkspace() == nil || !txn.GetWorkspace().Readonly() {
		return fallback, nil
	}
	qc := c.proc.GetQueryClient()
	if qc == nil || req.CurrentCN.ID == "" {
		return fallback, nil
	}

	if err := ctx.Err(); err != nil {
		return fallback, err
	}
	sampler, ok := rel.(interface {
		SampleCountTombstoneObjects(context.Context, int) ([][]byte, error)
	})
	if !ok {
		return fallback, nil
	}
	if c.execType == plan2.ExecTypeAP_MULTICN && len(req.QueryWorkers) <= 1 {
		return fallback, nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, starCountCacheProbeTimeout)
	defer cancel()
	objects, err := sampler.SampleCountTombstoneObjects(probeCtx, defaultStarCountTombstoneThreshold)
	if err != nil || len(objects) == 0 {
		return fallback, ctx.Err()
	}
	fs, err := fileservice.Get[fileservice.FileService](c.proc.GetFileService(), defines.SharedFileServiceName)
	if err != nil {
		return fallback, ctx.Err()
	}
	localHits, err := objectio.CountCachedObjectFiles(probeCtx, fs, objects)
	if err != nil || localHits == len(objects) {
		return fallback, ctx.Err()
	}

	workers := req.QueryWorkers
	if c.execType != plan2.ExecTypeAP_MULTICN {
		// The query remains single-CN. Discover eligible workers for this
		// scan using the same pool and routing policy as regular AP queries.
		placement, _, err := c.evaluateQueryPlacementForExecType(probeCtx, queryCandidateModeExecution, plan2.ExecTypeAP_MULTICN)
		if err != nil || !placement.Satisfied {
			return fallback, ctx.Err()
		}
		workers = placement.Workers
	}
	targets := make(schedule.Workers, 0, starCountCacheProbeWorkers-1)
	for _, worker := range workers {
		if worker.ID != "" && worker.ID != req.CurrentCN.ID &&
			worker.Addr != c.addr && worker.Addr != "" &&
			validateRemoteRunAddress(worker.Addr, c.addr) == nil {
			targets = append(targets, worker)
			if len(targets) == cap(targets) {
				break
			}
		}
	}
	if len(targets) == 0 {
		return fallback, ctx.Err()
	}

	cluster, err := clusterservice.GetMOClusterWithContext(probeCtx, c.proc.GetService())
	if err != nil {
		return fallback, ctx.Err()
	}
	worker, found := bestStarCountCacheWorker(probeCtx, targets, localHits, len(objects),
		func(ctx context.Context, worker schedule.Worker) (int, bool) {
			var addr string
			err := clusterservice.GetCNServiceWithoutWorkingStateWithContext(
				ctx, cluster, clusterservice.NewServiceIDSelector(worker.ID), func(cn metadata.CNService) bool {
					if cn.PipelineServiceAddress == worker.Addr && schedulableEngineWorkState(cn.WorkState) {
						addr = cn.QueryAddress
					}
					return false
				})
			if err != nil || addr == "" {
				return 0, false
			}
			req := qc.NewRequest(query.CmdMethod_CountCachedObjectFiles)
			req.CountCachedObjectFilesRequest = &query.CountCachedObjectFilesRequest{ObjectIds: objects}
			resp, err := qc.SendMessage(ctx, addr, req)
			if err != nil || resp == nil {
				return 0, false
			}
			defer qc.Release(resp)
			score := resp.CountCachedObjectFilesResponse
			if score == nil {
				return 0, false
			}
			return int(score.Hits), true
		})
	if !found || ctx.Err() != nil {
		return fallback, ctx.Err()
	}
	return schedule.ScanDecision{
		Workers:   schedule.Workers{worker},
		LocalOnly: true,
		Reason:    schedule.ReasonScanStarCountCache,
	}, nil
}

func bestStarCountCacheWorker(
	ctx context.Context,
	targets schedule.Workers,
	localHits, sampleSize int,
	probe func(context.Context, schedule.Worker) (int, bool),
) (schedule.Worker, bool) {
	type result struct {
		worker schedule.Worker
		hits   int
		ok     bool
	}
	results := make(chan result, len(targets))
	for _, worker := range targets {
		go func() {
			hits, ok := probe(ctx, worker)
			results <- result{worker, hits, ok}
		}()
	}

	var best schedule.Worker
	bestHits := localHits
	uniqueBest := false
	for range targets {
		select {
		case <-ctx.Done():
			return schedule.Worker{}, false
		case r := <-results:
			if !r.ok || r.hits < 0 || r.hits > sampleSize {
				return schedule.Worker{}, false
			}
			if r.hits > bestHits {
				best, bestHits, uniqueBest = r.worker, r.hits, true
			} else if r.hits == bestHits {
				uniqueBest = false
			}
		}
	}
	return best, uniqueBest && ctx.Err() == nil
}
