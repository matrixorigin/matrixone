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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"go.uber.org/zap"
)

func (c *Compile) generateNodes(node *plan.Node) (engine.Nodes, error) {
	rel, _, _, err := c.handleDbRelContext(node, false)
	if err != nil {
		return nil, err
	}

	forceSingle := forceSingleScan(node)
	if node.NodeType == plan.Node_TABLE_CLONE {
		forceSingle = true
	}

	stats := toScheduleScanStats(node)
	scanRequest := schedule.ScanRequest{
		QueryWorkers:         c.scheduledQueryWorkers(),
		CurrentCN:            c.currentCNWorker(),
		QueryPlacementReason: c.queryPlacement.Reason,
		Stats:                stats,
		ForceSingle:          forceSingle,
		ForceMultiCN:         plan2.GetForceScanOnMultiCN() || plan2.IsIvfSearchEntriesInternalScan(node),
		OneCNBlockThreshold:  int32(plan2.BlockThresholdForOneCN),
	}
	scanPlacement := schedule.DecideScanPlacement(scanRequest)
	c.schedulingTrace.RecordScan(c.schedulingAttempt, scanRequest, scanPlacement)
	c.recordScanSchedulingMetrics(scanPlacement, stats, forceSingle)
	c.maybeLogScanPlacement(scanPlacement, stats, forceSingle)
	if scanPlacement.LocalOnly {
		return c.localScanNodes(scanPlacement.Workers, stats, forceSingle, node, rel)
	}

	return c.materializeScanNodes(scanPlacement.Workers, stats, node, rel)
}

func forceSingleScan(node *plan.Node) bool {
	if len(node.AggList) == 0 {
		return false
	}
	partialResults, _, _ := checkAggOptimize(node)
	if partialResults != nil {
		return true
	}
	if node.Stats != nil && node.Stats.ForceOneCN {
		return true
	}
	for _, agg := range node.AggList {
		if f, ok := agg.Expr.(*plan.Expr_F); ok {
			if (uint64(f.F.Func.Obj) & function.Distinct) != 0 {
				return true
			}
		}
	}
	return false
}

func (c *Compile) localScanNodes(
	workers schedule.Workers,
	stats *schedule.ScanStats,
	forceSingle bool,
	node *plan.Node,
	rel engine.Relation,
) (engine.Nodes, error) {
	mcpu := 1
	if stats != nil && stats.Dop > 0 {
		mcpu = int(stats.Dop)
	}
	if forceSingle {
		mcpu = 1
	}
	if len(workers) == 0 {
		return engine.Nodes{{
			Addr:  c.addr,
			Mcpu:  normalizeMcpu(mcpu),
			CNCNT: 1,
		}}, nil
	}

	engNode := c.materializeScheduledWorker(workers[0])
	engNode.Mcpu = normalizeMcpu(mcpu)
	engNode.CNCNT = 1
	engNode.CNIDX = 0
	if engNode.Addr != "" && engNode.Addr != c.addr {
		remoteTombstones := remoteScanTombstoneAttacher{
			c:    c,
			node: node,
			rel:  rel,
		}
		if err := remoteTombstones.attach(&engNode); err != nil {
			return nil, err
		}
	}
	return engine.Nodes{engNode}, nil
}

func (c *Compile) materializeScanNodes(
	workers schedule.Workers,
	stats *schedule.ScanStats,
	node *plan.Node,
	rel engine.Relation,
) (engine.Nodes, error) {
	remoteTombstones := remoteScanTombstoneAttacher{
		c:    c,
		node: node,
		rel:  rel,
	}
	nodes := make(engine.Nodes, 0, len(workers))
	for i := range workers {
		mcpu := normalizeMcpu(workers[i].Mcpu)
		if stats != nil && stats.Dop > 0 {
			mcpu = min(mcpu, int(stats.Dop))
		}
		engNode := c.materializeScheduledWorker(workers[i])
		engNode.Mcpu = mcpu
		engNode.CNCNT = int32(len(workers))
		engNode.CNIDX = int32(i)
		if engNode.Addr != c.addr {
			if err := remoteTombstones.attach(&engNode); err != nil {
				return nil, err
			}
		}
		nodes = append(nodes, engNode)
	}
	return nodes, nil
}

type remoteScanTombstoneAttacher struct {
	c          *Compile
	node       *plan.Node
	rel        engine.Relation
	collected  bool
	tombstones engine.Tombstoner
}

func (a *remoteScanTombstoneAttacher) attach(node *engine.Node) error {
	if !a.collected {
		tombstones, err := collectTombstones(a.c, a.node, a.rel, engine.Policy_CollectAllTombstones)
		if err != nil {
			return err
		}
		a.tombstones = tombstones
		a.collected = true
	}
	node.Data = readutil.BuildEmptyRelData()
	_ = node.Data.AttachTombstones(a.tombstones)
	return nil
}

func toScheduleScanStats(node *plan.Node) *schedule.ScanStats {
	if node == nil || node.Stats == nil {
		return nil
	}
	return &schedule.ScanStats{
		BlockNum:   node.Stats.BlockNum,
		Dop:        node.Stats.Dop,
		ForceOneCN: node.Stats.ForceOneCN,
	}
}

func (c *Compile) maybeLogScanPlacement(
	decision schedule.ScanDecision,
	stats *schedule.ScanStats,
	forceSingle bool,
) {
	if c.execType != plan2.ExecTypeAP_MULTICN || !decision.LocalOnly || !shouldWarnScanPlacement(decision.Reason) {
		return
	}

	fields := []zap.Field{
		zap.String("reason", decision.Reason),
		zap.String("query-placement-reason", c.queryPlacement.Reason),
		zap.String("exec-type", queryExecTypeString(c.execType)),
		zap.String("current-cn-policy", c.queryPlacement.CurrentCNPolicy.String()),
		zap.Int("query-worker-count", len(c.cnList)),
		zap.Int("scan-worker-count", len(decision.Workers)),
		zap.Bool("force-single", forceSingle),
		zap.Bool("is-internal", c.isInternal),
	}
	currentCN := c.currentCNWorker()
	fields = append(fields,
		zap.String("current-cn-id", currentCN.ID),
		zap.String("current-cn-address", currentCN.Addr),
	)
	if stats != nil {
		fields = append(fields,
			zap.Int32("block-num", stats.BlockNum),
			zap.Int32("dop", stats.Dop),
			zap.Bool("force-one-cn", stats.ForceOneCN),
		)
	}
	if len(decision.Workers) > 0 {
		fields = append(fields,
			zap.String("selected-worker-id", decision.Workers[0].ID),
			zap.String("selected-worker-address", decision.Workers[0].Addr),
		)
	}

	getQueryScheduleLogger().WarnWithConfig(
		"scan-schedule-local-only-"+decision.Reason,
		"scan schedule kept execution on a single CN",
		queryScheduleLogRateLimit,
		fields...)
}

func shouldWarnScanPlacement(reason string) bool {
	switch reason {
	case schedule.ReasonScanNoWorkers,
		schedule.ReasonScanMissingStats,
		schedule.ReasonScanSingleWorker,
		schedule.ReasonScanQueryLocalExec,
		schedule.ReasonScanQueryFallbackCN:
		return true
	default:
		return false
	}
}
