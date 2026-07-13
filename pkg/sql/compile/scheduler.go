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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

var queryScheduleLogRateLimit = logutil.RateLimitConfig{
	Interval:       time.Minute,
	BurstCount:     1,
	SampleInterval: 1_000_000_000,
}

var queryScheduleLogger struct {
	once   sync.Once
	logger *logutil.RateLimitedLogger
}

func getQueryScheduleLogger() *logutil.RateLimitedLogger {
	queryScheduleLogger.once.Do(func() {
		queryScheduleLogger.logger = logutil.NewRateLimitedLogger(logutil.GetGlobalLogger())
	})
	return queryScheduleLogger.logger
}

func (c *Compile) scheduleQueryWorkers() (engine.Nodes, error) {
	placement, err := c.decideQueryPlacement()
	if err != nil {
		return nil, err
	}
	c.queryPlacement = placement
	if !placement.Satisfied {
		getQueryScheduleLogger().WarnWithConfig(
			"query-schedule-unsatisfied-placement",
			"query schedule cannot satisfy placement",
			queryScheduleLogRateLimit,
			c.querySchedulePlacementFields(placement)...)
		return nil, moerr.NewInternalErrorNoCtxf(
			"query schedule cannot satisfy placement, reason %s, current CN policy %s",
			placement.Reason,
			placement.CurrentCNPolicy.String())
	}
	nodes := c.materializeScheduledWorkers(placement.Workers)
	if c.execType == plan2.ExecTypeAP_MULTICN {
		if placement.Reason == schedule.ReasonNoCandidateCN {
			getQueryScheduleLogger().WarnWithConfig(
				"query-schedule-no-candidate-cn",
				"query schedule falls back to local CN",
				queryScheduleLogRateLimit,
				c.querySchedulePlacementFields(placement)...)
		}
	}
	if err := c.validateScheduledQueryRoutes(nodes, placement); err != nil {
		return nil, err
	}
	return nodes, nil
}

func (c *Compile) querySchedulePlacementFields(placement schedule.QueryDecision) []zap.Field {
	currentCN := c.currentCNWorker()
	return []zap.Field{
		zap.String("reason", placement.Reason),
		zap.String("current-cn-policy", placement.CurrentCNPolicy.String()),
		zap.String("exec-type", queryExecTypeString(c.execType)),
		zap.String("current-cn-id", currentCN.ID),
		zap.String("current-cn-address", currentCN.Addr),
		zap.Int("worker-count", len(placement.Workers)),
		zap.Bool("is-internal", c.isInternal),
	}
}

func (c *Compile) validateScheduledQueryRoutes(nodes engine.Nodes, placement schedule.QueryDecision) error {
	if c.execType != plan2.ExecTypeAP_MULTICN || len(nodes) <= 1 {
		return nil
	}
	for _, node := range nodes {
		if node.Addr != "" {
			continue
		}
		getQueryScheduleLogger().WarnWithConfig(
			"query-schedule-unroutable-selected-worker",
			"query schedule selected a worker without route for multi-CN execution",
			queryScheduleLogRateLimit,
			append(c.querySchedulePlacementFields(placement),
				zap.String("worker-id", node.Id),
				zap.Int("worker-mcpu", node.Mcpu))...)
		return moerr.NewInternalErrorNoCtxf(
			"query schedule selected worker %s without address for multi-CN execution",
			node.Id)
	}
	return nil
}

func (c *Compile) getCandidateCNs() (engine.Nodes, error) {
	if c.e == nil {
		return nil, moerr.NewInternalErrorNoCtx("compile engine is not initialized")
	}
	return c.e.Nodes(c.isInternal, c.tenant, c.uid, c.cnLabel)
}

func (c *Compile) decideQueryPlacement() (schedule.QueryDecision, error) {
	currentCN := c.currentCNWorker()
	req := schedule.QueryRequest{
		ExecKind:        toScheduleExecKind(c.execType),
		CurrentCN:       currentCN,
		CurrentCNPolicy: c.currentCNPolicy(),
	}
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return schedule.DecideQueryPlacement(req), nil
	}

	candidates, err := c.getCandidateCNs()
	if err != nil {
		return schedule.QueryDecision{}, err
	}
	var qry *plan.Query
	if c.pn != nil {
		qry = c.pn.GetQuery()
	}
	if hasMixedCommit(candidates) && queryHasIvfSearchEntriesInternalScan(qry) {
		c.execType = plan2.ExecTypeAP_ONECN
		req.ExecKind = schedule.QueryExecAPOneCN
		return schedule.DecideQueryPlacement(req), nil
	}

	rawCandidateCount := len(candidates)
	req.Candidates = toScheduleCandidateWorkers(candidates)
	if len(req.Candidates) < rawCandidateCount {
		getQueryScheduleLogger().WarnWithConfig(
			"query-schedule-unroutable-cn",
			"query schedule dropped unroutable CN candidates",
			queryScheduleLogRateLimit,
			zap.Int("candidate-count", rawCandidateCount),
			zap.Int("routable-count", len(req.Candidates)),
			zap.String("current-cn-policy", req.CurrentCNPolicy.String()),
			zap.String("exec-type", queryExecTypeString(c.execType)),
			zap.String("current-cn-id", req.CurrentCN.ID),
			zap.String("current-cn-address", req.CurrentCN.Addr),
			zap.Bool("is-internal", c.isInternal))
	}
	return schedule.DecideQueryPlacement(req), nil
}

func hasMixedCommit(nodes engine.Nodes) bool {
	for i := range nodes {
		if nodes[i].HasMixedCommit {
			return true
		}
	}
	return false
}

func queryHasIvfSearchEntriesInternalScan(qry *plan.Query) bool {
	if qry == nil {
		return false
	}
	for _, node := range qry.GetNodes() {
		if plan2.IsIvfSearchEntriesInternalScan(node) {
			return true
		}
	}
	return false
}

func (c *Compile) currentCNWorker() schedule.Worker {
	currentCN := getEngineNode(c)
	if c.proc != nil {
		currentCN.Id = c.proc.GetService()
	}
	return toScheduleWorker(currentCN)
}

func (c *Compile) currentCNPolicy() schedule.CurrentCNPolicy {
	if c.proc != nil && c.proc.Base.QueryClient != nil {
		return schedule.CurrentCNRequired
	}
	return schedule.CurrentCNAllowed
}

func toScheduleExecKind(execType plan2.ExecType) schedule.QueryExecKind {
	switch execType {
	case plan2.ExecTypeTP:
		return schedule.QueryExecTP
	case plan2.ExecTypeAP_ONECN:
		return schedule.QueryExecAPOneCN
	default:
		return schedule.QueryExecAPMultiCN
	}
}

func queryExecTypeString(execType plan2.ExecType) string {
	switch execType {
	case plan2.ExecTypeTP:
		return "tp"
	case plan2.ExecTypeAP_ONECN:
		return "ap-one-cn"
	case plan2.ExecTypeAP_MULTICN:
		return "ap-multi-cn"
	default:
		return "unknown"
	}
}

func toScheduleWorker(node engine.Node) schedule.Worker {
	return schedule.Worker{
		ID:   node.Id,
		Addr: node.Addr,
		Mcpu: normalizeMcpu(node.Mcpu),
	}
}

// toScheduleCandidateWorkers converts engine discovery results into schedulable
// candidates. Candidate workers without a route cannot be selected for remote
// execution, so they are dropped here.
func toScheduleCandidateWorkers(nodes engine.Nodes) schedule.Workers {
	if len(nodes) == 0 {
		return nil
	}
	workers := make(schedule.Workers, 0, len(nodes))
	for _, node := range nodes {
		worker := toScheduleWorker(node)
		if worker.Addr == "" {
			continue
		}
		workers = append(workers, worker)
	}
	if len(workers) == 0 {
		return nil
	}
	return workers
}

// toScheduledQueryWorkers converts already-scheduled query workers into stage
// or scan workers. Unlike candidate discovery, a worker with only local identity
// and no remote route is still a valid single-CN execution target and must be kept.
func toScheduledQueryWorkers(nodes engine.Nodes) schedule.Workers {
	if len(nodes) == 0 {
		return nil
	}
	workers := make(schedule.Workers, 0, len(nodes))
	for _, node := range nodes {
		if node.Id == "" && node.Addr == "" && node.Mcpu == 0 {
			continue
		}
		worker := toScheduleWorker(node)
		workers = append(workers, worker)
	}
	if len(workers) == 0 {
		return nil
	}
	return workers
}

func (c *Compile) scheduledQueryWorkers() schedule.Workers {
	return toScheduledQueryWorkers(c.cnList)
}

func toEngineNode(worker schedule.Worker) engine.Node {
	return engine.Node{
		Id:   worker.ID,
		Addr: worker.Addr,
		Mcpu: normalizeMcpu(worker.Mcpu),
	}
}

func (c *Compile) materializeScheduledWorker(worker schedule.Worker) engine.Node {
	node := toEngineNode(worker)
	if node.Addr == "" && c.canUseLocalExecutionRoute(worker) {
		node.Addr = c.addr
	}
	return node
}

func (c *Compile) materializeScheduledWorkers(workers schedule.Workers) engine.Nodes {
	if len(workers) == 0 {
		return nil
	}
	nodes := make(engine.Nodes, 0, len(workers))
	for _, worker := range workers {
		nodes = append(nodes, c.materializeScheduledWorker(worker))
	}
	return nodes
}

func (c *Compile) canUseLocalExecutionRoute(worker schedule.Worker) bool {
	// At this materialization boundary, an empty Addr in already-scheduled
	// workers can only represent the local current CN: candidate discovery drops
	// unroutable remote workers, and multi-CN query output is route-validated.
	return c.addr != "" && worker.Addr == ""
}

func normalizeMcpu(mcpu int) int {
	if mcpu < 1 {
		return 1
	}
	return mcpu
}
