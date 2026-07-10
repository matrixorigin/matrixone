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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
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
		zap.Int("dropped-worker-count", len(placement.Dropped)),
		zap.Bool("is-internal", c.isInternal),
	}
}

func (c *Compile) validateScheduledQueryRoutes(nodes engine.Nodes, placement schedule.QueryDecision) error {
	if c.execType != plan2.ExecTypeAP_MULTICN {
		return nil
	}
	for _, node := range nodes {
		if !schedulableEngineWorkState(node.WorkState) {
			c.recordSchedulingFailure(
				scheduleFailureRuntimeIneligibleSelectedWorker,
				toScheduleWorker(node),
			)
			recordSelectedWorkerFailureMetric(scheduleFailureRuntimeIneligibleSelectedWorker)
			getQueryScheduleLogger().WarnWithConfig(
				"query-schedule-runtime-ineligible-selected-worker",
				"query schedule selected a runtime-ineligible worker",
				queryScheduleLogRateLimit,
				append(c.querySchedulePlacementFields(placement),
					zap.String("worker-id", node.Id),
					zap.String("worker-address", node.Addr),
					zap.String("worker-state", node.WorkState.String()),
					zap.Int("worker-mcpu", node.Mcpu))...)
			return moerr.NewInternalErrorNoCtxf(
				"query schedule selected worker %s with runtime state %s",
				node.Id,
				node.WorkState.String())
		}
	}
	if len(nodes) <= 1 {
		return nil
	}
	for _, node := range nodes {
		if node.Addr != "" {
			continue
		}
		c.recordSchedulingFailure(
			scheduleFailureUnroutableSelectedWorker,
			toScheduleWorker(node),
		)
		recordSelectedWorkerFailureMetric(scheduleFailureUnroutableSelectedWorker)
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

func schedulableEngineWorkState(state metadata.WorkState) bool {
	switch state {
	case metadata.WorkState_Draining, metadata.WorkState_Drained:
		return false
	default:
		return true
	}
}

func (c *Compile) getCandidateCNs() (engine.Nodes, error) {
	if c.e == nil {
		return nil, moerr.NewInternalErrorNoCtx("compile engine is not initialized")
	}
	return c.e.Nodes(c.isInternal, c.tenant, c.uid, c.cnLabel)
}

func (c *Compile) decideQueryPlacement() (schedule.QueryDecision, error) {
	currentCN := c.currentCNWorker()
	if c.execType == plan2.ExecTypeAP_MULTICN {
		currentCN = c.currentCNWorkerWithRuntimeState()
	}
	req := schedule.QueryRequest{
		ExecKind:        toScheduleExecKind(c.execType),
		CurrentCN:       currentCN,
		CurrentCNPolicy: c.currentCNPolicy(),
	}
	if c.execType != plan2.ExecTypeAP_MULTICN {
		decision := schedule.DecideQueryPlacement(req)
		c.queryRawCandidateCount = 0
		c.queryCandidateWorkerCount = 0
		c.recordQuerySchedulingTrace(req.CurrentCN, decision, 0, 0)
		c.recordQuerySchedulingMetrics(decision, 0, 0)
		return decision, nil
	}

	candidates, err := c.getCandidateCNs()
	if err != nil {
		c.recordSchedulingFailure(scheduleFailureCandidateDiscovery, schedule.Worker{})
		return schedule.QueryDecision{}, err
	}

	rawCandidateCount := len(candidates)
	req.Candidates = toScheduleCandidateWorkers(candidates)
	decision := schedule.DecideQueryPlacement(req)
	c.queryRawCandidateCount = rawCandidateCount
	c.queryCandidateWorkerCount = len(req.Candidates)
	c.recordQuerySchedulingTrace(req.CurrentCN, decision, rawCandidateCount, len(req.Candidates))
	c.recordQuerySchedulingMetrics(decision, rawCandidateCount, len(req.Candidates))
	if len(decision.Dropped) > 0 {
		getQueryScheduleLogger().WarnWithConfig(
			"query-schedule-dropped-cn-candidates",
			"query schedule dropped CN candidates",
			queryScheduleLogRateLimit,
			c.queryScheduleDroppedCandidateFields(decision, rawCandidateCount, len(req.Candidates))...)
	}
	return decision, nil
}

func (c *Compile) SetSchedulingTraceRecorder(recorder *schedule.TraceRecorder) {
	c.schedulingTrace = recorder
	c.schedulingAttempt = 0
}

func (c *Compile) beginSchedulingTraceAttempt() {
	c.schedulingAttempt = c.schedulingTrace.StartAttempt()
}

func (c *Compile) recordQuerySchedulingTrace(
	current schedule.Worker,
	decision schedule.QueryDecision,
	rawCandidateCount int,
	candidateWorkerCount int,
) {
	c.schedulingTrace.RecordQuery(
		c.schedulingAttempt,
		current,
		decision,
		rawCandidateCount,
		candidateWorkerCount,
	)
}

func (c *Compile) recordSchedulingFailure(category string, worker schedule.Worker) {
	c.schedulingTrace.RecordFailure(c.schedulingAttempt, category, worker)
}

func (c *Compile) queryScheduleDroppedCandidateFields(
	placement schedule.QueryDecision,
	rawCandidateCount int,
	candidateWorkerCount int,
) []zap.Field {
	counts := droppedWorkerReasonCounts(placement.Dropped)
	fields := c.querySchedulePlacementFields(placement)
	fields = append(fields,
		zap.Int("candidate-count", rawCandidateCount),
		zap.Int("candidate-worker-count", candidateWorkerCount),
		zap.Int("dropped-unroutable-count", counts[schedule.ReasonDroppedUnroutableCN]),
		zap.Int("dropped-draining-count", counts[schedule.ReasonDroppedDrainingCN]),
		zap.Int("dropped-drained-count", counts[schedule.ReasonDroppedDrainedCN]),
		zap.Int("dropped-duplicate-count", counts[schedule.ReasonDroppedDuplicateCN]),
	)
	return fields
}

func droppedWorkerReasonCounts(dropped schedule.DroppedWorkers) map[string]int {
	counts := make(map[string]int, len(dropped))
	for _, worker := range dropped {
		counts[worker.Reason]++
	}
	return counts
}

func (c *Compile) currentCNWorker() schedule.Worker {
	currentCN := getEngineNode(c)
	if c.proc != nil {
		currentCN.Id = c.proc.GetService()
	}
	return toScheduleWorker(currentCN)
}

func (c *Compile) currentCNWorkerWithRuntimeState() schedule.Worker {
	worker := c.currentCNWorker()
	worker.State = toScheduleWorkerState(c.currentCNWorkState(worker.ID))
	return worker
}

func (c *Compile) currentCNWorkState(serviceID string) metadata.WorkState {
	// State lookup is best-effort. Missing runtime or cluster metadata falls
	// back to Unknown, which the scheduler treats as schedulable. Only an
	// explicit Draining/Drained signal should block current-CN placement.
	if serviceID == "" {
		return metadata.WorkState_Unknown
	}
	rt := moruntime.ServiceRuntime(serviceID)
	if rt == nil {
		return metadata.WorkState_Unknown
	}
	v, ok := rt.GetGlobalVariables(moruntime.ClusterService)
	if !ok {
		return metadata.WorkState_Unknown
	}
	cluster, ok := v.(clusterservice.MOCluster)
	if !ok || cluster == nil {
		return metadata.WorkState_Unknown
	}

	state := metadata.WorkState_Unknown
	cluster.GetCNServiceWithoutWorkingState(
		clusterservice.NewServiceIDSelector(serviceID),
		func(cn metadata.CNService) bool {
			state = cn.WorkState
			return false
		})
	return state
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
		ID:    node.Id,
		Addr:  node.Addr,
		Mcpu:  normalizeMcpu(node.Mcpu),
		State: toScheduleWorkerState(node.WorkState),
	}
}

// toScheduleCandidateWorkers converts engine discovery results into schedulable
// candidates. Runtime eligibility and route validation are handled by the
// schedule layer so candidate drops remain observable.
func toScheduleCandidateWorkers(nodes engine.Nodes) schedule.Workers {
	if len(nodes) == 0 {
		return nil
	}
	workers := make(schedule.Workers, 0, len(nodes))
	for _, node := range nodes {
		worker := toScheduleWorker(node)
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
		Id:        worker.ID,
		Addr:      worker.Addr,
		Mcpu:      normalizeMcpu(worker.Mcpu),
		WorkState: toEngineWorkState(worker.State),
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

func toScheduleWorkerState(state metadata.WorkState) schedule.WorkerState {
	switch state {
	case metadata.WorkState_Working:
		return schedule.WorkerStateWorking
	case metadata.WorkState_Draining:
		return schedule.WorkerStateDraining
	case metadata.WorkState_Drained:
		return schedule.WorkerStateDrained
	default:
		return schedule.WorkerStateUnknown
	}
}

func toEngineWorkState(state schedule.WorkerState) metadata.WorkState {
	switch state {
	case schedule.WorkerStateWorking:
		return metadata.WorkState_Working
	case schedule.WorkerStateDraining:
		return metadata.WorkState_Draining
	case schedule.WorkerStateDrained:
		return metadata.WorkState_Drained
	default:
		return metadata.WorkState_Unknown
	}
}
