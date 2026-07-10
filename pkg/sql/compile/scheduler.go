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
	"reflect"
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

type queryCandidatePoolRequest = engine.QueryCandidatePoolRequest

type queryCandidateMode uint8

const (
	queryCandidateModeExecution queryCandidateMode = iota
	queryCandidateModePreview
)

type discoveredQueryCandidates struct {
	legacyNodes engine.Nodes
	candidates  engine.QueryCandidates
	source      schedule.CandidateSource
}

type queryCandidateDiscoverer interface {
	discover(context.Context) (discoveredQueryCandidates, error)
}

// legacyEngineNodesCandidateDiscoverer captures pool constraints only because
// Engine.Nodes still combines discovery with pool/label resolution. The
// discoverer interface itself intentionally has no pool-policy input.
type legacyEngineNodesCandidateDiscoverer struct {
	engine      engine.Engine
	poolRequest queryCandidatePoolRequest
}

func (d legacyEngineNodesCandidateDiscoverer) discover(ctx context.Context) (discoveredQueryCandidates, error) {
	if err := ctx.Err(); err != nil {
		return discoveredQueryCandidates{}, err
	}
	if nilEngineValue(d.engine) {
		return discoveredQueryCandidates{}, moerr.NewInternalErrorNoCtx("compile engine is not initialized")
	}
	nodes, err := d.engine.Nodes(
		d.poolRequest.IsInternal,
		d.poolRequest.Tenant,
		d.poolRequest.Username,
		cloneCNLabels(d.poolRequest.CNLabel),
	)
	if err != nil {
		return discoveredQueryCandidates{}, err
	}
	return discoveredQueryCandidates{
		legacyNodes: nodes,
		source:      schedule.CandidateSourceEngineNodes,
	}, nil
}

type engineQueryCandidateDiscoverer struct {
	provider engine.QueryCandidateDiscoverer
}

func (d engineQueryCandidateDiscoverer) discover(ctx context.Context) (discoveredQueryCandidates, error) {
	candidates, err := d.provider.DiscoverQueryCandidates(ctx)
	if err != nil {
		return discoveredQueryCandidates{}, err
	}
	return discoveredQueryCandidates{
		candidates: candidates,
		source:     schedule.CandidateSourceClusterInventory,
	}, nil
}

func unwrapSchedulingEngine(e engine.Engine) (engine.Engine, bool) {
	const maxEntireEngineDepth = 8
	for depth := 0; depth < maxEntireEngineDepth; depth++ {
		if nilEngineValue(e) {
			return nil, false
		}
		entire, ok := e.(*engine.EntireEngine)
		if !ok {
			return e, true
		}
		if entire == nil || entire.Engine == nil {
			return nil, false
		}
		e = entire.Engine
	}
	return nil, false
}

func nilEngineValue(e engine.Engine) bool {
	if e == nil {
		return true
	}
	value := reflect.ValueOf(e)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

type resolvedQueryCandidates struct {
	workers    schedule.Workers
	resolution schedule.CandidateResolution
}

type queryCandidatePoolResolver interface {
	resolve(context.Context, discoveredQueryCandidates, queryCandidatePoolRequest) (resolvedQueryCandidates, error)
}

// legacyEngineNodesPoolResolver is an explicit compatibility boundary:
// Engine.Nodes currently applies tenant/label pool constraints while discovering
// nodes, so this resolver is intentionally a one-to-one adapter.
type legacyEngineNodesPoolResolver struct{}

func (legacyEngineNodesPoolResolver) resolve(
	ctx context.Context,
	discovered discoveredQueryCandidates,
	_ queryCandidatePoolRequest,
) (resolvedQueryCandidates, error) {
	if err := ctx.Err(); err != nil {
		return resolvedQueryCandidates{}, err
	}
	workers := toScheduleCandidateWorkers(discovered.legacyNodes)
	return resolvedQueryCandidates{
		workers: workers,
		resolution: schedule.CandidateResolution{
			DiscoverySource: discovered.source,
			PoolResolution:  schedule.PoolResolutionLegacyEngineNodes,
			DiscoveredCount: len(discovered.legacyNodes),
		},
	}, nil
}

type engineQueryCandidatePoolResolver struct {
	provider engine.QueryCandidatePoolResolver
}

func (r engineQueryCandidatePoolResolver) resolve(
	ctx context.Context,
	discovered discoveredQueryCandidates,
	request queryCandidatePoolRequest,
) (resolvedQueryCandidates, error) {
	request.CNLabel = cloneCNLabels(request.CNLabel)
	nodes, err := r.provider.ResolveQueryCandidatePool(
		ctx,
		discovered.candidates,
		request,
	)
	if err != nil {
		return resolvedQueryCandidates{}, err
	}
	return resolvedQueryCandidates{
		workers: toScheduleCandidateWorkers(nodes),
		resolution: schedule.CandidateResolution{
			DiscoverySource: discovered.source,
			PoolResolution:  schedule.PoolResolutionTenantLabels,
			DiscoveredCount: len(discovered.candidates),
		},
	}, nil
}

func queryCandidatePipeline(
	e engine.Engine,
	poolRequest queryCandidatePoolRequest,
	mode queryCandidateMode,
) (queryCandidateDiscoverer, queryCandidatePoolResolver, error) {
	base, ok := unwrapSchedulingEngine(e)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("compile engine is not initialized")
	}
	discoverer, hasDiscoverer := base.(engine.QueryCandidateDiscoverer)
	resolver, hasResolver := base.(engine.QueryCandidatePoolResolver)
	switch {
	case hasDiscoverer && hasResolver:
		return engineQueryCandidateDiscoverer{provider: discoverer},
			engineQueryCandidatePoolResolver{provider: resolver}, nil
	case !hasDiscoverer && !hasResolver:
		// Engine.Nodes has no context, so preview cannot bound its latency.
		if mode == queryCandidateModePreview {
			return nil, nil, moerr.NewInternalErrorNoCtx(
				"query scheduling preview requires context-aware candidate discovery and pool resolution")
		}
		return legacyEngineNodesCandidateDiscoverer{engine: base, poolRequest: poolRequest},
			legacyEngineNodesPoolResolver{}, nil
	default:
		return nil, nil, moerr.NewInternalErrorNoCtx(
			"compile engine must implement both query candidate discovery and pool resolution")
	}
}

func cloneCNLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

func (c *Compile) evaluateQueryPlacement(
	ctx context.Context,
	mode queryCandidateMode,
) (schedule.QueryDecision, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	currentCN := c.currentCNWorker()
	req := schedule.QueryRequest{
		ExecKind:        toScheduleExecKind(c.execType),
		CurrentCN:       currentCN,
		CurrentCNPolicy: c.currentCNPolicy(),
	}
	if c.execType != plan2.ExecTypeAP_MULTICN {
		req.CandidateResolution = schedule.CandidateResolution{
			DiscoverySource: schedule.CandidateSourceNotRequired,
			PoolResolution:  schedule.PoolResolutionNotRequired,
		}
		return schedule.DecideQueryPlacement(req), "", nil
	}
	if err := ctx.Err(); err != nil {
		return schedule.QueryDecision{}, scheduleFailureCandidateDiscovery, err
	}

	poolRequest := queryCandidatePoolRequest{
		IsInternal: c.isInternal,
		Tenant:     c.tenant,
		Username:   c.uid,
		CNLabel:    c.cnLabel,
	}
	discoverer, poolResolver, err := queryCandidatePipeline(c.e, poolRequest, mode)
	if err != nil {
		return schedule.QueryDecision{}, scheduleFailureCandidateProvider, err
	}
	discovered, err := discoverer.discover(ctx)
	if err != nil {
		return schedule.QueryDecision{}, scheduleFailureCandidateDiscovery, err
	}
	if discovered.source == schedule.CandidateSourceClusterInventory {
		currentCN = currentCNWorkerFromCandidates(currentCN, discovered.candidates)
	} else {
		currentCN, err = c.currentCNWorkerWithRuntimeState(ctx)
		if err != nil {
			return schedule.QueryDecision{}, scheduleFailureCandidateDiscovery, err
		}
	}
	req.CurrentCN = currentCN
	resolved, err := poolResolver.resolve(ctx, discovered, poolRequest)
	if err != nil {
		return schedule.QueryDecision{}, scheduleFailurePoolResolution, err
	}
	req.Candidates = resolved.workers
	req.CandidateResolution = resolved.resolution
	return schedule.DecideQueryPlacement(req), "", nil
}

func currentCNWorkerFromCandidates(
	current schedule.Worker,
	candidates engine.QueryCandidates,
) schedule.Worker {
	for _, candidate := range candidates {
		service := candidate.Service
		if current.ID != "" {
			if service.ServiceID != current.ID {
				continue
			}
		} else if current.Addr == "" || service.PipelineServiceAddress != current.Addr {
			continue
		}
		current.State = toScheduleWorkerState(service.WorkState)
		return current
	}
	return current
}

func (c *Compile) decideQueryPlacement() (schedule.QueryDecision, error) {
	decision, failureCategory, err := c.evaluateQueryPlacement(
		c.querySchedulingContext(),
		queryCandidateModeExecution,
	)
	if err != nil {
		c.recordSchedulingFailure(failureCategory, schedule.Worker{})
		return schedule.QueryDecision{}, err
	}
	c.recordQuerySchedulingTrace(decision)
	c.recordQuerySchedulingMetrics(decision)
	if len(decision.Dropped) > 0 {
		getQueryScheduleLogger().WarnWithConfig(
			"query-schedule-dropped-cn-candidates",
			"query schedule dropped CN candidates",
			queryScheduleLogRateLimit,
			c.queryScheduleDroppedCandidateFields(decision)...)
	}
	return decision, nil
}

func (c *Compile) querySchedulingContext() context.Context {
	if c.proc == nil {
		return context.Background()
	}
	if c.proc.Base != nil {
		if ctx := c.proc.GetTopContext(); ctx != nil {
			return ctx
		}
	}
	if c.proc.Ctx != nil {
		return c.proc.Ctx
	}
	return context.Background()
}

func (c *Compile) SetSchedulingTraceRecorder(recorder *schedule.TraceRecorder) {
	c.schedulingTrace = recorder
	c.schedulingAttempt = 0
}

func (c *Compile) beginSchedulingTraceAttempt() {
	c.schedulingAttempt = c.schedulingTrace.StartAttempt()
}

func (c *Compile) recordQuerySchedulingTrace(
	decision schedule.QueryDecision,
) {
	c.schedulingTrace.RecordQuery(
		c.schedulingAttempt,
		decision,
	)
}

func (c *Compile) recordSchedulingFailure(category string, worker schedule.Worker) {
	c.schedulingTrace.RecordFailure(c.schedulingAttempt, category, worker)
}

func (c *Compile) queryScheduleDroppedCandidateFields(
	placement schedule.QueryDecision,
) []zap.Field {
	counts := droppedWorkerReasonCounts(placement.Dropped)
	fields := c.querySchedulePlacementFields(placement)
	fields = append(fields,
		zap.String("candidate-source", string(placement.CandidateResolution.DiscoverySource)),
		zap.String("pool-resolution", string(placement.CandidateResolution.PoolResolution)),
		zap.Int("candidate-count", placement.CandidateResolution.DiscoveredCount),
		zap.Int("candidate-worker-count", placement.ResolvedCandidateCount),
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

func (c *Compile) currentCNWorkerWithRuntimeState(ctx context.Context) (schedule.Worker, error) {
	worker := c.currentCNWorker()
	state, err := c.currentCNWorkState(ctx, worker.ID)
	worker.State = toScheduleWorkerState(state)
	return worker, err
}

func (c *Compile) currentCNWorkState(ctx context.Context, serviceID string) (metadata.WorkState, error) {
	// State lookup is best-effort. Missing runtime or cluster metadata falls
	// back to Unknown, which the scheduler treats as schedulable. Only an
	// explicit Draining/Drained signal should block current-CN placement.
	if serviceID == "" {
		return metadata.WorkState_Unknown, nil
	}
	rt := moruntime.ServiceRuntime(serviceID)
	if rt == nil {
		return metadata.WorkState_Unknown, nil
	}
	v, ok := rt.GetGlobalVariables(moruntime.ClusterService)
	if !ok {
		return metadata.WorkState_Unknown, nil
	}
	cluster, ok := v.(clusterservice.MOCluster)
	if !ok || cluster == nil {
		return metadata.WorkState_Unknown, nil
	}

	state := metadata.WorkState_Unknown
	err := clusterservice.GetCNServiceWithoutWorkingStateWithContext(
		ctx,
		cluster,
		clusterservice.NewServiceIDSelector(serviceID),
		func(cn metadata.CNService) bool {
			state = cn.WorkState
			return false
		})
	return state, err
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
