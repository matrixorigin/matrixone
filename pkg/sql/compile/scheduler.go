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
	nodes := toEngineNodes(placement.Workers)
	if c.execType == plan2.ExecTypeAP_MULTICN {
		if placement.Reason == schedule.ReasonNoCandidateCN {
			getQueryScheduleLogger().WarnWithConfig(
				"query-schedule-no-candidate-cn",
				"query schedule falls back to local CN",
				queryScheduleLogRateLimit,
				c.querySchedulePlacementFields(placement)...)
		}
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

	rawCandidateCount := len(candidates)
	req.Candidates = toScheduleWorkers(candidates)
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

func toScheduleWorkers(nodes engine.Nodes) schedule.Workers {
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

func toEngineNode(worker schedule.Worker) engine.Node {
	return engine.Node{
		Id:   worker.ID,
		Addr: worker.Addr,
		Mcpu: normalizeMcpu(worker.Mcpu),
	}
}

func toEngineNodes(workers schedule.Workers) engine.Nodes {
	if len(workers) == 0 {
		return nil
	}
	nodes := make(engine.Nodes, 0, len(workers))
	for _, worker := range workers {
		nodes = append(nodes, toEngineNode(worker))
	}
	return nodes
}

func normalizeMcpu(mcpu int) int {
	if mcpu < 1 {
		return 1
	}
	return mcpu
}
