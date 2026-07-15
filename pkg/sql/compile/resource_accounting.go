// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package compile

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

type executionResourceRecorder struct {
	root      *resource.Root
	stats     *statistic.StatsInfo
	execution resource.ExecutionSummary
	published bool
}

type attemptMemoryRecorder struct {
	pool  *mpool.MPool
	exact bool
}

type remoteTerminalEnvelope struct {
	Plan          *models.PhyPlan              `json:"plan,omitempty"`
	Delta         resource.Delta               `json:"resource"`
	Memory        resource.MemoryDomainSummary `json:"memory"`
	MemoryQuality resource.QualityFlags        `json:"memory_quality,omitempty"`
}

func newExecutionResourceRecorder(ctx context.Context) *executionResourceRecorder {
	root := resource.RootFromContext(ctx)
	if root == nil {
		return nil
	}
	return &executionResourceRecorder{
		root:  root,
		stats: statistic.StatsInfoFromContext(ctx),
	}
}

func (r *executionResourceRecorder) publish() {
	if r != nil && !r.published {
		if phases, ok := r.stats.ClaimRootPhaseResource(); ok {
			r.execution.Quality |= phases.Quality |
				resource.MergeUsage(&r.execution.Usage, phases.Usage)
		}
		r.root.MergeExecution(r.execution)
		r.published = true
	}
}

func beginAttemptMemory(pool *mpool.MPool) attemptMemoryRecorder {
	// The coordinator uses the session MPool, which is shared by nested and
	// background execution. Resetting its epoch here makes overlapping attempts
	// erase each other's peaks. Until coordinator attempts own isolated pools,
	// leave this domain explicitly missing instead of publishing a false peak.
	return attemptMemoryRecorder{
		pool:  pool,
		exact: false,
	}
}

func (r *executionResourceRecorder) finishAttempt(
	generation uint64,
	attemptStart time.Time,
	preRunWall time.Duration,
	stats *statistic.StatsInfo,
	scopes []*Scope,
	anal *AnalyzeModule,
	localAddress string,
	outcome resource.Outcome,
	retried bool,
	memory attemptMemoryRecorder,
) {
	if r == nil {
		return
	}
	attempt := resource.NewAttempt(generation, 0, 1)
	requireMemoryReport := attempt.MarkMemoryDomainDispatched(0)
	delta := collectScopeResourceDelta(scopes, localAddress)
	remoteUsage, remoteMemory, remoteQuality, remoteReports := anal.remoteResourceSummary()
	delta.Quality |= remoteQuality | resource.MergeUsage(&delta.Usage, remoteUsage)
	if expected := countExpectedRemoteScopes(scopes, localAddress); remoteReports < expected {
		delta.Quality |= resource.QualityPartial | resource.QualityMissingFragment
	}

	var coordinator resource.LocalRecorder
	preRunNS := durationNS(preRunWall, &delta.Quality)
	var lockWaitNS uint64
	if stats != nil {
		lockWaitNS = signedCounter(stats.PrepareRunStage.CompilePreRunOnceWaitLock, &delta.Quality)
	}
	coordinator.AddActiveInterval(preRunNS, lockWaitNS, 0)
	coordinator.AddWait(resource.WaitLock, lockWaitNS)
	if stats != nil {
		addS3Requests(&coordinator, stats.PrepareRunStage.ScopePrepareS3Request, &delta.Quality)
	}
	coordinatorDelta := coordinator.Snapshot()
	delta.Quality |= coordinatorDelta.Quality | resource.MergeUsage(&delta.Usage, coordinatorDelta.Usage)

	if memory.exact && requireMemoryReport == resource.PublishAccepted {
		domain, quality := memory.pool.ResourceSnapshot()
		delta.Quality |= quality
		attempt.PublishMemoryDomain(generation, 0, domain)
	}

	attempt.AddLocal(delta)
	attempt.BeginClosing()
	wallNS := durationNS(time.Since(attemptStart), &delta.Quality)
	summary := attempt.Seal(wallNS, outcome)
	summary.Quality |= delta.Quality | resource.MergeMemoryTotals(&summary.Memory, remoteMemory)
	r.execution.AddAttempt(summary, retried)
}

func addS3Requests(
	recorder *resource.LocalRecorder,
	requests statistic.S3Request,
	quality *resource.QualityFlags,
) {
	add := func(op resource.S3Op, value int64) {
		if value < 0 {
			*quality |= resource.QualityInvariantFailure
			return
		}
		recorder.AddS3Request(op, uint64(value))
	}
	add(resource.S3List, requests.List)
	add(resource.S3Head, requests.Head)
	add(resource.S3Put, requests.Put)
	add(resource.S3Get, requests.Get)
	add(resource.S3Delete, requests.Delete)
	add(resource.S3DeleteMulti, requests.DeleteMul)
}

func collectScopeResourceDelta(scopes []*Scope, localAddress string) resource.Delta {
	var result resource.Delta
	seen := make(map[vm.Operator]struct{})
	var visit func(*Scope)
	visit = func(scope *Scope) {
		if scope == nil {
			return
		}
		if scope.Magic == Remote && !scope.ipAddrMatch(localAddress) {
			return
		}
		for _, pre := range scope.PreScopes {
			visit(pre)
		}
		_ = vm.HandleAllOp(scope.RootOp, func(_ vm.Operator, op vm.Operator) error {
			if op == nil {
				return nil
			}
			if _, ok := seen[op]; ok {
				return nil
			}
			seen[op] = struct{}{}
			analyzer := op.GetOperatorBase().OpAnalyzer
			if analyzer == nil {
				return nil
			}
			delta := analyzer.GetOpStats().ResourceDelta()
			result.Quality |= delta.Quality | resource.MergeUsage(&result.Usage, delta.Usage)
			return nil
		})
	}
	for _, scope := range scopes {
		visit(scope)
	}
	return result
}

func countExpectedRemoteScopes(scopes []*Scope, localAddress string) uint64 {
	var count uint64
	var visit func(*Scope)
	visit = func(scope *Scope) {
		if scope == nil {
			return
		}
		if scope.Magic == Remote && !scope.ipAddrMatch(localAddress) {
			count++
			return
		}
		for _, pre := range scope.PreScopes {
			visit(pre)
		}
	}
	for _, scope := range scopes {
		visit(scope)
	}
	return count
}

func durationNS(value time.Duration, flags *resource.QualityFlags) uint64 {
	if value < 0 {
		*flags |= resource.QualityInvariantFailure
		return 0
	}
	return uint64(value.Nanoseconds())
}

func signedCounter(value int64, flags *resource.QualityFlags) uint64 {
	if value < 0 {
		*flags |= resource.QualityInvariantFailure
		return 0
	}
	return uint64(value)
}
