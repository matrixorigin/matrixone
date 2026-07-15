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
	"math"
	"sync/atomic"
	"time"

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
	recorder := &executionResourceRecorder{
		root:  root,
		stats: statistic.StatsInfoFromContext(ctx),
	}
	if phases, ok := recorder.stats.ClaimRootPhaseResource(); ok {
		recorder.root.AddLocal(phases)
	}
	return recorder
}

func (r *executionResourceRecorder) publish() {
	if r != nil && !r.published {
		r.root.MergeExecution(r.execution)
		r.published = true
	}
}

func (r *executionResourceRecorder) finishAttempt(
	generation uint64,
	attemptStart time.Time,
	preRunWall time.Duration,
	remoteWait time.Duration,
	stats *statistic.StatsInfo,
	scopes []*Scope,
	anal *AnalyzeModule,
	localAddress string,
	outcome resource.Outcome,
	retried bool,
) {
	if r == nil {
		return
	}
	attempt := resource.NewAttempt(generation, 0, 0)
	delta := collectScopeResourceDelta(scopes, localAddress)
	var (
		remoteUsage   resource.Usage
		remoteMemory  resource.MemoryTotals
		remoteQuality resource.QualityFlags
		remoteReports uint64
	)
	if anal != nil {
		remoteUsage, remoteMemory, remoteQuality, remoteReports = anal.remoteResourceSummary()
	}
	delta.Quality |= remoteQuality | resource.MergeUsage(&delta.Usage, remoteUsage)
	var missingRemote uint64
	if expected := countExpectedRemoteScopes(scopes, localAddress); remoteReports < expected {
		missingRemote = expected - remoteReports
		delta.Quality |= resource.QualityPartial | resource.QualityMissingFragment
	}

	var coordinator resource.LocalRecorder
	preRunNS := durationNS(preRunWall, &delta.Quality)
	var lockWaitNS uint64
	var filesystemWaitNS uint64
	remoteWaitNS := durationNS(remoteWait, &delta.Quality)
	if stats != nil {
		lockWaitNS = signedCounter(stats.PrepareRunStage.CompilePreRunOnceWaitLock, &delta.Quality)
		if generation > 0 {
			filesystemWaitNS = signedCounter(stats.PlanStage.BuildPlanStatsIOConsumption, &delta.Quality)
			compileWait := signedCounter(atomic.LoadInt64(&stats.CompileStage.CompileIOConsumption), &delta.Quality)
			if filesystemWaitNS > math.MaxUint64-compileWait {
				filesystemWaitNS = math.MaxUint64
				delta.Quality |= resource.QualityInvariantFailure
			} else {
				filesystemWaitNS += compileWait
			}
		}
	}
	totalWaitNS := lockWaitNS
	if totalWaitNS > math.MaxUint64-filesystemWaitNS {
		totalWaitNS = math.MaxUint64
		delta.Quality |= resource.QualityInvariantFailure
	} else {
		totalWaitNS += filesystemWaitNS
	}
	if totalWaitNS > math.MaxUint64-remoteWaitNS {
		totalWaitNS = math.MaxUint64
		delta.Quality |= resource.QualityInvariantFailure
	} else {
		totalWaitNS += remoteWaitNS
	}
	coordinator.AddActiveInterval(preRunNS, totalWaitNS, 0)
	coordinator.AddWait(resource.WaitLock, lockWaitNS)
	coordinator.AddWait(resource.WaitFilesystem, filesystemWaitNS)
	coordinator.AddWait(resource.WaitRemote, remoteWaitNS)
	if stats != nil {
		if generation > 0 {
			addS3Requests(&coordinator, stats.PlanStage.BuildPlanS3Request, &delta.Quality)
			addS3Requests(&coordinator, stats.CompileStage.CompileS3Request, &delta.Quality)
		}
		addS3Requests(&coordinator, stats.PrepareRunStage.ScopePrepareS3Request, &delta.Quality)
	}
	coordinatorDelta := coordinator.Snapshot()
	delta.Quality |= coordinatorDelta.Quality | resource.MergeUsage(&delta.Usage, coordinatorDelta.Usage)

	attempt.AddLocal(delta)
	attempt.BeginClosing()
	wallNS := durationNS(time.Since(attemptStart), &delta.Quality)
	summary := attempt.Seal(wallNS, outcome)
	summary.Quality |= delta.Quality | resource.MergeMemoryTotals(&summary.Memory, remoteMemory)
	if missingRemote > 0 {
		addMissing := func(value *uint64) {
			if *value > math.MaxUint64-missingRemote {
				*value = math.MaxUint64
				summary.Quality |= resource.QualityInvariantFailure
				return
			}
			*value += missingRemote
		}
		addMissing(&summary.MissingFragmentCount)
		addMissing(&summary.MissingMemoryDomainCount)
		summary.Quality |= resource.QualityPartial | resource.QualityMissingMemoryDomain
	}
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
