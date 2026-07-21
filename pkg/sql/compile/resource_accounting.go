// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	root         *resource.Root
	stats        *statistic.StatsInfo
	execution    resource.ExecutionSummary
	published    bool
	ownsAttempts bool
}

const remoteTerminalResourceVersion = 1

// remoteTerminalEnvelope keeps PhyPlan fields at the top level so clients from
// before resource accounting can still decode the terminal plan during a
// rolling upgrade. New clients use TerminalResourceVersion to distinguish the
// appended resource facts from a legacy bare PhyPlan payload.
type remoteTerminalEnvelope struct {
	models.PhyPlan
	TerminalResourceVersion  uint32                `json:"terminal_resource_version,omitempty"`
	Delta                    resource.Delta        `json:"resource_delta"`
	Memory                   resource.MemoryTotals `json:"memory"`
	MissingFragmentCount     uint64                `json:"missing_fragment_count,omitempty"`
	MissingMemoryDomainCount uint64                `json:"missing_memory_domain_count,omitempty"`
}

// remoteResourceAggregate is the already-reduced terminal output sent by one
// remote hop. Delta contains local plus descendant usage and quality.
type remoteResourceAggregate struct {
	Delta                    resource.Delta
	Memory                   resource.MemoryTotals
	MissingFragmentCount     uint64
	MissingMemoryDomainCount uint64
}

func newExecutionResourceRecorder(
	ctx context.Context,
	attemptOwnerEligible bool,
) *executionResourceRecorder {
	root := resource.RootFromContext(ctx)
	if root == nil {
		return nil
	}
	recorder := &executionResourceRecorder{
		root:         root,
		stats:        statistic.StatsInfoFromContext(ctx),
		ownsAttempts: attemptOwnerEligible && root.TryClaimAttemptOwner(),
	}
	if phases, ok := recorder.stats.ClaimRootPhaseResource(); ok {
		recorder.root.AddLocal(phases)
	}
	return recorder
}

func (r *executionResourceRecorder) publish() {
	if r != nil && !r.published {
		execution := r.execution
		if !r.ownsAttempts {
			// Child execution resource facts belong to the statement, but child
			// compile/run generations are not retries of the top-level statement.
			execution.AttemptCount = 0
			execution.RetryWallNS = 0
		}
		r.root.MergeExecution(execution)
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
	retried bool,
) {
	if r == nil {
		return
	}
	delta := collectScopeResourceDelta(scopes, localAddress)
	remote := remoteResourceSnapshot{}
	if anal != nil {
		remote = anal.remoteResourceSummary()
	}
	remoteAggregate := composeRemoteResourceAggregate(
		delta,
		resource.MemoryDomainSummary{},
		0,
		remote,
		countExpectedRemoteScopes(scopes, localAddress),
	)
	delta = remoteAggregate.Delta

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
	addMeasuredActive(&coordinator, preRunNS, totalWaitNS)
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

	wallNS := durationNS(time.Since(attemptStart), &delta.Quality)
	summary := resource.AttemptSummary{WallNS: wallNS}
	summary.Quality |= delta.Quality | resource.MergeUsage(&summary.Usage, delta.Usage)
	summary.Quality |= resource.MergeMemoryTotals(&summary.Memory, remoteAggregate.Memory)
	var quality resource.QualityFlags
	summary.MissingFragmentCount, quality = addCheckedRemoteCounter(
		summary.MissingFragmentCount, remoteAggregate.MissingFragmentCount, summary.Quality)
	summary.Quality = quality
	summary.MissingMemoryDomainCount, quality = addCheckedRemoteCounter(
		summary.MissingMemoryDomainCount, remoteAggregate.MissingMemoryDomainCount, summary.Quality)
	summary.Quality = quality
	r.execution.AddAttempt(summary, retried)
}

// addMeasuredActive publishes active time only when the producer-local wait
// interval is a valid subset of its wall interval. Some legacy phase counters
// aggregate parallel I/O waits and can legitimately exceed coordinator wall;
// their wait remains useful, but subtracting it would fabricate active time.
func addMeasuredActive(recorder *resource.LocalRecorder, wallNS, waitNS uint64) {
	if waitNS <= wallNS {
		recorder.AddActiveInterval(wallNS, waitNS, 0)
	}
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

// addCheckedRemoteCounter adds an aggregate report/missing counter without
// allowing wraparound to masquerade as a valid complete result.
func addCheckedRemoteCounter(value, add uint64, quality resource.QualityFlags) (uint64, resource.QualityFlags) {
	if math.MaxUint64-value < add {
		return math.MaxUint64, quality | resource.QualityInvariantFailure
	}
	return value + add, quality
}

// composeRemoteResourceAggregate composes one hop's captured local resource
// facts with an already-reduced descendant aggregate. It is pure so every
// remote hop and the coordinator use the same merge algebra.
func composeRemoteResourceAggregate(
	localDelta resource.Delta,
	localMemory resource.MemoryDomainSummary,
	localMemoryQuality resource.QualityFlags,
	descendant remoteResourceSnapshot,
	expectedDirect uint64,
) remoteResourceAggregate {
	result := remoteResourceAggregate{Delta: localDelta}
	result.Delta.Quality |= descendant.Quality | localMemoryQuality
	result.Delta.Quality |= resource.MergeUsage(&result.Delta.Usage, descendant.Usage)
	result.Delta.Quality |= resource.MergeMemoryDomain(&result.Memory, localMemory)
	result.Delta.Quality |= resource.MergeMemoryTotals(&result.Memory, descendant.Memory)
	result.MissingFragmentCount = descendant.MissingFragmentCount
	result.MissingMemoryDomainCount = descendant.MissingMemoryDomainCount
	if descendant.MissingFragmentCount > 0 {
		result.Delta.Quality |= resource.QualityPartial | resource.QualityMissingFragment
	}
	if descendant.MissingMemoryDomainCount > 0 {
		result.Delta.Quality |= resource.QualityPartial | resource.QualityMissingMemoryDomain
	}
	if descendant.DirectReportCount < expectedDirect {
		directMissing := expectedDirect - descendant.DirectReportCount
		result.MissingFragmentCount, result.Delta.Quality = addCheckedRemoteCounter(
			result.MissingFragmentCount, directMissing, result.Delta.Quality)
		result.MissingMemoryDomainCount, result.Delta.Quality = addCheckedRemoteCounter(
			result.MissingMemoryDomainCount, directMissing, result.Delta.Quality)
		result.Delta.Quality |= resource.QualityPartial |
			resource.QualityMissingFragment | resource.QualityMissingMemoryDomain
	}
	return result
}

func collectScopeResourceDelta(scopes []*Scope, localAddress string) resource.Delta {
	var result resource.Delta
	seen := make(map[vm.Operator]struct{})
	var visit func(*Scope)
	visit = func(scope *Scope) {
		if scope == nil {
			return
		}
		if scope.Magic == Remote && !scope.ipAddrMatch(localAddress) && !scope.resourceExecutedLocally {
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
		if scope.Magic == Remote && !scope.ipAddrMatch(localAddress) && !scope.resourceExecutedLocally {
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
