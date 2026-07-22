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

package statistic

import (
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/resource"
)

// RootPhaseResource converts the measured parse, plan, and compile phase
// intervals into the typed resource model. It deliberately excludes execution
// and permission-auth child statements, which have their own execution owners.
func (stats *StatsInfo) RootPhaseResource() resource.Delta {
	if stats == nil {
		return resource.Delta{}
	}
	var recorder resource.LocalRecorder
	var quality resource.QualityFlags
	duration := func(value int64) uint64 {
		if value < 0 {
			quality |= resource.QualityInvariantFailure
			return 0
		}
		return uint64(value)
	}
	count := func(op resource.S3Op, value int64) {
		if value < 0 {
			quality |= resource.QualityInvariantFailure
			return
		}
		recorder.AddS3Request(op, uint64(value))
	}
	recorder.AddActiveInterval(duration(stats.ParseStage.ParseDuration.Nanoseconds()), 0, 0)
	planWall := duration(stats.PlanStage.PlanDuration.Nanoseconds())
	planWait := duration(stats.PlanStage.BuildPlanStatsIOConsumption)
	if !addMeasuredActive(&recorder, planWall, planWait) {
		quality |= resource.QualityPartial
	}
	recorder.AddWait(resource.WaitFilesystem, planWait)
	compileWall := duration(stats.CompileStage.CompileDuration.Nanoseconds())
	compileWait := duration(atomic.LoadInt64(&stats.CompileStage.CompileIOConsumption))
	if !addMeasuredActive(&recorder, compileWall, compileWait) {
		quality |= resource.QualityPartial
	}
	recorder.AddWait(resource.WaitFilesystem, compileWait)

	for _, requests := range [...]S3Request{
		stats.PlanStage.BuildPlanS3Request,
		stats.CompileStage.CompileS3Request,
	} {
		count(resource.S3List, requests.List)
		count(resource.S3Head, requests.Head)
		count(resource.S3Put, requests.Put)
		count(resource.S3Get, requests.Get)
		count(resource.S3Delete, requests.Delete)
		count(resource.S3DeleteMulti, requests.DeleteMul)
	}
	delta := recorder.Snapshot()
	delta.Quality |= quality
	return delta
}

func addMeasuredActive(recorder *resource.LocalRecorder, wallNS, waitNS uint64) bool {
	if waitNS <= wallNS {
		recorder.AddActiveInterval(wallNS, waitNS, 0)
		return true
	}
	return false
}

// ResetRetryAttemptResource starts fresh generation-specific build and prepare
// counters after the previous attempt has consumed them.
func (stats *StatsInfo) ResetRetryAttemptResource() {
	if stats == nil {
		return
	}
	stats.PlanStage.PlanDuration = 0
	stats.PlanStage.PlanStartTime = time.Time{}
	resetS3Request(&stats.PlanStage.BuildPlanS3Request)
	atomic.StoreInt64(&stats.PlanStage.BuildPlanStatsIOConsumption, 0)
	stats.CompileStage.CompileDuration = 0
	stats.CompileStage.CompileStartTime = time.Time{}
	resetS3Request(&stats.CompileStage.CompileS3Request)
	atomic.StoreInt64(&stats.CompileStage.CompileIOConsumption, 0)
	resetS3Request(&stats.PrepareRunStage.ScopePrepareS3Request)
}

func resetS3Request(request *S3Request) {
	atomic.StoreInt64(&request.List, 0)
	atomic.StoreInt64(&request.Head, 0)
	atomic.StoreInt64(&request.Put, 0)
	atomic.StoreInt64(&request.Get, 0)
	atomic.StoreInt64(&request.Delete, 0)
	atomic.StoreInt64(&request.DeleteMul, 0)
}

// ClaimRootPhaseResource transfers this StatsInfo's phase resource facts to
// exactly one owner. Nested/background executions use distinct StatsInfo
// objects and therefore remain independently attributable.
func (stats *StatsInfo) ClaimRootPhaseResource() (resource.Delta, bool) {
	if stats == nil || !atomic.CompareAndSwapUint32(&stats.resourceClaimed, 0, 1) {
		return resource.Delta{}, false
	}
	return stats.RootPhaseResource(), true
}
