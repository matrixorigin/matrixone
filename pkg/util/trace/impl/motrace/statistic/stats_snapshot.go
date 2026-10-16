// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statistic

import "sync/atomic"

// Snapshot returns a detached diagnostic value while atomic counters may still
// be updated. Each counter is loaded independently; the result is not a single
// consistent instant across counters. The caller must keep owner-managed phase
// timings, PermissionAuth and WaitActiveCost stable and must not call Reset
// concurrently.
func (stats *StatsInfo) Snapshot() StatsInfo {
	if stats == nil {
		return StatsInfo{}
	}
	var snapshot StatsInfo
	snapshot.resourceClaimed = atomic.LoadUint32(&stats.resourceClaimed)
	snapshot.ParseStage = stats.ParseStage
	snapshot.PlanStage.PlanDuration = stats.PlanStage.PlanDuration
	snapshot.PlanStage.PlanStartTime = stats.PlanStage.PlanStartTime
	snapshot.PlanStage.BuildPlanS3Request = stats.PlanStage.BuildPlanS3Request.snapshot()
	snapshot.PlanStage.BuildPlanStatsS3 = stats.PlanStage.BuildPlanStatsS3.snapshot()
	snapshot.PlanStage.BuildPlanStatsIOConsumption = atomic.LoadInt64(&stats.PlanStage.BuildPlanStatsIOConsumption)
	snapshot.PlanStage.BuildPlanStatsDuration = atomic.LoadInt64(&stats.PlanStage.BuildPlanStatsDuration)
	snapshot.PlanStage.BuildPlanStatsInCacheDuration = atomic.LoadInt64(&stats.PlanStage.BuildPlanStatsInCacheDuration)
	snapshot.PlanStage.BuildPlanResolveVarDuration = atomic.LoadInt64(&stats.PlanStage.BuildPlanResolveVarDuration)
	snapshot.CompileStage.CompileDuration = stats.CompileStage.CompileDuration
	snapshot.CompileStage.CompileStartTime = stats.CompileStage.CompileStartTime
	snapshot.CompileStage.CompileS3Request = stats.CompileStage.CompileS3Request.snapshot()
	snapshot.CompileStage.CompileExpandRangesS3 = stats.CompileStage.CompileExpandRangesS3.snapshot()
	snapshot.CompileStage.CompileIOConsumption = atomic.LoadInt64(&stats.CompileStage.CompileIOConsumption)
	snapshot.CompileStage.CompileTableScanDuration = atomic.LoadInt64(&stats.CompileStage.CompileTableScanDuration)
	snapshot.PrepareRunStage.CompilePreRunOnceDuration = atomic.LoadInt64(&stats.PrepareRunStage.CompilePreRunOnceDuration)
	snapshot.PrepareRunStage.CompilePreRunOnceWaitLock = atomic.LoadInt64(&stats.PrepareRunStage.CompilePreRunOnceWaitLock)
	snapshot.PrepareRunStage.ScopePrepareDuration = atomic.LoadInt64(&stats.PrepareRunStage.ScopePrepareDuration)
	snapshot.PrepareRunStage.ScopePrepareS3Request = stats.PrepareRunStage.ScopePrepareS3Request.snapshot()
	snapshot.PrepareRunStage.BuildReaderDuration = atomic.LoadInt64(&stats.PrepareRunStage.BuildReaderDuration)
	snapshot.ExecuteStage.ExecutionDuration = stats.ExecuteStage.ExecutionDuration
	snapshot.ExecuteStage.ExecutionStartTime = stats.ExecuteStage.ExecutionStartTime
	snapshot.ExecuteStage.ExecutionEndTime = stats.ExecuteStage.ExecutionEndTime
	snapshot.ExecuteStage.OutputDuration = atomic.LoadInt64(&stats.ExecuteStage.OutputDuration)
	snapshot.OtherStage.TxnIncrStatementS3 = stats.OtherStage.TxnIncrStatementS3.snapshot()
	snapshot.PermissionAuth = stats.PermissionAuth
	snapshot.IOAccessTimeConsumption = atomic.LoadInt64(&stats.IOAccessTimeConsumption)
	snapshot.S3FSPrefetchFileIOMergerTimeConsumption = atomic.LoadInt64(&stats.S3FSPrefetchFileIOMergerTimeConsumption)
	snapshot.LocalFSReadIOMergerTimeConsumption = atomic.LoadInt64(&stats.LocalFSReadIOMergerTimeConsumption)
	snapshot.S3FSReadIOMergerTimeConsumption = atomic.LoadInt64(&stats.S3FSReadIOMergerTimeConsumption)
	snapshot.WaitActiveCost = stats.WaitActiveCost
	return snapshot
}

func (s *S3Request) snapshot() S3Request {
	return S3Request{
		List:      atomic.LoadInt64(&s.List),
		Head:      atomic.LoadInt64(&s.Head),
		Put:       atomic.LoadInt64(&s.Put),
		Get:       atomic.LoadInt64(&s.Get),
		Delete:    atomic.LoadInt64(&s.Delete),
		DeleteMul: atomic.LoadInt64(&s.DeleteMul),
	}
}
