// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package statistic

import (
	"sync/atomic"

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
	recorder.AddActiveInterval(planWall, planWait, 0)
	recorder.AddWait(resource.WaitFilesystem, planWait)
	compileWall := duration(stats.CompileStage.CompileDuration.Nanoseconds())
	compileWait := duration(atomic.LoadInt64(&stats.CompileStage.CompileIOConsumption))
	recorder.AddActiveInterval(compileWall, compileWait, 0)
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

// ClaimRootPhaseResource transfers this StatsInfo's phase resource facts to
// exactly one owner. Nested/background executions use distinct StatsInfo
// objects and therefore remain independently attributable.
func (stats *StatsInfo) ClaimRootPhaseResource() (resource.Delta, bool) {
	if stats == nil || !atomic.CompareAndSwapUint32(&stats.resourceClaimed, 0, 1) {
		return resource.Delta{}, false
	}
	return stats.RootPhaseResource(), true
}
