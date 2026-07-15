// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package statistic

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/stretchr/testify/require"
)

func TestRootPhaseResource(t *testing.T) {
	stats := NewStatsInfo()
	stats.ParseStage.ParseDuration = 10 * time.Nanosecond
	stats.PlanStage.PlanDuration = 30 * time.Nanosecond
	stats.PlanStage.BuildPlanStatsIOConsumption = 7
	stats.CompileStage.CompileDuration = 20 * time.Nanosecond
	stats.CompileStage.CompileIOConsumption = 5
	stats.PlanStage.BuildPlanS3Request = S3Request{Get: 2, Head: 1}
	stats.CompileStage.CompileS3Request = S3Request{Put: 3}

	delta := stats.RootPhaseResource()
	require.Equal(t, uint64(48), delta.Usage.ExclusiveActiveNS)
	require.Equal(t, uint64(12), delta.Usage.WaitNS[resource.WaitFilesystem])
	require.Equal(t, uint64(2), delta.Usage.S3Requests[resource.S3Get])
	require.Equal(t, uint64(1), delta.Usage.S3Requests[resource.S3Head])
	require.Equal(t, uint64(3), delta.Usage.S3Requests[resource.S3Put])
	require.Zero(t, delta.Quality)
	claimed, ok := stats.ClaimRootPhaseResource()
	require.True(t, ok)
	require.Equal(t, delta, claimed)
	_, ok = stats.ClaimRootPhaseResource()
	require.False(t, ok)
}

func TestResetRetryBuildResource(t *testing.T) {
	stats := NewStatsInfo()
	stats.PlanStage.PlanDuration = time.Second
	stats.PlanStage.PlanStartTime = time.Now()
	stats.PlanStage.BuildPlanStatsIOConsumption = 7
	stats.PlanStage.BuildPlanS3Request = S3Request{List: 1, Get: 2}
	stats.CompileStage.CompileDuration = time.Second
	stats.CompileStage.CompileStartTime = time.Now()
	stats.CompileStage.CompileIOConsumption = 5
	stats.CompileStage.CompileS3Request = S3Request{Put: 3, DeleteMul: 4}

	stats.ResetRetryBuildResource()

	require.Zero(t, stats.PlanStage.PlanDuration)
	require.True(t, stats.PlanStage.PlanStartTime.IsZero())
	require.Zero(t, stats.PlanStage.BuildPlanStatsIOConsumption)
	require.Equal(t, S3Request{}, stats.PlanStage.BuildPlanS3Request)
	require.Zero(t, stats.CompileStage.CompileDuration)
	require.True(t, stats.CompileStage.CompileStartTime.IsZero())
	require.Zero(t, stats.CompileStage.CompileIOConsumption)
	require.Equal(t, S3Request{}, stats.CompileStage.CompileS3Request)
}
