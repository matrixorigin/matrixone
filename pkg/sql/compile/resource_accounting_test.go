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
	"encoding/json"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestExecutionResourceRecorder(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	stats := statistic.NewStatsInfo()
	stats.ParseStage.ParseDuration = 5 * time.Nanosecond
	ctx := statistic.ContextWithStatsInfo(context.Background(), stats)
	recorder := newExecutionResourceRecorder(resource.ContextWithRoot(ctx, root))
	require.NotNil(t, recorder)
	require.Equal(t, uint64(5), root.PreResponseSummary().Usage.ExclusiveActiveNS)

	anal := &AnalyzeModule{}
	anal.appendRemoteResource(
		resource.Delta{Usage: resource.Usage{ExclusiveActiveNS: 30}},
		resource.MemoryDomainSummary{
			AllocatedBytes: 100,
			FreedBytes:     100,
			PeakLiveBytes:  80,
		},
		0,
	)
	recorder.finishAttempt(
		0,
		time.Now().Add(-time.Millisecond),
		10*time.Microsecond,
		0,
		nil,
		nil,
		anal,
		"local:6001",
		resource.OutcomeSuccess,
		false,
	)
	recorder.publish()

	summary := root.PreResponseSummary()
	require.Equal(t, uint64(1), summary.AttemptCount)
	require.GreaterOrEqual(t, summary.Usage.ExclusiveActiveNS, uint64(35))
	require.Equal(t, uint64(80), summary.Memory.MaxDomainPeakLiveBytes)
	require.Zero(t, summary.Quality&resource.QualityMissingMemoryDomain)
	require.Zero(t, summary.Quality&resource.QualityMissingFragment)
}

func TestRetryBuildAndRemoteWaitBelongToRetryAttempt(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	recorder := newExecutionResourceRecorder(resource.ContextWithRoot(context.Background(), root))
	require.NotNil(t, recorder)

	stats := statistic.NewStatsInfo()
	stats.PlanStage.BuildPlanStatsIOConsumption = 7
	stats.CompileStage.CompileIOConsumption = 5
	stats.PrepareRunStage.CompilePreRunOnceWaitLock = 3
	stats.PlanStage.BuildPlanS3Request = statistic.S3Request{Get: 2}
	stats.CompileStage.CompileS3Request = statistic.S3Request{Put: 1}
	recorder.finishAttempt(
		1,
		time.Now().Add(-time.Millisecond),
		100*time.Nanosecond,
		11*time.Nanosecond,
		stats,
		nil,
		nil,
		"local:6001",
		resource.OutcomeSuccess,
		false,
	)
	recorder.publish()

	summary := root.PreResponseSummary()
	require.Equal(t, uint64(74), summary.Usage.ExclusiveActiveNS)
	require.Equal(t, uint64(12), summary.Usage.WaitNS[resource.WaitFilesystem])
	require.Equal(t, uint64(3), summary.Usage.WaitNS[resource.WaitLock])
	require.Equal(t, uint64(11), summary.Usage.WaitNS[resource.WaitRemote])
	require.Equal(t, uint64(2), summary.Usage.S3Requests[resource.S3Get])
	require.Equal(t, uint64(1), summary.Usage.S3Requests[resource.S3Put])
}

func TestRetryScopePrepareRequestsAreGenerationLocal(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	recorder := newExecutionResourceRecorder(resource.ContextWithRoot(context.Background(), root))
	stats := statistic.NewStatsInfo()
	stats.PrepareRunStage.ScopePrepareS3Request = statistic.S3Request{Get: 2}
	recorder.finishAttempt(
		0, time.Now(), 0, 0, stats, nil, nil, "local:6001",
		resource.OutcomeError, true,
	)

	stats.ResetRetryAttemptResource()
	stats.PrepareRunStage.ScopePrepareS3Request = statistic.S3Request{Get: 3}
	recorder.finishAttempt(
		1, time.Now(), 0, 0, stats, nil, nil, "local:6001",
		resource.OutcomeSuccess, false,
	)
	recorder.publish()

	summary := root.PreResponseSummary()
	require.Equal(t, uint64(5), summary.Usage.S3Requests[resource.S3Get])
	require.Equal(t, uint64(2), summary.AttemptCount)
}

func TestRemoteTerminalEnvelope(t *testing.T) {
	anal := &AnalyzeModule{}
	sender := &messageSenderOnClient{anal: anal}
	envelope := remoteTerminalEnvelope{
		Delta: resource.Delta{
			Usage:   resource.Usage{ExclusiveActiveNS: 11, S3ReadBytes: 12},
			Quality: resource.QualityPartial,
		},
		Memory: resource.MemoryDomainSummary{
			AllocatedBytes: 20,
			FreedBytes:     20,
			PeakLiveBytes:  15,
		},
	}
	data, err := json.Marshal(envelope)
	require.NoError(t, err)
	require.NoError(t, sender.dealRemoteTerminal(data))
	require.NoError(t, sender.dealRemoteTerminal(data))

	usage, memory, quality, reports := anal.remoteResourceSummary()
	require.Equal(t, uint64(1), reports)
	require.Equal(t, uint64(11), usage.ExclusiveActiveNS)
	require.Equal(t, uint64(12), usage.S3ReadBytes)
	require.Equal(t, uint64(15), memory.MaxDomainPeakLiveBytes)
	require.NotZero(t, quality&resource.QualityPartial)
}

func TestCountExpectedRemoteScopes(t *testing.T) {
	scopes := []*Scope{
		{Magic: Normal, NodeInfo: engine.Node{Addr: "local:6001"}},
		{Magic: Remote, NodeInfo: engine.Node{Addr: "remote-a:6001"}},
		{
			Magic:    Merge,
			NodeInfo: engine.Node{Addr: "local:6001"},
			PreScopes: []*Scope{
				{Magic: Remote, NodeInfo: engine.Node{Addr: "remote-b:6001"}},
			},
		},
	}
	require.Equal(t, uint64(2), countExpectedRemoteScopes(scopes, "local:6001"))
}

func TestMissingRemoteCountsFragmentAndMemoryDomain(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	ctx := resource.ContextWithRoot(context.Background(), root)
	recorder := newExecutionResourceRecorder(ctx)
	scopes := []*Scope{{Magic: Remote, NodeInfo: engine.Node{Addr: "remote:6001"}}}

	recorder.finishAttempt(
		0,
		time.Now(),
		0,
		0,
		nil,
		scopes,
		nil,
		"local:6001",
		resource.OutcomeError,
		false,
	)
	recorder.publish()

	summary := root.PreResponseSummary()
	require.Equal(t, uint64(1), summary.MissingFragmentCount)
	require.Equal(t, uint64(1), summary.MissingMemoryDomainCount)
	require.NotZero(t, summary.Quality&resource.QualityMissingFragment)
	require.NotZero(t, summary.Quality&resource.QualityMissingMemoryDomain)
}
