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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestExecutionResourceRecorder(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	stats := statistic.NewStatsInfo()
	stats.ParseStage.ParseDuration = 5 * time.Nanosecond
	ctx := statistic.ContextWithStatsInfo(context.Background(), stats)
	recorder := newExecutionResourceRecorder(resource.ContextWithRoot(ctx, root), true)
	require.NotNil(t, recorder)
	require.Equal(t, uint64(5), root.PreResponseSummary().Usage.ExclusiveActiveNS)

	anal := &AnalyzeModule{}
	anal.appendRemoteResource(
		resource.Delta{Usage: resource.Usage{ExclusiveActiveNS: 30}},
		resource.MemoryTotals{
			AllocatedBytes:              100,
			FreedBytes:                  100,
			MaxDomainPeakLiveBytes:      80,
			SumDomainPeakLiveBytesBound: 80,
		},
		0,
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
	recorder := newExecutionResourceRecorder(resource.ContextWithRoot(context.Background(), root), true)
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

func TestAggregateWaitLargerThanCoordinatorWallIsNotInvalid(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	recorder := newExecutionResourceRecorder(resource.ContextWithRoot(context.Background(), root), true)
	stats := statistic.NewStatsInfo()
	stats.PrepareRunStage.CompilePreRunOnceWaitLock = 30
	stats.PlanStage.BuildPlanStatsIOConsumption = 20

	recorder.finishAttempt(
		1, time.Now().Add(-time.Millisecond), 10*time.Nanosecond, 0,
		stats, nil, nil, "local:6001", resource.OutcomeSuccess, false,
	)
	recorder.publish()

	summary := root.PreResponseSummary()
	require.Zero(t, summary.Usage.ExclusiveActiveNS)
	require.Equal(t, uint64(30), summary.Usage.WaitNS[resource.WaitLock])
	require.Equal(t, uint64(20), summary.Usage.WaitNS[resource.WaitFilesystem])
	require.Zero(t, summary.Quality)
}

func TestRetryScopePrepareRequestsAreGenerationLocal(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	recorder := newExecutionResourceRecorder(resource.ContextWithRoot(context.Background(), root), true)
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

func TestChildExecutionDoesNotInflateStatementAttemptCount(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	ctx := resource.ContextWithRoot(context.Background(), root)
	parent := newExecutionResourceRecorder(ctx, true)
	parent.finishAttempt(
		0, time.Now(), 5*time.Nanosecond, 0, nil, nil, nil, "",
		resource.OutcomeSuccess, false,
	)
	parent.publish()

	child := newExecutionResourceRecorder(ctx, false)

	child.finishAttempt(
		0, time.Now(), 10*time.Nanosecond, 0, nil, nil, nil, "",
		resource.OutcomeError, true,
	)
	child.finishAttempt(
		1, time.Now(), 20*time.Nanosecond, 0, nil, nil, nil, "",
		resource.OutcomeSuccess, false,
	)
	child.publish()

	summary := root.PreResponseSummary()
	require.Equal(t, uint64(1), summary.AttemptCount)
	require.Zero(t, summary.RetryWallNS)
	require.Equal(t, uint64(35), summary.Usage.ExclusiveActiveNS)
}

func TestOnlyOneEligibleExecutionOwnsStatementAttempts(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	ctx := resource.ContextWithRoot(context.Background(), root)

	child := newExecutionResourceRecorder(ctx, false)
	require.False(t, child.ownsAttempts)

	owner := newExecutionResourceRecorder(ctx, true)
	require.True(t, owner.ownsAttempts)
	secondEligible := newExecutionResourceRecorder(ctx, true)
	require.False(t, secondEligible.ownsAttempts)

	child.finishAttempt(
		0, time.Now(), 3*time.Nanosecond, 0, nil, nil, nil, "",
		resource.OutcomeSuccess, false,
	)
	child.publish()
	owner.finishAttempt(
		0, time.Now(), 5*time.Nanosecond, 0, nil, nil, nil, "",
		resource.OutcomeError, true,
	)
	owner.finishAttempt(
		1, time.Now(), 7*time.Nanosecond, 0, nil, nil, nil, "",
		resource.OutcomeSuccess, false,
	)
	owner.publish()
	secondEligible.finishAttempt(
		0, time.Now(), 11*time.Nanosecond, 0, nil, nil, nil, "",
		resource.OutcomeSuccess, false,
	)
	secondEligible.publish()

	summary := root.PreResponseSummary()
	require.Equal(t, uint64(2), summary.AttemptCount)
	require.Greater(t, summary.RetryWallNS, uint64(0))
	require.Equal(t, uint64(26), summary.Usage.ExclusiveActiveNS)
}

func TestRemoteTerminalEnvelope(t *testing.T) {
	anal := &AnalyzeModule{}
	sender := &messageSenderOnClient{anal: anal}
	envelope := remoteTerminalEnvelope{
		Delta: resource.Delta{
			Usage:   resource.Usage{ExclusiveActiveNS: 11, S3ReadBytes: 12},
			Quality: resource.QualityPartial,
		},
		Memory: resource.MemoryTotals{
			AllocatedBytes:              20,
			FreedBytes:                  20,
			MaxDomainPeakLiveBytes:      15,
			SumDomainPeakLiveBytesBound: 15,
		},
	}
	data, err := json.Marshal(envelope)
	require.NoError(t, err)
	require.NoError(t, sender.dealRemoteTerminal(data))
	require.NoError(t, sender.dealRemoteTerminal(data))

	summary := anal.remoteResourceSummary()
	require.Equal(t, uint64(1), summary.DirectReportCount)
	require.Equal(t, uint64(11), summary.Usage.ExclusiveActiveNS)
	require.Equal(t, uint64(12), summary.Usage.S3ReadBytes)
	require.Equal(t, uint64(15), summary.Memory.MaxDomainPeakLiveBytes)
	require.NotZero(t, summary.Quality&resource.QualityPartial)
}

func TestCountExpectedRemoteScopes(t *testing.T) {
	scopes := []*Scope{
		{Magic: Normal, NodeInfo: engine.Node{Addr: "local:6001"}},
		{Magic: Remote, NodeInfo: engine.Node{Addr: "remote-a:6001"}},
		{
			Magic:                   Remote,
			NodeInfo:                engine.Node{Addr: "remote-fallback:6001"},
			resourceExecutedLocally: true,
		},
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

func TestLocalRemoteFallbackContributesResourceFacts(t *testing.T) {
	op := value_scan.NewArgument()
	defer op.Release()
	op.GetOperatorBase().OpAnalyzer = process.NewAnalyzer(0, false, false, "fallback")
	stats := op.GetOperatorBase().OpAnalyzer.GetOpStats()
	stats.TimeConsumed = 11
	stats.SpillSize = 7

	fallback := &Scope{
		Magic:                   Remote,
		NodeInfo:                engine.Node{Addr: "remote-fallback:6001"},
		RootOp:                  op,
		resourceExecutedLocally: true,
	}
	actualRemote := &Scope{
		Magic:    Remote,
		NodeInfo: engine.Node{Addr: "remote:6001"},
	}

	delta := collectScopeResourceDelta([]*Scope{fallback, actualRemote}, "local:6001")
	require.Equal(t, uint64(11), delta.Usage.ExclusiveActiveNS)
	require.Equal(t, uint64(7), delta.Usage.SpillBytes)
	require.Equal(t, uint64(1), countExpectedRemoteScopes(
		[]*Scope{fallback, actualRemote}, "local:6001"))
}

func TestMissingRemoteCountsFragmentAndMemoryDomain(t *testing.T) {
	root := resource.NewRoot(resource.ConnExternal)
	ctx := resource.ContextWithRoot(context.Background(), root)
	recorder := newExecutionResourceRecorder(ctx, true)
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

func TestComposeRemoteResourceAggregateAcrossHops(t *testing.T) {
	cn3 := composeRemoteResourceAggregate(
		resource.Delta{Usage: resource.Usage{ExclusiveActiveNS: 3}, Outcome: resource.OutcomeSuccess},
		resource.MemoryDomainSummary{PeakLiveBytes: 9},
		0,
		remoteResourceSnapshot{},
		0,
	)
	cn2 := composeRemoteResourceAggregate(
		resource.Delta{Usage: resource.Usage{ExclusiveActiveNS: 2}, Outcome: resource.OutcomeError},
		resource.MemoryDomainSummary{PeakLiveBytes: 7},
		0,
		remoteResourceSnapshot{
			Usage:             cn3.Delta.Usage,
			Memory:            cn3.Memory,
			Quality:           cn3.Delta.Quality,
			DirectReportCount: 1,
		},
		1,
	)
	cn1 := composeRemoteResourceAggregate(
		resource.Delta{Usage: resource.Usage{ExclusiveActiveNS: 1}, Outcome: resource.OutcomeCanceled},
		resource.MemoryDomainSummary{PeakLiveBytes: 5},
		0,
		remoteResourceSnapshot{
			Usage:             cn2.Delta.Usage,
			Memory:            cn2.Memory,
			Quality:           cn2.Delta.Quality,
			DirectReportCount: 1,
		},
		1,
	)
	require.Equal(t, uint64(6), cn1.Delta.Usage.ExclusiveActiveNS)
	require.Equal(t, resource.OutcomeCanceled, cn1.Delta.Outcome)
	require.Equal(t, resource.OutcomeError, cn2.Delta.Outcome)
	require.Equal(t, resource.OutcomeSuccess, cn3.Delta.Outcome)
	require.Equal(t, uint64(9), cn1.Memory.MaxDomainPeakLiveBytes)
	require.Equal(t, uint64(21), cn1.Memory.SumDomainPeakLiveBytesBound)
}

func TestComposeRemoteResourceAggregateForwardsMissingExactlyOnce(t *testing.T) {
	aggregate := composeRemoteResourceAggregate(
		resource.Delta{},
		resource.MemoryDomainSummary{},
		0,
		remoteResourceSnapshot{
			MissingFragmentCount:     2,
			MissingMemoryDomainCount: 3,
			Quality:                  resource.QualityInvariantFailure,
			DirectReportCount:        1,
		},
		3,
	)
	require.Equal(t, uint64(4), aggregate.MissingFragmentCount)
	require.Equal(t, uint64(5), aggregate.MissingMemoryDomainCount)
	require.NotZero(t, aggregate.Delta.Quality&resource.QualityMissingFragment)
	require.NotZero(t, aggregate.Delta.Quality&resource.QualityMissingMemoryDomain)
	require.NotZero(t, aggregate.Delta.Quality&resource.QualityInvariantFailure)

	noUnderflow := composeRemoteResourceAggregate(
		resource.Delta{}, resource.MemoryDomainSummary{}, 0,
		remoteResourceSnapshot{DirectReportCount: 4}, 3)
	require.Zero(t, noUnderflow.MissingFragmentCount)
	require.Zero(t, noUnderflow.MissingMemoryDomainCount)
}

func TestRemoteResourceCounterSaturates(t *testing.T) {
	anal := &AnalyzeModule{
		remoteMissingFragments:     math.MaxUint64,
		remoteMissingMemoryDomains: math.MaxUint64,
		remoteReports:              math.MaxUint64,
	}
	anal.appendRemoteResource(resource.Delta{}, resource.MemoryTotals{}, 1, 1)
	snapshot := anal.remoteResourceSummary()
	require.Equal(t, uint64(math.MaxUint64), snapshot.MissingFragmentCount)
	require.Equal(t, uint64(math.MaxUint64), snapshot.MissingMemoryDomainCount)
	require.Equal(t, uint64(math.MaxUint64), snapshot.DirectReportCount)
	require.NotZero(t, snapshot.Quality&resource.QualityInvariantFailure)
	aggregate := composeRemoteResourceAggregate(
		resource.Delta{}, resource.MemoryDomainSummary{}, 0,
		remoteResourceSnapshot{MissingFragmentCount: math.MaxUint64}, 0)
	require.Equal(t, uint64(math.MaxUint64), aggregate.MissingFragmentCount)
	require.Zero(t, aggregate.Delta.Quality&resource.QualityInvariantFailure)
	aggregate = composeRemoteResourceAggregate(
		resource.Delta{}, resource.MemoryDomainSummary{}, 0,
		remoteResourceSnapshot{MissingFragmentCount: math.MaxUint64, DirectReportCount: 0}, 1)
	require.Equal(t, uint64(math.MaxUint64), aggregate.MissingFragmentCount)
	require.NotZero(t, aggregate.Delta.Quality&resource.QualityInvariantFailure)
}

func TestAnalyzeModuleResetClearsRemoteResourceAggregate(t *testing.T) {
	anal := &AnalyzeModule{}
	anal.appendRemoteResource(
		resource.Delta{Usage: resource.Usage{S3ReadBytes: 11}, Quality: resource.QualityPartial},
		resource.MemoryTotals{MaxDomainPeakLiveBytes: 8}, 2, 3)
	anal.Reset(false, false)
	snapshot := anal.remoteResourceSummary()
	require.Equal(t, remoteResourceSnapshot{}, snapshot)
}

func TestAnalyzeModuleRemoteResourceConcurrentAccess(t *testing.T) {
	anal := &AnalyzeModule{}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				anal.appendRemoteResource(resource.Delta{Usage: resource.Usage{SpillBytes: 1}}, resource.MemoryTotals{}, 1, 1)
				_ = anal.remoteResourceSummary()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			anal.Reset(false, false)
		}
	}()
	wg.Wait()
	// The exact total is intentionally unspecified because Reset races with
	// append; the protected snapshot must remain internally consistent.
	snapshot := anal.remoteResourceSummary()
	require.LessOrEqual(t, snapshot.DirectReportCount, uint64(400))
	require.LessOrEqual(t, snapshot.MissingFragmentCount, uint64(400))
	require.LessOrEqual(t, snapshot.MissingMemoryDomainCount, uint64(400))
}
