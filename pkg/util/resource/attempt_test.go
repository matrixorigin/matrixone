// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

package resource

import (
	"context"
	"sync"
	"testing"
)

func TestAttemptTerminalLifecycle(t *testing.T) {
	attempt := NewAttempt(7, 2, 1)
	if got := attempt.MarkFragmentDispatched(0); got != PublishAccepted {
		t.Fatalf("dispatch fragment: %v", got)
	}
	if got := attempt.MarkFragmentDispatched(1); got != PublishAccepted {
		t.Fatalf("dispatch fragment: %v", got)
	}
	if got := attempt.MarkMemoryDomainDispatched(0); got != PublishAccepted {
		t.Fatalf("dispatch memory: %v", got)
	}
	if !attempt.BeginClosing() {
		t.Fatal("begin closing rejected")
	}

	delta := Delta{Usage: Usage{ExclusiveActiveNS: 10}, Outcome: OutcomeSuccess}
	if got := attempt.PublishFragment(7, 0, delta); got != PublishAccepted {
		t.Fatalf("publish fragment: %v", got)
	}
	if got := attempt.PublishFragment(7, 0, delta); got != PublishDuplicate {
		t.Fatalf("duplicate fragment: %v", got)
	}
	if got := attempt.PublishFragment(6, 1, delta); got != PublishStaleGeneration {
		t.Fatalf("stale fragment: %v", got)
	}
	if got := attempt.PublishMemoryDomain(7, 0, MemoryDomainSummary{
		AllocatedBytes: 100,
		FreedBytes:     100,
		PeakLiveBytes:  80,
	}); got != PublishAccepted {
		t.Fatalf("publish memory: %v", got)
	}

	summary := attempt.Seal(500, OutcomeError)
	if summary.Usage.ExclusiveActiveNS != 10 || summary.Memory.MaxDomainPeakLiveBytes != 80 {
		t.Fatalf("unexpected totals: %+v", summary)
	}
	if summary.MissingFragmentCount != 1 || summary.MissingMemoryDomainCount != 0 {
		t.Fatalf("unexpected missing counts: %+v", summary)
	}
	wantFlags := QualityPartial | QualityMissingFragment
	if summary.Quality&wantFlags != wantFlags {
		t.Fatalf("quality %b does not contain %b", summary.Quality, wantFlags)
	}
	if again := attempt.Seal(999, OutcomeSuccess); again != summary {
		t.Fatalf("seal is not idempotent: first=%+v second=%+v", summary, again)
	}
	if got := attempt.PublishFragment(7, 1, delta); got != PublishAfterSeal {
		t.Fatalf("publish after seal: %v", got)
	}
}

func TestRootLifecycle(t *testing.T) {
	root := NewRoot(ConnExternal)
	ctx := ContextWithRoot(context.Background(), root)
	if RootFromContext(ctx) != root {
		t.Fatal("root not found in context")
	}
	var execution ExecutionSummary
	execution.AddAttempt(AttemptSummary{
		Usage:   Usage{ExclusiveActiveNS: 10},
		WallNS:  20,
		Outcome: OutcomeSuccess,
	}, false)
	if !root.MergeExecution(execution) {
		t.Fatal("execution rejected")
	}
	if !root.AddProtocolOutput(30, 2) {
		t.Fatal("protocol output rejected")
	}
	if !root.AddMemoryDomain(MemoryDomainSummary{
		AllocatedBytes: 64,
		FreedBytes:     64,
		PeakLiveBytes:  64,
	}) {
		t.Fatal("memory domain rejected")
	}
	pre := root.PreResponseSummary()
	if pre.StatementWallNS != 0 || pre.Usage.ClientEgressBytes != 30 {
		t.Fatalf("unexpected pre-response summary: %+v", pre)
	}
	sealed := root.Seal(40)
	if sealed.StatementWallNS != 40 || sealed.Usage.ExclusiveActiveNS != 10 ||
		sealed.Usage.ClientEgressBytes != 30 || sealed.OutputPacketCount != 2 ||
		sealed.AttemptCount != 1 || sealed.Memory.MaxDomainPeakLiveBytes != 64 {
		t.Fatalf("unexpected sealed root: %+v", sealed)
	}
	if root.AddProtocolOutput(1, 1) || root.MergeExecution(ExecutionSummary{}) {
		t.Fatal("sealed root accepted publication")
	}
	if again := root.Seal(999); again != sealed {
		t.Fatalf("seal is not idempotent: first=%+v second=%+v", sealed, again)
	}
}

func TestUndispatchedSlotsAreNotMissing(t *testing.T) {
	attempt := NewAttempt(1, 2, 1)
	if got := attempt.MarkFragmentSendFailed(0); got != PublishAccepted {
		t.Fatalf("send failure: %v", got)
	}
	if got := attempt.MarkMemoryDomainUnused(0); got != PublishAccepted {
		t.Fatalf("unused memory: %v", got)
	}
	attempt.BeginClosing()
	summary := attempt.Seal(1, OutcomeError)
	if summary.MissingFragmentCount != 0 || summary.MissingMemoryDomainCount != 0 ||
		summary.Quality&(QualityMissingFragment|QualityMissingMemoryDomain) != 0 {
		t.Fatalf("undispatched work marked missing: %+v", summary)
	}
}

func TestConcurrentTerminalPublication(t *testing.T) {
	const slots = 64
	attempt := NewAttempt(42, slots, 0)
	for slot := 0; slot < slots; slot++ {
		if got := attempt.MarkFragmentDispatched(slot); got != PublishAccepted {
			t.Fatalf("dispatch %d: %v", slot, got)
		}
	}
	attempt.BeginClosing()

	var wg sync.WaitGroup
	wg.Add(slots)
	for slot := 0; slot < slots; slot++ {
		go func(slot int) {
			defer wg.Done()
			delta := Delta{Usage: Usage{ExclusiveActiveNS: uint64(slot + 1)}}
			if got := attempt.PublishFragment(42, slot, delta); got != PublishAccepted {
				t.Errorf("publish %d: %v", slot, got)
			}
		}(slot)
	}
	wg.Wait()

	summary := attempt.Seal(100, OutcomeSuccess)
	want := uint64(slots * (slots + 1) / 2)
	if summary.Usage.ExclusiveActiveNS != want || summary.MissingFragmentCount != 0 || summary.Quality != 0 {
		t.Fatalf("unexpected summary: %+v, want active %d", summary, want)
	}
}

func TestMemoryDomainValidationAndPeakMerge(t *testing.T) {
	var totals MemoryTotals
	flags := MergeMemoryDomain(&totals, MemoryDomainSummary{
		AllocatedBytes: 100,
		FreedBytes:     100,
		PeakLiveBytes:  80,
	})
	flags |= MergeMemoryDomain(&totals, MemoryDomainSummary{
		AllocatedBytes:     70,
		FreedBytes:         60,
		PeakLiveBytes:      40,
		LiveBytesAtSeal:    10,
		CrossPoolFreeCount: 1,
	})
	if totals.MaxDomainPeakLiveBytes != 80 || totals.SumDomainPeakLiveBytesBound != 120 {
		t.Fatalf("peak merge is not max/sum-bound: %+v", totals)
	}
	want := QualityCrossPoolFree | QualityNonZeroLiveAtSeal | QualityInvariantFailure
	if flags&want != want {
		t.Fatalf("flags %b do not contain %b", flags, want)
	}
}

func TestStatementMergeAlgebra(t *testing.T) {
	left := StatementResourceSummary{
		Usage:           Usage{ExclusiveActiveNS: 10},
		Memory:          MemoryTotals{MaxDomainPeakLiveBytes: 100, SumDomainPeakLiveBytesBound: 100},
		StatementWallNS: 20,
		AttemptCount:    1,
		ConnType:        ConnExternal,
	}
	right := StatementResourceSummary{
		Usage:           Usage{ExclusiveActiveNS: 30},
		Memory:          MemoryTotals{MaxDomainPeakLiveBytes: 80, SumDomainPeakLiveBytesBound: 80},
		StatementWallNS: 40,
		AttemptCount:    2,
		ConnType:        ConnExternal,
	}
	left.Merge(right)
	if left.Usage.ExclusiveActiveNS != 40 || left.Memory.MaxDomainPeakLiveBytes != 100 ||
		left.Memory.SumDomainPeakLiveBytesBound != 180 || left.StatementWallNS != 60 ||
		left.AttemptCount != 3 || left.Quality&QualityAggregated == 0 {
		t.Fatalf("unexpected merged summary: %+v", left)
	}
}
