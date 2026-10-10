// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
package models

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/stretchr/testify/require"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestStatementDiagnosticLevel(t *testing.T) {
	for _, tc := range []struct {
		name               string
		elapsed, threshold time.Duration
		s                  resource.StatementResourceSummary
		failed, scheduling bool
		level              int
		reason             uint64
	}{
		{name: "normal", elapsed: time.Millisecond, threshold: time.Second},
		{name: "latency boundary", elapsed: time.Second, threshold: time.Second},
		{name: "summary", elapsed: time.Second + 1, threshold: time.Second, level: 1, reason: 1},
		{name: "record all cheap", elapsed: time.Millisecond, level: 1, reason: 1},
		{name: "logical floor", elapsed: 4 * time.Second, level: 2, reason: 1},
		{name: "physical floor", elapsed: 16 * time.Second, level: 3, reason: 1},
		{name: "failure", failed: true, threshold: time.Second, level: 2, reason: 1 << 6},
		{name: "schedule", scheduling: true, threshold: time.Second, level: 1, reason: 1 << 9},
		{name: "memory", s: resource.StatementResourceSummary{Memory: resource.MemoryTotals{MaxDomainPeakLiveBytes: 256 << 20}}, threshold: time.Second, level: 2, reason: 1 << 3},
		{name: "spill", s: resource.StatementResourceSummary{Usage: resource.Usage{SpillBytes: 1}}, threshold: time.Second, level: 1, reason: 1 << 5},
		{name: "S3 overflow", s: resource.StatementResourceSummary{Usage: resource.Usage{S3ReadBytes: math.MaxUint64, S3WriteBytes: 1}}, threshold: time.Second, level: 3, reason: 1 << 4},
		{name: "retry", s: resource.StatementResourceSummary{AttemptCount: 2}, threshold: time.Second, level: 2, reason: 1 << 7},
		{name: "quality", s: resource.StatementResourceSummary{Quality: resource.QualityInvariantFailure}, threshold: time.Second, level: 3, reason: 1 << 8},
		{name: "negative elapsed", elapsed: -1, threshold: time.Second},
		{name: "threshold overflow", elapsed: time.Duration(math.MaxInt64), threshold: time.Duration(math.MaxInt64)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			level, reasons := SelectStatementDiagnosticLevel(tc.elapsed, tc.threshold, &tc.s, tc.failed, tc.scheduling)
			require.Equal(t, tc.level, level)
			require.Equal(t, tc.reason, reasons)
		})
	}
	for _, err := range []error{context.Canceled, fmt.Errorf("wrapped: %w", context.DeadlineExceeded), fmt.Errorf("other")} {
		require.NotEqual(t, "success", DiagnosticOutcome(err))
	}
	require.Equal(t, "cancelled", DiagnosticOutcome(context.Canceled))
	require.Equal(t, "timeout", DiagnosticOutcome(fmt.Errorf("wrapped: %w", context.DeadlineExceeded)))
}

func TestStatementDiagnosticBudgets(t *testing.T) {
	for level, budget := range map[int]int{1: DiagnosticsHeaderBudget, 2: DiagnosticsL2Budget, 3: DiagnosticsL3Budget} {
		t.Run(fmt.Sprint(level), func(t *testing.T) {
			summary := resource.StatementResourceSummary{StatementWallNS: math.MaxUint64, AttemptCount: math.MaxUint64, RetryWallNS: math.MaxUint64, MissingFragmentCount: math.MaxUint64, MissingMemoryDomainCount: math.MaxUint64, Quality: resource.QualityFlags(math.MaxUint64), Memory: resource.MemoryTotals{MaxDomainPeakLiveBytes: math.MaxUint64, SumDomainPeakLiveBytesBound: math.MaxUint64}, Usage: resource.Usage{ExclusiveActiveNS: math.MaxUint64, S3ReadBytes: math.MaxUint64, S3WriteBytes: math.MaxUint64, ClientEgressBytes: math.MaxUint64, SpillBytes: math.MaxUint64}}
			for i := range summary.Usage.WaitNS {
				summary.Usage.WaitNS[i] = math.MaxUint64
			}
			for i := range summary.Usage.S3Requests {
				summary.Usage.S3Requests[i] = math.MaxUint64
			}
			d := &StatementDiagnostics{Version: 1, Level: level, CapturedLevel: level, Reasons: math.MaxUint64, Outcome: "cancelled", Detail: DiagnosticDetail{Capture: "not_selected_before_release", LogicalTotal: 1000000, PhysicalVisited: 4096, PhysicalTruncated: true}}
			d.SetSummary(summary, time.Duration(math.MaxInt64), time.Duration(math.MaxInt64), true)
			s := BoundDiagnosticString(strings.Repeat("\x00\\\"界\xff", 10000), 64)
			d.Scheduling = &DiagnosticScheduling{AttemptCount: math.MaxInt, SelectedCount: math.MaxInt, DroppedCount: math.MaxInt, FailureCount: math.MaxInt, ExecKind: s, Reason: s, RequestedPool: s, ResolvedPool: s}
			for i := 0; i < 32; i++ {
				n := DiagnosticNode{Index: i, Kind: s, Object: BoundDiagnosticString(strings.Repeat("\x00界", 1000), 128), ElapsedNS: math.MaxInt64, WaitNS: math.MaxInt64, InputRows: math.MaxInt64, OutputRows: math.MaxInt64, InputBytes: math.MaxInt64, OutputBytes: math.MaxInt64, MemoryBytes: math.MaxInt64, SpillBytes: math.MaxInt64, ReadBytes: math.MaxInt64, S3ReadBytes: math.MaxInt64, NetworkBytes: math.MaxInt64}
				if level >= 2 {
					d.Logical = append(d.Logical, n)
				}
				if level >= 3 {
					d.Physical = append(d.Physical, n)
				}
			}
			var b bytes.Buffer
			require.NoError(t, d.WriteJSON(&b))
			require.LessOrEqual(t, b.Len(), budget)
			require.True(t, json.Valid(b.Bytes()))
			require.True(t, utf8.Valid(b.Bytes()))
			var parsed ExplainData
			require.NoError(t, json.Unmarshal(b.Bytes(), &parsed))
			require.Equal(t, uint64(math.MaxUint64), parsed.StatementDiagnostics.Summary.S3ReadBytes)
			require.Equal(t, 1000000-len(parsed.StatementDiagnostics.Logical), parsed.StatementDiagnostics.Detail.LogicalOmitted)
		})
	}
}

func TestBoundDiagnosticString(t *testing.T) {
	for _, s := range []string{"abc", "界界界", strings.Repeat("\x00", 200), strings.Repeat("\xff", 200), strings.Repeat("\\\"\u2028", 200)} {
		for limit := 0; limit < 130; limit++ {
			p := BoundDiagnosticString(s, limit)
			require.True(t, utf8.ValidString(p))
			raw, err := diagnosticJSON(p)
			require.NoError(t, err)
			require.LessOrEqual(t, len(raw)-2, limit)
		}
	}
}

func TestDiagnosticOutcomeWrappedMOErrors(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		err  error
		want string
	}{
		{fmt.Errorf("wrapped: %w", moerr.NewQueryInterrupted(ctx)), "cancelled"},
		{errors.Join(moerr.NewInternalErrorNoCtx("secondary"), fmt.Errorf("wrapped: %w", moerr.NewQueryTimeout(ctx))), "timeout"},
		{moerr.NewInternalErrorNoCtx("context canceled"), "failed"},
	} {
		require.Equal(t, tc.want, DiagnosticOutcome(tc.err))
	}
}

func TestDiagnosticRenderingMeaningfulFacts(t *testing.T) {
	d := &StatementDiagnostics{Version: 1, Level: 2, CapturedLevel: 2, Reasons: 1 | 1<<63, Outcome: "success", Summary: &DiagnosticSummary{ActiveNS: math.MaxUint64}, Logical: []DiagnosticNode{{Index: 7, Kind: "TABLE_SCAN", Object: "db.t", AnalyzeAvailable: true, ReadBytes: 1024}}}
	text := RenderStatementDiagnostics(d, VerboseOption)
	require.Contains(t, text, "reasons=elapsed,unknown(")
	require.Contains(t, text, "18446744073709551615ns")
	require.Contains(t, text, "node[7] TABLE_SCAN db.t read=1024B")
	require.NotContains(t, text, "memory=0B")
	require.NotContains(t, text, "op=0")
}
