// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sidecar

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestCompareRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		comparison ComparisonMode
		native     []Row
		offloaded  []Row
		wantErr    bool
	}{
		{
			name:       "ordered preserves order",
			comparison: ComparisonOrdered,
			native:     []Row{{TextCell("a")}, {TextCell("b")}},
			offloaded:  []Row{{TextCell("b")}, {TextCell("a")}},
			wantErr:    true,
		},
		{
			name:       "unordered ignores order",
			comparison: ComparisonUnordered,
			native:     []Row{{TextCell("a")}, {TextCell("b")}},
			offloaded:  []Row{{TextCell("b")}, {TextCell("a")}},
		},
		{
			name:       "unordered preserves duplicate count",
			comparison: ComparisonUnordered,
			native:     []Row{{TextCell("a")}, {TextCell("a")}, {TextCell("b")}},
			offloaded:  []Row{{TextCell("a")}, {TextCell("b")}, {TextCell("b")}},
			wantErr:    true,
		},
		{
			name:       "null differs from empty text",
			comparison: ComparisonOrdered,
			native:     []Row{{NullCell()}},
			offloaded:  []Row{{TextCell("")}},
			wantErr:    true,
		},
		{
			name:       "text differs from binary",
			comparison: ComparisonOrdered,
			native:     []Row{{TextCell("same bytes")}},
			offloaded:  []Row{{BinaryCell([]byte("same bytes"))}},
			wantErr:    true,
		},
		{
			name:       "length prefix prevents cell-boundary collision",
			comparison: ComparisonOrdered,
			native:     []Row{{TextCell("ab"), TextCell("c")}},
			offloaded:  []Row{{TextCell("a"), TextCell("bc")}},
			wantErr:    true,
		},
		{
			name:       "driver spellings stay exact",
			comparison: ComparisonOrdered,
			native:     []Row{{TextCell("NaN"), TextCell("1.2300"), TextCell("2026-08-03 12:34:56.000001")}},
			offloaded:  []Row{{TextCell("nan"), TextCell("1.23"), TextCell("2026-08-03 12:34:56.000001")}},
			wantErr:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := compareRows(test.comparison, test.native, test.offloaded)
			if (err != nil) != test.wantErr {
				t.Fatalf("compareRows() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestCompareRequiresExecutionEvidence(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	report.Offloaded.Evidence.Backend = BackendDuckDBCPU
	err := Compare(report)
	if err == nil || !strings.Contains(err.Error(), "offloaded execution evidence") {
		t.Fatalf("Compare() error = %v, want offloaded evidence mismatch", err)
	}

	report = successfulReport()
	report.Offloaded.Evidence.Fallback = true
	err = Compare(report)
	if err == nil || !strings.Contains(err.Error(), "fallback") {
		t.Fatalf("Compare() error = %v, want fallback mismatch", err)
	}
}

func TestCompareAllowsPreStartTerminalStates(t *testing.T) {
	t.Parallel()

	for _, outcome := range []Outcome{OutcomeFailed, OutcomeCancelled} {
		t.Run(outcome.String(), func(t *testing.T) {
			t.Parallel()
			report := failedReport()
			report.Case.NativeExpectation = Expectation{Backend: BackendUnknown, Outcome: outcome}
			report.Case.OffloadedExpectation = Expectation{Backend: BackendUnknown, Outcome: outcome}
			report.Native.Evidence = ExecutionEvidence{Backend: BackendUnknown, Outcome: outcome}
			report.Offloaded.Evidence = ExecutionEvidence{Backend: BackendUnknown, Outcome: outcome}
			if err := Compare(report); err != nil {
				t.Fatalf("Compare() error = %v for matching pre-start %s", err, outcome)
			}
		})
	}
}

func TestCompareChecksSchemaBeforeRows(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	report.Offloaded.Schema[0].DatabaseType = "VARBINARY"
	err := Compare(report)
	var mismatchError *MismatchError
	if !errors.As(err, &mismatchError) || mismatchError.Field != "schema" {
		t.Fatalf("Compare() error = %v, want schema mismatch", err)
	}
}

func TestCompareSchemaHonorsMetadataAvailability(t *testing.T) {
	t.Parallel()

	report := successfulReport()
	report.Native.Schema[0].Length = 10
	report.Offloaded.Schema[0].Length = 20
	if err := Compare(report); err != nil {
		t.Fatalf("Compare() error = %v for unknown length metadata", err)
	}

	report.Native.Schema[0].LengthKnown = true
	report.Offloaded.Schema[0].LengthKnown = true
	if err := Compare(report); err == nil {
		t.Fatal("Compare() accepted different known column lengths")
	}
}

func TestCompareRejectsInvalidExpectations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Case)
	}{
		{name: "empty ID", mutate: func(testCase *Case) { testCase.ID = "" }},
		{name: "empty SQL", mutate: func(testCase *Case) { testCase.SQL = "" }},
		{name: "invalid comparison", mutate: func(testCase *Case) { testCase.Comparison = ComparisonUnknown }},
		{name: "invalid backend", mutate: func(testCase *Case) { testCase.OffloadedExpectation.Backend = Backend(99) }},
		{name: "success without backend", mutate: func(testCase *Case) { testCase.OffloadedExpectation.Backend = BackendUnknown }},
		{name: "pre-start fallback", mutate: func(testCase *Case) {
			testCase.OffloadedExpectation = Expectation{Backend: BackendUnknown, Outcome: OutcomeFailed, Fallback: true}
		}},
		{name: "native wrong backend", mutate: func(testCase *Case) { testCase.NativeExpectation.Backend = BackendSiriusGPU }},
		{name: "native fallback", mutate: func(testCase *Case) { testCase.NativeExpectation.Fallback = true }},
		{name: "Sirius marked fallback", mutate: func(testCase *Case) { testCase.OffloadedExpectation.Fallback = true }},
		{name: "fallback backend unmarked", mutate: func(testCase *Case) { testCase.OffloadedExpectation.Backend = BackendDuckDBCPU }},
		{name: "pending outcome", mutate: func(testCase *Case) { testCase.OffloadedExpectation.Outcome = OutcomePending }},
		{name: "invalid outcome", mutate: func(testCase *Case) { testCase.OffloadedExpectation.Outcome = Outcome(99) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			report := successfulReport()
			test.mutate(&report.Case)
			if err := Compare(report); err == nil {
				t.Fatal("Compare() accepted invalid case")
			}
		})
	}
}

func TestCompareErrorIdentityIgnoresMessage(t *testing.T) {
	t.Parallel()

	report := failedReport()
	report.Native.Error.Message = "native diagnostic"
	report.Offloaded.Error.Message = "different sidecar diagnostic"
	if err := Compare(report); err != nil {
		t.Fatalf("Compare() error = %v, want matching stable identity", err)
	}

	report.Offloaded.Error.SQLState = "HY000"
	if err := Compare(report); err == nil {
		t.Fatal("Compare() succeeded for different SQLSTATE")
	}
}

func TestCompareTerminalObservationsIncludePartialResults(t *testing.T) {
	t.Parallel()

	for _, outcome := range []Outcome{OutcomeFailed, OutcomeCancelled} {
		t.Run(outcome.String(), func(t *testing.T) {
			t.Parallel()

			newReport := func() Report {
				report := failedReport()
				report.Case.NativeExpectation.Outcome = outcome
				report.Case.OffloadedExpectation.Outcome = outcome
				report.Native.Evidence.Outcome = outcome
				report.Offloaded.Evidence.Outcome = outcome
				report.Native.Rows = []Row{{TextCell("delivered-before-terminal")}}
				report.Offloaded.Rows = []Row{{TextCell("delivered-before-terminal")}}
				return report
			}

			report := newReport()
			if err := Compare(report); err != nil {
				t.Fatalf("Compare() error = %v for matching partial %s observations", err, outcome)
			}

			report = newReport()
			report.Offloaded.Rows = nil
			err := Compare(report)
			var mismatchError *MismatchError
			if !errors.As(err, &mismatchError) || mismatchError.Field != "row count" {
				t.Fatalf("Compare() error = %v, want partial-row mismatch", err)
			}

			report = newReport()
			report.Offloaded.Rows[0][0] = TextCell("different-partial-row")
			err = Compare(report)
			if !errors.As(err, &mismatchError) || mismatchError.Field != "row data" {
				t.Fatalf("Compare() error = %v, want partial-row-data mismatch", err)
			}

			report = newReport()
			report.Offloaded.Schema[0].DatabaseType = "VARBINARY"
			err = Compare(report)
			if !errors.As(err, &mismatchError) || mismatchError.Field != "schema" {
				t.Fatalf("Compare() error = %v, want partial-schema mismatch", err)
			}
		})
	}
}

func TestCompareRejectsInconsistentObservations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*Report)
	}{
		{
			name: "success with SQL error",
			mutate: func(report *Report) {
				report.Offloaded.Error = &SQLError{Code: 1}
			},
		},
		{
			name: "failure without SQL error",
			mutate: func(report *Report) {
				failed := failedReport()
				*report = failed
				report.Offloaded.Error = nil
			},
		},
		{
			name: "error without stable identity",
			mutate: func(report *Report) {
				failed := failedReport()
				*report = failed
				report.Offloaded.Error = &SQLError{Message: "unstable text only"}
			},
		},
		{
			name: "row width differs from schema",
			mutate: func(report *Report) {
				report.Offloaded.Rows[0] = append(report.Offloaded.Rows[0], TextCell("extra"))
			},
		},
		{
			name: "null contains data",
			mutate: func(report *Report) {
				report.Offloaded.Rows[0][0] = Cell{Kind: CellNull, Data: []byte("not null")}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			report := successfulReport()
			test.mutate(&report)
			if err := Compare(report); err == nil {
				t.Fatal("Compare() succeeded for inconsistent observation")
			}
		})
	}
}

func TestRunWrapsRunnerFailure(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runner := runnerFunc(func(ctx context.Context, _ Case, _ Mode) (Observation, error) {
		return Observation{}, ctx.Err()
	})
	_, err := Run(ctx, runner, baseCase("cancelled"))
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "native mode") {
		t.Fatalf("Run() error = %v, want wrapped native context cancellation", err)
	}
}

func TestRunConcurrentCasesKeepEvidenceQueryScoped(t *testing.T) {
	const caseCount = 64

	runner := runnerFunc(func(_ context.Context, testCase Case, mode Mode) (Observation, error) {
		observation := successfulObservation(mode)
		observation.Rows = []Row{{TextCell(testCase.ID)}}
		return observation, nil
	})

	start := make(chan struct{})
	errorsByCase := make(chan error, caseCount)
	var workers sync.WaitGroup
	workers.Add(caseCount)
	for i := 0; i < caseCount; i++ {
		go func() {
			defer workers.Done()
			<-start
			testCase := baseCase(fmt.Sprintf("concurrent-%02d", i))
			_, err := Run(context.Background(), runner, testCase)
			errorsByCase <- err
		}()
	}
	close(start)
	workers.Wait()
	close(errorsByCase)

	for err := range errorsByCase {
		if err != nil {
			t.Errorf("Run() error = %v", err)
		}
	}
}

func TestBinaryCellCopiesInput(t *testing.T) {
	t.Parallel()

	input := []byte("before")
	cell := BinaryCell(input)
	input[0] = 'X'
	if got := string(cell.Data); got != "before" {
		t.Fatalf("BinaryCell() data = %q, want copied input", got)
	}
}

type runnerFunc func(context.Context, Case, Mode) (Observation, error)

func (f runnerFunc) Run(ctx context.Context, testCase Case, mode Mode) (Observation, error) {
	return f(ctx, testCase, mode)
}

func baseCase(id string) Case {
	return Case{
		ID:                   id,
		SQL:                  "SELECT value FROM test_data",
		Comparison:           ComparisonOrdered,
		NativeExpectation:    Expectation{Backend: BackendMatrixOneNative, Outcome: OutcomeSucceeded},
		OffloadedExpectation: Expectation{Backend: BackendSiriusGPU, Outcome: OutcomeSucceeded},
	}
}

func successfulObservation(mode Mode) Observation {
	backend := BackendMatrixOneNative
	if mode == ModeOffloaded {
		backend = BackendSiriusGPU
	}
	return Observation{
		Schema: []Column{{Name: "value", DatabaseType: "VARCHAR", NullableKnown: true}},
		Rows:   []Row{{TextCell("value")}},
		Evidence: ExecutionEvidence{
			Backend: backend,
			Outcome: OutcomeSucceeded,
		},
	}
}

func successfulReport() Report {
	testCase := baseCase("success")
	return Report{
		Case:      testCase,
		Native:    successfulObservation(ModeNative),
		Offloaded: successfulObservation(ModeOffloaded),
	}
}

func failedReport() Report {
	testCase := baseCase("failure")
	testCase.NativeExpectation.Outcome = OutcomeFailed
	testCase.OffloadedExpectation.Outcome = OutcomeFailed
	native := successfulObservation(ModeNative)
	offloaded := successfulObservation(ModeOffloaded)
	native.Rows = nil
	offloaded.Rows = nil
	native.Evidence.Outcome = OutcomeFailed
	offloaded.Evidence.Outcome = OutcomeFailed
	native.Error = &SQLError{Code: 1064, SQLState: "42000", Class: "syntax"}
	offloaded.Error = &SQLError{Code: 1064, SQLState: "42000", Class: "syntax"}
	return Report{Case: testCase, Native: native, Offloaded: offloaded}
}
