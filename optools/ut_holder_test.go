// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package optools

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestUTAdmissionHolderEvidence(t *testing.T) {
	const start = `{"Time":"2026-09-16T00:00:00Z","Action":"start","Package":"example/dml"}`
	const acquire = `{"Time":"2026-09-16T00:00:01Z","Action":"output","Package":"example/dml","Test":"TestFirst","Output":"MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=admission-acquire duration=1s status=ready wait=1s"}`
	const release = `{"Time":"2026-09-16T00:00:04Z","Action":"output","Package":"example/dml","Output":"MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=admission-release duration=1ms status=ready admission_released=true"}`
	const terminal = `{"Time":"2026-09-16T00:00:09Z","Action":"pass","Package":"example/dml"}`
	const aggregate = `{"Output":"MO_UT_SETUP fixture=shared-cluster phase=start duration=1s status=ready"}`
	for _, tt := range []struct {
		name   string
		events []string
		want   string
		absent string
	}{
		{"explicit", []string{start, acquire, release, terminal}, "explicit=1 process-exit-inferred=0 unresolved=0", "evidence=process-exit-inferred"},
		{"holder without duration", []string{aggregate, start, strings.ReplaceAll(acquire, "duration=1s ", ""), strings.ReplaceAll(release, "duration=1ms ", ""), terminal}, "explicit=1 process-exit-inferred=0 unresolved=0", "evidence=process-exit-inferred"},
		{"escaped marker", []string{start, strings.ReplaceAll(acquire, "MO_UT_SETUP", `MO_UT\u005fSETUP`), release, terminal}, "explicit=1 process-exit-inferred=0 unresolved=0", "evidence=process-exit-inferred"},
		{"process exit", []string{start, acquire, terminal}, "duration=8.00s evidence=process-exit-inferred package=example/dml", "evidence=explicit"},
		{"cancelled capture", []string{start, acquire, "{truncated"}, "explicit=0 process-exit-inferred=0 unresolved=1", "admission holder duration="},
		{"missing invocation start", []string{acquire, terminal}, "explicit=0 process-exit-inferred=0 unresolved=1", "admission holder duration="},
		{"startless capture after terminated invocation", []string{start, terminal, strings.ReplaceAll(acquire, "00:00:01Z", "00:00:10Z"), strings.ReplaceAll(terminal, "00:00:09Z", "00:00:20Z")}, "explicit=0 process-exit-inferred=0 unresolved=1", "admission holder duration="},
		{"new invocation after terminated invocation", []string{start, terminal, strings.ReplaceAll(start, "00:00:00Z", "00:00:10Z"), strings.ReplaceAll(acquire, "00:00:01Z", "00:00:11Z"), strings.ReplaceAll(terminal, "00:00:09Z", "00:00:20Z")}, "duration=9.00s evidence=process-exit-inferred", "unresolved=1"},
		{"overlapping package invocations", []string{start, acquire, start, terminal}, "explicit=0 process-exit-inferred=0 unresolved=1", "admission holder duration="},
		{"multiple process ids", []string{start, acquire, strings.ReplaceAll(acquire, "pid=10", "pid=11"), terminal}, "explicit=0 process-exit-inferred=0 unresolved=2", "admission holder duration="},
		{"restart generation", []string{start, acquire, release, strings.ReplaceAll(acquire, "00:00:01Z", "00:00:05Z"), terminal}, "explicit=1 process-exit-inferred=1 unresolved=0", "unresolved=1"},
		{"subtest terminal is not process exit", []string{start, acquire, strings.ReplaceAll(terminal, `"Action":"pass"`, `"Action":"pass","Test":"TestFirst"`)}, "explicit=0 process-exit-inferred=0 unresolved=1", "admission holder duration="},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "report.json")
			if err := os.WriteFile(path, []byte(strings.Join(tt.events, "\n")), 0600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command("python3", "summarize_ut_setup.py", path).CombinedOutput()
			if err != nil {
				t.Fatalf("summary: %v\n%s", err, out)
			}
			text := string(out)
			if !strings.Contains(text, tt.want) || strings.Contains(text, tt.absent) {
				t.Fatalf("want %q, exclude %q:\n%s", tt.want, tt.absent, text)
			}
			if !strings.Contains(text, "wait totals overlap holder work") {
				t.Fatal("missing overlap warning")
			}
		})
	}
}

func TestUTSetupSummaryKeepsAggregateAndHolderFiltersIndependent(t *testing.T) {
	t.Run("aggregate does not require package or time", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "report.json")
		report := `{"Output":"MO_UT_SETUP fixture=shared-cluster phase=start duration=1s status=ready"}`
		if err := os.WriteFile(path, []byte(report), 0600); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command("python3", "summarize_ut_setup.py", path).CombinedOutput()
		if err != nil || !strings.Contains(string(out), "fixture=shared-cluster phase=start count=1 total=1.00s") {
			t.Fatalf("aggregate without package/time: %v\n%s", err, out)
		}
	})
	t.Run("holder without any aggregate stays silent", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "report.json")
		report := `{"Time":"2026-09-16T00:00:01Z","Action":"output","Package":"example/dml","Output":"MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=admission-acquire status=ready"}`
		if err := os.WriteFile(path, []byte(report), 0600); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command("python3", "summarize_ut_setup.py", path).CombinedOutput()
		if err != nil || len(out) != 0 {
			t.Fatalf("holder-only report should be silent: %v\n%s", err, out)
		}
	})
	t.Run("invalid time still contributes to aggregate", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "report.json")
		report := `{"Time":"not-a-time","Action":"output","Package":"example/dml","Output":"MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=admission-acquire duration=1s status=ready"}`
		if err := os.WriteFile(path, []byte(report), 0600); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command("python3", "summarize_ut_setup.py", path).CombinedOutput()
		if err != nil || !strings.Contains(string(out), "fixture=embedded-cluster phase=admission-acquire count=1") || strings.Contains(string(out), "admission generations:") {
			t.Fatalf("invalid time must affect only holder evidence: %v\n%s", err, out)
		}
	})
	t.Run("invalid UTF-8 remains an error", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "report.json")
		if err := os.WriteFile(path, []byte{0xff, '\n'}, 0600); err != nil {
			t.Fatal(err)
		}
		out, err := exec.Command("python3", "summarize_ut_setup.py", path).CombinedOutput()
		if err == nil || !strings.Contains(string(out), "UnicodeDecodeError") {
			t.Fatalf("invalid UTF-8 must fail: %v\n%s", err, out)
		}
	})
}

func TestUTAdmissionHolderHelperIgnoresInvalidJSONNumbers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	if err := os.WriteFile(path, []byte(`{"other":`+strings.Repeat("1", 5000)+`}`), 0600); err != nil {
		t.Fatal(err)
	}
	script := `from pathlib import Path
from summarize_ut_setup import admission_holders
import sys
assert admission_holders(Path(sys.argv[1])) == []
`
	out, err := exec.Command("python3", "-c", script, path).CombinedOutput()
	if err != nil {
		t.Fatalf("standalone helper must ignore malformed numbers: %v\n%s", err, out)
	}
}

func TestUTSetupSummaryBoundsEarlyHolderState(t *testing.T) {
	const start = `{"Time":"2026-09-16T00:00:00Z","Action":"start","Package":"example/dml"}`
	const acquire = `{"Time":"2026-09-16T00:00:01Z","Action":"output","Package":"example/dml","Test":"TestFirst","Output":"MO_UT_SETUP fixture=embedded-cluster cluster_id=1 pid=10 phase=admission-acquire status=ready"}`
	const filler = `{"Time":"2026-09-16T00:00:02Z","Action":"start","Package":"example/filler"}`
	const terminal = `{"Time":"2026-09-16T00:00:09Z","Action":"pass","Package":"example/dml"}`
	const aggregate = `{"Output":"MO_UT_SETUP fixture=shared-cluster phase=start duration=1s status=ready"}`
	const script = `from pathlib import Path
import sys
from summarize_ut_setup import main
original = Path.open
opens = 0
def counted(path, *args, **kwargs):
    global opens
    if path == Path(sys.argv[1]):
        opens += 1
    return original(path, *args, **kwargs)
Path.open = counted
status = main(["summary", sys.argv[1]])
assert opens == int(sys.argv[2]), (opens, sys.argv[2])
raise SystemExit(status)
`
	for _, tc := range []struct {
		name       string
		aggregate  string
		wantOpens  string
		wantHolder bool
		repeats    int
		padding    int
	}{
		{name: "no totals", wantOpens: "1", repeats: 4100},
		{name: "late aggregate replays early holder", aggregate: aggregate, wantOpens: "2", wantHolder: true, repeats: 4100},
		{name: "large event replays early holder", aggregate: aggregate, wantOpens: "2", wantHolder: true, repeats: 1, padding: 1024 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "report.json")
			fillerEvent := filler
			if tc.padding > 0 {
				fillerEvent = strings.TrimSuffix(filler, "}") + `,"Padding":"` + strings.Repeat(" ", tc.padding) + `"}`
			}
			report := start + "\n" + acquire + "\n" + strings.Repeat(fillerEvent+"\n", tc.repeats) + terminal + "\n" + tc.aggregate
			if err := os.WriteFile(path, []byte(report), 0600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command("python3", "-c", script, path, tc.wantOpens).CombinedOutput()
			if err != nil {
				t.Fatalf("summary fallback: %v\n%s", err, out)
			}
			if tc.wantHolder && !strings.Contains(string(out), "duration=8.00s evidence=process-exit-inferred package=example/dml") {
				t.Fatalf("early holder evidence was lost: %s", out)
			}
			if !tc.wantHolder && len(out) != 0 {
				t.Fatalf("holder-only report should be silent: %s", out)
			}
		})
	}
}

func TestUTSetupSummaryDefersHolderErrorsUntilTotals(t *testing.T) {
	const script = `import sys
import summarize_ut_setup as summary
def fail(_self, _event):
    raise RuntimeError("injected holder failure")
summary.AdmissionHolders._observe = fail
try:
    result = summary.main(["summary", sys.argv[1]])
except RuntimeError as error:
    assert str(error) == "injected holder failure"
    print("holder failure surfaced")
    raise SystemExit(7)
raise SystemExit(result)
`
	const start = `{"Time":"2026-09-16T00:00:00Z","Action":"start","Package":"example/dml"}`
	const aggregate = `{"Output":"MO_UT_SETUP fixture=shared-cluster phase=start duration=1s status=ready"}`
	for _, hasTotals := range []bool{false, true} {
		t.Run(fmt.Sprintf("totals=%t", hasTotals), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "report.json")
			report := start
			if hasTotals {
				report += "\n" + aggregate
			}
			if err := os.WriteFile(path, []byte(report), 0600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command("python3", "-c", script, path).CombinedOutput()
			if !hasTotals {
				if err != nil || len(out) != 0 {
					t.Fatalf("no totals should be silent: %v\n%s", err, out)
				}
				return
			}
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 7 {
				t.Fatalf("expected holder failure: %v\n%s", err, out)
			}
			text := string(out)
			if summaryAt, errorAt := strings.Index(text, "fixture=shared-cluster"), strings.Index(text, "holder failure surfaced"); summaryAt < 0 || errorAt <= summaryAt {
				t.Fatalf("holder failure must follow the aggregate summary: %s", text)
			}
		})
	}
}
