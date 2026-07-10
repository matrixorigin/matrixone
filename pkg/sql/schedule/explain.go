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

package schedule

import (
	"fmt"
	"strings"
)

const (
	maxSchedulingExplainLines   = 128
	maxSchedulingExplainWorkers = 16
	maxSchedulingExplainValue   = 128
)

// ExplainLines renders bounded, human-readable scheduling data for SQL
// EXPLAIN. The structured trace remains the canonical representation.
func ExplainLines(trace Trace) []string {
	if trace.Empty() {
		return nil
	}
	b := schedulingExplainBuilder{}
	b.add(fmt.Sprintf("Scheduling (%s):", normalizedTraceMode(trace.Mode)))
	for i := range trace.Attempts {
		if b.full() {
			break
		}
		attempt := &trace.Attempts[i]
		b.add(fmt.Sprintf("  Attempt %d:", attempt.Sequence))
		if attempt.Query != nil {
			appendQueryExplain(&b, attempt.Query)
		}
		for j := range attempt.Failures {
			if b.full() {
				break
			}
			failure := &attempt.Failures[j]
			line := fmt.Sprintf("    Failure: category=%s", explainValue(failure.Category))
			if failure.Worker != nil {
				line += " worker=" + explainWorker(*failure.Worker)
			}
			b.add(line)
		}
		for j := range attempt.Scans {
			if b.full() {
				break
			}
			scan := &attempt.Scans[j]
			b.add(fmt.Sprintf(
				"    Scan %d: reason=%s local-only=%t query-workers=%d selected=%d workers=%s stats=%t blocks=%d dop=%d force-one-cn=%t force-single=%t force-multi-cn=%t",
				j+1,
				explainValue(scan.Reason),
				scan.LocalOnly,
				scan.QueryWorkerCount,
				scan.SelectedCount,
				explainWorkers(scan.Selected, scan.SelectedTruncated),
				scan.HasStats,
				scan.BlockCount,
				scan.DOP,
				scan.ForceOneCN,
				scan.ForceSingle,
				scan.ForceMultiCN,
			))
		}
		for j := range attempt.Stages {
			if b.full() {
				break
			}
			stage := &attempt.Stages[j]
			b.add(fmt.Sprintf(
				"    Stage %d: kind=%s reason=%s query-workers=%d selected=%d workers=%s",
				j+1,
				explainValue(string(stage.Kind)),
				explainValue(stage.Reason),
				stage.QueryWorkerCount,
				stage.SelectedCount,
				explainWorkers(stage.Selected, stage.SelectedTruncated),
			))
		}
		if attempt.DetailsOmitted {
			b.add("    Local scan/stage details omitted")
		}
		if attempt.Truncated {
			b.add(fmt.Sprintf(
				"    Attempt details truncated: scans=%d stages=%d failures=%d",
				attempt.ScanCount,
				attempt.StageCount,
				attempt.FailureCount,
			))
		}
	}
	if trace.Truncated {
		b.add(fmt.Sprintf("  Trace truncated: attempts=%d", trace.AttemptCount))
	}
	return b.finish()
}

func appendQueryExplain(b *schedulingExplainBuilder, query *QueryTrace) {
	b.add(fmt.Sprintf(
		"    Query: exec=%s current-cn-policy=%s reason=%s satisfied=%t fallback=%t",
		explainValue(query.ExecKind),
		explainValue(query.CurrentCNPolicy),
		explainValue(query.Reason),
		query.Satisfied,
		query.Fallback,
	))
	b.add(fmt.Sprintf(
		"    Candidates: source=%s pool-resolution=%s discovered=%d resolved=%d selected=%d dropped=%d",
		explainValue(query.CandidateSource),
		explainValue(query.PoolResolution),
		query.DiscoveredCount,
		query.ResolvedCount,
		query.SelectedCount,
		query.DroppedCount,
	))
	b.add("    Current CN: " + explainWorker(query.CurrentCN))
	if query.SelectedOmitted {
		b.add("    Selected workers: local details omitted")
	} else {
		b.add("    Selected workers: " + explainWorkers(query.Selected, query.SelectedTruncated))
	}
	if len(query.Dropped) > 0 {
		parts := make([]string, 0, len(query.Dropped))
		for i := range query.Dropped {
			parts = append(parts, fmt.Sprintf("%s=%d", explainValue(query.Dropped[i].Reason), query.Dropped[i].Count))
		}
		if query.DroppedTruncated {
			parts = append(parts, "...")
		}
		b.add("    Dropped candidates: " + strings.Join(parts, ", "))
	}
}

type schedulingExplainBuilder struct {
	lines     []string
	truncated bool
}

func (b *schedulingExplainBuilder) add(line string) {
	if len(b.lines) >= maxSchedulingExplainLines-1 {
		b.truncated = true
		return
	}
	b.lines = append(b.lines, line)
}

func (b *schedulingExplainBuilder) full() bool {
	if len(b.lines) < maxSchedulingExplainLines-1 {
		return false
	}
	b.truncated = true
	return true
}

func (b *schedulingExplainBuilder) finish() []string {
	if b.truncated {
		b.lines = append(b.lines, "  Scheduling explain output truncated")
	}
	return b.lines
}

func normalizedTraceMode(mode TraceMode) TraceMode {
	if mode == TraceModePreview {
		return mode
	}
	return TraceModeExecution
}

func explainWorkers(workers []WorkerTrace, truncated bool) string {
	if len(workers) == 0 {
		if truncated {
			return "[...]"
		}
		return "[]"
	}
	limit := min(len(workers), maxSchedulingExplainWorkers)
	parts := make([]string, 0, limit+1)
	for i := 0; i < limit; i++ {
		parts = append(parts, explainWorker(workers[i]))
	}
	if truncated || len(workers) > limit {
		parts = append(parts, "...")
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func explainWorker(worker WorkerTrace) string {
	parts := make([]string, 0, 4)
	if worker.ID != "" {
		parts = append(parts, "id="+explainValue(worker.ID))
	}
	if worker.Routable {
		parts = append(parts, "route=available")
	}
	if worker.Mcpu != 0 {
		parts = append(parts, fmt.Sprintf("mcpu=%d", worker.Mcpu))
	}
	if worker.State != "" {
		parts = append(parts, "state="+explainValue(worker.State))
	}
	if len(parts) == 0 {
		return "unknown"
	}
	return "{" + strings.Join(parts, " ") + "}"
}

func explainValue(value string) string {
	if value == "" {
		return "unspecified"
	}
	truncated := len(value) > maxSchedulingExplainValue
	if truncated {
		value = value[:maxSchedulingExplainValue]
	}
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r == '\uFFFD' {
			return '?'
		}
		return r
	}, value)
	if truncated {
		return value + "..."
	}
	return value
}
