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

package models

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

const (
	DiagnosticsVersion      = 1
	DiagnosticsHeaderBudget = 2048
	DiagnosticsL2Budget     = 8192
	DiagnosticsL3Budget     = 49152
)

// StatementDiagnostics is a detached projection, never an accounting source.
// It contains no SQL text, live plan pointers, maps or per-instance timing arrays.
type StatementDiagnostics struct {
	Version       int                   `json:"version"`
	Level         int                   `json:"level"`
	CapturedLevel int                   `json:"captured_level"`
	Reasons       uint64                `json:"reasons"`
	Outcome       string                `json:"outcome"`
	Summary       *DiagnosticSummary    `json:"summary,omitempty"`
	Phases        *DiagnosticPhases     `json:"phases,omitempty"`
	Scheduling    *DiagnosticScheduling `json:"scheduling,omitempty"`
	Detail        DiagnosticDetail      `json:"detail"`
	Logical       []DiagnosticNode      `json:"logical,omitempty"`
	Physical      []DiagnosticNode      `json:"physical,omitempty"`
}

type DiagnosticSummary struct {
	WallNS                  uint64                         `json:"wall_ns"`
	ElapsedNS               uint64                         `json:"elapsed_ns"`
	WaitActiveKnown         bool                           `json:"wait_active_known"`
	WaitActiveNS            uint64                         `json:"wait_active_ns,omitempty"`
	ActiveNS                uint64                         `json:"active_ns"`
	WaitNS                  [resource.WaitKindCount]uint64 `json:"wait_ns"`
	MaxDomainPeakBytes      uint64                         `json:"max_domain_peak_bytes"`
	SumDomainPeakBoundBytes uint64                         `json:"sum_domain_peak_bound_bytes,omitempty"`
	S3ReadBytes             uint64                         `json:"s3_read_bytes,omitempty"`
	S3WriteBytes            uint64                         `json:"s3_write_bytes,omitempty"`
	S3Requests              [resource.S3OpCount]uint64     `json:"s3_requests"`
	EgressBytes             uint64                         `json:"egress_bytes,omitempty"`
	SpillBytes              uint64                         `json:"spill_bytes,omitempty"`
	Attempts                uint64                         `json:"attempts"`
	RetryWallNS             uint64                         `json:"retry_wall_ns,omitempty"`
	Quality                 resource.QualityFlags          `json:"quality"`
	MissingFragments        uint64                         `json:"missing_fragments,omitempty"`
	MissingMemoryDomains    uint64                         `json:"missing_memory_domains,omitempty"`
}

type DiagnosticPhases struct {
	ParseNS   int64 `json:"parse_ns,omitempty"`
	PlanNS    int64 `json:"plan_ns,omitempty"`
	CompileNS int64 `json:"compile_ns,omitempty"`
	PrepareNS int64 `json:"prepare_ns,omitempty"`
	ExecuteNS int64 `json:"execute_ns,omitempty"`
}

type DiagnosticScheduling struct {
	AttemptCount  int    `json:"attempt_count"`
	ExecKind      string `json:"exec_kind,omitempty"`
	Reason        string `json:"reason,omitempty"`
	Fallback      bool   `json:"fallback,omitempty"`
	SelectedCount int    `json:"selected_count,omitempty"`
	DroppedCount  int    `json:"dropped_count,omitempty"`
	FailureCount  int    `json:"failure_count,omitempty"`
	RequestedPool string `json:"requested_pool,omitempty"`
	ResolvedPool  string `json:"resolved_pool,omitempty"`
	Truncated     bool   `json:"truncated,omitempty"`
}

type DiagnosticDetail struct {
	Capture                   string `json:"capture"`
	LogicalTotal              int    `json:"logical_total,omitempty"`
	LogicalOmitted            int    `json:"logical_omitted,omitempty"`
	PhysicalVisited           int    `json:"physical_visited,omitempty"`
	PhysicalCandidatesOmitted int    `json:"physical_candidates_omitted,omitempty"`
	PhysicalTruncated         bool   `json:"physical_truncated,omitempty"`
}

// DiagnosticNode fields have explicit units. Physical rows link to logical
// nodes by array index, without duplicating object names or tree structure.
type DiagnosticNode struct {
	Index            int                   `json:"index"`
	Scope            int                   `json:"scope,omitempty"`
	Remote           bool                  `json:"remote,omitempty"`
	Operator         int                   `json:"operator,omitempty"`
	Kind             string                `json:"kind"`
	Object           string                `json:"object,omitempty"`
	AnalyzeAvailable bool                  `json:"analyze_available"`
	ElapsedNS        int64                 `json:"elapsed_ns,omitempty"`
	WaitNS           int64                 `json:"wait_ns,omitempty"`
	InputRows        int64                 `json:"input_rows,omitempty"`
	OutputRows       int64                 `json:"output_rows,omitempty"`
	InputBytes       int64                 `json:"input_bytes,omitempty"`
	OutputBytes      int64                 `json:"output_bytes,omitempty"`
	MemoryBytes      int64                 `json:"memory_bytes,omitempty"`
	SpillBytes       int64                 `json:"spill_bytes,omitempty"`
	ReadBytes        int64                 `json:"read_bytes,omitempty"`
	S3ReadBytes      int64                 `json:"s3_read_bytes,omitempty"`
	NetworkBytes     int64                 `json:"network_bytes,omitempty"`
	CallCount        int                   `json:"call_count,omitempty"`
	Quality          resource.QualityFlags `json:"quality,omitempty"`
}

func diagnosticAdd(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}
func diagnosticDuration(d time.Duration) uint64 {
	if d < 0 {
		return 0
	}
	return uint64(d)
}

// SelectStatementDiagnosticLevel uses existing facts; it does no traversal,
// allocation, locking or resource accounting. Reason bit positions are v1 ABI.
func SelectStatementDiagnosticLevel(elapsed, threshold time.Duration, summary *resource.StatementResourceSummary, failed, scheduling bool) (level int, reasons uint64) {
	admit := func(bit uint, value uint64, l1, l2, l3 uint64) {
		if value >= l1 {
			reasons |= 1 << bit
			if level < 1 {
				level = 1
			}
		}
		if value >= l2 && level < 2 {
			level = 2
		}
		if value >= l3 && level < 3 {
			level = 3
		}
	}
	floor := threshold
	if floor < time.Second {
		floor = time.Second
	}
	if elapsed >= 0 && elapsed > threshold {
		level = 1
		reasons |= 1
	}
	if uint64(diagnosticDuration(elapsed)) >= diagnosticAdd(uint64(floor), diagnosticAdd(uint64(floor), diagnosticAdd(uint64(floor), uint64(floor)))) {
		level = 2
		reasons |= 1
	}
	if uint64(floor) <= math.MaxUint64/16 && diagnosticDuration(elapsed) >= uint64(floor)*16 {
		level = 3
		reasons |= 1
	}
	if summary != nil {
		u := summary.Usage
		admit(1, u.ExclusiveActiveNS, uint64(time.Second), uint64(4*time.Second), uint64(16*time.Second))
		var waits uint64
		for _, n := range u.WaitNS {
			waits = diagnosticAdd(waits, n)
		}
		admit(2, waits, uint64(time.Second), uint64(4*time.Second), uint64(16*time.Second))
		admit(3, summary.Memory.MaxDomainPeakLiveBytes, 64<<20, 256<<20, 1<<30)
		admit(4, diagnosticAdd(u.S3ReadBytes, u.S3WriteBytes), 64<<20, 256<<20, 1<<30)
		admit(5, u.SpillBytes, 1, 16<<20, 256<<20)
		if summary.AttemptCount > 1 {
			reasons |= 1 << 7
			if level < 2 {
				level = 2
			}
		}
		if summary.Quality != 0 {
			reasons |= 1 << 8
			if level < 2 {
				level = 2
			}
		}
		if summary.Quality&(resource.QualityInvariantFailure|resource.QualityCrossPoolFree) != 0 {
			level = 3
		}
	}
	if failed {
		reasons |= 1 << 6
		if level < 2 {
			level = 2
		}
	}
	if scheduling {
		reasons |= 1 << 9
		if level < 1 {
			level = 1
		}
	}
	return
}

func DiagnosticOutcome(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, context.DeadlineExceeded) || diagnosticMOErrorCode(err, moerr.ErrQueryTimeout) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) || diagnosticMOErrorCode(err, moerr.ErrQueryInterrupted) {
		return "cancelled"
	}
	return "failed"
}

// Follow Go wrappers and errors.Join without classifying by message text.
func diagnosticMOErrorCode(err error, code uint16) bool {
	if moerr.IsMoErrCode(err, code) {
		return true
	}
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			if diagnosticMOErrorCode(child, code) {
				return true
			}
		}
	case interface{ Unwrap() error }:
		return diagnosticMOErrorCode(e.Unwrap(), code)
	}
	return false
}

func (d *StatementDiagnostics) SetSummary(s resource.StatementResourceSummary, wall, wait time.Duration, waitKnown bool) {
	waitKnown = waitKnown && wait >= 0
	if !waitKnown {
		wait = 0
	}
	wall = max(0, wall)
	elapsed := wall - wait
	if elapsed < 0 {
		elapsed = 0
	}
	d.Summary = &DiagnosticSummary{WallNS: diagnosticDuration(wall), ElapsedNS: diagnosticDuration(elapsed), WaitActiveKnown: waitKnown, WaitActiveNS: diagnosticDuration(wait), ActiveNS: s.Usage.ExclusiveActiveNS, WaitNS: s.Usage.WaitNS, MaxDomainPeakBytes: s.Memory.MaxDomainPeakLiveBytes, SumDomainPeakBoundBytes: s.Memory.SumDomainPeakLiveBytesBound, S3ReadBytes: s.Usage.S3ReadBytes, S3WriteBytes: s.Usage.S3WriteBytes, S3Requests: s.Usage.S3Requests, EgressBytes: s.Usage.ClientEgressBytes, SpillBytes: s.Usage.SpillBytes, Attempts: s.AttemptCount, RetryWallNS: s.RetryWallNS, Quality: s.Quality, MissingFragments: s.MissingFragmentCount, MissingMemoryDomains: s.MissingMemoryDomainCount}
}

func (d *StatementDiagnostics) SetPhases(s *statistic.StatsInfo) {
	if s == nil {
		return
	}
	positive := func(n int64) int64 {
		if n < 0 {
			return 0
		}
		return n
	}
	d.Phases = &DiagnosticPhases{ParseNS: positive(int64(s.ParseStage.ParseDuration)), PlanNS: positive(int64(s.PlanStage.PlanDuration)), CompileNS: positive(int64(s.CompileStage.CompileDuration)), PrepareNS: positive(s.PrepareRunStage.ScopePrepareDuration), ExecuteNS: positive(int64(s.ExecuteStage.ExecutionDuration))}
}

// BoundDiagnosticString clones a UTF-8 prefix bounded by JSON-escaped bytes,
// so retaining a selected identifier never retains its large backing storage.
func BoundDiagnosticString(s string, budget int) string {
	end, cost := 0, 0
	for end < len(s) {
		r, n := utf8.DecodeRuneInString(s[end:])
		c := n
		if r == utf8.RuneError && n == 1 {
			c = 3
		}
		if r < 0x20 {
			c = 6
		}
		if r == '"' || r == '\\' {
			c = 2
		}
		if r == '\u2028' || r == '\u2029' {
			c = 6
		}
		if cost+c > budget {
			break
		}
		cost += c
		end += n
	}
	// Repair only the bounded copied prefix; never scan the unselected suffix.
	return strings.Clone(strings.ToValidUTF8(s[:end], "�"))
}

func diagnosticJSON(v any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte{'\n'}), nil
}

// WriteJSON encodes individual selected rows, reserving a fixed header budget.
// It never builds/clones a full plan or cuts serialized JSON mid-value.
func (d *StatementDiagnostics) WriteJSON(out *bytes.Buffer) error {
	budget := DiagnosticsHeaderBudget
	if d.Level == 2 {
		budget = DiagnosticsL2Budget
	}
	if d.Level >= 3 {
		budget = DiagnosticsL3Budget
	}
	header := *d
	header.Logical = nil
	header.Physical = nil
	var rows bytes.Buffer
	appendRows := func(name string, nodes []DiagnosticNode, limit int) (int, error) {
		if len(nodes) == 0 {
			return 0, nil
		}
		kept := 0
		for _, n := range nodes {
			row, err := diagnosticJSON(n)
			if err != nil {
				return kept, err
			}
			overhead := 1
			if kept == 0 {
				overhead = len(name) + 6
			}
			if rows.Len()+len(row)+overhead+1 > limit {
				continue
			}
			if kept == 0 {
				rows.WriteString(",\"")
				rows.WriteString(name)
				rows.WriteString("\":[")
			} else {
				rows.WriteByte(',')
			}
			rows.Write(row)
			kept++
		}
		if kept > 0 {
			rows.WriteByte(']')
		}
		return kept, nil
	}
	logicalLimit := budget - DiagnosticsHeaderBudget
	if d.Level >= 3 && logicalLimit > 16<<10 {
		logicalLimit = 16 << 10
	}
	logical, err := appendRows("logical", d.Logical, logicalLimit)
	if err != nil {
		return err
	}
	physical, err := appendRows("physical", d.Physical, budget-DiagnosticsHeaderBudget)
	if err != nil {
		return err
	}
	header.Detail.LogicalOmitted = max(0, header.Detail.LogicalTotal-logical)
	header.Detail.PhysicalCandidatesOmitted = len(d.Physical) - physical
	// Optional scheduling identifiers have a lower priority than authoritative
	// scalars. Their individual strings were bounded before retention.
	h, err := diagnosticJSON(header)
	if err != nil {
		return err
	}
	const envelope = "{\"statement_diagnostics\":"
	if len(h)+len(envelope)+1 > DiagnosticsHeaderBudget {
		if header.Scheduling != nil {
			s := *header.Scheduling
			s.RequestedPool = ""
			s.ResolvedPool = ""
			s.Reason = ""
			s.Truncated = true
			header.Scheduling = &s
		}
		header.Phases = nil
		h, err = diagnosticJSON(header)
		if err != nil {
			return err
		}
	}
	if len(h)+len(envelope)+1 > DiagnosticsHeaderBudget {
		return moerr.NewInternalErrorNoCtxf("statement diagnostic scalar header exceeds %d bytes", DiagnosticsHeaderBudget)
	}
	out.Reset()
	out.WriteString(envelope)
	out.Write(h[:len(h)-1])
	out.Write(rows.Bytes())
	out.WriteString("}}")
	return nil
}

// RenderStatementDiagnostics presents only captured facts. Parallel elapsed
// counters are labelled as such, never relabelled as CPU or statement totals.
func RenderStatementDiagnostics(d *StatementDiagnostics, option ExplainOption) string {
	if d.Version != DiagnosticsVersion {
		return fmt.Sprintf("Unsupported statement diagnostics version %d\n", d.Version)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Statement diagnostics L%d (captured L%d): %s; reasons=%s\n", d.Level, d.CapturedLevel, d.Outcome, diagnosticReasonNames(d.Reasons))
	if s := d.Summary; s != nil {
		fmt.Fprintf(&b, "wall=%s elapsed=%s active=%s wait_ns=%v\n", diagnosticTime(s.WallNS), diagnosticTime(s.ElapsedNS), diagnosticTime(s.ActiveNS), s.WaitNS)
		fmt.Fprintf(&b, "max-domain-peak=%dB sum-domain-peak-bound=%dB; S3 read/write=%d/%dB requests=%v; spill=%dB egress=%dB\n", s.MaxDomainPeakBytes, s.SumDomainPeakBoundBytes, s.S3ReadBytes, s.S3WriteBytes, s.S3Requests, s.SpillBytes, s.EgressBytes)
		fmt.Fprintf(&b, "attempts=%d retry-wall=%dns quality=%s missing-fragments=%d missing-memory-domains=%d\n", s.Attempts, s.RetryWallNS, s.Quality.String(), s.MissingFragments, s.MissingMemoryDomains)
	}
	fmt.Fprintf(&b, "detail=%s logical-omitted=%d physical-visited=%d physical-bounded=%t physical-candidates-omitted=%d\n", d.Detail.Capture, d.Detail.LogicalOmitted, d.Detail.PhysicalVisited, d.Detail.PhysicalTruncated, d.Detail.PhysicalCandidatesOmitted)
	if option == NormalOption {
		return b.String()
	}
	if p := d.Phases; p != nil {
		fmt.Fprintf(&b, "phase_ns parse=%d plan=%d compile=%d prepare=%d execute=%d\n", p.ParseNS, p.PlanNS, p.CompileNS, p.PrepareNS, p.ExecuteNS)
	}
	if s := d.Scheduling; s != nil {
		fmt.Fprintf(&b, "scheduling kind=%s reason=%s attempts=%d selected=%d dropped=%d failures=%d fallback=%t pool=%s->%s bounded=%t\n", s.ExecKind, s.Reason, s.AttemptCount, s.SelectedCount, s.DroppedCount, s.FailureCount, s.Fallback, s.RequestedPool, s.ResolvedPool, s.Truncated)
	}
	render := func(label string, rows []DiagnosticNode) {
		for _, n := range rows {
			fmt.Fprintf(&b, "%s[%d] %s", label, n.Index, n.Kind)
			if n.Object != "" {
				fmt.Fprintf(&b, " %s", n.Object)
			}
			if label == "instance" {
				fmt.Fprintf(&b, " scope=%d remote=%t op=%d", n.Scope, n.Remote, n.Operator)
			}
			if !n.AnalyzeAvailable {
				b.WriteString(" analysis-unavailable")
			}
			if n.ElapsedNS != 0 {
				fmt.Fprintf(&b, " elapsed=%s", diagnosticTime(uint64(n.ElapsedNS)))
			}
			if n.WaitNS != 0 {
				fmt.Fprintf(&b, " wait=%s", diagnosticTime(uint64(n.WaitNS)))
			}
			if n.InputRows != 0 || n.OutputRows != 0 {
				fmt.Fprintf(&b, " rows=%d->%d", n.InputRows, n.OutputRows)
			}
			if n.InputBytes != 0 || n.OutputBytes != 0 {
				fmt.Fprintf(&b, " bytes=%d->%d", n.InputBytes, n.OutputBytes)
			}
			if n.MemoryBytes != 0 {
				fmt.Fprintf(&b, " memory=%dB", n.MemoryBytes)
			}
			if n.SpillBytes != 0 {
				fmt.Fprintf(&b, " spill=%dB", n.SpillBytes)
			}
			if n.ReadBytes != 0 {
				fmt.Fprintf(&b, " read=%dB", n.ReadBytes)
			}
			if n.S3ReadBytes != 0 {
				fmt.Fprintf(&b, " S3-read=%dB", n.S3ReadBytes)
			}
			if n.NetworkBytes != 0 {
				fmt.Fprintf(&b, " network=%dB", n.NetworkBytes)
			}
			if n.CallCount != 0 {
				fmt.Fprintf(&b, " calls=%d", n.CallCount)
			}
			if n.Quality != 0 {
				fmt.Fprintf(&b, " quality=%s", n.Quality.String())
			}
			b.WriteByte('\n')
		}
	}
	render("node", d.Logical)
	if option == AnalyzeOption {
		render("instance", d.Physical)
	}
	return b.String()
}

func diagnosticTime(ns uint64) string {
	if ns > math.MaxInt64 {
		return fmt.Sprintf("%dns", ns)
	}
	return time.Duration(ns).String()
}
func diagnosticReasonNames(reasons uint64) string {
	var names []string
	for bit, name := range [...]string{"elapsed", "active", "wait", "memory", "S3-bytes", "spill", "failure", "retry", "quality", "scheduling"} {
		mask := uint64(1) << bit
		if reasons&mask != 0 {
			names = append(names, name)
			reasons &^= mask
		}
	}
	if reasons != 0 {
		names = append(names, fmt.Sprintf("unknown(%#x)", reasons))
	}
	return strings.Join(names, ",")
}
