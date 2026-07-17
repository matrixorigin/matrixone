// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

// Outcome is the terminal result of an attempt or fragment.
type Outcome uint8

const (
	OutcomeUnknown Outcome = iota
	OutcomeSuccess
	OutcomeError
	OutcomeCanceled
	OutcomePanic
)

// Delta is the immutable terminal output of one producer or fragment.
type Delta struct {
	Usage   Usage
	Quality QualityFlags
	Outcome Outcome
}

// MemoryDomainSummary is the exact terminal state of one isolated MPool
// accounting epoch.
type MemoryDomainSummary struct {
	AllocatedBytes     uint64
	FreedBytes         uint64
	PeakLiveBytes      uint64
	LiveBytesAtSeal    uint64
	CrossPoolFreeCount uint64
}

// Validate checks allocator conservation and ownership invariants.
func (m MemoryDomainSummary) Validate() QualityFlags {
	var flags QualityFlags
	if m.FreedBytes > m.AllocatedBytes ||
		m.AllocatedBytes-m.FreedBytes != m.LiveBytesAtSeal ||
		m.PeakLiveBytes < m.LiveBytesAtSeal {
		flags |= QualityInvariantFailure
	}
	if m.CrossPoolFreeCount != 0 {
		flags |= QualityCrossPoolFree | QualityInvariantFailure
	}
	if m.LiveBytesAtSeal != 0 {
		flags |= QualityNonZeroLiveAtSeal
	}
	return flags
}

// MemoryTotals preserves additive allocator facts and both useful peak bounds.
type MemoryTotals struct {
	AllocatedBytes              uint64
	FreedBytes                  uint64
	MaxDomainPeakLiveBytes      uint64
	SumDomainPeakLiveBytesBound uint64
	LiveBytesAtSeal             uint64
	CrossPoolFreeCount          uint64
}

// MergeMemoryDomain merges one physical domain exactly once.
func MergeMemoryDomain(dst *MemoryTotals, domain MemoryDomainSummary) QualityFlags {
	flags := domain.Validate()
	dst.AllocatedBytes, flags = addChecked(dst.AllocatedBytes, domain.AllocatedBytes, flags)
	dst.FreedBytes, flags = addChecked(dst.FreedBytes, domain.FreedBytes, flags)
	dst.LiveBytesAtSeal, flags = addChecked(dst.LiveBytesAtSeal, domain.LiveBytesAtSeal, flags)
	dst.CrossPoolFreeCount, flags = addChecked(dst.CrossPoolFreeCount, domain.CrossPoolFreeCount, flags)
	dst.SumDomainPeakLiveBytesBound, flags = addChecked(
		dst.SumDomainPeakLiveBytesBound, domain.PeakLiveBytes, flags)
	if domain.PeakLiveBytes > dst.MaxDomainPeakLiveBytes {
		dst.MaxDomainPeakLiveBytes = domain.PeakLiveBytes
	}
	return flags
}

// AttemptSummary is the immutable result of one compile/run generation.
type AttemptSummary struct {
	Usage  Usage
	Memory MemoryTotals

	WallNS                   uint64
	MissingFragmentCount     uint64
	MissingMemoryDomainCount uint64
	Quality                  QualityFlags
	Outcome                  Outcome
}

// ExecutionSummary is fixed-size in retry count.
type ExecutionSummary struct {
	Usage  Usage
	Memory MemoryTotals

	AttemptCount             uint64
	RetryWallNS              uint64
	MissingFragmentCount     uint64
	MissingMemoryDomainCount uint64
	Quality                  QualityFlags
	LastOutcome              Outcome
}

// AddAttempt merges a sealed attempt and retains no attempt object. retried is
// true only when the execution actually proceeds to another attempt.
func (s *ExecutionSummary) AddAttempt(attempt AttemptSummary, retried bool) {
	s.Quality |= attempt.Quality | MergeUsage(&s.Usage, attempt.Usage)
	s.Quality |= MergeMemoryTotals(&s.Memory, attempt.Memory)
	s.AttemptCount, s.Quality = addChecked(s.AttemptCount, 1, s.Quality)
	if retried {
		s.RetryWallNS, s.Quality = addChecked(s.RetryWallNS, attempt.WallNS, s.Quality)
	}
	s.MissingFragmentCount, s.Quality = addChecked(
		s.MissingFragmentCount, attempt.MissingFragmentCount, s.Quality)
	s.MissingMemoryDomainCount, s.Quality = addChecked(
		s.MissingMemoryDomainCount, attempt.MissingMemoryDomainCount, s.Quality)
	s.LastOutcome = attempt.Outcome
}

// ConnType is root metadata, not an additive resource.
type ConnType uint8

const (
	ConnUnknown ConnType = iota
	ConnInternal
	ConnExternal
)

// StatementResourceSummary is the only statement and short-group merge
// algebra. Serialization and plan diagnostics consume this value but never add
// resources independently.
type StatementResourceSummary struct {
	Usage  Usage
	Memory MemoryTotals

	StatementWallNS          uint64
	AttemptCount             uint64
	RetryWallNS              uint64
	MissingFragmentCount     uint64
	MissingMemoryDomainCount uint64
	OutputPacketCount        uint64
	Quality                  QualityFlags
	ConnType                 ConnType
}

// MergeExecution merges one sealed logical execution into its root.
func (s *StatementResourceSummary) MergeExecution(execution ExecutionSummary) {
	s.Quality |= execution.Quality | MergeUsage(&s.Usage, execution.Usage)
	s.Quality |= MergeMemoryTotals(&s.Memory, execution.Memory)
	s.AttemptCount, s.Quality = addChecked(s.AttemptCount, execution.AttemptCount, s.Quality)
	s.RetryWallNS, s.Quality = addChecked(s.RetryWallNS, execution.RetryWallNS, s.Quality)
	s.MissingFragmentCount, s.Quality = addChecked(
		s.MissingFragmentCount, execution.MissingFragmentCount, s.Quality)
	s.MissingMemoryDomainCount, s.Quality = addChecked(
		s.MissingMemoryDomainCount, execution.MissingMemoryDomainCount, s.Quality)
}

// Merge combines statement summaries for compound SQL or short-statement
// aggregation. Connection type is preserved and conflicting non-zero values
// are explicitly flagged.
func (s *StatementResourceSummary) Merge(other StatementResourceSummary) {
	s.Quality |= other.Quality | QualityAggregated | MergeUsage(&s.Usage, other.Usage)
	s.Quality |= MergeMemoryTotals(&s.Memory, other.Memory)
	s.StatementWallNS, s.Quality = addChecked(s.StatementWallNS, other.StatementWallNS, s.Quality)
	s.AttemptCount, s.Quality = addChecked(s.AttemptCount, other.AttemptCount, s.Quality)
	s.RetryWallNS, s.Quality = addChecked(s.RetryWallNS, other.RetryWallNS, s.Quality)
	s.MissingFragmentCount, s.Quality = addChecked(
		s.MissingFragmentCount, other.MissingFragmentCount, s.Quality)
	s.MissingMemoryDomainCount, s.Quality = addChecked(
		s.MissingMemoryDomainCount, other.MissingMemoryDomainCount, s.Quality)
	s.OutputPacketCount, s.Quality = addChecked(s.OutputPacketCount, other.OutputPacketCount, s.Quality)
	if s.ConnType == ConnUnknown {
		s.ConnType = other.ConnType
	} else if other.ConnType != ConnUnknown && s.ConnType != other.ConnType {
		s.Quality |= QualityInvariantFailure
	}
}

// MergeMemoryTotals composes already-reduced physical memory domains.
func MergeMemoryTotals(dst *MemoryTotals, delta MemoryTotals) QualityFlags {
	var flags QualityFlags
	dst.AllocatedBytes, flags = addChecked(dst.AllocatedBytes, delta.AllocatedBytes, flags)
	dst.FreedBytes, flags = addChecked(dst.FreedBytes, delta.FreedBytes, flags)
	dst.SumDomainPeakLiveBytesBound, flags = addChecked(
		dst.SumDomainPeakLiveBytesBound, delta.SumDomainPeakLiveBytesBound, flags)
	dst.LiveBytesAtSeal, flags = addChecked(dst.LiveBytesAtSeal, delta.LiveBytesAtSeal, flags)
	dst.CrossPoolFreeCount, flags = addChecked(dst.CrossPoolFreeCount, delta.CrossPoolFreeCount, flags)
	if delta.MaxDomainPeakLiveBytes > dst.MaxDomainPeakLiveBytes {
		dst.MaxDomainPeakLiveBytes = delta.MaxDomainPeakLiveBytes
	}
	return flags
}
