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

// Delta is the immutable terminal output of one producer or fragment.
type Delta struct {
	Usage   Usage
	Quality QualityFlags
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

// AllocationAccountTotals is the fixed-size terminal observation of activated
// allocation generations. It is diagnostic only: these bytes are a subset of
// allocator memory and are never added to MemoryTotals or fed back into
// admission.
type AllocationAccountTotals struct {
	GenerationCount       uint64
	ValidGenerationCount  uint64
	FailedGenerationCount uint64
	MaxGenerationPeak     uint64
	SumGenerationPeak     uint64
	LiveBytesAtTerminal   uint64
}

func (t *AllocationAccountTotals) AddGeneration(
	peak uint64,
	liveAtTerminal uint64,
	valid bool,
) QualityFlags {
	var quality QualityFlags
	t.GenerationCount, quality = addChecked(t.GenerationCount, 1, quality)
	if valid {
		t.ValidGenerationCount, quality = addChecked(
			t.ValidGenerationCount,
			1,
			quality,
		)
	} else {
		t.FailedGenerationCount, quality = addChecked(
			t.FailedGenerationCount,
			1,
			quality|QualityInvariantFailure,
		)
	}
	if peak > t.MaxGenerationPeak {
		t.MaxGenerationPeak = peak
	}
	t.SumGenerationPeak, quality = addChecked(
		t.SumGenerationPeak,
		peak,
		quality,
	)
	t.LiveBytesAtTerminal, quality = addChecked(
		t.LiveBytesAtTerminal,
		liveAtTerminal,
		quality,
	)
	if liveAtTerminal != 0 {
		quality |= QualityNonZeroLiveAtSeal | QualityInvariantFailure
	}
	return quality
}

func MergeAllocationAccountTotals(
	dst *AllocationAccountTotals,
	delta AllocationAccountTotals,
) QualityFlags {
	var quality QualityFlags
	dst.GenerationCount, quality = addChecked(
		dst.GenerationCount,
		delta.GenerationCount,
		quality,
	)
	dst.ValidGenerationCount, quality = addChecked(
		dst.ValidGenerationCount,
		delta.ValidGenerationCount,
		quality,
	)
	dst.FailedGenerationCount, quality = addChecked(
		dst.FailedGenerationCount,
		delta.FailedGenerationCount,
		quality,
	)
	dst.SumGenerationPeak, quality = addChecked(
		dst.SumGenerationPeak,
		delta.SumGenerationPeak,
		quality,
	)
	dst.LiveBytesAtTerminal, quality = addChecked(
		dst.LiveBytesAtTerminal,
		delta.LiveBytesAtTerminal,
		quality,
	)
	if delta.MaxGenerationPeak > dst.MaxGenerationPeak {
		dst.MaxGenerationPeak = delta.MaxGenerationPeak
	}
	if delta.FailedGenerationCount != 0 || delta.LiveBytesAtTerminal != 0 {
		quality |= QualityInvariantFailure
	}
	if delta.ValidGenerationCount > delta.GenerationCount ||
		delta.FailedGenerationCount >
			delta.GenerationCount-delta.ValidGenerationCount ||
		delta.MaxGenerationPeak > delta.SumGenerationPeak {
		quality |= QualityInvariantFailure
	}
	if delta.LiveBytesAtTerminal != 0 {
		quality |= QualityNonZeroLiveAtSeal
	}
	return quality
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
	Usage      Usage
	Memory     MemoryTotals
	Allocation AllocationAccountTotals

	WallNS                   uint64
	MissingFragmentCount     uint64
	MissingMemoryDomainCount uint64
	Quality                  QualityFlags
}

// ExecutionSummary is fixed-size in retry count.
type ExecutionSummary struct {
	Usage      Usage
	Memory     MemoryTotals
	Allocation AllocationAccountTotals

	AttemptCount             uint64
	RetryWallNS              uint64
	MissingFragmentCount     uint64
	MissingMemoryDomainCount uint64
	Quality                  QualityFlags
}

// AddAttempt merges a sealed attempt and retains no attempt object. retried is
// true only when the execution actually proceeds to another attempt.
func (s *ExecutionSummary) AddAttempt(attempt AttemptSummary, retried bool) {
	s.Quality |= attempt.Quality | MergeUsage(&s.Usage, attempt.Usage)
	s.Quality |= MergeMemoryTotals(&s.Memory, attempt.Memory)
	s.Quality |= MergeAllocationAccountTotals(&s.Allocation, attempt.Allocation)
	s.AttemptCount, s.Quality = addChecked(s.AttemptCount, 1, s.Quality)
	if retried {
		s.RetryWallNS, s.Quality = addChecked(s.RetryWallNS, attempt.WallNS, s.Quality)
	}
	s.MissingFragmentCount, s.Quality = addChecked(
		s.MissingFragmentCount, attempt.MissingFragmentCount, s.Quality)
	s.MissingMemoryDomainCount, s.Quality = addChecked(
		s.MissingMemoryDomainCount, attempt.MissingMemoryDomainCount, s.Quality)
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
	Usage      Usage
	Memory     MemoryTotals
	Allocation AllocationAccountTotals

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
	s.Quality |= MergeAllocationAccountTotals(&s.Allocation, execution.Allocation)
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
	s.Quality |= MergeAllocationAccountTotals(&s.Allocation, other.Allocation)
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
