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

package resource

import (
	"bytes"
	"encoding/json"
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// AllocationOwnerMaxID mirrors common/mpool.AllocationOwnerMax. This
// package deliberately cannot import mpool: mpool already exports terminal
// resource facts through this lower-level package.
const AllocationOwnerMaxID uint8 = 63

// AllocationOwnerTotals preserves both useful peak bounds for one owner class.
// LiveBytesAtTerminal is additive and should be zero for successful terminal
// generations.
type AllocationOwnerTotals struct {
	MaxGenerationPeak   uint64
	SumGenerationPeak   uint64
	LiveBytesAtTerminal uint64
}

// allocationOwnerSet is immutable after publication. Values correspond to set
// bits in ascending owner-ID order. Copying an AllocationAccountTotals can
// therefore share this terminal detail safely; every mutation replaces the set.
type allocationOwnerSet struct {
	mask   uint64
	values []AllocationOwnerTotals
}

func (s *allocationOwnerSet) validShape() bool {
	return s == nil ||
		(s.mask&1 == 0 &&
			len(s.values) == bits.OnesCount64(s.mask) &&
			len(s.values) <= int(AllocationOwnerMaxID))
}

func (s *allocationOwnerSet) value(
	owner uint8,
) (AllocationOwnerTotals, bool) {
	if s == nil || owner == 0 || owner > AllocationOwnerMaxID {
		return AllocationOwnerTotals{}, false
	}
	bit := uint64(1) << owner
	if s.mask&bit == 0 {
		return AllocationOwnerTotals{}, false
	}
	return s.values[bits.OnesCount64(s.mask&(bit-1))], true
}

// AddOwnerGeneration adds one exact terminal owner observation. The sparse set
// is copy-on-write because AllocationAccountTotals values are copied while
// propagating terminal summaries between attempts, remotes, and statement roots.
func (t *AllocationAccountTotals) AddOwnerGeneration(
	owner uint8,
	peak uint64,
	liveAtTerminal uint64,
) QualityFlags {
	if t == nil || owner == 0 || owner > AllocationOwnerMaxID {
		return QualityInvariantFailure
	}
	if !t.owners.validShape() {
		return QualityInvariantFailure
	}

	bit := uint64(1) << owner
	oldMask := uint64(0)
	oldValues := []AllocationOwnerTotals(nil)
	if t.owners != nil {
		oldMask = t.owners.mask
		oldValues = t.owners.values
	}
	present := oldMask&bit != 0
	index := bits.OnesCount64(oldMask & (bit - 1))
	count := len(oldValues)
	if !present {
		count++
	}
	values := make([]AllocationOwnerTotals, count)
	if present {
		copy(values, oldValues)
	} else {
		copy(values[:index], oldValues[:index])
		copy(values[index+1:], oldValues[index:])
	}

	value := values[index]
	var quality QualityFlags
	if peak > value.MaxGenerationPeak {
		value.MaxGenerationPeak = peak
	}
	value.SumGenerationPeak, quality = addChecked(
		value.SumGenerationPeak,
		peak,
		quality,
	)
	value.LiveBytesAtTerminal, quality = addChecked(
		value.LiveBytesAtTerminal,
		liveAtTerminal,
		quality,
	)
	values[index] = value
	t.owners = &allocationOwnerSet{mask: oldMask | bit, values: values}

	if liveAtTerminal > peak {
		quality |= QualityInvariantFailure
	}
	if liveAtTerminal != 0 {
		quality |= QualityNonZeroLiveAtSeal | QualityInvariantFailure
	}
	return quality
}

func mergeAllocationOwnerTotals(
	dst *AllocationAccountTotals,
	delta *AllocationAccountTotals,
) QualityFlags {
	if dst == nil || delta == nil ||
		!dst.owners.validShape() || !delta.owners.validShape() {
		return QualityInvariantFailure
	}
	if delta.owners == nil {
		return 0
	}
	if dst.owners == nil {
		// The set is immutable, so this is safe and avoids an allocation in the
		// common one-producer propagation path.
		dst.owners = delta.owners
		return 0
	}

	left := dst.owners
	right := delta.owners
	unionMask := left.mask | right.mask
	values := make([]AllocationOwnerTotals, 0, bits.OnesCount64(unionMask))
	leftIndex := 0
	rightIndex := 0
	var quality QualityFlags
	for mask := unionMask; mask != 0; mask &= mask - 1 {
		bit := mask & -mask
		var value AllocationOwnerTotals
		if left.mask&bit != 0 {
			value = left.values[leftIndex]
			leftIndex++
		}
		if right.mask&bit != 0 {
			deltaValue := right.values[rightIndex]
			rightIndex++
			if deltaValue.MaxGenerationPeak > value.MaxGenerationPeak {
				value.MaxGenerationPeak = deltaValue.MaxGenerationPeak
			}
			value.SumGenerationPeak, quality = addChecked(
				value.SumGenerationPeak,
				deltaValue.SumGenerationPeak,
				quality,
			)
			value.LiveBytesAtTerminal, quality = addChecked(
				value.LiveBytesAtTerminal,
				deltaValue.LiveBytesAtTerminal,
				quality,
			)
		}
		values = append(values, value)
	}
	dst.owners = &allocationOwnerSet{mask: unionMask, values: values}
	return quality
}

// Owner returns the aggregate for one present owner ID. Unknown IDs within the
// bounded range remain observable during rolling upgrades.
func (t *AllocationAccountTotals) Owner(
	owner uint8,
) (AllocationOwnerTotals, bool) {
	if t == nil || !t.owners.validShape() {
		return AllocationOwnerTotals{}, false
	}
	return t.owners.value(owner)
}

// HasOwnerAttribution reports whether this summary contains any owner facts.
func (t *AllocationAccountTotals) HasOwnerAttribution() bool {
	return t != nil && t.owners != nil && t.owners.mask != 0
}

// OwnerAttributionCoversTotals reports whether the published owner peaks can
// cover both aggregate peak bounds. Owner peaks are upper bounds that may
// overlap in time, so coverage is one-sided rather than equality. This check
// detects omitted v4 owner entries; an explicitly mixed legacy summary may
// carry QualityMissingAllocationOwner instead.
func (t *AllocationAccountTotals) OwnerAttributionCoversTotals() bool {
	if t == nil || !t.owners.validShape() {
		return false
	}
	if t.MaxGenerationPeak == 0 && t.SumGenerationPeak == 0 {
		return true
	}
	if !t.HasOwnerAttribution() {
		return false
	}
	var maxPeakCoverage uint64
	var sumPeakCoverage uint64
	for _, owner := range t.owners.values {
		maxPeakCoverage = saturatingAddOwnerCoverage(
			maxPeakCoverage,
			owner.MaxGenerationPeak,
		)
		sumPeakCoverage = saturatingAddOwnerCoverage(
			sumPeakCoverage,
			owner.SumGenerationPeak,
		)
	}
	return maxPeakCoverage >= t.MaxGenerationPeak &&
		sumPeakCoverage >= t.SumGenerationPeak
}

func saturatingAddOwnerCoverage(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}

// OwnerCount returns the bounded number of attributed owner classes.
func (t *AllocationAccountTotals) OwnerCount() int {
	if t == nil || !t.owners.validShape() || t.owners == nil {
		return 0
	}
	return len(t.owners.values)
}

// Clone detaches the immutable owner set for consumers that require a fully
// reference-independent object graph, such as asynchronous plan export.
func (t AllocationAccountTotals) Clone() AllocationAccountTotals {
	if t.owners == nil {
		return t
	}
	values := make([]AllocationOwnerTotals, len(t.owners.values))
	copy(values, t.owners.values)
	t.owners = &allocationOwnerSet{mask: t.owners.mask, values: values}
	return t
}

// Validate checks bounded owner and generation aggregation invariants.
func (t *AllocationAccountTotals) Validate() QualityFlags {
	if t == nil || !t.owners.validShape() {
		return QualityInvariantFailure
	}
	var quality QualityFlags
	if t.ValidGenerationCount > t.GenerationCount ||
		t.FailedGenerationCount >
			t.GenerationCount-t.ValidGenerationCount ||
		t.MaxGenerationPeak > t.SumGenerationPeak ||
		t.GenerationCount == 0 &&
			(t.MaxGenerationPeak != 0 || t.SumGenerationPeak != 0) {
		quality |= QualityInvariantFailure
	}
	if t.FailedGenerationCount != 0 || t.LiveBytesAtTerminal != 0 {
		quality |= QualityInvariantFailure
	}
	if t.LiveBytesAtTerminal != 0 {
		quality |= QualityNonZeroLiveAtSeal
	}

	var ownerLive uint64
	var hasMeasuredOwner bool
	if t.owners != nil {
		for _, value := range t.owners.values {
			if value.MaxGenerationPeak == 0 &&
				value.SumGenerationPeak == 0 &&
				value.LiveBytesAtTerminal == 0 {
				quality |= QualityInvariantFailure
			} else {
				hasMeasuredOwner = true
			}
			if value.MaxGenerationPeak > value.SumGenerationPeak ||
				value.MaxGenerationPeak > t.MaxGenerationPeak ||
				value.SumGenerationPeak > t.SumGenerationPeak ||
				value.LiveBytesAtTerminal > value.SumGenerationPeak {
				quality |= QualityInvariantFailure
			}
			ownerLive, quality = addChecked(
				ownerLive,
				value.LiveBytesAtTerminal,
				quality,
			)
		}
	}
	if t.HasOwnerAttribution() &&
		(t.GenerationCount == 0 ||
			t.SumGenerationPeak != 0 && !hasMeasuredOwner) {
		quality |= QualityInvariantFailure
	}
	// A missing owner set is a valid legacy summary. Once owner facts are
	// present, their terminal-live sum must agree with the account total.
	if t.HasOwnerAttribution() && ownerLive != t.LiveBytesAtTerminal {
		quality |= QualityInvariantFailure
	}
	return quality
}

type allocationOwnerTotalsJSON struct {
	Owner               uint8
	MaxGenerationPeak   uint64
	SumGenerationPeak   uint64
	LiveBytesAtTerminal uint64
}

type allocationAccountTotalsJSON struct {
	GenerationCount       uint64
	ValidGenerationCount  uint64
	FailedGenerationCount uint64
	MaxGenerationPeak     uint64
	SumGenerationPeak     uint64
	LiveBytesAtTerminal   uint64
	Owners                []allocationOwnerTotalsJSON `json:"Owners,omitempty"`
}

type allocationOwnerTotalsJSONList struct {
	values [AllocationOwnerMaxID]allocationOwnerTotalsJSON
	count  uint8
}

func (l *allocationOwnerTotalsJSONList) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '[' {
		return moerr.NewInvalidInputNoCtx("allocation owners must be a JSON array")
	}
	for decoder.More() {
		if l.count >= uint8(len(l.values)) {
			return moerr.NewInvalidInputNoCtxf(
				"allocation owners exceed bounded cardinality %d",
				len(l.values),
			)
		}
		if err = decoder.Decode(&l.values[l.count]); err != nil {
			return err
		}
		l.count++
	}
	_, err = decoder.Token()
	return err
}

type allocationAccountTotalsDecodeJSON struct {
	GenerationCount       uint64
	ValidGenerationCount  uint64
	FailedGenerationCount uint64
	MaxGenerationPeak     uint64
	SumGenerationPeak     uint64
	LiveBytesAtTerminal   uint64
	Owners                allocationOwnerTotalsJSONList
}

// MarshalJSON keeps sparse owner storage private and emits owner IDs in stable
// ascending order.
func (t AllocationAccountTotals) MarshalJSON() ([]byte, error) {
	if !t.owners.validShape() {
		return nil, moerr.NewInternalErrorNoCtx("invalid allocation owner storage")
	}
	wire := allocationAccountTotalsJSON{
		GenerationCount:       t.GenerationCount,
		ValidGenerationCount:  t.ValidGenerationCount,
		FailedGenerationCount: t.FailedGenerationCount,
		MaxGenerationPeak:     t.MaxGenerationPeak,
		SumGenerationPeak:     t.SumGenerationPeak,
		LiveBytesAtTerminal:   t.LiveBytesAtTerminal,
	}
	if t.owners != nil {
		wire.Owners = make(
			[]allocationOwnerTotalsJSON,
			0,
			len(t.owners.values),
		)
		index := 0
		for mask := t.owners.mask; mask != 0; mask &= mask - 1 {
			owner := uint8(bits.TrailingZeros64(mask))
			value := t.owners.values[index]
			index++
			wire.Owners = append(wire.Owners, allocationOwnerTotalsJSON{
				Owner:               owner,
				MaxGenerationPeak:   value.MaxGenerationPeak,
				SumGenerationPeak:   value.SumGenerationPeak,
				LiveBytesAtTerminal: value.LiveBytesAtTerminal,
			})
		}
	}
	return json.Marshal(wire)
}

// UnmarshalJSON accepts unknown-but-bounded owner IDs so mixed-version remote
// execution does not discard new attribution classes.
func (t *AllocationAccountTotals) UnmarshalJSON(data []byte) error {
	var wire allocationAccountTotalsDecodeJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	decoded := AllocationAccountTotals{
		GenerationCount:       wire.GenerationCount,
		ValidGenerationCount:  wire.ValidGenerationCount,
		FailedGenerationCount: wire.FailedGenerationCount,
		MaxGenerationPeak:     wire.MaxGenerationPeak,
		SumGenerationPeak:     wire.SumGenerationPeak,
		LiveBytesAtTerminal:   wire.LiveBytesAtTerminal,
	}
	owners := wire.Owners.values[:wire.Owners.count]
	// At most 63 terminal entries: insertion sort avoids another allocation.
	for index := 1; index < len(owners); index++ {
		for current := index; current > 0 &&
			owners[current].Owner < owners[current-1].Owner; current-- {
			owners[current], owners[current-1] = owners[current-1], owners[current]
		}
	}
	if len(owners) != 0 {
		set := &allocationOwnerSet{
			values: make([]AllocationOwnerTotals, len(owners)),
		}
		for index, owner := range owners {
			if owner.Owner == 0 || owner.Owner > AllocationOwnerMaxID {
				return moerr.NewInvalidInputNoCtxf(
					"allocation owner ID %d is out of range",
					owner.Owner,
				)
			}
			bit := uint64(1) << owner.Owner
			if set.mask&bit != 0 {
				return moerr.NewInvalidInputNoCtxf(
					"allocation owner ID %d is duplicated",
					owner.Owner,
				)
			}
			set.mask |= bit
			set.values[index] = AllocationOwnerTotals{
				MaxGenerationPeak:   owner.MaxGenerationPeak,
				SumGenerationPeak:   owner.SumGenerationPeak,
				LiveBytesAtTerminal: owner.LiveBytesAtTerminal,
			}
		}
		decoded.owners = set
	}
	*t = decoded
	return nil
}
