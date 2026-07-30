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

// Package resource defines the allocation-free resource accounting algebra.
// It intentionally depends only on the standard library so producers can use
// one model without introducing an execution or observability dependency cycle.
package resource

import (
	"math"
	"strings"
)

// WaitKind identifies a producer-local blocking reason.
type WaitKind uint8

const (
	WaitLock WaitKind = iota
	WaitFilesystem
	WaitIOMerger
	WaitRemote
	WaitOutput
	WaitOther

	WaitKindCount
)

func (f QualityFlags) String() string {
	if f == 0 {
		return "complete"
	}
	names := make([]string, 0, 8)
	for _, flag := range [...]struct {
		value QualityFlags
		name  string
	}{
		{QualityPartial, "partial"},
		{QualityMissingFragment, "missing-fragment"},
		{QualityMissingMemoryDomain, "missing-memory-domain"},
		{QualityInvariantFailure, "invariant-failure"},
		{QualityProjectionOverflow, "projection-overflow"},
		{QualityCrossPoolFree, "cross-pool-free"},
		{QualityNonZeroLiveAtSeal, "live-at-seal"},
		{QualityAggregated, "aggregated"},
	} {
		if f&flag.value != 0 {
			names = append(names, flag.name)
			f &^= flag.value
		}
	}
	if f != 0 {
		names = append(names, "unknown")
	}
	return strings.Join(names, "|")
}

// S3Op identifies an observed physical object-store operation.
type S3Op uint8

const (
	S3Head S3Op = iota
	S3Get
	S3Put
	S3List
	S3Delete
	S3DeleteMulti

	S3OpCount
)

// QualityFlags describes known incompleteness or invariant failures. Flags are
// facts about the summary; they are never used to fabricate missing resources.
type QualityFlags uint64

const (
	QualityPartial QualityFlags = 1 << iota
	QualityMissingFragment
	QualityMissingMemoryDomain
	QualityInvariantFailure
	QualityProjectionOverflow
	QualityCrossPoolFree
	QualityNonZeroLiveAtSeal
	QualityAggregated
)

// Usage contains only additive quantities. It is deliberately compact enough
// to live inside existing producer-local analyzer state.
type Usage struct {
	ExclusiveActiveNS uint64
	WaitNS            [WaitKindCount]uint64

	S3Requests   [S3OpCount]uint64
	S3ReadBytes  uint64
	S3WriteBytes uint64

	ClientEgressBytes uint64
	SpillBytes        uint64
}

// MergeUsage adds delta into dst and saturates on overflow. Saturation is
// always accompanied by QualityInvariantFailure, so a wrapped value can never
// appear valid.
func MergeUsage(dst *Usage, delta Usage) QualityFlags {
	var flags QualityFlags
	dst.ExclusiveActiveNS, flags = addChecked(dst.ExclusiveActiveNS, delta.ExclusiveActiveNS, flags)
	for i := range dst.WaitNS {
		dst.WaitNS[i], flags = addChecked(dst.WaitNS[i], delta.WaitNS[i], flags)
	}
	for i := range dst.S3Requests {
		dst.S3Requests[i], flags = addChecked(dst.S3Requests[i], delta.S3Requests[i], flags)
	}
	dst.S3ReadBytes, flags = addChecked(dst.S3ReadBytes, delta.S3ReadBytes, flags)
	dst.S3WriteBytes, flags = addChecked(dst.S3WriteBytes, delta.S3WriteBytes, flags)
	dst.ClientEgressBytes, flags = addChecked(dst.ClientEgressBytes, delta.ClientEgressBytes, flags)
	dst.SpillBytes, flags = addChecked(dst.SpillBytes, delta.SpillBytes, flags)
	return flags
}

// ExclusiveActive returns wall minus producer-local waits and child-call wall.
// Invalid local algebra contributes zero and is explicitly flagged.
func ExclusiveActive(wallNS, localWaitNS, childCallNS uint64) (uint64, QualityFlags) {
	nonActive, overflow := add(localWaitNS, childCallNS)
	if overflow || nonActive > wallNS {
		return 0, QualityInvariantFailure
	}
	return wallNS - nonActive, 0
}

// TotalWaitNS returns the sum of all wait kinds, saturating and flagging an
// overflow rather than wrapping.
func (u Usage) TotalWaitNS() (uint64, QualityFlags) {
	var total uint64
	var flags QualityFlags
	for _, wait := range u.WaitNS {
		total, flags = addChecked(total, wait, flags)
	}
	return total, flags
}

func addChecked(a, b uint64, flags QualityFlags) (uint64, QualityFlags) {
	value, overflow := add(a, b)
	if overflow {
		return math.MaxUint64, flags | QualityInvariantFailure
	}
	return value, flags
}

func add(a, b uint64) (uint64, bool) {
	if math.MaxUint64-a < b {
		return math.MaxUint64, true
	}
	return a + b, false
}
