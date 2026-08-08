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

package docfilter

import (
	"fmt"
	"math"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// MemoryAdmission is the narrow physical-memory contract used by CN resource
// throttlers. It intentionally matches rscthrottler.RSCThrottler without
// coupling filter ownership to the CN service implementation.
type MemoryAdmission interface {
	Acquire(int64) (int64, bool)
	Release(int64) int64
}

// MemoryAdmissionError means the local CN could not prove enough physical
// headroom for the filter's build/reconstruction peak. Callers must surface or
// retry the query; silently disabling a prefilter can change Top-K results.
type MemoryAdmissionError struct {
	Requested int64
}

func (e *MemoryAdmissionError) Error() string {
	return fmt.Sprintf(
		"docfilter: physical-memory admission denied for %d bytes",
		e.Requested,
	)
}

// AdmissionForService resolves the production CN throttler at the allocation
// site. Tests and non-CN processes legitimately return nil and retain the
// ordinary Build/New behavior.
func AdmissionForService(service string) MemoryAdmission {
	rt := moruntime.ServiceRuntime(service)
	if rt == nil {
		return nil
	}
	value, ok := rt.GetGlobalVariables(moruntime.CNMemoryThrottler)
	if !ok {
		return nil
	}
	admission, _ := value.(MemoryAdmission)
	return admission
}

func acquireMemory(admission MemoryAdmission, bytes int64) (func(), error) {
	if admission == nil || bytes <= 0 {
		return nil, nil
	}
	if _, ok := admission.Acquire(bytes); !ok {
		return nil, &MemoryAdmissionError{Requested: bytes}
	}
	return func() { admission.Release(bytes) }, nil
}

// refreshBeforeBuildRelease closes the gap between dropping a forward-looking
// reservation and the next admission observing the returned Go payload in RSS.
// The production throttler exposes these optional methods; the narrow base
// interface keeps tests and non-CN callers independent of its implementation.
func refreshBeforeBuildRelease(admission MemoryAdmission) {
	type refreshDecider interface {
		ShouldRefreshBeforeRelease() bool
	}
	type forceRefresher interface {
		ForceRefresh()
	}

	decider, hasDecider := admission.(refreshDecider)
	if hasDecider && !decider.ShouldRefreshBeforeRelease() {
		return
	}
	if refresher, ok := admission.(forceRefresher); ok {
		refresher.ForceRefresh()
	}
}

// cbitmapBuildPeakUpperBound covers the largest dense representation the
// router may select. Serialization writes directly into the final Go payload,
// so the peak is one live C object plus that payload, with no intermediate C
// serialization buffer.
func cbitmapBuildPeakUpperBound(maxBits uint64) int64 {
	const cbitmapObjectBytes = uint64(3 * 8)
	bitmapBytes := ((maxBits + 63) / 64) * 8
	// While serializing, the live C object and the final tagged Go payload
	// coexist. There is no intermediate C serialization buffer.
	const serializedHeaderBytes = uint64(2 * 8)
	return int64(
		cbitmapObjectBytes + bitmapBytes +
			1 + serializedHeaderBytes + bitmapBytes,
	)
}

func sorted64BuildPeakUpperBound(v *vector.Vector) (int64, bool) {
	return sorted64BuildPeakUpperBoundForCount(integerValueCountUpperBound(v))
}

func sorted64BuildPeakUpperBoundForCount(count uint64) (int64, bool) {
	// Tagged construction uses one aligned scratch word before the canonical
	// [count][values] payload and shifts left in the same allocation.
	if count > uint64((math.MaxInt64/8)-2) {
		return 0, false
	}
	return int64((count + 2) * 8), true
}

func buildAllocationBytes(v *vector.Vector) (int64, bool) {
	if v == nil {
		return 0, false
	}
	if SupportsBitset(*v.GetType()) {
		valueRange, ok := integerValueRange(v)
		if !ok {
			return 0, false
		}
		sortedBytes, ok := sorted64BuildPeakUpperBoundForCount(valueRange.count)
		if !ok {
			return 0, false
		}
		bitCap := cbitmapBitCapForCount(valueRange.count)
		if bitCap == 0 || valueRange.count == 0 {
			return sortedBytes, true
		}
		base := uint64(0)
		if CbitmapUseOffset {
			base = valueRange.min
		}
		span := valueRange.max - base
		if span >= bitCap {
			return sortedBytes, true
		}
		return cbitmapBuildPeakUpperBound(span + 1), true
	}
	cBytes, ok := bloomfilter.EstimateCBloomFilterMemoryBytes(
		int64(v.Length()), bloomFpProbability)
	if !ok || cBytes > (math.MaxInt64-1)/2 {
		return 0, false
	}
	// The live C bloom and the final tagged Go payload coexist during marshal.
	return cBytes*2 + 1, true
}

func reconstructAllocationBytes(tag byte, payload []byte) (int64, bool) {
	switch tag {
	case TagSorted64:
		// The payload itself is the live filter and remains Go-owned.
		// Charge the consumer CN for the serialized payload size: these
		// bytes become long-lived reader state and would otherwise
		// accumulate outside the throttler across many concurrent remote
		// sparse filters.
		if len(payload) > math.MaxInt64-1 {
			return 0, false
		}
		return int64(len(payload)), true
	case TagCbitmap:
		// Serialized header is two uint64s; the live C object header is three.
		if len(payload) > math.MaxInt64-8 {
			return 0, false
		}
		return int64(len(payload) + 8), true
	case TagBloom:
		return int64(len(payload)), true
	case TagCRoaring:
		// Compatibility only: new producers emit Sorted64. CRoaring's public API
		// does not expose exact allocator usage, so reserve a deliberately broad
		// expansion bound for mixed-version clusters. This path disappears once
		// all senders use TagSorted64.
		const expansion = int64(32)
		const fixed = int64(64 << 10)
		if uint64(len(payload)) > uint64((math.MaxInt64-fixed)/expansion) {
			return 0, false
		}
		return int64(len(payload))*expansion + fixed, true
	default:
		return 0, false
	}
}

type sharedMemoryRelease struct {
	refs    atomic.Int32
	release func()
}

func newSharedMemoryRelease(release func()) *sharedMemoryRelease {
	if release == nil {
		return nil
	}
	lease := &sharedMemoryRelease{release: release}
	lease.refs.Store(1)
	return lease
}

func (l *sharedMemoryRelease) share() {
	if l != nil {
		l.refs.Add(1)
	}
}

func (l *sharedMemoryRelease) free() {
	if l != nil && l.refs.Add(-1) == 0 {
		l.release()
		l.release = nil
	}
}
