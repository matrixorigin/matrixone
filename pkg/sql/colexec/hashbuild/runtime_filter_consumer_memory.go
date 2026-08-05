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

package hashbuild

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// IVF/fulltext membership-filter consumers build an opaque C structure from
	// the unique-key vector. During docfilter.Build, the C structure, its C
	// serialization buffer, and the Go payload can coexist. The serialized
	// vector is already allocation-accounted; this reservation covers only the
	// downstream C/Go expansion that MPool cannot observe.
	runtimeFilterConsumerBytesPerKey   = uint64(16)
	runtimeFilterConsumerPayloadCopies = uint64(2)
	runtimeFilterConsumerFixedOverhead = uint64(64 << 10)
)

// runtimeFilterConsumerMemoryBound returns a conservative upper bound for the
// opaque downstream docfilter build. It scales with both cardinality and the
// actual serialized payload, so larger data sets remain subject to the same
// CN-wide admission policy instead of crossing a case-specific row cutoff.
func runtimeFilterConsumerMemoryBound(payloadBytes, cardinality int) (int64, bool) {
	if payloadBytes < 0 || cardinality < 0 {
		return 0, false
	}
	payload := uint64(payloadBytes)
	card := uint64(cardinality)
	if payload > (math.MaxUint64-runtimeFilterConsumerFixedOverhead)/runtimeFilterConsumerPayloadCopies {
		return 0, false
	}
	bound := payload*runtimeFilterConsumerPayloadCopies + runtimeFilterConsumerFixedOverhead
	if card > (math.MaxUint64-bound)/runtimeFilterConsumerBytesPerKey {
		return 0, false
	}
	bound += card * runtimeFilterConsumerBytesPerKey
	if bound > math.MaxInt64 {
		return 0, false
	}
	return int64(bound), true
}

// reserveRuntimeFilterConsumerMemory holds CN RSS headroom from publication
// until the runtime-filter message is destroyed. That lifetime covers the IVF
// docfilter payload and all reader shares reconstructed from it. A missing
// throttler is allowed for non-CN/test processes; production CNs always install
// CNMemoryThrottler during service startup.
func reserveRuntimeFilterConsumerMemory(
	proc *process.Process,
	payloadBytes, cardinality int,
) (release func(), requested int64, granted bool) {
	requested, ok := runtimeFilterConsumerMemoryBound(payloadBytes, cardinality)
	if !ok {
		return nil, 0, false
	}
	if proc == nil {
		return nil, requested, false
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return nil, 0, true
	}
	value, exists := rt.GetGlobalVariables(moruntime.CNMemoryThrottler)
	if !exists {
		return nil, 0, true
	}
	throttler, ok := value.(rscthrottler.RSCThrottler)
	if !ok || throttler == nil {
		return nil, requested, false
	}
	if _, ok = throttler.Acquire(requested); !ok {
		return nil, requested, false
	}
	return func() { throttler.Release(requested) }, requested, true
}

func combineRuntimeFilterMemoryReleases(first, second func()) func() {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	return func() {
		first()
		second()
	}
}
