// Copyright 2021 Matrix Origin
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

package memory

import (
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// DeviceLoadBudgetNumerator / DeviceLoadBudgetDenominator define the fraction of currently
// free VRAM that new sub-index loads are allowed to consume. Matches the
// build-time fraction (60%) so build and search bound with the same rule.
const (
	DeviceLoadBudgetNumerator   = 6
	DeviceLoadBudgetDenominator = 10
)

// DeviceAdmitLoadAggregate is the load-time VRAM admission gate: given the aggregate
// bytes the caller is about to load (sum of persisted-tar sizes across all
// sub-indexes being brought online in one query) and a callback that queries
// current free VRAM in bytes, return nil if the load fits `60% × freeBytes`,
// or an error naming both figures.
//
// This is the counterpart to the build-time RowsFittingFreeMem cap. Auto-
// rotation splits a build into multiple sub-indexes each sized to fit 60% of
// build-time free VRAM — but at search time ALL sub-indexes must be resident
// simultaneously (the search fan-outs across them), so the sum can exceed the
// current free VRAM even when each individual sub-index fit at build time.
// Without this gate, that scenario commits N sub-indexes at build then OOMs
// at first query. Fail admission loudly instead.
//
// The natural `newBytes` value is the persisted tar size (idx.FileSize) —
// it already includes every eagerly-resident payload: raw dataset (CAGRA) or
// PQ codes (IVFPQ), graph, ids, deleted bitset, AND the INCLUDE column
// filter_*.bin blobs written by FilterStore. That matches what has to come
// back into VRAM at load, so no separate per-payload term is needed.
//
// If newBytes == 0 (unknown / not measured), returns nil — the caller has
// nothing to admit against.
func DeviceAdmitLoadAggregate(newBytes uint64, freeBytesGetter func() (uint64, error), who string) error {
	if newBytes == 0 {
		return nil
	}
	free, err := freeBytesGetter()
	if err != nil {
		// Cannot query — same policy as RowsFittingFreeMem: refuse rather
		// than guess, because a load that would OOM at first query is worse
		// than a load that never happens.
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"%s: cannot query free VRAM to admit %d bytes of index load: %v",
			who, newBytes, err))
	}
	budget := free / DeviceLoadBudgetDenominator * DeviceLoadBudgetNumerator
	if newBytes > budget {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"%s: index load needs %d bytes of VRAM across all sub-indexes but only %d bytes "+
				"available (60%% of %d free); "+
				"either evict cached indexes, drop and rebuild at a smaller max_index_capacity, "+
				"or run this query on a larger GPU",
			who, newBytes, budget, free))
	}
	return nil
}

// DeviceAdmitLoad is the multi-GPU counterpart to DeviceAdmitLoadAggregate: given a
// per-device byte demand (device ID → bytes that will land on that device once
// every sub-index in the batch is loaded), verify each individual device has
// enough free VRAM. Single-GPU aggregate accounting over-rejects real N-GPU
// SHARDED loads by ~N× (each device only holds tar/N, but the aggregate check
// compares the sum against one card's free bytes); mirroring the check
// per-device avoids that false reject while still catching a real OOM on ANY
// participating card.
//
// The `perDeviceBytes` map already encodes the topology decided by the caller:
//
//   - SINGLE: {devices[0]: totalBytes}
//   - REPLICATED: {each device: totalBytes} (every device holds a full copy)
//   - SHARDED with real N-GPU: {devN: shardBytes} — one entry per device
//   - SHARDED under gpu_multi_simulation on 1 physical GPU: the aliased device
//     (usually 0) accumulates all shards, which correctly matches physical
//     residency
//
// A device with 0 demand is skipped. `freeBytesGetter(dev)` returns current
// free bytes on that specific device; on error the whole admission fails
// (same fail-loud policy as the single-device path).
//
// NOT AN ADMISSION GATE ON ITS OWN. This only SAMPLES free VRAM; it reserves
// nothing, so two concurrent loads of different indexes both observe the same
// free bytes, both pass, and together overcommit the card. Load paths must use
// DeviceReserveLoad, which applies this same rule while holding the claim.
// Retained for the aggregate/no-concurrency cases and for its test coverage.
func DeviceAdmitLoad(perDeviceBytes map[int]uint64, freeBytesGetter func(int) (uint64, error), who string) error {
	for dev, want := range perDeviceBytes {
		if want == 0 {
			continue
		}
		free, err := freeBytesGetter(dev)
		if err != nil {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"%s: cannot query free VRAM on device %d to admit %d bytes of index load: %v",
				who, dev, want, err))
		}
		budget := free / DeviceLoadBudgetDenominator * DeviceLoadBudgetNumerator
		if want > budget {
			return moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"%s: device %d needs %d bytes of VRAM for the index load but only %d bytes "+
					"available (60%% of %d free); "+
					"either evict cached indexes, drop and rebuild at a smaller max_index_capacity, "+
					"or run this query on a larger GPU / more GPUs",
				who, dev, want, budget, free))
		}
	}
	return nil
}

// DeviceLoadBytes attributes one sub-index's persisted tar bytes to the
// physical devices that will hold it once loaded.
//
// `devices` must be the topology already RESOLVED against the tar manifest
// (cuvs.ResolveDevicesForTarLoad), not the raw session device list: under
// gpu_multi_simulation the resolved list aliases several logical shards onto
// one physical device (e.g. [0,0,0,0]), and accumulating into the map by
// device id then correctly charges that single card for all of them.
//
// shardCount is the manifest's shard count; 0 means "manifest says
// non-sharded", in which case a SHARDED-mode index is charged conservatively
// as a full copy per device rather than being silently under-counted.
func DeviceLoadBytes(mode vectorindex.DistributionMode, devices []int, shardCount uint32, bytes uint64) map[int]uint64 {
	perDev := make(map[int]uint64, len(devices))
	if bytes == 0 || len(devices) == 0 {
		return perDev
	}
	switch mode {
	case vectorindex.DistributionMode_SHARDED:
		if shardCount == 0 {
			// Manifest disagrees with the configured mode. Charge the full tar
			// to every device: over-estimating rejects a load that would have
			// fit, which is recoverable; under-estimating OOMs at first query.
			for _, d := range devices {
				perDev[d] += bytes
			}
			return perDev
		}
		per := bytes / uint64(shardCount)
		for _, d := range devices {
			perDev[d] += per
		}
	case vectorindex.DistributionMode_REPLICATED:
		// Every device holds a full copy.
		for _, d := range devices {
			perDev[d] += bytes
		}
	default:
		// SINGLE_GPU: one device hosts the whole thing.
		perDev[devices[0]] += bytes
	}
	return perDev
}

// loadReservations tracks VRAM claimed by loads that have been admitted but
// whose memory is not resident yet.
//
// DeviceAdmitLoad on its own is only a CHECK: it samples free VRAM and
// reserves nothing. The index cache deduplicates concurrent loads by cache key
// alone (cache.VectorIndexCache.Search -> IndexMap.LoadOrStore), so two
// DIFFERENT cold indexes load concurrently, both sample the same free bytes,
// both pass, and together overcommit the card. Sampling and claiming under one
// lock closes that window: the second caller sees the first caller's claim even
// though the first has not allocated anything yet.
//
// A claim only has to cover the gap between admission and residency. Once the
// load finishes, the memory is either really allocated (so the next
// cudaMemGetInfo already reflects it) or was freed by the failure path — either
// way the claim is redundant and must be dropped, which is why release is
// called on both the success and the failure path rather than at eviction.
var loadReservations = struct {
	mu       sync.Mutex
	reserved map[int]uint64
}{reserved: make(map[int]uint64)}

// releaseReservedLocked drops `claimed` from the in-flight totals.
// Caller must hold loadReservations.mu.
func releaseReservedLocked(claimed map[int]uint64) {
	for dev, n := range claimed {
		cur, ok := loadReservations.reserved[dev]
		if !ok {
			continue
		}
		if cur > n {
			loadReservations.reserved[dev] = cur - n
		} else {
			delete(loadReservations.reserved, dev)
		}
	}
}

// DeviceReserveLoad is the admitting counterpart to DeviceAdmitLoad: it
// applies the same per-device 60%-of-free rule, but also RESERVES the bytes so
// a concurrent load of a different index cannot spend the same headroom twice.
//
// On success it returns a release function that must be called exactly once,
// on every path out of the load — success and failure alike. It is idempotent,
// so `defer release()` plus an early explicit call is safe.
//
// On failure nothing stays reserved and release is nil.
func DeviceReserveLoad(
	perDeviceBytes map[int]uint64,
	freeBytesGetter func(int) (uint64, error),
	who string,
) (release func(), err error) {
	loadReservations.mu.Lock()
	defer loadReservations.mu.Unlock()

	claimed := make(map[int]uint64, len(perDeviceBytes))
	for dev, want := range perDeviceBytes {
		if want == 0 {
			continue
		}
		free, gerr := freeBytesGetter(dev)
		if gerr != nil {
			releaseReservedLocked(claimed)
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"%s: cannot query free VRAM on device %d to admit %d bytes of index load: %v",
				who, dev, want, gerr))
		}
		budget := free / DeviceLoadBudgetDenominator * DeviceLoadBudgetNumerator
		inflight := loadReservations.reserved[dev]
		if inflight+want > budget {
			releaseReservedLocked(claimed)
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"%s: device %d needs %d bytes of VRAM for the index load but only %d bytes "+
					"available (60%% of %d free, %d already reserved by concurrent loads); "+
					"either evict cached indexes, drop and rebuild at a smaller max_index_capacity, "+
					"or run this query on a larger GPU / more GPUs",
				who, dev, want, budget-min64(budget, inflight), free, inflight))
		}
		loadReservations.reserved[dev] += want
		claimed[dev] += want
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			loadReservations.mu.Lock()
			defer loadReservations.mu.Unlock()
			releaseReservedLocked(claimed)
		})
	}, nil
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
