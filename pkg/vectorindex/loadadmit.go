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

package vectorindex

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// LoadBudgetNumerator / LoadBudgetDenominator define the fraction of currently
// free VRAM that new sub-index loads are allowed to consume. Matches the
// build-time fraction (60%) so build and search bound with the same rule.
const (
	LoadBudgetNumerator   = 6
	LoadBudgetDenominator = 10
)

// AdmitLoadFits is the load-time VRAM admission gate: given the aggregate
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
func AdmitLoadFits(newBytes uint64, freeBytesGetter func() (uint64, error), who string) error {
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
	budget := free / LoadBudgetDenominator * LoadBudgetNumerator
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

// AdmitLoadFitsPerDevice is the multi-GPU counterpart to AdmitLoadFits: given a
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
func AdmitLoadFitsPerDevice(perDeviceBytes map[int]uint64, freeBytesGetter func(int) (uint64, error), who string) error {
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
		budget := free / LoadBudgetDenominator * LoadBudgetNumerator
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
