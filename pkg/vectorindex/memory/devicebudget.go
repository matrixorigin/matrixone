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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
)

// DeviceDistinct returns the distinct PHYSICAL device ids in first-seen order.
//
// Under gpu_multi_simulation the logical device list aliases one card several
// times ([0,0,0,0]). Anything that QUERIES hardware must iterate this rather
// than the raw list, or it asks the same card N times and reports the answer as
// though N cards had been surveyed. (Anything that ATTRIBUTES bytes wants the
// raw list, so the aliased shards accumulate onto the one physical card.)
func DeviceDistinct(devices []int) []int {
	if len(devices) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(devices))
	out := make([]int, 0, len(devices))
	for _, d := range devices {
		if _, dup := seen[d]; dup {
			continue
		}
		seen[d] = struct{}{}
		out = append(out, d)
	}
	return out
}

// DeviceRowsFittingFunc reports how many rows of perRowBytes fit in the free
// memory of one device. It exists so the sizing POLICY below can be exercised
// without a GPU; production passes cuvs.RowsFittingFreeMem, whose signature this
// matches exactly.
type DeviceRowsFittingFunc func(dev int, perRowBytes uint64) (rows int64, freeBytes uint64, err error)

// DeviceBuildBytes attributes a build's total device demand to the physical
// devices that will hold it.
//
// SHARDED splits the work, so each device holds total/N. REPLICATED puts a full
// copy on every device. SINGLE_GPU puts it all on the first. The map keys by
// device id, so under gpu_multi_simulation -- where the list aliases one card
// several times -- the shards correctly accumulate back onto that one card
// instead of pretending there are N.
func DeviceBuildBytes(mode vectorindex.DistributionMode, devices []int, totalBytes uint64) map[int]uint64 {
	perDev := make(map[int]uint64, len(devices))
	if totalBytes == 0 || len(devices) == 0 {
		return perDev
	}
	switch mode {
	case vectorindex.DistributionMode_SHARDED:
		per := totalBytes / uint64(len(devices))
		if per == 0 {
			per = totalBytes
		}
		for _, d := range devices {
			perDev[d] += per
		}
	case vectorindex.DistributionMode_REPLICATED:
		for _, d := range devices {
			perDev[d] += totalBytes
		}
	default:
		perDev[devices[0]] += totalBytes
	}
	return perDev
}

// DeviceLoadFits refuses a set of sub-indexes BEFORE any of them is loaded, when
// their aggregate resident footprint cannot be held.
//
// A build is deliberately allowed to rotate into N sub-indexes that no single
// device could hold at once: the build only ever materialises one at a time. A
// SEARCH is the opposite -- it reaches every list of every sub-index, so all N
// have to be resident together. Per-load admission alone cannot express that:
// each individual load fits, so the loader admits the early sub-indexes, spends
// the budget on them, and is refused on a later one. The query then fails having
// already paid for most of the memory, and the operator sees a refusal naming a
// sub-index rather than the real problem, which is the total.
//
// Checking the sum first turns that into one refusal, before anything is
// allocated, that names the aggregate. It does NOT reserve: the per-deserialize
// claims in device_memory.hpp still do the actual admission, and taking a claim
// here as well would double-count the same bytes and refuse loads that fit.
// This is a pre-flight check, so a peer that allocates between the check and the
// loads is still caught -- by those per-load claims, one layer down.
//
// DESIGN DECISION -- this refusal is deferred to load on purpose.
//
// The reviewed alternative was to reject (or roll back) CREATE when the aggregate
// exceeds what the building host can hold, or to add bounded load/search/evict so an
// oversized index stays searchable on one device. Neither is done:
//
//   - An index is a persisted artifact and the build-time device set is not the
//     search-time device set. Sub-indexes that overflow one card are fully searchable
//     on two, or on the sharded multi-server layout rotation exists to feed; rejecting
//     CREATE would bake the building host's topology into the artifact.
//   - Bounded load/evict is single-node machinery that sharded search supersedes.
//
// So the contract is: the build succeeds, table_function.warnAggregateNotResident warns
// at CREATE time while the operator is watching, and a query fails HERE -- deterministically,
// before any allocation, naming the total and the levers. Revisit when search can span
// devices/servers: this gate then sums against the larger budget and stops refusing.
//
// totalBytes is the aggregate on-disk size of every sub-index to be loaded; the
// packed tar is a good proxy for the resident footprint because it holds exactly
// what gets materialised (PQ codes / dataset, graph, ids, bitset, filter blobs).
// Attribution follows the distribution mode, as everywhere else.
func DeviceLoadFits(
	mode vectorindex.DistributionMode, devices []int, totalBytes uint64,
	rowsFitting DeviceRowsFittingFunc,
) error {
	if totalBytes == 0 || len(devices) == 0 || rowsFitting == nil {
		return nil
	}
	for dev, need := range DeviceBuildBytes(mode, DeviceDistinct(devices), totalBytes) {
		if need == 0 {
			continue
		}
		// Ask how many ONE-BYTE rows fit: that answer IS the byte budget, computed
		// by the same code every other admission on this path uses, so the 60%
		// fraction is not duplicated here.
		//
		// Do NOT instead ask whether `need` rows-of-need-bytes fit and test for 0.
		// rows_fitting_gpu_mem clamps its result to a minimum of 1 (helper.cpp), so
		// it never reports "does not fit" and any predicate built on it is silently
		// always-true. That mistake shipped once and was caught only by running this
		// against a real device.
		budgetBytes, free, err := rowsFitting(dev, 1)
		if err != nil {
			// Never guess. An unmeasurable device is exactly the condition that
			// made the previous version admit a load it could not complete.
			return moerr.NewInternalErrorNoCtxf(
				"vector index load: cannot measure device %d to admit %d bytes: %v", dev, need, err)
		}
		if budgetBytes < 0 || need > uint64(budgetBytes) {
			return moerr.NewInternalErrorNoCtxf(
				"vector index load: this index needs %d bytes resident on device %d to be "+
					"searched, but only %d bytes may be claimed (%d bytes free), because a query "+
					"reads every sub-index at once. The index built successfully -- rotation "+
					"bounds each build, not the search. Rebuild with a narrower storage type "+
					"(QUANTIZATION), index fewer rows, or use a GPU with more memory",
				need, dev, budgetBytes, free)
		}
	}
	return nil
}
