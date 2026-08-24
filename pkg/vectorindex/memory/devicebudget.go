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
	"strconv"
	"strings"

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

// ShardRank extracts N from a "shard_N.bin" component name, or -1 when the name
// is not a shard (index.bin, and anything unrecognised).
func ShardRank(name string) int {
	const pfx, sfx = "shard_", ".bin"
	if !strings.HasPrefix(name, pfx) || !strings.HasSuffix(name, sfx) {
		return -1
	}
	n, err := strconv.Atoi(name[len(pfx) : len(name)-len(sfx)])
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// PeakDeviceBytes reduces per-sub-index component sizes to what the BUSIEST single
// device must hold.
//
// Which device holds what depends on the device list, and reducing to a max too
// early gets it wrong:
//
//   - distinct cards, SHARDED: rank i lands on devices[i], so each card holds one
//     shard per sub-index and the answer is the max across ranks;
//   - ALIASED devices (gpu_multi_simulation presents [0,0,0,0]): every rank resolves
//     to the same physical card, which therefore holds ALL the shards. Taking a max
//     there under-states it by the shard count -- the same aliasing DeviceBuildBytes
//     handles by accumulating rather than dividing;
//   - index.bin (SINGLE_GPU/REPLICATED): one component that every participating
//     device holds in full.
//
// So group by physical device first, sum within a group, and take the max across
// groups. Summed across sub-indexes, because a query reads all of them at once.
//
// A shard rank with no matching device entry is charged to devices[0] rather than
// dropped: silently ignoring a component would under-state demand, which is the
// direction that admits an index that cannot load.
func PeakDeviceBytes(devices []int, perSubIndex []map[string]int64) int64 {
	if len(devices) == 0 || len(perSubIndex) == 0 {
		return 0
	}
	perDev := make(map[int]int64, len(devices))
	for _, comps := range perSubIndex {
		for name, sz := range comps {
			if sz <= 0 {
				continue
			}
			rank := ShardRank(name)
			switch {
			case rank < 0:
				// Not a shard: every participating device holds the whole thing.
				for _, d := range DeviceDistinct(devices) {
					perDev[d] += sz
				}
			case rank < len(devices):
				perDev[devices[rank]] += sz
			default:
				perDev[devices[0]] += sz
			}
		}
	}
	var peak int64
	for _, v := range perDev {
		if v > peak {
			peak = v
		}
	}
	return peak
}

// DeviceMaxAdmissibleFunc reports the most VRAM any admission could ever grant on
// a device -- the governor's budget fraction of TOTAL memory, not of free.
// Indirected like DeviceRowsFittingFunc so the rule is testable without a GPU.
type DeviceMaxAdmissibleFunc func(dev int) (uint64, error)

// DeviceAggregateFitsHardware refuses a finished build that could never be
// resident on the DEVICE HARDWARE, whatever is free at the time.
//
// Deliberately a different question from DeviceLoadFits. That admits against a
// fraction of CURRENTLY-FREE memory, so its refusals are situational -- evict
// something and the same index loads. This compares against the card's TOTAL
// capacity, so a refusal is permanent: no eviction, no quieter moment, and no
// larger device set helps, because distribution_mode is persisted and neither
// SINGLE_GPU (all of it on devices[0]) nor REPLICATED (all of it on every device)
// redistributes when hardware is added.
//
// That permanence is what makes it safe to fail CREATE on. An index refused here
// is one whose every future query is guaranteed to fail, so reporting it at CREATE
// turns a deferred, confusing failure into an immediate, actionable one -- without
// the false positives a free-memory or modelled-cost predicate produces.
//
// perDeviceBytes is what ONE device must hold (cuvs builder PerDeviceBytes): the
// sum over sub-indexes of the largest device-resident component in each. It is
// already per-device, so there is no distribution-mode attribution here -- under
// SHARDED the caller has taken the biggest shard rather than an even division,
// which would under-state it.
//
// The threshold is the budget fraction of TOTAL, not total itself. DeviceLoadFits
// admits against that fraction of FREE, and free never exceeds total, so an index
// above this bound is refused by every future query at every free level -- while
// one merely above the CURRENT budget may well load once something is evicted.
// Comparing against raw total would leave that band committing artifacts whose
// every query fails, which is the defect this gate exists to close.
func DeviceAggregateFitsHardware(
	devices []int, perDeviceBytes uint64, maxAdmissible DeviceMaxAdmissibleFunc,
) error {
	if perDeviceBytes == 0 || len(devices) == 0 || maxAdmissible == nil {
		return nil
	}
	for _, dev := range DeviceDistinct(devices) {
		total, err := maxAdmissible(dev)
		if err != nil {
			// Never guess. An unreadable device is not permission to commit an
			// index that may be unsearchable.
			return moerr.NewInternalErrorNoCtxf(
				"vector index build: cannot read the admissible VRAM of device %d to validate a %d byte index: %v",
				dev, perDeviceBytes, err)
		}
		if perDeviceBytes > total {
			return moerr.NewInvalidInputNoCtxf(
				"vector index build: one device must hold %d MB of this index to serve a query, "+
					"but device %d can admit at most %d MB even when completely idle. Every "+
					"query reads all sub-indexes at once, so rotation cannot help and this "+
					"index could never be queried on this GPU. Rebuild with a narrower storage "+
					"type (QUANTIZATION), index fewer rows, or use a GPU with more memory",
				perDeviceBytes>>20, dev, total>>20)
		}
	}
	return nil
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
// This is the SITUATIONAL half of a two-gate pair. It admits against a fraction of
// currently-FREE VRAM, so a refusal here means "not right now" -- evict something,
// or come back when the card is quieter, and the same index loads.
//
// The permanent half is DeviceAggregateFitsHardware, which CREATE runs against the
// budget fraction of TOTAL. An index above that bound is refused here at every free
// level, so it is rejected at build time rather than committed and then failing
// every query. An index below it may still be refused here transiently, which is
// correct and is why CREATE does not use this gate.
//
// totalBytes is the aggregate on-disk size of every sub-index to be loaded. That
// OVER-states device residency: the tar also carries host-resident members --
// ids.bin, the INCLUDE blobs, the quantizer and the bitset -- which never reach
// the GPU (~8% of an IVF-PQ tar, ~11% for a narrow vector). The over-statement is
// in the over-refuse direction and is accepted here because the models come from
// metadata, which persists only the tar total, and because the authoritative claim
// taken per deserialize in C++ sizes itself from the real component anyway. The
// build side, which has the packed components to hand, uses the exact device bytes
// instead (cuvs.PackSizes.Device).
//
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
