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

// DeviceAggregateFitsFree refuses a set of sub-indexes BEFORE any of them is
// loaded, when the busiest device could not hold them all right now.
//
// The situational twin of DeviceAggregateFitsHardware, and deliberately fed the
// SAME quantity: per-device device-resident bytes, reduced by PeakDeviceBytes from
// the packed components the caller measured with cuvs.MeasureTar. The two gates
// differ only in the bound -- total VRAM there, free VRAM here -- so an artifact
// CREATE accepted can be refused here transiently, but never permanently.
//
// That symmetry is the point. Sizing this from metadata FileSize instead would
// admit against the whole tar, including ids.bin, the INCLUDE blobs, the quantizer
// and the bitset, none of which reach the GPU -- and CREATE, which charges only
// the device-resident share, would then commit artifacts this gate refuses at
// every free level.
//
// Checked before the first deserialize because per-sub-index admission alone
// cannot express an aggregate: each fits, so the loader admits the early ones,
// spends the budget, and is refused on a later one having already paid for most
// of the memory, reporting a single sub-index rather than the total.
func DeviceAggregateFitsFree(
	devices []int, perDeviceBytes uint64, rowsFitting DeviceRowsFittingFunc,
) error {
	if perDeviceBytes == 0 || len(devices) == 0 || rowsFitting == nil {
		return nil
	}
	for _, dev := range DeviceDistinct(devices) {
		// perRow = 1 makes rows_fitting_gpu_mem return the byte budget itself,
		// computed by the same C++ every other admission on this path uses, so the
		// budget fraction is not duplicated here. Do NOT ask whether N rows of N
		// bytes fit and test for zero: that function clamps its result to a minimum
		// of 1, so any predicate built on it is silently always-true.
		budget, free, err := rowsFitting(dev, 1)
		if err != nil {
			return moerr.NewInternalErrorNoCtxf(
				"vector index load: cannot measure device %d to admit %d bytes: %v",
				dev, perDeviceBytes, err)
		}
		if budget < 0 || perDeviceBytes > uint64(budget) {
			return moerr.NewInternalErrorNoCtxf(
				"vector index load: this index needs %d MB resident on device %d to be searched, "+
					"but only %d MB may be claimed there right now (%d MB free), because a query "+
					"reads every sub-index at once. Evict cached indexes, or retry when the device "+
					"is quieter",
				perDeviceBytes>>20, dev, uint64(budget)>>20, free>>20)
		}
	}
	return nil
}

// DeviceMaxAdmissibleFunc reports the most VRAM any admission could ever grant on
// a device -- the governor's budget fraction of TOTAL memory, not of free.
// Indirected like DeviceRowsFittingFunc so the rule is testable without a GPU.
type DeviceMaxAdmissibleFunc func(dev int) (uint64, error)

// DeviceAggregateFitsHardware refuses a finished build that could never be
// resident on the DEVICE HARDWARE, whatever is free at the time.
//
// Compares against the card's TOTAL capacity, not what is free, so a refusal is
// permanent: no eviction, no quieter moment, and no
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
// The threshold is the budget fraction of TOTAL, not total itself: the
// per-deserialize claims in C++ admit against that fraction of FREE, and free
// never exceeds total, so an index above this bound is refused at every load
// however idle the card. Comparing against raw total would leave that band
// committing artifacts whose every query fails.
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
