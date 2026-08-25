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

// mib renders a byte count for an operator-facing refusal.
//
// Plain n>>20 rounds every non-zero value under a megabyte to "0 MB", so a
// refusal can read "needs 0 MB but only 0 MB may be claimed (0 MB free)" -- which
// looks like a bug in the gate rather than a statement about the index, and gives
// the operator nothing to size a fix from. Anything below a megabyte prints as
// bytes instead. Zero really is zero.
func mib(n uint64) string {
	if n < 1<<20 {
		return fmt.Sprintf("%d bytes", n)
	}
	return fmt.Sprintf("%d MB", n>>20)
}

// DeviceBudget carries BOTH bounds an aggregate gate can be measured against,
// derived from ONE budget fraction.
//
// They are paired deliberately. The two gates are the same comparison against
// different pools -- total VRAM for the permanent CREATE refusal, free VRAM for
// the situational load refusal -- and while each took its own function, each
// CALLER chose its own fraction. They chose differently: the load pre-flight sat
// on the 75% default while IVF-PQ's own claim used 65%, so an index sized between
// the two passed the pre-flight and threw on the first deserialize, after the
// whole artifact had been downloaded. Handing both gates one value obtained one
// way (cuvs.BudgetFor) makes that disagreement unrepresentable rather than merely
// caught.
//
// An INTERFACE, not a struct of closures, so that the implementation can live in
// pkg/cuvs without either package importing the other -- Go satisfies it
// structurally. Production passes cuvs.BudgetFor(indexType); tests pass a fake.
// A nil budget means "no bound available", and the gates decline to judge, which
// is the existing no-op-on-missing-input behaviour. Implement it with a VALUE
// type, as cuvs.DeviceBudget does: a typed-nil pointer is not == nil as an
// interface, so it would pass that guard and then panic on the first method call
// -- which a nil func value could not do.
//
// Both bounds on one value is the point: a type cannot supply one fraction to
// MaxAdmissible and a different one to RowsFitting.
type DeviceBudget interface {
	// MaxAdmissible: the budget fraction of TOTAL VRAM -- the most any admission
	// could EVER grant on a device. Used by the permanent CREATE gate.
	MaxAdmissible(dev int) (uint64, error)
	// RowsFitting: the budget fraction of FREE VRAM, in the rows-of-perRow-bytes
	// form the C++ exposes. Used by the situational load gate.
	RowsFitting(dev int, perRowBytes uint64) (rows int64, freeBytes uint64, err error)
}

// deviceCeiling reports one device's admission ceiling, plus the raw reading it
// was derived from (total or free) for the refusal message.
type deviceCeiling func(dev int) (ceiling, observed uint64, err error)

// deviceOverBudget names the device that could not hold the demand.
type deviceOverBudget struct {
	dev               int
	ceiling, observed uint64
}

// deviceMeasureFailure names the device that could not be measured.
type deviceMeasureFailure struct {
	dev   int
	cause error
}

// deviceAggregateExceeds is the ONE comparison both aggregate gates make: walk the
// distinct physical devices, ask each for its ceiling, and report the first that
// cannot hold need. Returns (nil, nil) when every device can.
//
// Factored out because the two gates differ ONLY in which pool they measure and
// how they word the refusal -- the iteration, the DeviceDistinct aliasing rule and
// the refuse-rather-than-guess response to an unmeasurable device were duplicated,
// and duplicated admission logic is what this subsystem keeps getting wrong.
//
// It deliberately does NOT build the error. The two refusals are not variants of
// one sentence: one says the index can never be queried on this GPU and to rebuild
// it smaller, the other says not right now and to retry. Collapsing them behind a
// mode flag would trade duplicated logic for a function that is half message
// selection, which is worse.
func deviceAggregateExceeds(
	devices []int, need uint64, ceilingOf deviceCeiling,
) (*deviceOverBudget, *deviceMeasureFailure) {
	for _, dev := range DeviceDistinct(devices) {
		ceiling, observed, err := ceilingOf(dev)
		if err != nil {
			return nil, &deviceMeasureFailure{dev: dev, cause: err}
		}
		if need > ceiling {
			return &deviceOverBudget{dev: dev, ceiling: ceiling, observed: observed}, nil
		}
	}
	return nil, nil
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
//
// SAFE TO CALL ON A PARTIAL AGGREGATE, which is how the loader fails fast. Every
// sub-index only ADDS bytes to the device that holds it, so PeakDeviceBytes is
// monotone: a running peak over budget guarantees the finished one is too, at
// that same free reading. The caller therefore re-checks after each sub-index it
// measures and refuses as soon as the running total is over, rather than
// downloading the remaining tars to reach a conclusion it already has.
//
// measured/total say how much of the index the figure covers. A partial refusal
// must not print its number as though it were the whole index: the operator sizes
// their fix from it, and the untouched sub-indexes only make it larger. Pass
// measured == total for a complete aggregate.
func DeviceAggregateFitsFree(
	devices []int, perDeviceBytes uint64, measured, total int, budget DeviceBudget,
) error {
	if perDeviceBytes == 0 || len(devices) == 0 || budget == nil {
		return nil
	}
	// "needs N MB" for the whole index; "needs at least N MB" while sub-indexes
	// remain unmeasured, with the count so the shortfall is not read as the total.
	scope := ""
	atLeast := ""
	if measured < total {
		atLeast = "at least "
		scope = fmt.Sprintf(" (%d of %d sub-indexes measured; the rest were not downloaded)",
			measured, total)
	}
	// perRow = 1 makes rows_fitting_gpu_mem return the byte budget itself, computed
	// by the same C++ every other admission on this path uses, so the fraction is
	// not duplicated here. Do NOT ask whether N rows of N bytes fit and test for
	// zero: that function clamps its result to a minimum of 1, so any predicate
	// built on it is silently always-true.
	over, unmeasured := deviceAggregateExceeds(devices, perDeviceBytes, func(dev int) (uint64, uint64, error) {
		rows, free, ferr := budget.RowsFitting(dev, 1)
		// A negative row count means the C++ could not size anything; treat it as a
		// zero ceiling so the refusal quotes 0 rather than the huge number an
		// int64 -> uint64 conversion of a negative produces.
		if rows < 0 {
			rows = 0
		}
		return uint64(rows), free, ferr
	})
	if unmeasured != nil {
		return moerr.NewInternalErrorNoCtxf(
			"vector index load: cannot measure device %d to admit %d bytes: %v",
			unmeasured.dev, perDeviceBytes, unmeasured.cause)
	}
	if over != nil {
		return moerr.NewInternalErrorNoCtxf(
			"vector index load: this index needs %s%s resident on device %d to be searched%s, "+
				"but only %s may be claimed there right now (%s free), because a query "+
				"reads every sub-index at once. Evict cached indexes, or retry when the device "+
				"is quieter",
			atLeast, mib(perDeviceBytes), over.dev, scope, mib(over.ceiling), mib(over.observed))
	}
	return nil
}

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
	devices []int, perDeviceBytes uint64, budget DeviceBudget,
) error {
	if perDeviceBytes == 0 || len(devices) == 0 || budget == nil {
		return nil
	}
	over, unmeasured := deviceAggregateExceeds(devices, perDeviceBytes, func(dev int) (uint64, uint64, error) {
		total, terr := budget.MaxAdmissible(dev)
		// The ceiling IS the reading here: there is no second figure to quote, the
		// way free memory accompanies a situational refusal.
		return total, total, terr
	})
	if unmeasured != nil {
		// Never guess. An unreadable device is not permission to commit an index
		// that may be unsearchable.
		return moerr.NewInternalErrorNoCtxf(
			"vector index build: cannot read the admissible VRAM of device %d to validate a %d byte index: %v",
			unmeasured.dev, perDeviceBytes, unmeasured.cause)
	}
	if over != nil {
		return moerr.NewInvalidInputNoCtxf(
			"vector index build: one device must hold %s of this index to serve a query, "+
				"but device %d can admit at most %s even when completely idle. Every "+
				"query reads all sub-indexes at once, so rotation cannot help and this "+
				"index could never be queried on this GPU. Rebuild with a narrower storage "+
				"type (QUANTIZATION), index fewer rows, or use a GPU with more memory",
			mib(perDeviceBytes), over.dev, mib(over.ceiling))
	}
	return nil
}
