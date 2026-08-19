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
