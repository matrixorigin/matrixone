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

package cache

import "sync"

// maxTSMemoEntry memoizes one index's max build_ts with its OWN mutex, so computing the value for
// one index never blocks another. The enclosing map is a sync.Map (no global map-wide lock).
type maxTSMemoEntry struct {
	mu       sync.Mutex
	ts       int64
	computed bool
}

// GetMaxTS returns the build_ts of the current generation an async index probe will search, derived
// identically by the coverage gate (at plan time) and the search operator (at execution) so the two
// cannot bind different generations, plus WHERE the value came from so a caller can reason about how
// authoritative it is:
//
//   - tier 1, indexFound=true: the loaded cache entry's build_ts -- exactly the generation execution
//     will reuse, so a stale (incl. cross-CN) entry drives a matching partial tail rather than a drop;
//   - tier 2, memoFound=true: another caller already computed the value for this key (a concurrent /
//     in-flight cold load) and this call read it from the shared memo -- no recompute;
//   - tier 3, both false: this call is the first for the key and ran compute (durable MAX(build_ts) as
//     of its transaction), under the per-key mutex so exactly ONE caller computes and the rest block
//     then read tier 2.
//
// The memo is deliberately SHARED across queries (keyed by index table), so a query planning against
// an index another query is still loading inherits that generation's build_ts rather than a newer
// durable one -- which keeps the coverage decision and the tail bound pinned to what execution reuses.
//
// It is for the CURRENT (bare-key) generation only. A {snapshot=...} read keys by SnapshotKey and
// loads the immutable as-of-snapshot generation, so its coverage and execution already agree and it
// must NOT route through here.
func (c *VectorIndexCache) GetMaxTS(indexTable string, compute func() (int64, error)) (ts int64, indexFound, memoFound bool, err error) {
	if v, ok := c.GetBuildTS(indexTable); ok {
		return v, true, false, nil
	}
	e, _ := c.maxTSMemo.LoadOrStore(indexTable, &maxTSMemoEntry{})
	ent := e.(*maxTSMemoEntry)
	ent.mu.Lock() // per-key mutex: one compute per key, others block then read the memo
	defer ent.mu.Unlock()
	if ent.computed {
		return ent.ts, false, true, nil
	}
	v, cerr := compute()
	if cerr != nil {
		// Do not memoize a failure, and do not leave the empty placeholder behind: drop it (only if
		// the map still holds this exact entry) so the next caller starts clean and retries.
		c.maxTSMemo.CompareAndDelete(indexTable, ent)
		return 0, false, false, cerr
	}
	ent.ts, ent.computed = v, true
	return v, false, false, nil
}

// RemoveMaxTSMemo drops the memo entry once Load has published the generation under indexTable: the
// loaded entry's build_ts is now authoritative (GetMaxTS's warm branch), so the bridge value is no
// longer needed and would only go stale. Safe when no entry exists.
func (c *VectorIndexCache) RemoveMaxTSMemo(indexTable string) {
	c.maxTSMemo.Delete(indexTable)
}
