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

// maxTSMemoEntry holds one index's shared cold-window build_ts. It is written once (via LoadOrStore
// at the struct literal) and read-only afterward, so it needs no mutex; the enclosing sync.Map has
// no global map-wide lock either.
type maxTSMemoEntry struct {
	ts int64
}

// GetMaxTS returns the build_ts of the current generation an async index probe will search, derived
// identically by the coverage gate (at plan time) and the search operator (at execution) so the two
// cannot bind different generations, plus WHERE the value came from so a caller can reason about it:
//
//   - tier 1, indexFound=true: the loaded cache entry's build_ts -- exactly the generation execution
//     will reuse, so a stale (incl. cross-CN) entry drives a matching partial tail rather than a drop;
//   - tier 2, memoFound=true: a concurrent cold caller is building an OLDER generation than this
//     caller's own snapshot could load, so this returns that (smaller) shared value -- the generation
//     execution will actually reuse;
//   - tier 3, both false: this caller's own durable MAX(build_ts) as of its transaction.
//
// The critical rule is the CLAMP: the shared memo is never returned unclamped -- the result is
// min(memo, own). A memo seeded by a concurrent NEWER-snapshot query must never hand this caller a
// generation its own snapshot cannot load, or its tail bound would exceed the generation execution
// binds and drop rows. So compute (own durable) runs on every cold call, even a memo hit: the memo is
// a cap-from-below (inherit a smaller in-flight generation), not a compute-skip.
//
// It is for the CURRENT (bare-key) generation only. A {snapshot=...} read keys by SnapshotKey and
// loads the immutable as-of-snapshot generation, so its coverage and execution already agree and it
// must NOT route through here.
func (c *VectorIndexCache) GetMaxTS(indexTable string, compute func() (int64, error)) (ts int64, indexFound, memoFound bool, err error) {
	if v, ok := c.GetBuildTS(indexTable); ok {
		return v, true, false, nil
	}
	// Own durable: the newest generation THIS caller's snapshot can actually load.
	own, cerr := compute()
	if cerr != nil {
		return 0, false, false, cerr
	}
	if own == 0 {
		// 0 = unknown / not-yet-built index. Never memoize it: the probe declines to a full scan
		// (no load, so nothing removes the memo), and a memoized 0 would then poison every later
		// caller into min(0, own)=0 -- permanently disabling the probe for this index even after it
		// builds. Return unknown without touching the shared entry.
		return 0, false, false, nil
	}
	e, loaded := c.maxTSMemo.LoadOrStore(indexTable, &maxTSMemoEntry{ts: own})
	if !loaded {
		return own, false, false, nil // first cold caller: seed the shared value with own
	}
	if memoTs := e.(*maxTSMemoEntry).ts; memoTs < own {
		// A concurrent loader is building an OLDER generation than this snapshot could load; inherit it
		// so the tail is bounded at the generation execution will reuse.
		return memoTs, false, true, nil
	}
	// The shared value is newer than this snapshot can load; clamp to own.
	return own, false, false, nil
}

// RemoveMaxTSMemo drops the memo entry once Load has published the generation under indexTable: the
// loaded entry's build_ts is now authoritative (GetMaxTS's warm branch), so the bridge value is no
// longer needed and would only go stale. Safe when no entry exists.
func (c *VectorIndexCache) RemoveMaxTSMemo(indexTable string) {
	c.maxTSMemo.Delete(indexTable)
}
