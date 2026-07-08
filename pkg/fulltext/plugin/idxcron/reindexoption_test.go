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

package idxcron

import "testing"

// reindexOptionForCounts fires a full REBUILD ("") once the dead-doc fraction
// (1 - liveRows/baseDocs) exceeds RebuildDeadPct (30), else MERGE.
func TestReindexOptionForCounts(t *testing.T) {
	cases := []struct {
		live, base int64
		want       string
		note       string
	}{
		{4, 4, "MERGE", "no deletes (dead 0%)"},
		{3, 4, "MERGE", "dead 25% < 30%"},
		{7, 10, "MERGE", "dead 30% exactly — not > threshold"},
		{2, 3, "", "dead 33% > 30%"},
		{2, 4, "", "dead 50%"},
		{0, 4, "", "dead 100%"},
		{4, 0, "MERGE", "empty base — fold, never rebuild"},
		{5, 4, "MERGE", "live > base (unfolded tail inserts) — dead negative"},
	}
	for _, c := range cases {
		if got := reindexOptionForCounts(c.live, c.base); got != c.want {
			t.Errorf("reindexOptionForCounts(%d,%d)=%q want %q (%s)", c.live, c.base, got, c.want, c.note)
		}
	}
}
