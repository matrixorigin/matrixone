// Copyright 2026 MatrixOrigin
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

package frontend

import "testing"

// TestSpoolCoversAffectedRows locks the DML RETURNING spool-vs-affected gate
// (issue #31422): strict equality everywhere except the changed-row UPDATE
// semantics of a default-capability session, where the spooled matched-row
// images must still cover the counted changed rows. A no-op UPDATE RETURNING
// on such a session must return its row image, not the internal 20101 error,
// and the original guard must keep rejecting every other divergence.
func TestSpoolCoversAffectedRows(t *testing.T) {
	cases := []struct {
		name              string
		spooled, affected uint64
		updateChangedRows bool
		want              bool
	}{
		{"equal-counts-consistent", 1, 1, false, true},
		{"equal-zero-consistent", 0, 0, false, true},
		{"non-update-mismatch-errors", 3, 1, false, false},
		{"insert-mismatch-errors", 1, 0, false, false},
		{"delete-mismatch-errors", 0, 1, false, false},
		{"update-found-rows-mismatch-errors", 3, 1, false, false},
		{"update-changed-noop-legal", 1, 0, true, true},
		{"update-changed-partial-noop-legal", 3, 1, true, true},
		{"update-changed-spool-below-affected-errors", 0, 1, true, false},
		{"update-changed-partial-loss-errors", 1, 2, true, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := spoolCoversAffectedRows(c.spooled, c.affected, c.updateChangedRows); got != c.want {
				t.Fatalf("spoolCoversAffectedRows(%d, %d, %v) = %v, want %v",
					c.spooled, c.affected, c.updateChangedRows, got, c.want)
			}
		})
	}
}
