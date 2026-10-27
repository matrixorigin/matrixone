// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import "testing"

func TestQueryHasODKUResultTracking(t *testing.T) {
	for _, tc := range []struct {
		name string
		q    *Query
		want bool
	}{
		{name: "no query"},
		{name: "no nodes", q: &Query{}},
		{name: "nil node", q: &Query{Nodes: []*Node{nil}}},
		{name: "legacy allocator", q: &Query{Nodes: []*Node{{PreInsertCtx: &PreInsertCtx{HasAutoCol: true}}}}},
		{name: "tracked allocator", q: &Query{Nodes: []*Node{nil, {PreInsertCtx: &PreInsertCtx{TrackOdkuResult: true}}}}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.q.HasODKUResultTracking(); got != tc.want {
				t.Fatalf("HasODKUResultTracking() = %v, want %v", got, tc.want)
			}
		})
	}
}
