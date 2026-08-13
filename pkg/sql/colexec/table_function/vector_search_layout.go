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

package table_function

// The vector search table functions declare a fixed column list (pkid, score, and for
// IVF-FLAT the INCLUDE columns), but the planner is free to project only the columns the
// query actually reads: `select l2_distance(v, q) from t order by l2_distance(v, q)
// limit 3` keeps score and prunes pkid. createResultBatch sizes the batch from
// TableFunction.Attrs, so a pruned column shifts every position after it. Writing to a
// hard-coded Vecs[0]/Vecs[1] then appends a pk (int64) into the score vector (float64)
// and panics the CN with "interface conversion: interface {} is int64, not float64".
//
// Resolve the output slots by NAME once, and let a missing column mean "the planner does
// not want it" rather than "write it somewhere else".
func vectorSearchAttrPos(attrs []string, name string) int {
	for i, attr := range attrs {
		if attr == name {
			return i
		}
	}
	return -1
}
