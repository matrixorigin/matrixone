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

// vectorSearchSlots is that resolution done ONCE per result layout: -1 means the planner
// pruned the column. The layout cannot change while a batch is being filled -- Attrs are
// fixed when createResultBatch builds it -- so resolving per emitted row was a linear
// string scan repeated 8192 times a batch, and for IVF-FLAT once more per INCLUDE column
// per row, in the vector-search output hot path.
type vectorSearchSlots struct {
	pk    int
	score int
	// include is parallel to the state's includeColumns, so the emit loop indexes it
	// positionally instead of rebuilding the prefixed name and scanning for it.
	include []int
}

// resolveVectorSearchSlots resolves pk/score and, when includeColumns is non-empty, one slot
// per INCLUDE column. Call it right after createResultBatch, with the same Attrs.
func resolveVectorSearchSlots(attrs []string, includeColumns []string, includePrefix string) vectorSearchSlots {
	slots := vectorSearchSlots{
		pk:    vectorSearchAttrPos(attrs, "pkid"),
		score: vectorSearchAttrPos(attrs, "score"),
	}
	if len(includeColumns) > 0 {
		slots.include = make([]int, len(includeColumns))
		for i, col := range includeColumns {
			slots.include[i] = vectorSearchAttrPos(attrs, includePrefix+col)
		}
	}
	return slots
}
