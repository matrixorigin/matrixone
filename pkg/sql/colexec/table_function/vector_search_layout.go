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

func vectorSearchAttrPos(attrs []string, name string) int {
	for i, attr := range attrs {
		if attr == name {
			return i
		}
	}
	return -1
}

type vectorSearchSlots struct {
	pk      int
	score   int
	include []int
}

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
