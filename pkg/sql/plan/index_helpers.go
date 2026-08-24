// Copyright 2025 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func isSpatialIndexDef(idxDef *planpb.IndexDef) bool {
	return idxDef != nil && catalog.IsRTreeIndexAlgo(idxDef.IndexAlgo)
}

func indexTableStoresSerializedKey(idxDef *planpb.IndexDef) bool {
	return idxDef != nil && !isSpatialIndexDef(idxDef) && len(idxDef.Parts) > 1
}

// indexTableStoredKeySerialFunc identifies the serializer used to materialize
// an index-table key. Non-unique serialized keys retain NULL components so the
// hidden table can represent every base-table row.
func indexTableStoredKeySerialFunc(idxDef *planpb.IndexDef) string {
	if idxDef != nil && !idxDef.Unique && indexTableStoresSerializedKey(idxDef) {
		return "serial_full"
	}
	return "serial"
}

// indexTableComparisonSerialFunc identifies the serializer for SQL comparison
// operands. serial and serial_full encode non-NULL components identically, but
// serial propagates NULL. That distinction lets the access predicate preserve
// SQL three-valued comparison semantics without a decoded row-by-row recheck.
func indexTableComparisonSerialFunc() string {
	return "serial"
}

func indexLookupColumnName(idxDef *planpb.IndexDef) string {
	if isSpatialIndexDef(idxDef) {
		return catalog.IndexTablePrimaryColName
	}
	return catalog.IndexTableIndexColName
}

func indexPrimaryPartName(idxDef *planpb.IndexDef) string {
	if idxDef == nil || len(idxDef.Parts) == 0 {
		return ""
	}
	return catalog.ResolveAlias(idxDef.Parts[0])
}
