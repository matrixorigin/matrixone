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

type indexPKPartKind uint8

const (
	indexPKPartAbsent indexPKPartKind = iota
	indexPKPartFull
	indexPKPartPrefix
)

// classifyIndexPKPart follows the same alias and prefix rules as index writes.
func classifyIndexPKPart(parts []string, pkName string, prefixLengths map[string]int) indexPKPartKind {
	for _, part := range parts {
		if catalog.ResolveAlias(part) == pkName {
			if prefixLengths[pkName] > 0 {
				return indexPKPartPrefix
			}
			return indexPKPartFull
		}
	}
	return indexPKPartAbsent
}

func indexNeedsOwnerMatch(tableDef *planpb.TableDef, indexDef *planpb.IndexDef, prefixLengths map[string]int) bool {
	return tableDef != nil && tableDef.Pkey != nil && indexDef != nil &&
		indexDef.TableExist && !indexDef.Unique && catalog.IsRegularIndexAlgo(indexDef.IndexAlgo) &&
		!catalog.IsRTreeIndexAlgo(indexDef.IndexAlgo) &&
		classifyIndexPKPart(indexDef.Parts, tableDef.Pkey.PkeyColName, prefixLengths) == indexPKPartPrefix
}

func isSpatialIndexDef(idxDef *planpb.IndexDef) bool {
	return idxDef != nil && catalog.IsRTreeIndexAlgo(idxDef.IndexAlgo)
}

func indexTableStoresSerializedKey(idxDef *planpb.IndexDef) bool {
	return idxDef != nil && !isSpatialIndexDef(idxDef) && len(idxDef.Parts) > 1
}

func indexTableLookupSerialFunc(idxDef *planpb.IndexDef) string {
	if idxDef != nil && !idxDef.Unique && indexTableStoresSerializedKey(idxDef) {
		return "serial_full"
	}
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
