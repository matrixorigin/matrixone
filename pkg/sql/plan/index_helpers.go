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
	if tableDef == nil || tableDef.Pkey == nil || indexDef == nil || !indexDef.TableExist || indexDef.Unique ||
		!catalog.IsRegularIndexAlgo(indexDef.IndexAlgo) || catalog.IsRTreeIndexAlgo(indexDef.IndexAlgo) {
		return false
	}
	kind := classifyIndexPKPart(indexDef.Parts, tableDef.Pkey.PkeyColName, prefixLengths)
	return kind == indexPKPartPrefix || (len(indexDef.Parts) == 1 && kind == indexPKPartAbsent)
}

// bindIndexOwnerProbe keeps full owner equality in the join while allowing
// the hidden index scan to prune using the physical key alone.
func (builder *QueryBuilder) bindIndexOwnerProbe(baseOwner *Expr, scan *Node, keyPos int32) (*Expr, error) {
	ownerPos, err := builder.indexTableColumnPosition(scan.TableDef, catalog.IndexTablePrimaryColName)
	if err != nil {
		return nil, err
	}
	tag := scan.BindingTags[0]
	indexOwner := GetColExpr(scan.TableDef.Cols[ownerPos].Typ, tag, ownerPos)
	indexOwner.GetCol().Name = catalog.IndexTablePrimaryColName
	ownerCond, err := bindStoredKeyEquality(builder, baseOwner, indexOwner, indexOwner.Typ)
	if err != nil {
		return nil, err
	}
	if builder.indexOwnerProbes == nil {
		builder.indexOwnerProbes = make(map[int32]indexOwnerProbe)
	}
	builder.indexOwnerProbes[tag] = indexOwnerProbe{keyCol: keyPos, ownerCol: ownerPos}
	return ownerCond, nil
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
