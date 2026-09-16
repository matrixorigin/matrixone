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
	"github.com/matrixorigin/matrixone/pkg/container/types"
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

// indexOnlyHasOpaqueNativeValue reports whether an index-only plan would have
// to expose a native-0900 key as a user value. Native keys are deliberately
// irreversible: the index stores UCA weights (or the validated UTF-8 bytes
// for 0900_bin), while the base table retains the original string. Returning
// those bytes from an index-only scan would silently corrupt query results.
// Such plans must backfill from the base table instead.
func indexOnlyHasOpaqueNativeValue(idxDef *planpb.IndexDef, tableDef *planpb.TableDef) bool {
	if idxDef == nil || tableDef == nil {
		return false
	}
	for _, part := range idxDef.Parts {
		col, ok := tableDef.Name2ColIndex[catalog.ResolveAlias(part)]
		if !ok || col < 0 || int(col) >= len(tableDef.Cols) {
			continue
		}
		if isNative0900PlanType(tableDef.Cols[col].Typ) {
			return true
		}
	}
	// A unique index-only scan may map the hidden index payload (the base
	// table primary key) back to the visible primary-key column. A native
	// string primary key uses an opaque hidden physical key, so that mapping
	// also requires a base-table lookup.
	if idxDef.Unique && tableDef.Pkey != nil {
		for _, name := range tableDef.Pkey.Names {
			col, ok := tableDef.Name2ColIndex[name]
			if !ok || col < 0 || int(col) >= len(tableDef.Cols) {
				continue
			}
			if isNative0900PlanType(tableDef.Cols[col].Typ) {
				return true
			}
		}
	}
	return false
}

func indexHasNative0900Parts(idxDef *planpb.IndexDef, tableDef *planpb.TableDef) bool {
	if idxDef == nil || tableDef == nil {
		return false
	}
	for _, part := range idxDef.Parts {
		col, ok := tableDef.Name2ColIndex[catalog.ResolveAlias(part)]
		if !ok || col < 0 || int(col) >= len(tableDef.Cols) {
			continue
		}
		if isNative0900PlanType(tableDef.Cols[col].Typ) {
			return true
		}
	}
	return false
}

// indexPhysicalKeyFormatUsable fails closed for a native-0900 index whose
// persisted metadata still says legacy. Such an index may contain raw text
// from an older writer, while the current probe path produces opaque weights.
func indexPhysicalKeyFormatUsable(idxDef *planpb.IndexDef, tableDef *planpb.TableDef) bool {
	if idxDef == nil || tableDef == nil {
		return true
	}
	// Protobuf preserves unknown integers. Do not treat an unknown relation or
	// index format as legacy, even when the index has no currently visible
	// native text part; the persisted object may still be written by a newer
	// key producer.
	if types.ValidateKeyFormat(tableDef.KeyFormat) != nil ||
		types.ValidateKeyFormat(idxDef.KeyFormat) != nil ||
		types.ValidateCollationVersion(tableDef.CollationVersion) != nil {
		return false
	}
	for _, col := range tableDef.Cols {
		if col == nil || col.Typ.Id < 0 || col.Typ.Id > 255 {
			if col != nil {
				return false
			}
			continue
		}
		if types.ValidateCollationTypeMetadata(
			types.T(col.Typ.Id), col.Typ.Charset, col.Typ.CollationVersion,
		) != nil {
			return false
		}
	}
	return !indexHasNative0900Parts(idxDef, tableDef) ||
		idxDef.KeyFormat == uint32(types.PADSpaceKeyV1)
}

func isNative0900PlanType(typ planpb.Type) bool {
	if typ.Id < 0 || typ.Id > 255 || types.ValidateCollationTypeMetadata(
		types.T(typ.Id), typ.Charset, typ.CollationVersion,
	) != nil || !types.T(typ.Id).IsMySQLString() {
		return false
	}
	runtimeType := types.NewWithCharsetVersion(
		types.T(typ.Id), typ.Width, typ.Scale,
		uint8(typ.Charset), uint8(typ.CollationVersion),
	)
	return types.NeedsCollationKey(runtimeType, types.PADSpaceKeyV1)
}
