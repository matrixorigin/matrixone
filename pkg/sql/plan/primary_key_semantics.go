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

package plan

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func primaryKeyColumnPositions(tableDef *pbplan.TableDef) ([]int32, bool) {
	if tableDef == nil || tableDef.Pkey == nil || tableDef.Pkey.PkeyColName == "" ||
		strings.EqualFold(tableDef.Pkey.PkeyColName, catalog.FakePrimaryKeyColName) {
		return nil, false
	}

	pkNames := tableDef.Pkey.Names
	if len(pkNames) == 0 {
		// A hidden composite key does not reveal its user-visible components.
		if strings.EqualFold(tableDef.Pkey.PkeyColName, catalog.CPrimaryKeyColName) ||
			hasCaseInsensitivePrefix(tableDef.Pkey.PkeyColName, catalog.PrefixPriColName) ||
			hasCaseInsensitivePrefix(tableDef.Pkey.PkeyColName, catalog.PrefixCBColName) {
			return nil, false
		}
		pkNames = []string{tableDef.Pkey.PkeyColName}
	} else if len(pkNames) == 1 {
		// A simple primary key is stored in that same user-visible column.
		// Conflicting redundant metadata must not let Names impersonate a key
		// whose storage identity is described by PkeyColName.
		if !strings.EqualFold(pkNames[0], tableDef.Pkey.PkeyColName) {
			return nil, false
		}
	} else if !isCompositePrimaryKeyStorageColumnName(tableDef.Pkey.PkeyColName, pkNames) {
		// Current catalogs use CPrimaryKeyColName; upgraded catalogs can retain
		// the older encoded composite-key prefix. A user-visible component name
		// or an unknown storage identity conflicts with the component list, so
		// neither redundant view is safe to use as a proof.
		return nil, false
	}

	positions := make([]int32, 0, len(pkNames))
	seen := make(map[int32]struct{}, len(pkNames))
	for _, name := range pkNames {
		pos, ok := tableColumnPosition(tableDef, name)
		if !ok {
			return nil, false
		}
		if _, duplicate := seen[pos]; duplicate {
			// Duplicate metadata can hide a missing composite-key component.
			return nil, false
		}
		seen[pos] = struct{}{}
		positions = append(positions, pos)
	}
	return positions, len(positions) > 0
}

func isCompositePrimaryKeyStorageColumnName(name string, components []string) bool {
	for _, component := range components {
		if len(component) == 0 {
			return false
		}
	}
	if strings.EqualFold(name, catalog.CPrimaryKeyColName) {
		return true
	}
	var legacy strings.Builder
	legacy.WriteString(catalog.PrefixPriColName)
	for _, component := range components {
		if len(component) > 999 {
			return false
		}
		length := strconv.Itoa(len(component))
		legacy.WriteString(strings.Repeat("0", 3-len(length)))
		legacy.WriteString(length)
		legacy.WriteString(component)
	}
	return strings.EqualFold(name, legacy.String())
}

func hasCaseInsensitivePrefix(value, prefix string) bool {
	return len(value) >= len(prefix) && strings.EqualFold(value[:len(prefix)], prefix)
}

// sqlEqualityCompatiblePrimaryKeyColumnPositions returns a primary key only
// when SQL equality implies identical storage-key identity for every component.
// This is the shared premise behind singleton-group elimination, physical group
// key reduction, HashOnPK, aggregate pullup, and primary-key functional
// dependency. Keep the type gate as an allowlist so a new representation cannot
// silently become a uniqueness proof before its equality contract is reviewed.
func sqlEqualityCompatiblePrimaryKeyColumnPositions(tableDef *pbplan.TableDef) ([]int32, bool) {
	positions, ok := primaryKeyColumnPositions(tableDef)
	if !ok {
		return nil, false
	}
	for _, pos := range positions {
		if pos < 0 || int(pos) >= len(tableDef.Cols) || tableDef.Cols[pos] == nil ||
			!primaryKeyColumnTypeSupportsSQLEqualityProof(tableDef.Cols[pos].Typ) {
			return nil, false
		}
	}
	return positions, true
}

func primaryKeyColumnTypeSupportsSQLEqualityProof(typ pbplan.Type) bool {
	oid := types.T(typ.Id)
	if !primaryKeyTypeSupportsSQLEqualityProof(oid) ||
		oid.IsDecimal() && !validDecimalPlanType(typ) {
		return false
	}
	if oid == types.T_varchar {
		// Storage keys preserve the original bytes. Only the legacy and opaque
		// binary VARCHAR domains currently promise raw, NO PAD equality.
		// utf8mb4_bin is PAD SPACE, while utf8mb4_general_ci additionally folds
		// case and weights, so either can identify storage-distinct keys.
		return rawVarcharEqualityDomain(typ)
	}
	return true
}

func primaryKeyTypeSupportsSQLEqualityProof(oid types.T) bool {
	switch oid {
	case types.T_bool,
		types.T_bit,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_varchar,
		types.T_binary, types.T_varbinary,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_uuid, types.T_year:
		return true
	default:
		// In particular, FLOAT/DOUBLE storage keys preserve signed-zero bits
		// while SQL equality canonicalizes +0 and -0. CHAR storage keys preserve
		// trailing spaces while comparison uses pad-space equality. Those primary
		// keys therefore cannot prove singleton groups, join-key uniqueness, or
		// functional dependency under SQL equality.
		return false
	}
}

// sqlEqualityJoinUsesOneIdentityDomain rejects resolved cross-type equality as
// a storage-key uniqueness proof. Most mixed operands are normalized by casts,
// which direct-column proofs already reject. DATETIME/TIMESTAMP is a direct
// special case, however, and its session-time-zone conversion is not injective
// across DST gaps and folds.
func sqlEqualityJoinUsesOneIdentityDomain(left, right pbplan.Type) bool {
	if left.Id != right.Id {
		return false
	}
	oid := types.T(left.Id)
	if oid.IsDecimal() {
		return validDecimalPlanType(left) && validDecimalPlanType(right) &&
			left.Scale == right.Scale
	}
	if oid == types.T_varchar {
		return rawVarcharEqualityDomain(left) && rawVarcharEqualityDomain(right)
	}
	return true
}

func rawVarcharEqualityDomain(typ pbplan.Type) bool {
	return types.T(typ.Id) == types.T_varchar &&
		(typ.Charset == uint32(types.CharsetLegacy) ||
			typ.Charset == uint32(types.CharsetBinary))
}

func containsAllSQLEqualityCompatiblePKs(cols []int32, tableDef *pbplan.TableDef) bool {
	pks, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(tableDef)
	if !ok {
		return false
	}
	for _, pk := range pks {
		found := false
		for _, col := range cols {
			if col == pk {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func tableColumnPosition(tableDef *pbplan.TableDef, name string) (int32, bool) {
	if tableDef == nil {
		return 0, false
	}
	var indexedPos int32
	foundIndexed := false
	for indexedName, pos := range tableDef.Name2ColIndex {
		if !strings.EqualFold(indexedName, name) {
			continue
		}
		if foundIndexed && indexedPos != pos {
			return 0, false
		}
		indexedPos = pos
		foundIndexed = true
	}
	foundPos := int32(0)
	foundColumn := false
	for pos, col := range tableDef.Cols {
		if col != nil && strings.EqualFold(col.Name, name) {
			if foundColumn {
				return 0, false
			}
			foundPos = int32(pos)
			foundColumn = true
		}
	}
	if foundIndexed {
		if !foundColumn || indexedPos != foundPos {
			// Conflicting catalog views must not be repaired speculatively inside
			// a correctness proof. Scan the column list even when the name index
			// resolves so a duplicate case-insensitive name cannot be hidden by a
			// preferred map entry.
			return 0, false
		}
		return indexedPos, true
	}
	return foundPos, foundColumn
}
