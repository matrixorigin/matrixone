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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func isEnumPlanType(typ *plan.Type) bool {
	return typ != nil && typ.Id == int32(types.T_enum) && typ.GetEnumvalues() != ""
}

// isSetPlanType identifies SET columns by piggy-backing on T_uint64 storage
// plus a non-empty Enumvalues string. This is safe today because Enumvalues
// is only populated for ENUM/SET, but it is fragile — any future codepath
// that attaches metadata to that field on a plain uint64 column would be
// misidentified as a SET.
//
// TODO: promote SET to its own dedicated OID (e.g. T_set) once the backlog
// of uint64-storage consumers can be audited. Until then this discriminator
// is the least invasive way to avoid a breaking change across the vector /
// expr / encoding stack.
func isSetPlanType(typ *plan.Type) bool {
	return typ != nil && typ.Id == int32(types.T_uint64) && typ.GetEnumvalues() != ""
}

func isEnumOrSetPlanType(typ *plan.Type) bool {
	return isEnumPlanType(typ) || isSetPlanType(typ)
}

// isGeometryPlanType treats all spatial subtypes (POINT, LINESTRING,
// POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON,
// GEOMETRYCOLLECTION, GEOMETRY) as the same storage-level OID; the
// subtype is only carried in FamilyString / Enumvalues and is purely
// declarative. Storage and comparison treat them identically. Good enough
// for the 3.0-dev compatibility scope, but a future revision may want a
// proper per-subtype discriminator for WKT/WKB validation.
func isGeometryPlanType(typ *plan.Type) bool {
	return typ != nil && typ.Id == int32(types.T_geometry)
}

func geometrySubtypeName(typ *plan.Type) string {
	if !isGeometryPlanType(typ) {
		return ""
	}
	subtype := strings.TrimSpace(typ.GetEnumvalues())
	if strings.EqualFold(subtype, "GEOMETRY") {
		return ""
	}
	return strings.ToUpper(subtype)
}

func normalizeGeometrySubtype(subtype string) string {
	switch strings.ToUpper(strings.TrimSpace(subtype)) {
	case "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION":
		return strings.ToUpper(strings.TrimSpace(subtype))
	default:
		return ""
	}
}
