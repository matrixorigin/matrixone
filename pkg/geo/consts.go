// Copyright 2021 - 2024 Matrix Origin
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

// Package geo is MatrixOne's self-contained GIS engine: the geometry model,
// WKT/WKB readers and writers, and the Cartesian (planar) and geodetic
// (spherical, via github.com/golang/geo) algorithm kernels.
//
// It has no dependency on pkg/sql so it can be unit-tested in isolation. The
// SQL layer stores geometry values as bare WKB inside a varlena and supplies
// the SRID from the column type; this package never embeds an SRID in the
// bytes it produces.
package geo

// Subtype identifies the kind of a geometry. The values match the standard WKB
// geometry-type codes for the seven concrete kinds (1..7); GENERIC (0) is the
// unconstrained "any geometry" used for a plain GEOMETRY column.
//
// The SQL layer stores this value in types.Type.Scale.
type Subtype uint8

const (
	GENERIC            Subtype = 0
	POINT              Subtype = 1
	LINESTRING         Subtype = 2
	POLYGON            Subtype = 3
	MULTIPOINT         Subtype = 4
	MULTILINESTRING    Subtype = 5
	MULTIPOLYGON       Subtype = 6
	GEOMETRYCOLLECTION Subtype = 7
)

// SRID values supported by the engine. Any other SRID is rejected by the
// computation kernels.
const (
	// SRIDPlanar is unitless planar (Cartesian) geometry.
	SRIDPlanar uint32 = 0
	// SRIDWGS84 is WGS 84 longitude/latitude; computations are geodetic and
	// return meters / square meters.
	SRIDWGS84 uint32 = 4326
)

// String returns the canonical WKT keyword for the subtype. GENERIC maps to
// "GEOMETRY".
func (s Subtype) String() string {
	switch s {
	case GENERIC:
		return "GEOMETRY"
	case POINT:
		return "POINT"
	case LINESTRING:
		return "LINESTRING"
	case POLYGON:
		return "POLYGON"
	case MULTIPOINT:
		return "MULTIPOINT"
	case MULTILINESTRING:
		return "MULTILINESTRING"
	case MULTIPOLYGON:
		return "MULTIPOLYGON"
	case GEOMETRYCOLLECTION:
		return "GEOMETRYCOLLECTION"
	default:
		return "GEOMETRY"
	}
}

// Valid reports whether s is one of the eight known subtypes.
func (s Subtype) Valid() bool {
	return s <= GEOMETRYCOLLECTION
}

// SupportedSRID reports whether srid is one the computation kernels can handle.
func SupportedSRID(srid uint32) bool {
	return srid == SRIDPlanar || srid == SRIDWGS84
}
