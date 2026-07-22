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

package geo

// Well-Known Binary (WKB) constants shared by the float64 (wkb_read.go,
// wkb_write.go) and float32 (wkb_f32.go) codecs.
//
// MatrixOne stores the bytes produced here directly in the varlena cell with no
// SRID or format header (see docs/design/gisimpl.md §1.3): standard WKB for
// GEOMETRY, the float32-coordinate variant for GEOMETRY32.

const (
	wkbXDR byte = 0 // big-endian byte order marker
	wkbNDR byte = 1 // little-endian byte order marker (what we always write)
)

// WKB geometry type codes (2D). These match the Subtype values 1..7.
const (
	wkbPoint              uint32 = 1
	wkbLineString         uint32 = 2
	wkbPolygon            uint32 = 3
	wkbMultiPoint         uint32 = 4
	wkbMultiLineString    uint32 = 5
	wkbMultiPolygon       uint32 = 6
	wkbGeometryCollection uint32 = 7
)
