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

// This file holds the float32-coordinate WKB variant used by the GEOMETRY32
// type. The byte structure is identical to standard WKB (byte order, type
// codes, element counts) except each ordinate occupies 4 bytes (float32)
// instead of 8 (float64). The recursive codec is shared with the float64 path
// (wkb_read.go / wkb_write.go) via the internal f32 flag; this file only adds
// the public float32 API and the up-convert helper.
//
// As with float64 WKB, no SRID or format header is written: the SQL layer
// selects this codec based on the column OID (T_geometry32), and SRID lives in
// the column type.

// WriteWKBFloat32 encodes a Geometry with float32 coordinates. This is the
// on-disk form of a GEOMETRY32 value.
func WriteWKBFloat32(g Geometry) []byte {
	return appendGeom(nil, g, true)
}

// ReadWKBFloat32 decodes float32-coordinate WKB (the GEOMETRY32 storage form)
// into a Geometry whose coordinates are the float32 values widened to float64.
func ReadWKBFloat32(b []byte) (Geometry, error) {
	return readWKB(b, true)
}

// WKBFloat32ToStandard converts a float32-coordinate WKB payload into a standard
// float64 WKB payload. ST_AsWKB / ST_AsBinary use this so a GEOMETRY32 value is
// always emitted as interoperable standard WKB.
func WKBFloat32ToStandard(b []byte) ([]byte, error) {
	g, err := ReadWKBFloat32(b)
	if err != nil {
		return nil, err
	}
	return WriteWKB(g), nil
}
