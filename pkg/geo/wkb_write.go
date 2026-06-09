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

import (
	"encoding/binary"
	"math"
)

// WriteWKB encodes a Geometry as standard Well-Known Binary: little-endian
// (NDR), float64 coordinates, geometry type codes 1..7. The output is
// byte-compatible with PostGIS/MySQL ST_AsBinary and is what a GEOMETRY column
// stores verbatim in its varlena.
//
// An empty point is encoded as POINT(NaN NaN); empty lines/polygons/collections
// are encoded with a zero element count.
func WriteWKB(g Geometry) []byte {
	return appendGeom(nil, g, false)
}

// appendGeom appends the WKB encoding of g to b. When f32 is true, each ordinate
// is written as a 4-byte float32 instead of an 8-byte float64 (the GEOMETRY32
// variant); the structure is otherwise identical. Sub-geometries inherit f32.
func appendGeom(b []byte, g Geometry, f32 bool) []byte {
	switch v := g.(type) {
	case Point:
		b = appendHeader(b, wkbPoint)
		if v.IsEmpty {
			b = appendOrd(b, math.NaN(), f32)
			b = appendOrd(b, math.NaN(), f32)
		} else {
			b = appendOrd(b, v.X, f32)
			b = appendOrd(b, v.Y, f32)
		}
	case LineString:
		b = appendHeader(b, wkbLineString)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(v.Points)))
		for _, c := range v.Points {
			b = appendOrd(b, c.X, f32)
			b = appendOrd(b, c.Y, f32)
		}
	case Polygon:
		b = appendHeader(b, wkbPolygon)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(v.Rings)))
		for _, ring := range v.Rings {
			b = binary.LittleEndian.AppendUint32(b, uint32(len(ring)))
			for _, c := range ring {
				b = appendOrd(b, c.X, f32)
				b = appendOrd(b, c.Y, f32)
			}
		}
	case MultiPoint:
		b = appendHeader(b, wkbMultiPoint)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(v.Points)))
		for _, pt := range v.Points {
			b = appendGeom(b, pt, f32)
		}
	case MultiLineString:
		b = appendHeader(b, wkbMultiLineString)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(v.Lines)))
		for _, ls := range v.Lines {
			b = appendGeom(b, ls, f32)
		}
	case MultiPolygon:
		b = appendHeader(b, wkbMultiPolygon)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(v.Polygons)))
		for _, poly := range v.Polygons {
			b = appendGeom(b, poly, f32)
		}
	case GeometryCollection:
		b = appendHeader(b, wkbGeometryCollection)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(v.Geometries)))
		for _, sub := range v.Geometries {
			b = appendGeom(b, sub, f32)
		}
	}
	return b
}

// appendHeader writes the little-endian byte-order marker followed by the
// geometry type code.
func appendHeader(b []byte, typ uint32) []byte {
	b = append(b, wkbNDR)
	return binary.LittleEndian.AppendUint32(b, typ)
}

func appendOrd(b []byte, f float64, f32 bool) []byte {
	if f32 {
		return binary.LittleEndian.AppendUint32(b, math.Float32bits(float32(f)))
	}
	return binary.LittleEndian.AppendUint64(b, math.Float64bits(f))
}
