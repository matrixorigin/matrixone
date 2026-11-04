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
	return AppendWKB(nil, g)
}

// AppendWKB appends standard WKB to dst, reusing its capacity when possible.
// The caller owns dst; g is read-only. Like WriteWKB, unsupported geometries
// contribute no bytes.
func AppendWKB(dst []byte, g Geometry) []byte {
	return appendWKB(dst, g, false)
}

func appendWKB(dst []byte, g Geometry, f32 bool) []byte {
	ordinateSize := 8
	if f32 {
		ordinateSize = 4
	}
	size, ok := wkbSize(g, ordinateSize, 1)
	if ok && size > cap(dst)-len(dst) && size <= int(^uint(0)>>1)-len(dst) {
		out := make([]byte, len(dst), len(dst)+size)
		copy(out, dst)
		dst = out
	}
	return appendGeom(dst, g, f32)
}

// Sizing only visits structure, not coordinates. It is an optimization, not
// validation: unrepresentable sizes or unusually deep direct writer inputs
// fall back to the existing growing encoder.
func wkbSize(g Geometry, ordinateSize, depth int) (int, bool) {
	coordSize := 2 * ordinateSize
	switch v := g.(type) {
	case Point:
		return 5 + coordSize, true
	case LineString:
		return wkbCountSize(9, len(v.Points), coordSize)
	case Polygon:
		size := 9
		for _, ring := range v.Rings {
			ringSize, ok := wkbCountSize(4, len(ring), coordSize)
			if !ok {
				return 0, false
			}
			size, ok = wkbCountSize(size, 1, ringSize)
			if !ok {
				return 0, false
			}
		}
		return size, true
	case MultiPoint:
		return wkbCountSize(9, len(v.Points), 5+coordSize)
	case MultiLineString:
		size := 9
		for _, line := range v.Lines {
			child, ok := wkbSize(line, ordinateSize, depth)
			if !ok {
				return 0, false
			}
			size, ok = wkbCountSize(size, 1, child)
			if !ok {
				return 0, false
			}
		}
		return size, true
	case MultiPolygon:
		size := 9
		for _, polygon := range v.Polygons {
			child, ok := wkbSize(polygon, ordinateSize, depth)
			if !ok {
				return 0, false
			}
			size, ok = wkbCountSize(size, 1, child)
			if !ok {
				return 0, false
			}
		}
		return size, true
	case GeometryCollection:
		if depth > maxGeometryNestingDepth {
			return 0, false
		}
		size := 9
		for _, g := range v.Geometries {
			child, ok := wkbSize(g, ordinateSize, depth+1)
			if !ok {
				return 0, false
			}
			size, ok = wkbCountSize(size, 1, child)
			if !ok {
				return 0, false
			}
		}
		return size, true
	default:
		return 0, true
	}
}

func wkbCountSize(prefix, count, elementSize int) (int, bool) {
	if elementSize == 0 {
		return prefix, true
	}
	if count > (int(^uint(0)>>1)-prefix)/elementSize {
		return 0, false
	}
	return prefix + count*elementSize, true
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
