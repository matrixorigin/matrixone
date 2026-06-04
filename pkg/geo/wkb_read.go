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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ReadWKB decodes standard Well-Known Binary (float64 coordinates) into a
// Geometry. Each (sub-)geometry's byte order is honored independently, so both
// little-endian (NDR) and big-endian (XDR) input are accepted. Only 2D type
// codes 1..7 are supported; Z/M or EWKB-flagged codes are rejected.
func ReadWKB(b []byte) (Geometry, error) {
	return readWKB(b, false)
}

// readWKB is the shared entry point; f32 selects 4-byte float32 ordinates (the
// GEOMETRY32 variant) instead of 8-byte float64.
func readWKB(b []byte, f32 bool) (Geometry, error) {
	r := &wkbReader{b: b, f32: f32}
	g, err := r.readGeometry(1)
	if err != nil {
		return nil, err
	}
	if r.pos != len(b) {
		return nil, moerr.NewInvalidInputNoCtxf("invalid WKB: %d trailing byte(s)", len(b)-r.pos)
	}
	return g, nil
}

type wkbReader struct {
	b   []byte
	pos int
	f32 bool
}

func (r *wkbReader) ordSize() int {
	if r.f32 {
		return 4
	}
	return 8
}

func (r *wkbReader) remaining() int { return len(r.b) - r.pos }

func (r *wkbReader) readGeometry(depth int) (Geometry, error) {
	little, err := r.readByteOrder()
	if err != nil {
		return nil, err
	}
	typ, err := r.readUint32(little)
	if err != nil {
		return nil, err
	}
	switch typ {
	case wkbPoint:
		x, err := r.readOrd(little)
		if err != nil {
			return nil, err
		}
		y, err := r.readOrd(little)
		if err != nil {
			return nil, err
		}
		if math.IsNaN(x) && math.IsNaN(y) {
			return Point{IsEmpty: true}, nil
		}
		return Point{X: x, Y: y}, nil
	case wkbLineString:
		pts, err := r.readCoords(little)
		if err != nil {
			return nil, err
		}
		return LineString{Points: pts}, nil
	case wkbPolygon:
		nr, err := r.readCount(little, 4) // each ring has at least a 4-byte count
		if err != nil {
			return nil, err
		}
		var rings [][]Coord
		if nr > 0 {
			rings = make([][]Coord, nr)
			for i := range nr {
				ring, err := r.readCoords(little)
				if err != nil {
					return nil, err
				}
				rings[i] = ring
			}
		}
		return Polygon{Rings: rings}, nil
	case wkbMultiPoint:
		n, err := r.readCount(little, 5) // each sub-geom is at least 5 bytes
		if err != nil {
			return nil, err
		}
		var pts []Point
		if n > 0 {
			pts = make([]Point, n)
			for i := range n {
				sub, err := r.readGeometry(depth)
				if err != nil {
					return nil, err
				}
				pt, ok := sub.(Point)
				if !ok {
					return nil, r.errf("MULTIPOINT contains a non-point geometry")
				}
				pts[i] = pt
			}
		}
		return MultiPoint{Points: pts}, nil
	case wkbMultiLineString:
		n, err := r.readCount(little, 5)
		if err != nil {
			return nil, err
		}
		var lines []LineString
		if n > 0 {
			lines = make([]LineString, n)
			for i := range n {
				sub, err := r.readGeometry(depth)
				if err != nil {
					return nil, err
				}
				ls, ok := sub.(LineString)
				if !ok {
					return nil, r.errf("MULTILINESTRING contains a non-linestring geometry")
				}
				lines[i] = ls
			}
		}
		return MultiLineString{Lines: lines}, nil
	case wkbMultiPolygon:
		n, err := r.readCount(little, 5)
		if err != nil {
			return nil, err
		}
		var polys []Polygon
		if n > 0 {
			polys = make([]Polygon, n)
			for i := range n {
				sub, err := r.readGeometry(depth)
				if err != nil {
					return nil, err
				}
				poly, ok := sub.(Polygon)
				if !ok {
					return nil, r.errf("MULTIPOLYGON contains a non-polygon geometry")
				}
				polys[i] = poly
			}
		}
		return MultiPolygon{Polygons: polys}, nil
	case wkbGeometryCollection:
		// Bound nesting so a maliciously deep GEOMETRYCOLLECTION cannot recurse
		// to a fatal (unrecoverable) stack overflow.
		if depth > maxGeometryNestingDepth {
			return nil, r.errf("geometry collection nesting depth exceeds %d", maxGeometryNestingDepth)
		}
		n, err := r.readCount(little, 5)
		if err != nil {
			return nil, err
		}
		var geoms []Geometry
		if n > 0 {
			geoms = make([]Geometry, n)
			for i := range n {
				sub, err := r.readGeometry(depth + 1)
				if err != nil {
					return nil, err
				}
				geoms[i] = sub
			}
		}
		return GeometryCollection{Geometries: geoms}, nil
	default:
		return nil, r.errf("unsupported WKB geometry type %d (Z/M and EWKB are not supported)", typ)
	}
}

// readCoords reads a uint32 point count followed by that many (x, y) pairs.
func (r *wkbReader) readCoords(little bool) ([]Coord, error) {
	n, err := r.readCount(little, 2*r.ordSize())
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	coords := make([]Coord, n)
	for i := range n {
		x, err := r.readOrd(little)
		if err != nil {
			return nil, err
		}
		y, err := r.readOrd(little)
		if err != nil {
			return nil, err
		}
		coords[i] = Coord{X: x, Y: y}
	}
	return coords, nil
}

// readCount reads a uint32 element count and rejects values that cannot
// possibly fit in the remaining bytes (perElem is the minimum byte size of one
// element), guarding against huge allocations from malformed input.
func (r *wkbReader) readCount(little bool, perElem int) (uint32, error) {
	n, err := r.readUint32(little)
	if err != nil {
		return 0, err
	}
	if perElem > 0 && uint64(n) > uint64(r.remaining()/perElem) {
		return 0, r.errf("element count %d exceeds remaining %d bytes", n, r.remaining())
	}
	return n, nil
}

func (r *wkbReader) readByteOrder() (bool, error) {
	if r.pos >= len(r.b) {
		return false, r.errf("unexpected end of WKB (byte order)")
	}
	bo := r.b[r.pos]
	r.pos++
	switch bo {
	case wkbNDR:
		return true, nil
	case wkbXDR:
		return false, nil
	default:
		return false, r.errf("invalid WKB byte order marker %d", bo)
	}
}

func (r *wkbReader) readUint32(little bool) (uint32, error) {
	if r.pos+4 > len(r.b) {
		return 0, r.errf("unexpected end of WKB (uint32)")
	}
	var v uint32
	if little {
		v = binary.LittleEndian.Uint32(r.b[r.pos:])
	} else {
		v = binary.BigEndian.Uint32(r.b[r.pos:])
	}
	r.pos += 4
	return v, nil
}

func (r *wkbReader) readOrd(little bool) (float64, error) {
	sz := r.ordSize()
	if r.pos+sz > len(r.b) {
		return 0, r.errf("unexpected end of WKB (coordinate)")
	}
	var f float64
	if r.f32 {
		var bits uint32
		if little {
			bits = binary.LittleEndian.Uint32(r.b[r.pos:])
		} else {
			bits = binary.BigEndian.Uint32(r.b[r.pos:])
		}
		f = float64(math.Float32frombits(bits))
	} else {
		var bits uint64
		if little {
			bits = binary.LittleEndian.Uint64(r.b[r.pos:])
		} else {
			bits = binary.BigEndian.Uint64(r.b[r.pos:])
		}
		f = math.Float64frombits(bits)
	}
	r.pos += sz
	return f, nil
}

func (r *wkbReader) errf(format string, args ...any) error {
	return moerr.NewInvalidInputNoCtxf("invalid WKB: "+format+" (at offset %d)", append(args, r.pos)...)
}
