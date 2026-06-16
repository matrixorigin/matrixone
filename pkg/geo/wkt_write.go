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
	"strconv"
	"strings"
)

// WriteWKT renders a Geometry as Well-Known Text, matching MySQL's ST_AsText
// conventions: no space after the type keyword, no spaces after commas, the
// "<TYPE> EMPTY" form for empty geometries, and MULTIPOINT printed in the bare
// form "MULTIPOINT(1 1,2 2)".
//
// Coordinates use the shortest decimal that round-trips to the same float64
// (strconv 'g', -1), so WriteWKT(ParseWKT(x)) is stable.
func WriteWKT(g Geometry) string {
	var b strings.Builder
	writeWKT(&b, g)
	return b.String()
}

func writeWKT(b *strings.Builder, g Geometry) {
	switch v := g.(type) {
	case Point:
		b.WriteString("POINT")
		if v.IsEmpty {
			b.WriteString(" EMPTY")
			return
		}
		b.WriteByte('(')
		writeCoordXY(b, v.X, v.Y)
		b.WriteByte(')')
	case LineString:
		b.WriteString("LINESTRING")
		if v.Empty() {
			b.WriteString(" EMPTY")
			return
		}
		writeCoordSeq(b, v.Points)
	case Polygon:
		b.WriteString("POLYGON")
		if v.Empty() {
			b.WriteString(" EMPTY")
			return
		}
		writeRings(b, v.Rings)
	case MultiPoint:
		b.WriteString("MULTIPOINT")
		if v.Empty() {
			b.WriteString(" EMPTY")
			return
		}
		b.WriteByte('(')
		for i, pt := range v.Points {
			if i > 0 {
				b.WriteByte(',')
			}
			if pt.IsEmpty {
				b.WriteString("EMPTY")
			} else {
				writeCoordXY(b, pt.X, pt.Y)
			}
		}
		b.WriteByte(')')
	case MultiLineString:
		b.WriteString("MULTILINESTRING")
		if v.Empty() {
			b.WriteString(" EMPTY")
			return
		}
		b.WriteByte('(')
		for i, ls := range v.Lines {
			if i > 0 {
				b.WriteByte(',')
			}
			if ls.Empty() {
				b.WriteString("EMPTY")
			} else {
				writeCoordSeq(b, ls.Points)
			}
		}
		b.WriteByte(')')
	case MultiPolygon:
		b.WriteString("MULTIPOLYGON")
		if v.Empty() {
			b.WriteString(" EMPTY")
			return
		}
		b.WriteByte('(')
		for i, poly := range v.Polygons {
			if i > 0 {
				b.WriteByte(',')
			}
			if poly.Empty() {
				b.WriteString("EMPTY")
			} else {
				writeRings(b, poly.Rings)
			}
		}
		b.WriteByte(')')
	case GeometryCollection:
		b.WriteString("GEOMETRYCOLLECTION")
		if v.Empty() {
			b.WriteString(" EMPTY")
			return
		}
		b.WriteByte('(')
		for i, sub := range v.Geometries {
			if i > 0 {
				b.WriteByte(',')
			}
			writeWKT(b, sub)
		}
		b.WriteByte(')')
	}
}

func writeNum(b *strings.Builder, f float64) {
	b.WriteString(strconv.FormatFloat(f, 'g', -1, 64))
}

func writeCoordXY(b *strings.Builder, x, y float64) {
	writeNum(b, x)
	b.WriteByte(' ')
	writeNum(b, y)
}

func writeCoordSeq(b *strings.Builder, coords []Coord) {
	b.WriteByte('(')
	for i, c := range coords {
		if i > 0 {
			b.WriteByte(',')
		}
		writeCoordXY(b, c.X, c.Y)
	}
	b.WriteByte(')')
}

func writeRings(b *strings.Builder, rings [][]Coord) {
	b.WriteByte('(')
	for i, r := range rings {
		if i > 0 {
			b.WriteByte(',')
		}
		writeCoordSeq(b, r)
	}
	b.WriteByte(')')
}
