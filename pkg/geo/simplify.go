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

import "math"

// Simplify returns g with its vertices reduced using the Ramer-Douglas-Peucker
// algorithm with the given distance tolerance (planar units). Points and
// multipoints are returned unchanged. Rings that would degenerate below a
// triangle are kept as-is so polygons stay valid.
func Simplify(g Geometry, tol float64) Geometry {
	if tol <= 0 {
		return g
	}
	switch v := g.(type) {
	case LineString:
		return LineString{Points: douglasPeucker(v.Points, tol)}
	case Polygon:
		return Polygon{Rings: simplifyRings(v.Rings, tol)}
	case MultiLineString:
		lines := make([]LineString, len(v.Lines))
		for i, l := range v.Lines {
			lines[i] = LineString{Points: douglasPeucker(l.Points, tol)}
		}
		return MultiLineString{Lines: lines}
	case MultiPolygon:
		polys := make([]Polygon, len(v.Polygons))
		for i, p := range v.Polygons {
			polys[i] = Polygon{Rings: simplifyRings(p.Rings, tol)}
		}
		return MultiPolygon{Polygons: polys}
	case GeometryCollection:
		subs := make([]Geometry, len(v.Geometries))
		for i, sub := range v.Geometries {
			subs[i] = Simplify(sub, tol)
		}
		return GeometryCollection{Geometries: subs}
	default:
		return g
	}
}

func simplifyRings(rings [][]Coord, tol float64) [][]Coord {
	out := make([][]Coord, len(rings))
	for i, r := range rings {
		s := douglasPeucker(r, tol)
		// A ring needs at least 4 positions (3 distinct + closing point). Keep
		// the original when simplification would degenerate it.
		if len(s) < 4 {
			out[i] = r
			continue
		}
		// Preserve closure.
		if s[0] != s[len(s)-1] {
			s = append(s, s[0])
		}
		out[i] = s
	}
	return out
}

func douglasPeucker(pts []Coord, tol float64) []Coord {
	if len(pts) < 3 {
		// Return a copy, never the input's backing array: the recursive case
		// does append(left[:len(left)-1], right...), which would otherwise
		// overwrite the caller's coordinate slice (and simplifyRings would then
		// emit a corrupted ring via its degenerate `out[i] = r` branch).
		return append([]Coord(nil), pts...)
	}
	end := len(pts) - 1
	maxD := 0.0
	idx := 0
	for i := 1; i < end; i++ {
		d := perpDistance(pts[i], pts[0], pts[end])
		if d > maxD {
			maxD = d
			idx = i
		}
	}
	if maxD > tol {
		left := douglasPeucker(pts[:idx+1], tol)
		right := douglasPeucker(pts[idx:], tol)
		return append(left[:len(left)-1], right...)
	}
	return []Coord{pts[0], pts[end]}
}

// perpDistance is the perpendicular distance from p to the infinite line
// through a and b; when a == b it degrades to the distance from p to that point.
func perpDistance(p, a, b Coord) float64 {
	dx, dy := b.X-a.X, b.Y-a.Y
	if dx == 0 && dy == 0 {
		return math.Hypot(p.X-a.X, p.Y-a.Y)
	}
	return math.Abs(dx*(a.Y-p.Y)-(a.X-p.X)*dy) / math.Hypot(dx, dy)
}
