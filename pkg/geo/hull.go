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

import "sort"

// ConvexHull returns the convex hull of every coordinate in g: a Polygon when
// the points span an area, a LineString when they are collinear (2+ distinct
// points), a Point for a single distinct point, or POINT EMPTY when g has no
// coordinates. Computed with Andrew's monotone-chain algorithm (planar).
func ConvexHull(g Geometry) Geometry {
	var pts []Coord
	eachCoord(g, func(c Coord) { pts = append(pts, c) })
	hull := monotoneChain(pts)
	switch len(hull) {
	case 0:
		return Point{IsEmpty: true}
	case 1:
		return Point{X: hull[0].X, Y: hull[0].Y}
	case 2:
		return LineString{Points: []Coord{hull[0], hull[1]}}
	default:
		ring := make([]Coord, len(hull)+1)
		copy(ring, hull)
		ring[len(hull)] = hull[0]
		return Polygon{Rings: [][]Coord{ring}}
	}
}

// monotoneChain returns the convex-hull vertices in counter-clockwise order,
// without repeating the first vertex. Collinear points are dropped.
func monotoneChain(pts []Coord) []Coord {
	if len(pts) == 0 {
		return nil
	}
	sorted := make([]Coord, len(pts))
	copy(sorted, pts)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].X != sorted[j].X {
			return sorted[i].X < sorted[j].X
		}
		return sorted[i].Y < sorted[j].Y
	})
	uniq := sorted[:1]
	for _, p := range sorted[1:] {
		if p != uniq[len(uniq)-1] {
			uniq = append(uniq, p)
		}
	}
	n := len(uniq)
	if n < 3 {
		return uniq
	}

	cross := func(o, a, b Coord) float64 {
		return (a.X-o.X)*(b.Y-o.Y) - (a.Y-o.Y)*(b.X-o.X)
	}
	hull := make([]Coord, 0, 2*n)
	// Lower hull.
	for _, p := range uniq {
		for len(hull) >= 2 && cross(hull[len(hull)-2], hull[len(hull)-1], p) <= 0 {
			hull = hull[:len(hull)-1]
		}
		hull = append(hull, p)
	}
	// Upper hull.
	lower := len(hull) + 1
	for i := n - 2; i >= 0; i-- {
		p := uniq[i]
		for len(hull) >= lower && cross(hull[len(hull)-2], hull[len(hull)-1], p) <= 0 {
			hull = hull[:len(hull)-1]
		}
		hull = append(hull, p)
	}
	return hull[:len(hull)-1] // drop the repeated start vertex
}
