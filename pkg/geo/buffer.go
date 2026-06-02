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
	"errors"
	"math"
)

// circlePolygon approximates a disc of radius r centered at c with segs sides.
func circlePolygon(c Coord, r float64, segs int) Polygon {
	ring := make([]Coord, segs+1)
	for i := 0; i < segs; i++ {
		ang := 2 * math.Pi * float64(i) / float64(segs)
		ring[i] = Coord{X: c.X + r*math.Cos(ang), Y: c.Y + r*math.Sin(ang)}
	}
	ring[segs] = ring[0]
	return Polygon{Rings: [][]Coord{ring}}
}

// segmentRect is the rectangle covering all points within r of segment a-b
// (without the end caps, which the vertex discs supply).
func segmentRect(a, b Coord, r float64) Polygon {
	dx, dy := b.X-a.X, b.Y-a.Y
	length := math.Hypot(dx, dy)
	if length == 0 {
		return Polygon{}
	}
	nx, ny := -dy/length*r, dx/length*r
	ring := []Coord{
		{X: a.X + nx, Y: a.Y + ny},
		{X: b.X + nx, Y: b.Y + ny},
		{X: b.X - nx, Y: b.Y - ny},
		{X: a.X - nx, Y: a.Y - ny},
		{X: a.X + nx, Y: a.Y + ny},
	}
	return Polygon{Rings: [][]Coord{ring}}
}

// forEachSegment invokes fn for every edge of the linear/areal parts of g.
func forEachSegment(g Geometry, fn func(a, b Coord)) {
	emit := func(pts []Coord) {
		for i := 1; i < len(pts); i++ {
			fn(pts[i-1], pts[i])
		}
	}
	switch v := g.(type) {
	case LineString:
		emit(v.Points)
	case Polygon:
		for _, r := range v.Rings {
			emit(r)
		}
	case MultiLineString:
		for _, l := range v.Lines {
			emit(l.Points)
		}
	case MultiPolygon:
		for _, p := range v.Polygons {
			for _, r := range p.Rings {
				emit(r)
			}
		}
	case GeometryCollection:
		for _, sub := range v.Geometries {
			forEachSegment(sub, fn)
		}
	}
}

// Buffer returns the set of points within dist of g, computed as the union of
// discs at every vertex, rectangles along every edge, and the original areal
// part. quadSegs controls the disc approximation (segments per quarter circle).
// Only non-negative distances are supported.
func Buffer(g Geometry, dist float64, quadSegs int) (Geometry, error) {
	if dist < 0 {
		return nil, errors.New("ST_Buffer: negative distance is not supported")
	}
	if dist == 0 {
		return g, nil
	}
	segs := quadSegs * 4
	if segs < 8 {
		segs = 8
	}

	var prims []Geometry
	switch g.(type) {
	case Polygon, MultiPolygon, GeometryCollection:
		prims = append(prims, g) // fill the interior
	}
	forEachSegment(g, func(a, b Coord) {
		if rect := segmentRect(a, b, dist); len(rect.Rings) > 0 {
			prims = append(prims, rect)
		}
	})
	eachCoord(g, func(c Coord) {
		prims = append(prims, circlePolygon(c, dist, segs))
	})

	if len(prims) == 0 {
		return Polygon{}, nil
	}
	acc := prims[0]
	// The accumulator must be areal; GeometryCollection fill is unsupported here.
	if _, err := polygonRings(acc); err != nil {
		return nil, err
	}
	for _, p := range prims[1:] {
		u, err := Overlay(acc, p, OpUnion)
		if err != nil {
			return nil, err
		}
		acc = u
	}
	return acc, nil
}
