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

// SwapXY returns a copy of g with the X and Y of every coordinate swapped
// (ST_SwapXY).
func SwapXY(g Geometry) Geometry {
	swap := func(c Coord) Coord { return Coord{X: c.Y, Y: c.X} }
	swapRing := func(r []Coord) []Coord {
		out := make([]Coord, len(r))
		for i, c := range r {
			out[i] = swap(c)
		}
		return out
	}
	switch v := g.(type) {
	case Point:
		if v.IsEmpty {
			return v
		}
		return Point{X: v.Y, Y: v.X}
	case LineString:
		return LineString{Points: swapRing(v.Points)}
	case Polygon:
		rings := make([][]Coord, len(v.Rings))
		for i, r := range v.Rings {
			rings[i] = swapRing(r)
		}
		return Polygon{Rings: rings}
	case MultiPoint:
		pts := make([]Point, len(v.Points))
		for i, p := range v.Points {
			if p.IsEmpty {
				pts[i] = p
			} else {
				pts[i] = Point{X: p.Y, Y: p.X}
			}
		}
		return MultiPoint{Points: pts}
	case MultiLineString:
		lines := make([]LineString, len(v.Lines))
		for i, l := range v.Lines {
			lines[i] = LineString{Points: swapRing(l.Points)}
		}
		return MultiLineString{Lines: lines}
	case MultiPolygon:
		polys := make([]Polygon, len(v.Polygons))
		for i, p := range v.Polygons {
			polys[i] = SwapXY(p).(Polygon)
		}
		return MultiPolygon{Polygons: polys}
	case GeometryCollection:
		geoms := make([]Geometry, len(v.Geometries))
		for i, sub := range v.Geometries {
			geoms[i] = SwapXY(sub)
		}
		return GeometryCollection{Geometries: geoms}
	}
	return g
}
