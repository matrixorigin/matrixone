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

// Collect combines geometries into the most specific aggregate: a MultiPoint,
// MultiLineString, or MultiPolygon when every atomic part shares one kind, or a
// GeometryCollection otherwise. Nested multis and collections are flattened
// into their atomic parts first.
func Collect(gs ...Geometry) Geometry {
	var parts []Geometry
	for _, g := range gs {
		parts = append(parts, flattenParts(g)...)
	}
	return assembleParts(parts)
}

// flattenParts returns the atomic (Point/LineString/Polygon) members of g,
// recursing through multis and collections. Empty atoms are preserved.
func flattenParts(g Geometry) []Geometry {
	switch v := g.(type) {
	case MultiPoint:
		out := make([]Geometry, len(v.Points))
		for i, p := range v.Points {
			out[i] = p
		}
		return out
	case MultiLineString:
		out := make([]Geometry, len(v.Lines))
		for i, l := range v.Lines {
			out[i] = l
		}
		return out
	case MultiPolygon:
		out := make([]Geometry, len(v.Polygons))
		for i, p := range v.Polygons {
			out[i] = p
		}
		return out
	case GeometryCollection:
		var out []Geometry
		for _, sub := range v.Geometries {
			out = append(out, flattenParts(sub)...)
		}
		return out
	default:
		return []Geometry{g}
	}
}

func assembleParts(parts []Geometry) Geometry {
	if len(parts) == 0 {
		return GeometryCollection{}
	}
	allSame := true
	first := parts[0].Type()
	for _, p := range parts[1:] {
		if p.Type() != first {
			allSame = false
			break
		}
	}
	if allSame {
		switch first {
		case POINT:
			pts := make([]Point, len(parts))
			for i, p := range parts {
				pts[i] = p.(Point)
			}
			return MultiPoint{Points: pts}
		case LINESTRING:
			lines := make([]LineString, len(parts))
			for i, p := range parts {
				lines[i] = p.(LineString)
			}
			return MultiLineString{Lines: lines}
		case POLYGON:
			polys := make([]Polygon, len(parts))
			for i, p := range parts {
				polys[i] = p.(Polygon)
			}
			return MultiPolygon{Polygons: polys}
		}
	}
	return GeometryCollection{Geometries: parts}
}
