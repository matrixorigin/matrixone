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

// Coord is a single 2D coordinate. The engine does not support Z (elevation)
// or M (measure) ordinates.
type Coord struct {
	X, Y float64
}

// Geometry is implemented by every geometry kind in the model.
type Geometry interface {
	// Type returns the concrete subtype (POINT, LINESTRING, ...).
	Type() Subtype
	// Empty reports whether the geometry holds no coordinates.
	Empty() bool
}

// Point is a single location. An empty point (POINT EMPTY) carries no
// coordinate; its X/Y are ignored when IsEmpty is true.
type Point struct {
	X, Y    float64
	IsEmpty bool
}

func (Point) Type() Subtype  { return POINT }
func (p Point) Empty() bool  { return p.IsEmpty }
func (p Point) Coord() Coord { return Coord{X: p.X, Y: p.Y} }

// LineString is an ordered sequence of two or more coordinates (or zero, when
// empty).
type LineString struct {
	Points []Coord
}

func (LineString) Type() Subtype { return LINESTRING }
func (l LineString) Empty() bool { return len(l.Points) == 0 }

// Closed reports whether the line's first and last coordinates coincide. An
// empty line is not closed.
func (l LineString) Closed() bool {
	n := len(l.Points)
	if n == 0 {
		return false
	}
	return l.Points[0] == l.Points[n-1]
}

// Polygon is a set of linear rings. Rings[0] is the exterior ring and any
// further entries are interior rings (holes). Each ring is a closed sequence of
// coordinates (first == last).
type Polygon struct {
	Rings [][]Coord
}

func (Polygon) Type() Subtype { return POLYGON }
func (p Polygon) Empty() bool { return len(p.Rings) == 0 }

// MultiPoint is a collection of points.
type MultiPoint struct {
	Points []Point
}

func (MultiPoint) Type() Subtype { return MULTIPOINT }
func (m MultiPoint) Empty() bool { return len(m.Points) == 0 }

// MultiLineString is a collection of line strings.
type MultiLineString struct {
	Lines []LineString
}

func (MultiLineString) Type() Subtype { return MULTILINESTRING }
func (m MultiLineString) Empty() bool { return len(m.Lines) == 0 }

// MultiPolygon is a collection of polygons.
type MultiPolygon struct {
	Polygons []Polygon
}

func (MultiPolygon) Type() Subtype { return MULTIPOLYGON }
func (m MultiPolygon) Empty() bool { return len(m.Polygons) == 0 }

// GeometryCollection is a heterogeneous collection of geometries, which may
// itself contain nested collections.
type GeometryCollection struct {
	Geometries []Geometry
}

func (GeometryCollection) Type() Subtype { return GEOMETRYCOLLECTION }
func (g GeometryCollection) Empty() bool { return len(g.Geometries) == 0 }

// Compile-time assertions that every kind implements Geometry.
var (
	_ Geometry = Point{}
	_ Geometry = LineString{}
	_ Geometry = Polygon{}
	_ Geometry = MultiPoint{}
	_ Geometry = MultiLineString{}
	_ Geometry = MultiPolygon{}
	_ Geometry = GeometryCollection{}
)
