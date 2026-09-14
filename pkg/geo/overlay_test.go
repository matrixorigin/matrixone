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
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func overlayArea(t *testing.T, a, b Geometry, op BoolOp) (Geometry, float64) {
	t.Helper()
	g, err := Overlay(a, b, op)
	require.NoError(t, err)
	return g, CartesianArea(g)
}

func TestOverlayTwoOverlappingSquares(t *testing.T) {
	// A: [0,4]^2 (area 16). B: [2,6]x[2,6] (area 16). Overlap [2,4]^2 (area 4).
	a := wkt(t, "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")
	b := wkt(t, "POLYGON((2 2, 6 2, 6 6, 2 6, 2 2))")

	_, inter := overlayArea(t, a, b, OpIntersection)
	require.InDelta(t, 4.0, inter, 1e-9)

	_, union := overlayArea(t, a, b, OpUnion)
	require.InDelta(t, 28.0, union, 1e-9) // 16 + 16 - 4

	_, diff := overlayArea(t, a, b, OpDifference)
	require.InDelta(t, 12.0, diff, 1e-9) // 16 - 4

	_, xor := overlayArea(t, a, b, OpXOR)
	require.InDelta(t, 24.0, xor, 1e-9) // union - inter
}

func TestOverlayInvariants(t *testing.T) {
	a := wkt(t, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	b := wkt(t, "POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))")
	areaA, areaB := CartesianArea(a), CartesianArea(b)

	_, inter := overlayArea(t, a, b, OpIntersection)
	_, union := overlayArea(t, a, b, OpUnion)
	_, diffAB := overlayArea(t, a, b, OpDifference)
	_, diffBA := overlayArea(t, b, a, OpDifference)
	_, xor := overlayArea(t, a, b, OpXOR)

	// area(A) + area(B) == area(A∪B) + area(A∩B)
	require.InDelta(t, areaA+areaB, union+inter, 1e-9)
	// area(A\B) == area(A) - area(A∩B)
	require.InDelta(t, areaA-inter, diffAB, 1e-9)
	require.InDelta(t, areaB-inter, diffBA, 1e-9)
	// area(A xor B) == area(A∪B) - area(A∩B)
	require.InDelta(t, union-inter, xor, 1e-9)
}

func TestOverlayDisjoint(t *testing.T) {
	a := wkt(t, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")
	b := wkt(t, "POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))")

	_, inter := overlayArea(t, a, b, OpIntersection)
	require.InDelta(t, 0.0, inter, 1e-9)

	uni, union := overlayArea(t, a, b, OpUnion)
	require.InDelta(t, 2.0, union, 1e-9)
	require.Equal(t, MULTIPOLYGON, uni.Type())
}

func TestOverlayContained(t *testing.T) {
	// B fully inside A: union == A, intersection == B, difference is a ring.
	a := wkt(t, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	b := wkt(t, "POLYGON((3 3, 7 3, 7 7, 3 7, 3 3))")

	_, union := overlayArea(t, a, b, OpUnion)
	require.InDelta(t, 100.0, union, 1e-9)

	_, inter := overlayArea(t, a, b, OpIntersection)
	require.InDelta(t, 16.0, inter, 1e-9)

	diff, dArea := overlayArea(t, a, b, OpDifference)
	require.InDelta(t, 84.0, dArea, 1e-9) // polygon with a hole
	require.Equal(t, POLYGON, diff.Type())
	require.Len(t, diff.(Polygon).Rings, 2) // shell + hole
}

func TestOverlaySharedEdge(t *testing.T) {
	// Two unit-tall squares sharing the x=4 edge -> union is one 8x4 rectangle.
	a := wkt(t, "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")
	b := wkt(t, "POLYGON((4 0, 8 0, 8 4, 4 4, 4 0))")

	_, inter := overlayArea(t, a, b, OpIntersection)
	require.InDelta(t, 0.0, inter, 1e-9) // touching only along an edge

	uni, union := overlayArea(t, a, b, OpUnion)
	require.InDelta(t, 32.0, union, 1e-9)
	require.Equal(t, POLYGON, uni.Type()) // merged into a single polygon
}

func TestOverlayHalfOverlapInvariants(t *testing.T) {
	// Partial overlap with a shared partial edge.
	a := wkt(t, "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")
	b := wkt(t, "POLYGON((4 1, 8 1, 8 3, 4 3, 4 1))")
	areaA, areaB := CartesianArea(a), CartesianArea(b)
	_, inter := overlayArea(t, a, b, OpIntersection)
	_, union := overlayArea(t, a, b, OpUnion)
	require.InDelta(t, 0.0, inter, 1e-9)
	require.InDelta(t, areaA+areaB, union+inter, 1e-9)
}

func TestOverlaySymDifferenceCollinearPartialOverlap(t *testing.T) {
	// The polygons partially overlap and their top edges overlap collinearly.
	// XOR must keep the two remaining areal components separate.
	a := wkt(t, "POLYGON((0 0,4 0,4 2,0 2,0 0))")
	b := wkt(t, "POLYGON((-2 1,1 1,1 2,-2 2,-2 1))")

	wantA := Polygon{Rings: [][]Coord{{{X: 0, Y: 0}, {X: 4, Y: 0}, {X: 4, Y: 2}, {X: 1, Y: 2}, {X: 1, Y: 1}, {X: 0, Y: 1}, {X: 0, Y: 0}}}}
	wantB := Polygon{Rings: [][]Coord{{{X: -2, Y: 1}, {X: -2, Y: 2}, {X: 0, Y: 2}, {X: 0, Y: 1}, {X: -2, Y: 1}}}}
	wantComponents := canonicalOverlayComponents(t, MultiPolygon{Polygons: []Polygon{wantA, wantB}})
	wantShearedComponents := canonicalOverlayComponents(t, MultiPolygon{Polygons: []Polygon{shearOverlayTestPolygon(wantA), shearOverlayTestPolygon(wantB)}})

	for _, tc := range []struct {
		name       string
		a, b       Geometry
		touch      Coord
		components []string
	}{
		{name: "reported order", a: a, b: b, touch: Coord{X: 0, Y: 1}, components: wantComponents},
		{name: "reversed operands", a: b, b: a, touch: Coord{X: 0, Y: 1}, components: wantComponents},
		{name: "reversed first ring", a: reverseOverlayTestPolygon(a), b: b, touch: Coord{X: 0, Y: 1}, components: wantComponents},
		{name: "reversed second ring", a: a, b: reverseOverlayTestPolygon(b), touch: Coord{X: 0, Y: 1}, components: wantComponents},
		{name: "rotated ring starts", a: rotateOverlayTestPolygon(a, 2), b: rotateOverlayTestPolygon(b, 1), touch: Coord{X: 0, Y: 1}, components: wantComponents},
		{name: "integer shear", a: shearOverlayTestPolygon(a), b: shearOverlayTestPolygon(b), touch: Coord{X: 1, Y: 1}, components: wantShearedComponents},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, xorArea := overlayArea(t, tc.a, tc.b, OpXOR)
			require.Equal(t, MULTIPOLYGON, got.Type())
			require.Len(t, got.(MultiPolygon).Polygons, 2)
			require.InDelta(t, 9.0, xorArea, 1e-9)
			require.Equal(t, tc.components, canonicalOverlayComponents(t, got))
			polygons := overlayTestPolygons(t, got)
			for _, polygon := range polygons {
				requireSimpleOverlayPolygon(t, polygon)
			}
			requireOverlayPolygonsTouchOnlyAt(t, polygons[0], polygons[1], tc.touch)

			_, unionArea := overlayArea(t, tc.a, tc.b, OpUnion)
			_, intersectionArea := overlayArea(t, tc.a, tc.b, OpIntersection)
			require.InDelta(t, unionArea-intersectionArea, xorArea, 1e-9)
		})
	}

	_, unionArea := overlayArea(t, a, b, OpUnion)
	_, intersectionArea := overlayArea(t, a, b, OpIntersection)
	_, differenceAB := overlayArea(t, a, b, OpDifference)
	_, differenceBA := overlayArea(t, b, a, OpDifference)
	require.InDelta(t, 10.0, unionArea, 1e-9)
	require.InDelta(t, 1.0, intersectionArea, 1e-9)
	require.InDelta(t, 7.0, differenceAB, 1e-9)
	require.InDelta(t, 2.0, differenceBA, 1e-9)
}

func TestOverlaySymDifferenceBoundaryContacts(t *testing.T) {
	t.Run("identical polygons", func(t *testing.T) {
		a := wkt(t, "POLYGON((0 0,1 0,1 1,0 1,0 0))")
		b := wkt(t, "POLYGON((0 0,1 0,1 1,0 1,0 0))")
		for _, op := range []BoolOp{OpXOR, OpDifference} {
			got, area := overlayArea(t, a, b, op)
			require.True(t, got.Empty())
			require.Zero(t, area)
		}
		for _, op := range []BoolOp{OpUnion, OpIntersection} {
			got, area := overlayArea(t, a, b, op)
			require.Equal(t, POLYGON, got.Type())
			require.InDelta(t, 1.0, area, 1e-9)
			requireSimpleOverlayPolygon(t, got.(Polygon))
		}
	})

	t.Run("partial shared edge with opposite interiors", func(t *testing.T) {
		a := wkt(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))")
		b := wkt(t, "POLYGON((4 1,8 1,8 3,4 3,4 1))")
		got, area := overlayArea(t, a, b, OpXOR)
		require.Equal(t, POLYGON, got.Type())
		require.InDelta(t, 24.0, area, 1e-9)
		requireSimpleOverlayPolygon(t, got.(Polygon))
		_, intersectionArea := overlayArea(t, a, b, OpIntersection)
		require.Zero(t, intersectionArea)
	})

	t.Run("corner contact keeps separate components", func(t *testing.T) {
		a := wkt(t, "POLYGON((0 0,1 0,1 1,0 1,0 0))")
		b := wkt(t, "POLYGON((1 1,2 1,2 2,1 2,1 1))")
		got, area := overlayArea(t, a, b, OpXOR)
		require.Equal(t, MULTIPOLYGON, got.Type())
		require.Len(t, got.(MultiPolygon).Polygons, 2)
		require.InDelta(t, 2.0, area, 1e-9)
		for _, polygon := range got.(MultiPolygon).Polygons {
			requireSimpleOverlayPolygon(t, polygon)
		}
	})

	t.Run("vertex on edge contact keeps separate components", func(t *testing.T) {
		a := wkt(t, "POLYGON((0 0,2 0,2 2,0 2,0 0))")
		b := wkt(t, "POLYGON((2 1,3 0,3 2,2 1))")
		got, area := overlayArea(t, a, b, OpXOR)
		require.Equal(t, MULTIPOLYGON, got.Type())
		require.Len(t, got.(MultiPolygon).Polygons, 2)
		require.InDelta(t, 5.0, area, 1e-9)
		for _, polygon := range got.(MultiPolygon).Polygons {
			requireSimpleOverlayPolygon(t, polygon)
		}
	})

	t.Run("contained XOR preserves hole nesting", func(t *testing.T) {
		a := wkt(t, "POLYGON((0 0,10 0,10 10,0 10,0 0))")
		b := wkt(t, "POLYGON((3 3,7 3,7 7,3 7,3 3))")
		got, area := overlayArea(t, a, b, OpXOR)
		require.Equal(t, POLYGON, got.Type())
		require.InDelta(t, 84.0, area, 1e-9)
		require.Len(t, got.(Polygon).Rings, 2)
		requireSimpleOverlayPolygon(t, got.(Polygon))
		require.Equal(t, -1, PointInPolygon(Coord{X: 5, Y: 5}, got.(Polygon)))
		require.Equal(t, 1, PointInPolygon(Coord{X: 1, Y: 1}, got.(Polygon)))
	})

	t.Run("multipolygon retains independent component", func(t *testing.T) {
		a := MultiPolygon{Polygons: []Polygon{
			wkt(t, "POLYGON((0 0,4 0,4 2,0 2,0 0))").(Polygon),
			wkt(t, "POLYGON((10 0,12 0,12 2,10 2,10 0))").(Polygon),
		}}
		b := wkt(t, "POLYGON((-2 1,1 1,1 2,-2 2,-2 1))")
		got, area := overlayArea(t, a, b, OpXOR)
		require.Equal(t, MULTIPOLYGON, got.Type())
		require.Len(t, got.(MultiPolygon).Polygons, 3)
		require.InDelta(t, 13.0, area, 1e-9)
		for _, polygon := range got.(MultiPolygon).Polygons {
			requireSimpleOverlayPolygon(t, polygon)
		}
	})
}

func TestSplitRepeatedOverlayBoundaryWalk(t *testing.T) {
	branch := Coord{X: 0, Y: 0}
	walk := []Coord{
		branch, {X: 1, Y: 0}, {X: 1, Y: 1}, branch,
		{X: -1, Y: 0}, {X: -1, Y: -1}, branch,
	}
	branches := map[Coord]struct{}{branch: {}}

	rings, err := splitRepeatedRing(walk, branches)
	require.NoError(t, err)
	require.Len(t, rings, 2)
	for _, ring := range rings {
		polygon := Polygon{Rings: [][]Coord{ring}}
		requireSimpleOverlayPolygon(t, polygon)
		require.InDelta(t, 0.5, math.Abs(ringSignedArea(ring)), 1e-9)
	}
}

func TestOverlaySymDifferenceRectangleInvariants(t *testing.T) {
	// Fixed-seed, minimum-sized axis-aligned rectangles exercise many collinear
	// and endpoint configurations without a cluster, sleeps, or large fixtures.
	const cases = 120
	seed := uint64(28183)
	for i := 0; i < cases; i++ {
		seed = seed*6364136223846793005 + 1
		x1 := float64(int(seed>>32)%7 - 3)
		seed = seed*6364136223846793005 + 1
		x2 := x1 + float64((seed>>32)%4+1)
		seed = seed*6364136223846793005 + 1
		y1 := float64(int(seed>>32)%7 - 3)
		seed = seed*6364136223846793005 + 1
		y2 := y1 + float64((seed>>32)%4+1)
		a := overlayTestRectangle(x1, y1, x2, y2)

		seed = seed*6364136223846793005 + 1
		u1 := float64(int(seed>>32)%7 - 3)
		seed = seed*6364136223846793005 + 1
		u2 := u1 + float64((seed>>32)%4+1)
		seed = seed*6364136223846793005 + 1
		v1 := float64(int(seed>>32)%7 - 3)
		seed = seed*6364136223846793005 + 1
		v2 := v1 + float64((seed>>32)%4+1)
		b := overlayTestRectangle(u1, v1, u2, v2)

		intersectionWidth := math.Max(0, math.Min(x2, u2)-math.Max(x1, u1))
		intersectionHeight := math.Max(0, math.Min(y2, v2)-math.Max(y1, v1))
		wantArea := (x2-x1)*(y2-y1) + (u2-u1)*(v2-v1) - 2*intersectionWidth*intersectionHeight
		got, err := Overlay(a, b, OpXOR)
		if err != nil {
			t.Fatalf("case %d: Overlay(XOR) returned error: %v", i, err)
		}
		require.InDeltaf(t, wantArea, CartesianArea(got), 1e-9, "case %d: A=%v B=%v result=%s", i, a, b, WriteWKT(got))
		for _, polygon := range overlayTestPolygons(t, got) {
			requireSimpleOverlayPolygon(t, polygon)
		}
	}
}

func overlayTestRectangle(x1, y1, x2, y2 float64) Polygon {
	ring := []Coord{{X: x1, Y: y1}, {X: x2, Y: y1}, {X: x2, Y: y2}, {X: x1, Y: y2}, {X: x1, Y: y1}}
	return Polygon{Rings: [][]Coord{ring}}
}

func reverseOverlayTestPolygon(g Geometry) Polygon {
	p := g.(Polygon)
	ring := append([]Coord(nil), p.Rings[0]...)
	for left, right := 0, len(ring)-2; left < right; left, right = left+1, right-1 {
		ring[left], ring[right] = ring[right], ring[left]
	}
	ring[len(ring)-1] = ring[0]
	return Polygon{Rings: [][]Coord{ring}}
}

func rotateOverlayTestPolygon(g Geometry, offset int) Polygon {
	p := g.(Polygon)
	open := p.Rings[0][:len(p.Rings[0])-1]
	ring := make([]Coord, 0, len(open)+1)
	for i := range open {
		ring = append(ring, open[(i+offset)%len(open)])
	}
	ring = append(ring, ring[0])
	return Polygon{Rings: [][]Coord{ring}}
}

func shearOverlayTestPolygon(g Geometry) Polygon {
	p := g.(Polygon)
	ring := make([]Coord, len(p.Rings[0]))
	for i, point := range p.Rings[0] {
		ring[i] = Coord{X: point.X + point.Y, Y: point.Y}
	}
	return Polygon{Rings: [][]Coord{ring}}
}

func canonicalOverlayRing(ring []Coord) string {
	points := append([]Coord(nil), ring[:len(ring)-1]...)
	for changed := true; changed && len(points) > 3; {
		changed = false
		for i, current := range points {
			previous := points[(i+len(points)-1)%len(points)]
			next := points[(i+1)%len(points)]
			if ovSignedArea(previous, current, next) != 0 ||
				current.X < math.Min(previous.X, next.X) || current.X > math.Max(previous.X, next.X) ||
				current.Y < math.Min(previous.Y, next.Y) || current.Y > math.Max(previous.Y, next.Y) {
				continue
			}
			points = append(points[:i], points[i+1:]...)
			changed = true
			break
		}
	}

	best := ""
	for direction := 0; direction < 2; direction++ {
		for start := range points {
			var key strings.Builder
			for step := range points {
				index := (start + step) % len(points)
				if direction == 1 {
					index = (start - step + len(points)) % len(points)
				}
				point := points[index]
				key.WriteString(strconv.FormatFloat(point.X, 'g', -1, 64))
				key.WriteByte(',')
				key.WriteString(strconv.FormatFloat(point.Y, 'g', -1, 64))
				key.WriteByte(';')
			}
			candidate := key.String()
			if best == "" || candidate < best {
				best = candidate
			}
		}
	}
	return best
}

func canonicalOverlayComponents(t *testing.T, g Geometry) []string {
	t.Helper()
	polygons := overlayTestPolygons(t, g)
	components := make([]string, 0, len(polygons))
	for _, polygon := range polygons {
		rings := make([]string, len(polygon.Rings))
		for i, ring := range polygon.Rings {
			rings[i] = canonicalOverlayRing(ring)
		}
		sort.Strings(rings)
		components = append(components, strings.Join(rings, "|"))
	}
	sort.Strings(components)
	return components
}

func overlayTestPolygons(t *testing.T, g Geometry) []Polygon {
	t.Helper()
	switch polygonal := g.(type) {
	case Polygon:
		if polygonal.Empty() {
			return nil
		}
		return []Polygon{polygonal}
	case MultiPolygon:
		return polygonal.Polygons
	default:
		t.Fatalf("expected Polygon or MultiPolygon, got %s: %s", subtypeName(g.Type()), WriteWKT(g))
		return nil
	}
}

func requireSimpleOverlayPolygon(t *testing.T, polygon Polygon) {
	t.Helper()
	for ringIndex, ring := range polygon.Rings {
		require.GreaterOrEqual(t, len(ring), 4)
		require.Equal(t, ring[0], ring[len(ring)-1])
		n := len(ring) - 1
		for i := 0; i < n; i++ {
			for j := i + 1; j < n; j++ {
				if j == i+1 || (i == 0 && j == n-1) {
					continue
				}
				require.Falsef(t, SegmentsIntersect(ring[i], ring[i+1], ring[j], ring[j+1]), "ring %d edges %d and %d intersect: %v", ringIndex, i, j, ring)
			}
		}
	}
}

func requireOverlayPolygonsTouchOnlyAt(t *testing.T, a, b Polygon, touch Coord) {
	t.Helper()
	for _, ringA := range a.Rings {
		for i := 0; i < len(ringA)-1; i++ {
			for _, ringB := range b.Rings {
				for j := 0; j < len(ringB)-1; j++ {
					n, p, _ := segmentIntersection(ringA[i], ringA[i+1], ringB[j], ringB[j+1])
					if n == 0 {
						continue
					}
					require.Equal(t, 1, n, "components have a shared boundary segment")
					require.Equal(t, touch, p, "components intersect at an unexpected point")
					require.True(t, p == ringA[i] || p == ringA[i+1], "touch is not an endpoint of the first component")
					require.True(t, p == ringB[j] || p == ringB[j+1], "touch is not an endpoint of the second component")
				}
			}
		}
	}
}

func TestOverlayNonConvexFloat(t *testing.T) {
	// A non-convex "cross" polygon unioned with a float-coordinate disc must
	// keep the area invariant (snap-rounding makes near-coincident points
	// coincide exactly). Regression for the buffer union path.
	cross := wkt(t, "POLYGON((-1 0,0 0,0 -1,10 -1,10 0,11 0,11 10,10 10,10 11,0 11,0 10,-1 10,-1 0))")
	disc := Polygon(circlePolygon(Coord{X: 0, Y: 0}, 1, 16))

	areaCross, areaDisc := CartesianArea(cross), CartesianArea(disc)
	_, inter := overlayArea(t, cross, disc, OpIntersection)
	_, union := overlayArea(t, cross, disc, OpUnion)
	require.InDelta(t, areaCross+areaDisc, union+inter, 1e-6)
	require.Greater(t, union, areaCross) // the disc pokes outside the cross
}

func TestOverlayNonAreal(t *testing.T) {
	_, err := Overlay(wkt(t, "POINT(0 0)"), wkt(t, "POLYGON((0 0,1 0,1 1,0 1,0 0))"), OpUnion)
	require.Error(t, err)
}
