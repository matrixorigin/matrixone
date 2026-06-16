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
	"testing"
)

// regularPolygon builds an n-vertex regular polygon centered at (cx,cy). Many
// vertices and the overlap between two such polygons exercise the event queue
// and status line, which is where the previous sorted-slice implementation
// degraded toward O(n^2).
func regularPolygon(cx, cy, r float64, n int) Polygon {
	ring := make([]Coord, 0, n+1)
	for i := 0; i < n; i++ {
		a := 2 * math.Pi * float64(i) / float64(n)
		ring = append(ring, Coord{X: cx + r*math.Cos(a), Y: cy + r*math.Sin(a)})
	}
	ring = append(ring, ring[0])
	return Polygon{Rings: [][]Coord{ring}}
}

func benchmarkOverlay(b *testing.B, n int) {
	a := regularPolygon(0, 0, 10, n)
	c := regularPolygon(5, 0, 10, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Overlay(a, c, OpUnion); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOverlayUnion256(b *testing.B)  { benchmarkOverlay(b, 256) }
func BenchmarkOverlayUnion1024(b *testing.B) { benchmarkOverlay(b, 1024) }
func BenchmarkOverlayUnion4096(b *testing.B) { benchmarkOverlay(b, 4096) }
