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
	"fmt"
	"testing"
)

var writerBenchSink []byte

func BenchmarkWKBWriter(b *testing.B) {
	ring := make([]Coord, 4097)
	for i := 0; i < 4096; i++ {
		ring[i] = Coord{X: float64(i), Y: float64(i % 17)}
	}
	ring[4096] = ring[0]
	children := make([]Geometry, 128)
	for i := range children {
		children[i] = Point{X: float64(i), Y: 2}
	}
	for _, tc := range []struct {
		name string
		g    Geometry
	}{{"point", Point{X: 1, Y: 2}}, {"line2", LineString{Points: []Coord{{}, {X: 1, Y: 2}}}}, {"polygon4096", Polygon{Rings: [][]Coord{ring}}}, {"collection128", GeometryCollection{Geometries: children}}} {
		for _, f32 := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/float32=%v", tc.name, f32), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if f32 {
						var err error
						writerBenchSink, err = WriteWKBFloat32(tc.g)
						if err != nil {
							b.Fatal(err)
						}
					} else {
						writerBenchSink = WriteWKB(tc.g)
					}
				}
			})
		}
	}
}
