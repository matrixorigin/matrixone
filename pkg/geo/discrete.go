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

// coordsOf returns every coordinate in g in document order.
func coordsOf(g Geometry) []Coord {
	var pts []Coord
	eachCoord(g, func(c Coord) { pts = append(pts, c) })
	return pts
}

// HausdorffDistance returns the discrete Hausdorff distance (planar) between the
// vertex sets of two geometries: the larger of the two directed distances.
func HausdorffDistance(a, b Geometry) (float64, bool) {
	pa, pb := coordsOf(a), coordsOf(b)
	if len(pa) == 0 || len(pb) == 0 {
		return 0, false
	}
	return math.Max(directedHausdorff(pa, pb), directedHausdorff(pb, pa)), true
}

func directedHausdorff(pa, pb []Coord) float64 {
	maxMin := 0.0
	for _, a := range pa {
		minD := math.Inf(1)
		for _, b := range pb {
			if d := math.Hypot(a.X-b.X, a.Y-b.Y); d < minD {
				minD = d
			}
		}
		if minD > maxMin {
			maxMin = minD
		}
	}
	return maxMin
}

// FrechetDistance returns the discrete Fréchet distance (planar) between the
// vertex sequences of two geometries, via the Eiter-Mannila dynamic program.
func FrechetDistance(a, b Geometry) (float64, bool) {
	pa, pb := coordsOf(a), coordsOf(b)
	n, m := len(pa), len(pb)
	if n == 0 || m == 0 {
		return 0, false
	}
	ca := make([][]float64, n)
	for i := range ca {
		ca[i] = make([]float64, m)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < m; j++ {
			d := math.Hypot(pa[i].X-pb[j].X, pa[i].Y-pb[j].Y)
			switch {
			case i == 0 && j == 0:
				ca[i][j] = d
			case i == 0:
				ca[i][j] = math.Max(ca[0][j-1], d)
			case j == 0:
				ca[i][j] = math.Max(ca[i-1][0], d)
			default:
				prev := math.Min(ca[i-1][j], math.Min(ca[i-1][j-1], ca[i][j-1]))
				ca[i][j] = math.Max(prev, d)
			}
		}
	}
	return ca[n-1][m-1], true
}
