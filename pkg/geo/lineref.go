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

// planarLineLength returns the total Cartesian length of a coordinate chain.
func planarLineLength(pts []Coord) float64 {
	total := 0.0
	for i := 1; i < len(pts); i++ {
		total += math.Hypot(pts[i].X-pts[i-1].X, pts[i].Y-pts[i-1].Y)
	}
	return total
}

// pointAtLength returns the coordinate reached after travelling target Cartesian
// units along the chain. target is clamped to [0, totalLength].
func pointAtLength(pts []Coord, target float64) Point {
	if target <= 0 {
		return Point{X: pts[0].X, Y: pts[0].Y}
	}
	acc := 0.0
	for i := 1; i < len(pts); i++ {
		seg := math.Hypot(pts[i].X-pts[i-1].X, pts[i].Y-pts[i-1].Y)
		if acc+seg >= target {
			t := 0.0
			if seg > 0 {
				t = (target - acc) / seg
			}
			return Point{
				X: pts[i-1].X + t*(pts[i].X-pts[i-1].X),
				Y: pts[i-1].Y + t*(pts[i].Y-pts[i-1].Y),
			}
		}
		acc += seg
	}
	last := pts[len(pts)-1]
	return Point{X: last.X, Y: last.Y}
}

// InterpolatePoint returns the point at fraction f (0..1) of a line's total
// Cartesian length. Values outside [0,1] are clamped.
func InterpolatePoint(l LineString, f float64) (Point, error) {
	n := len(l.Points)
	if n == 0 {
		return Point{}, errors.New("ST_LineInterpolatePoint: empty line")
	}
	if n == 1 {
		return Point{X: l.Points[0].X, Y: l.Points[0].Y}, nil
	}
	if f < 0 {
		f = 0
	}
	if f > 1 {
		f = 1
	}
	return pointAtLength(l.Points, f*planarLineLength(l.Points)), nil
}

// InterpolatePoints returns points placed at every multiple of fraction f along
// the line (always including the 100% endpoint). A single resulting point is
// returned as a Point, otherwise as a MultiPoint.
func InterpolatePoints(l LineString, f float64) (Geometry, error) {
	if f <= 0 || f > 1 {
		return nil, errors.New("ST_LineInterpolatePoints: fraction must be in (0, 1]")
	}
	var pts []Point
	for k := 1; ; k++ {
		frac := float64(k) * f
		if frac >= 1.0-1e-12 {
			p, err := InterpolatePoint(l, 1.0)
			if err != nil {
				return nil, err
			}
			pts = append(pts, p)
			break
		}
		p, err := InterpolatePoint(l, frac)
		if err != nil {
			return nil, err
		}
		pts = append(pts, p)
	}
	if len(pts) == 1 {
		return pts[0], nil
	}
	return MultiPoint{Points: pts}, nil
}

// PointAtDistance returns the point reached after travelling dist Cartesian
// units along the line. dist must lie within [0, length].
func PointAtDistance(l LineString, dist float64) (Point, error) {
	n := len(l.Points)
	if n == 0 {
		return Point{}, errors.New("ST_PointAtDistance: empty line")
	}
	total := planarLineLength(l.Points)
	if dist < 0 || dist > total {
		return Point{}, errors.New("ST_PointAtDistance: distance is out of range")
	}
	return pointAtLength(l.Points, dist), nil
}
