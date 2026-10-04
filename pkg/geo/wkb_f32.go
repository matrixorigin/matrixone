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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// This file holds the float32-coordinate WKB variant used by the GEOMETRY32
// type. The byte structure is identical to standard WKB (byte order, type
// codes, element counts) except each ordinate occupies 4 bytes (float32)
// instead of 8 (float64). The recursive codec is shared with the float64 path
// (wkb_read.go / wkb_write.go) via the internal f32 flag; this file only adds
// the public float32 API and the up-convert helper.
//
// As with float64 WKB, no SRID or format header is written: the SQL layer
// selects this codec based on the column OID (T_geometry32), and SRID lives in
// the column type.

// WriteWKBFloat32 encodes a Geometry with float32 coordinates after checking
// every non-empty ordinate. A finite float64 can round to +/-Inf when narrowed
// to float32; emitting that value would create a payload that geometry readers
// reject. Empty points are the one intentional NaN encoding and bypass the
// ordinate check.
func WriteWKBFloat32(g Geometry) ([]byte, error) {
	if err := validateFloat32Geometry(g, 1); err != nil {
		return nil, err
	}
	return appendGeom(nil, g, true), nil
}

func validateFloat32Geometry(g Geometry, depth int) error {
	if g == nil {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	validateCoord := func(c Coord) error {
		if err := validateFloat32Ordinate(c.X); err != nil {
			return err
		}
		return validateFloat32Ordinate(c.Y)
	}

	switch v := g.(type) {
	case Point:
		if v.IsEmpty {
			return nil
		}
		return validateCoord(v.Coord())
	case LineString:
		for _, c := range v.Points {
			if err := validateCoord(c); err != nil {
				return err
			}
		}
	case Polygon:
		for _, ring := range v.Rings {
			for _, c := range ring {
				if err := validateCoord(c); err != nil {
					return err
				}
			}
		}
	case MultiPoint:
		for _, p := range v.Points {
			if p.IsEmpty {
				continue
			}
			if err := validateCoord(p.Coord()); err != nil {
				return err
			}
		}
	case MultiLineString:
		for _, line := range v.Lines {
			if err := validateFloat32Geometry(line, depth); err != nil {
				return err
			}
		}
	case MultiPolygon:
		for _, polygon := range v.Polygons {
			if err := validateFloat32Geometry(polygon, depth); err != nil {
				return err
			}
		}
	case GeometryCollection:
		if depth > maxGeometryNestingDepth {
			return moerr.NewInvalidInputNoCtxf("geometry collection nesting depth exceeds %d", maxGeometryNestingDepth)
		}
		for _, child := range v.Geometries {
			if err := validateFloat32Geometry(child, depth+1); err != nil {
				return err
			}
		}
	default:
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	return nil
}

func validateFloat32Ordinate(value float64) error {
	narrowed := float32(value)
	if math.IsNaN(value) || math.IsInf(value, 0) || math.IsInf(float64(narrowed), 0) || math.IsNaN(float64(narrowed)) {
		return moerr.NewInvalidInputNoCtx("geometry coordinate is not finite in GEOMETRY32")
	}
	return nil
}

// ReadWKBFloat32 decodes float32-coordinate WKB (the GEOMETRY32 storage form)
// into a Geometry whose coordinates are the float32 values widened to float64.
func ReadWKBFloat32(b []byte) (Geometry, error) {
	return readWKB(b, true)
}

// WKBFloat32ToStandard converts a float32-coordinate WKB payload into a standard
// float64 WKB payload. ST_AsWKB / ST_AsBinary use this so a GEOMETRY32 value is
// always emitted as interoperable standard WKB.
func WKBFloat32ToStandard(b []byte) ([]byte, error) {
	g, err := ReadWKBFloat32(b)
	if err != nil {
		return nil, err
	}
	return WriteWKB(g), nil
}
