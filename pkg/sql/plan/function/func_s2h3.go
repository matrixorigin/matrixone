// Copyright 2021 - 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"math"

	"github.com/golang/geo/s2"
	h3 "github.com/uber/h3-go/v4"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Implementation of the S2_* and H3_* spatial cell functions. See
// docs/design/s2h3_funcs.md for the catalog and semantics.
//
// Conventions shared by every function here:
//   - A POINT argument carries (longitude, latitude): X is longitude, Y is
//     latitude, matching ST_GeomFromText('POINT(lng lat)'). The geo decoder
//     returns (x, y), so x is lng and y is lat.
//   - A CellId (S2) and an H3Index (H3) are both BIGINT UNSIGNED (uint64). S2
//     stores its CellID as a uint64 directly; h3-go's Cell is an int64 whose
//     bit pattern is the H3 index, so we round-trip through uint64<->int64.
//   - Neighbour lists are returned as a JSON array of uint64. JSON preserves
//     the full 64-bit value (bytejson has a native uint64 type code); encoding
//     as a JSON float would lose precision for these large ids.

// earthRadiusMeters is the mean Earth radius used to turn S2's unit-sphere area
// (steradians) into square metres for S2_CellId_Area. It matches the radius H3
// uses internally so the two engines report comparable areas.
const earthRadiusMeters = 6371008.8

// h3DefaultResolution is the resolution used by the single-argument
// H3_H3Index(POINT) overload. 15 is the finest H3 resolution, the closest
// analogue to S2's leaf cell (which a point maps to uniquely).
const h3DefaultResolution = 15

// s2h3RowSkipped reports whether row i must be skipped (emitted as NULL) given
// the function's select list, following the same convention as the other
// builtins in this package.
func s2h3RowSkipped(selectList *FunctionSelectList, i uint64) bool {
	return selectList != nil && (selectList.IgnoreAllRow() ||
		(!selectList.ShouldEvalAllRow() && selectList.Contains(i)))
}

// s2h3CellToFixed evaluates a unary function whose input is a uint64 cell id and
// whose output is a fixed-size scalar (uint64 / int32 / float64 / bool).
func s2h3CellToFixed[T types.FixedSizeTExceptStrType](
	ivecs []*vector.Vector, result vector.FunctionResultWrapper, length int,
	selectList *FunctionSelectList, fn func(uint64) (T, error)) error {
	src := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
	rs := vector.MustFunctionResult[T](result)
	var zero T
	for i := uint64(0); i < uint64(length); i++ {
		if s2h3RowSkipped(selectList, i) {
			if err := rs.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		v, null := src.GetValue(i)
		if null {
			if err := rs.Append(zero, true); err != nil {
				return err
			}
			continue
		}
		r, err := fn(v)
		if err != nil {
			return err
		}
		if err := rs.Append(r, false); err != nil {
			return err
		}
	}
	return nil
}

// s2h3CellIntToU64 evaluates a (uint64 cell, int64) -> uint64 function, used for
// the parent-at-level overloads.
func s2h3CellIntToU64(
	ivecs []*vector.Vector, result vector.FunctionResultWrapper, length int,
	selectList *FunctionSelectList, fn func(uint64, int64) (uint64, error)) error {
	cells := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
	levels := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[uint64](result)
	for i := uint64(0); i < uint64(length); i++ {
		if s2h3RowSkipped(selectList, i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		c, n1 := cells.GetValue(i)
		l, n2 := levels.GetValue(i)
		if n1 || n2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		r, err := fn(c, l)
		if err != nil {
			return err
		}
		if err := rs.Append(r, false); err != nil {
			return err
		}
	}
	return nil
}

// s2h3BinCellToBool evaluates a (uint64 cell, uint64 cell) -> bool function, used
// for the are-neighbours predicates.
func s2h3BinCellToBool(
	ivecs []*vector.Vector, result vector.FunctionResultWrapper, length int,
	selectList *FunctionSelectList, fn func(uint64, uint64) (bool, error)) error {
	a := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
	b := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[1])
	rs := vector.MustFunctionResult[bool](result)
	for i := uint64(0); i < uint64(length); i++ {
		if s2h3RowSkipped(selectList, i) {
			if err := rs.Append(false, true); err != nil {
				return err
			}
			continue
		}
		av, n1 := a.GetValue(i)
		bv, n2 := b.GetValue(i)
		if n1 || n2 {
			if err := rs.Append(false, true); err != nil {
				return err
			}
			continue
		}
		r, err := fn(av, bv)
		if err != nil {
			return err
		}
		if err := rs.Append(r, false); err != nil {
			return err
		}
	}
	return nil
}

// s2h3CellToGeometry evaluates a unary uint64-cell -> GEOMETRY (bare WKB)
// function (the *_Center / *_Boundary functions).
func s2h3CellToGeometry(
	ivecs []*vector.Vector, result vector.FunctionResultWrapper, length int,
	selectList *FunctionSelectList, fn func(uint64) ([]byte, error)) error {
	src := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if s2h3RowSkipped(selectList, i) {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		v, null := src.GetValue(i)
		if null {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		wkb, err := fn(v)
		if err != nil {
			return err
		}
		if err := rs.AppendBytes(wkb, false); err != nil {
			return err
		}
	}
	return nil
}

// s2h3CellToJSON evaluates a unary uint64-cell -> JSON function (the neighbour
// lists).
func s2h3CellToJSON(
	ivecs []*vector.Vector, result vector.FunctionResultWrapper, length int,
	selectList *FunctionSelectList, fn func(uint64) (bytejson.ByteJson, error)) error {
	src := vector.GenerateFunctionFixedTypeParameter[uint64](ivecs[0])
	rs := vector.MustFunctionResult[types.Varlena](result)
	for i := uint64(0); i < uint64(length); i++ {
		if s2h3RowSkipped(selectList, i) {
			if err := rs.AppendByteJson(bytejson.Null, true); err != nil {
				return err
			}
			continue
		}
		v, null := src.GetValue(i)
		if null {
			if err := rs.AppendByteJson(bytejson.Null, true); err != nil {
				return err
			}
			continue
		}
		bj, err := fn(v)
		if err != nil {
			return err
		}
		if err := rs.AppendByteJson(bj, false); err != nil {
			return err
		}
	}
	return nil
}

// s2h3PointToU64 evaluates a unary POINT -> uint64 function (the indexing
// constructors), reading the point as (longitude, latitude).
func s2h3PointToU64(
	ivecs []*vector.Vector, result vector.FunctionResultWrapper, length int,
	selectList *FunctionSelectList, fn func(lng, lat float64) (uint64, error)) error {
	src := vector.GenerateFunctionStrParameter(ivecs[0])
	rs := vector.MustFunctionResult[uint64](result)
	for i := uint64(0); i < uint64(length); i++ {
		if s2h3RowSkipped(selectList, i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		v, null := src.GetStrValue(i)
		if null {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		lng, lat, err := parsePointXYFromPayload(v)
		if err != nil {
			return err
		}
		r, err := fn(lng, lat)
		if err != nil {
			return err
		}
		if err := rs.Append(r, false); err != nil {
			return err
		}
	}
	return nil
}

// uint64ListToJSON encodes a slice of uint64 ids as a JSON array, preserving the
// full 64-bit value of each id.
func uint64ListToJSON(ids []uint64) (bytejson.ByteJson, error) {
	arr := make([]any, len(ids))
	for i, id := range ids {
		arr[i] = id
	}
	return bytejson.CreateByteJSON(arr)
}

func validLatLng(lat, lng float64) error {
	// Guard NaN/Inf explicitly: a NaN comparison is always false, so the range
	// checks below would let it through. POINT input already rejects non-finite
	// coordinates upstream, but this keeps the choke point safe on its own.
	if math.IsNaN(lat) || math.IsNaN(lng) || math.IsInf(lat, 0) || math.IsInf(lng, 0) {
		return moerr.NewInvalidInputNoCtxf("longitude/latitude is not finite: (%v, %v)", lng, lat)
	}
	if lat < -90 || lat > 90 || lng < -180 || lng > 180 {
		return moerr.NewInvalidInputNoCtxf("longitude/latitude out of range: (%v, %v)", lng, lat)
	}
	return nil
}

// ---------------------------------------------------------------------------
// S2 functions
// ---------------------------------------------------------------------------

func s2RequireValid(c uint64) (s2.CellID, error) {
	cid := s2.CellID(c)
	if !cid.IsValid() {
		return 0, moerr.NewInvalidInputNoCtxf("invalid S2 CellId: %d", c)
	}
	return cid, nil
}

// S2CellId converts a POINT(longitude latitude) to the S2 leaf CellId.
func S2CellId(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3PointToU64(ivecs, result, length, selectList, func(lng, lat float64) (uint64, error) {
		if err := validLatLng(lat, lng); err != nil {
			return 0, err
		}
		return uint64(s2.CellIDFromLatLng(s2.LatLngFromDegrees(lat, lng))), nil
	})
}

// S2CellIdLevel returns the level (0..30) of a CellId.
func S2CellIdLevel(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellToFixed[int32](ivecs, result, length, selectList, func(c uint64) (int32, error) {
		cid, err := s2RequireValid(c)
		if err != nil {
			return 0, err
		}
		return int32(cid.Level()), nil
	})
}

// S2CellIdCenter returns the center of a CellId as a POINT(longitude latitude).
func S2CellIdCenter(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellToGeometry(ivecs, result, length, selectList, func(c uint64) ([]byte, error) {
		cid, err := s2RequireValid(c)
		if err != nil {
			return nil, err
		}
		ll := cid.LatLng()
		return geo.WriteWKB(geo.Point{X: ll.Lng.Degrees(), Y: ll.Lat.Degrees()}), nil
	})
}

// S2CellIdArea returns the approximate area of a CellId in square metres.
func S2CellIdArea(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellToFixed[float64](ivecs, result, length, selectList, func(c uint64) (float64, error) {
		cid, err := s2RequireValid(c)
		if err != nil {
			return 0, err
		}
		// ApproxArea is on the unit sphere (steradians); scale to m^2.
		return s2.CellFromCellID(cid).ApproxArea() * earthRadiusMeters * earthRadiusMeters, nil
	})
}

// S2CellIdParent returns the ancestor CellId at the given (coarser) level.
func S2CellIdParent(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellIntToU64(ivecs, result, length, selectList, func(c uint64, level int64) (uint64, error) {
		cid, err := s2RequireValid(c)
		if err != nil {
			return 0, err
		}
		if level < 0 || level > 30 {
			return 0, moerr.NewInvalidInputNoCtxf("S2 level must be between 0 and 30, got %d", level)
		}
		if int(level) > cid.Level() {
			return 0, moerr.NewInvalidInputNoCtxf("parent level %d is finer than cell level %d", level, cid.Level())
		}
		return uint64(cid.Parent(int(level))), nil
	})
}

// S2CellIdEdgeNeighbors returns the 4 edge-adjacent neighbours at this cell's
// level as a JSON array of uint64.
func S2CellIdEdgeNeighbors(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellToJSON(ivecs, result, length, selectList, func(c uint64) (bytejson.ByteJson, error) {
		cid, err := s2RequireValid(c)
		if err != nil {
			return bytejson.Null, err
		}
		ns := cid.EdgeNeighbors()
		ids := make([]uint64, len(ns))
		for i, n := range ns {
			ids[i] = uint64(n)
		}
		return uint64ListToJSON(ids)
	})
}

// S2CellIdAllNeighbors returns all neighbours (edge + vertex adjacent) at this
// cell's level as a JSON array of uint64.
func S2CellIdAllNeighbors(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellToJSON(ivecs, result, length, selectList, func(c uint64) (bytejson.ByteJson, error) {
		cid, err := s2RequireValid(c)
		if err != nil {
			return bytejson.Null, err
		}
		ns := cid.AllNeighbors(cid.Level())
		ids := make([]uint64, len(ns))
		for i, n := range ns {
			ids[i] = uint64(n)
		}
		return uint64ListToJSON(ids)
	})
}

// S2CellIdAreNeighbors reports whether two CellIds are neighbours, evaluated at
// the level of the first cell.
func S2CellIdAreNeighbors(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3BinCellToBool(ivecs, result, length, selectList, func(a, b uint64) (bool, error) {
		ca, err := s2RequireValid(a)
		if err != nil {
			return false, err
		}
		cb, err := s2RequireValid(b)
		if err != nil {
			return false, err
		}
		for _, n := range ca.AllNeighbors(ca.Level()) {
			if n == cb {
				return true, nil
			}
		}
		return false, nil
	})
}

// ---------------------------------------------------------------------------
// H3 functions
// ---------------------------------------------------------------------------

func h3RequireValid(h uint64) (h3.Cell, error) {
	c := h3.Cell(int64(h))
	if !c.IsValid() {
		return c, moerr.NewInvalidInputNoCtxf("invalid H3Index: %d", h)
	}
	return c, nil
}

func h3IndexAt(lng, lat float64, res int) (uint64, error) {
	if err := validLatLng(lat, lng); err != nil {
		return 0, err
	}
	cell, err := h3.LatLngToCell(h3.NewLatLng(lat, lng), res)
	if err != nil {
		return 0, moerr.NewInvalidInputNoCtxf("H3 LatLngToCell failed: %v", err)
	}
	return uint64(cell), nil
}

// H3H3Index converts a POINT(longitude latitude) to an H3Index at the default
// resolution (15, the finest).
func H3H3Index(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3PointToU64(ivecs, result, length, selectList, func(lng, lat float64) (uint64, error) {
		return h3IndexAt(lng, lat, h3DefaultResolution)
	})
}

// H3H3IndexWithRes converts a POINT(longitude latitude) to an H3Index at the
// given resolution (0..15).
func H3H3IndexWithRes(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	src := vector.GenerateFunctionStrParameter(ivecs[0])
	resolutions := vector.GenerateFunctionFixedTypeParameter[int64](ivecs[1])
	rs := vector.MustFunctionResult[uint64](result)
	for i := uint64(0); i < uint64(length); i++ {
		if s2h3RowSkipped(selectList, i) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		v, n1 := src.GetStrValue(i)
		res, n2 := resolutions.GetValue(i)
		if n1 || n2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		if res < 0 || res > int64(h3.MaxResolution) {
			return moerr.NewInvalidInputNoCtxf("H3 resolution must be between 0 and %d, got %d", int64(h3.MaxResolution), res)
		}
		lng, lat, err := parsePointXYFromPayload(v)
		if err != nil {
			return err
		}
		idx, err := h3IndexAt(lng, lat, int(res))
		if err != nil {
			return err
		}
		if err := rs.Append(idx, false); err != nil {
			return err
		}
	}
	return nil
}

// H3H3IndexResolution returns the resolution (0..15) of an H3Index.
func H3H3IndexResolution(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellToFixed[int32](ivecs, result, length, selectList, func(h uint64) (int32, error) {
		c, err := h3RequireValid(h)
		if err != nil {
			return 0, err
		}
		return int32(c.Resolution()), nil
	})
}

// H3H3IndexCenter returns the center of an H3Index cell as a POINT(longitude latitude).
func H3H3IndexCenter(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellToGeometry(ivecs, result, length, selectList, func(h uint64) ([]byte, error) {
		c, err := h3RequireValid(h)
		if err != nil {
			return nil, err
		}
		ll, err := h3.CellToLatLng(c)
		if err != nil {
			return nil, moerr.NewInvalidInputNoCtxf("H3 CellToLatLng failed: %v", err)
		}
		return geo.WriteWKB(geo.Point{X: ll.Lng, Y: ll.Lat}), nil
	})
}

// H3H3IndexBoundary returns the boundary vertices of an H3Index cell as a
// MULTIPOINT of POINT(longitude latitude).
func H3H3IndexBoundary(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellToGeometry(ivecs, result, length, selectList, func(h uint64) ([]byte, error) {
		c, err := h3RequireValid(h)
		if err != nil {
			return nil, err
		}
		boundary, err := c.Boundary()
		if err != nil {
			return nil, moerr.NewInvalidInputNoCtxf("H3 Boundary failed: %v", err)
		}
		mp := geo.MultiPoint{Points: make([]geo.Point, len(boundary))}
		for i, ll := range boundary {
			mp.Points[i] = geo.Point{X: ll.Lng, Y: ll.Lat}
		}
		return geo.WriteWKB(mp), nil
	})
}

// H3H3IndexParent returns the immediate parent (one resolution coarser).
func H3H3IndexParent(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellToFixed[uint64](ivecs, result, length, selectList, func(h uint64) (uint64, error) {
		c, err := h3RequireValid(h)
		if err != nil {
			return 0, err
		}
		if c.Resolution() == 0 {
			return 0, moerr.NewInvalidInputNoCtxf("H3Index at resolution 0 has no parent")
		}
		p, err := c.ImmediateParent()
		if err != nil {
			return 0, moerr.NewInvalidInputNoCtxf("H3 parent failed: %v", err)
		}
		return uint64(p), nil
	})
}

// H3H3IndexParentAtRes returns the ancestor at the given (coarser) resolution.
func H3H3IndexParentAtRes(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellIntToU64(ivecs, result, length, selectList, func(h uint64, res int64) (uint64, error) {
		c, err := h3RequireValid(h)
		if err != nil {
			return 0, err
		}
		if res < 0 || res > int64(h3.MaxResolution) {
			return 0, moerr.NewInvalidInputNoCtxf("H3 resolution must be between 0 and %d, got %d", int64(h3.MaxResolution), res)
		}
		if int(res) > c.Resolution() {
			return 0, moerr.NewInvalidInputNoCtxf("parent resolution %d is finer than cell resolution %d", res, c.Resolution())
		}
		p, err := c.Parent(int(res))
		if err != nil {
			return 0, moerr.NewInvalidInputNoCtxf("H3 parent failed: %v", err)
		}
		return uint64(p), nil
	})
}

// H3H3IndexNeighbors returns the immediate neighbours (the grid disk of radius 1
// excluding the cell itself) as a JSON array of uint64.
func H3H3IndexNeighbors(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3CellToJSON(ivecs, result, length, selectList, func(h uint64) (bytejson.ByteJson, error) {
		c, err := h3RequireValid(h)
		if err != nil {
			return bytejson.Null, err
		}
		disk, err := c.GridDisk(1)
		if err != nil {
			return bytejson.Null, moerr.NewInvalidInputNoCtxf("H3 GridDisk failed: %v", err)
		}
		ids := make([]uint64, 0, len(disk))
		for _, n := range disk {
			if n == c {
				continue
			}
			ids = append(ids, uint64(n))
		}
		return uint64ListToJSON(ids)
	})
}

// H3H3IndexAreNeighbors reports whether two H3Index cells are neighbours.
func H3H3IndexAreNeighbors(ivecs []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return s2h3BinCellToBool(ivecs, result, length, selectList, func(a, b uint64) (bool, error) {
		ca, err := h3RequireValid(a)
		if err != nil {
			return false, err
		}
		cb, err := h3RequireValid(b)
		if err != nil {
			return false, err
		}
		// Adjacency is only defined between cells at the same resolution. h3-go's
		// IsNeighbor returns a resolution-mismatch error for two valid cells at
		// different resolutions; match the documented "evaluated at the first
		// cell's resolution" semantics (and the S2 cross-level behavior) by
		// reporting cross-resolution pairs as not-neighbours rather than erroring.
		if ca.Resolution() != cb.Resolution() {
			return false, nil
		}
		ok, err := ca.IsNeighbor(cb)
		if err != nil {
			return false, moerr.NewInvalidInputNoCtxf("H3 IsNeighbor failed: %v", err)
		}
		return ok, nil
	})
}
