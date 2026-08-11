// Copyright 2026 Matrix Origin
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

package vector

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func writeSelectedRowsInt32(w io.Writer, value int32) error {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], uint32(value))
	return writeVectorMarshalBytes(w, encoded[:])
}

const (
	selectedRowsHasNull     = byte(1 << 0)
	selectedRowsHasGrouping = byte(1 << 1)
	selectedRowsKindShift   = 2
	selectedRowsKindMask    = byte(3 << selectedRowsKindShift)

	selectedRowsKindNone    = byte(0)
	selectedRowsKindUniform = byte(1)
	selectedRowsKindRows    = byte(2)
)

// MarshalSelectedRowsTo writes a bounded, private execution codec for the
// selected rows. Unlike MarshalBinaryTo it does not first materialize a
// selection Vector, which lets spill make progress when retained state has
// reached its allocation-account capacity.
func (v *Vector) MarshalSelectedRowsTo(w io.Writer, rows []int32) error {
	return v.marshalSelectedRowsTo(w, len(rows), func(i int) int {
		return int(rows[i])
	})
}

// MarshalSelectedFlagsTo is the flag-selection form used by aggregate state.
// It returns the encoded row count so callers can validate their record plan.
func (v *Vector) MarshalSelectedFlagsTo(w io.Writer, flags []uint8) (int, error) {
	count := 0
	for _, selected := range flags {
		if selected != 0 {
			count++
		}
	}
	next := 0
	lastRequest := -1
	err := v.marshalSelectedRowsTo(w, count, func(i int) int {
		// marshalSelectedRowsTo makes multiple ordered passes over the selected
		// rows (metadata, values, and optionally parameter kinds). Reset the
		// cursor at the start of each pass without materializing row indexes.
		if i <= lastRequest {
			next = 0
		}
		for next < len(flags) && flags[next] == 0 {
			next++
		}
		row := next
		next++
		lastRequest = i
		return row
	})
	return count, err
}

func (v *Vector) marshalSelectedRowsTo(
	w io.Writer,
	count int,
	rowAt func(int) int,
) error {
	if v == nil || w == nil || count < 0 || count > math.MaxInt32 {
		return moerr.NewInvalidInputNoCtx("invalid selected vector rows")
	}
	if err := writeSelectedRowsInt32(w, int32(count)); err != nil {
		return err
	}

	metadata := byte(0)
	var firstKind PrepareParamKind
	kindSeen := false
	kindMixed := false
	for i := 0; i < count; i++ {
		row := rowAt(i)
		if row < 0 || row >= v.length {
			return moerr.NewInvalidInputNoCtx("selected vector row out of range")
		}
		if v.IsNull(uint64(row)) {
			metadata |= selectedRowsHasNull
		} else if v.prepareParamKindSeen || len(v.prepareParamKinds) != 0 {
			kind := v.GetPrepareParamKindAt(row)
			if !kindSeen {
				firstKind, kindSeen = kind, true
			} else if kind != firstKind {
				kindMixed = true
			}
		}
		if v.gsp.Contains(uint64(row)) {
			metadata |= selectedRowsHasGrouping
		}
	}
	kindMode := selectedRowsKindNone
	if kindSeen {
		kindMode = selectedRowsKindUniform
		if kindMixed {
			kindMode = selectedRowsKindRows
		}
		metadata |= kindMode << selectedRowsKindShift
	}
	if err := writeVectorMarshalByte(w, metadata); err != nil {
		return err
	}
	if kindMode == selectedRowsKindUniform {
		if err := writeVectorMarshalByte(w, byte(firstKind)); err != nil {
			return err
		}
	}

	withRowFlags := metadata&(selectedRowsHasNull|selectedRowsHasGrouping) != 0
	for i := 0; i < count; i++ {
		row := rowAt(i)
		nullValue := v.IsNull(uint64(row))
		if withRowFlags {
			rowFlags := byte(0)
			if nullValue {
				rowFlags |= selectedRowsHasNull
			}
			if v.gsp.Contains(uint64(row)) {
				rowFlags |= selectedRowsHasGrouping
			}
			if err := writeVectorMarshalByte(w, rowFlags); err != nil {
				return err
			}
		}
		if nullValue {
			continue
		}
		value := v.GetRawBytesAt(row)
		if len(value) > math.MaxInt32 {
			return moerr.NewInvalidInputNoCtx("selected vector value exceeds wire format")
		}
		if err := writeSelectedRowsInt32(w, int32(len(value))); err != nil {
			return err
		}
		if err := writeVectorMarshalBytes(w, value); err != nil {
			return err
		}
	}
	if kindMode == selectedRowsKindRows {
		for i := 0; i < count; i++ {
			if err := writeVectorMarshalByte(w, byte(v.GetPrepareParamKindAt(rowAt(i)))); err != nil {
				return err
			}
		}
	}
	return nil
}

// UnmarshalSelectedRowsFrom restores rows written by MarshalSelectedRowsTo
// into an already typed Vector. All data-scaled storage is allocated through
// the Vector's immutable allocation selection.
func (v *Vector) UnmarshalSelectedRowsFrom(
	r io.Reader,
	expected int,
	mp *mpool.MPool,
) (retErr error) {
	if v == nil || r == nil || mp == nil || expected < 0 {
		return moerr.NewInvalidInputNoCtx("invalid selected vector decoder")
	}
	if v.IsConst() {
		return moerr.NewInvalidInputNoCtx(
			"selected vector decoder requires a non-constant destination")
	}
	count, err := types.ReadInt32AsInt(r)
	if err != nil {
		return err
	}
	if count != expected {
		return moerr.NewInvalidInputNoCtxf(
			"selected vector row count %d does not match %d", count, expected)
	}
	metadata, err := types.ReadByte(r)
	if err != nil {
		return err
	}
	kindMode := (metadata & selectedRowsKindMask) >> selectedRowsKindShift
	if metadata&^(selectedRowsHasNull|selectedRowsHasGrouping|selectedRowsKindMask) != 0 ||
		kindMode > selectedRowsKindRows {
		return moerr.NewInvalidInputNoCtx("invalid selected vector metadata")
	}
	var uniformKind PrepareParamKind
	if kindMode == selectedRowsKindUniform {
		encoded, err := types.ReadByte(r)
		if err != nil {
			return err
		}
		uniformKind = PrepareParamKind(encoded)
		if uniformKind > PrepareParamBoolean {
			return moerr.NewInvalidInputNoCtx("invalid selected vector parameter kind")
		}
	}

	v.CleanOnlyData()
	// Once decoding starts, an error must not publish a partially restored
	// vector. Physical capacity remains reusable and is still released by the
	// vector's ordinary owner.
	defer func() {
		if retErr != nil {
			v.CleanOnlyData()
		}
	}()
	if err := v.PreExtend(count, mp); err != nil {
		return err
	}
	if metadata&selectedRowsHasNull != 0 {
		if err := v.PreExtendNulls(count, mp); err != nil {
			return err
		}
	}
	if metadata&selectedRowsHasGrouping != 0 {
		if err := v.PreExtendGrouping(count, mp); err != nil {
			return err
		}
	}
	v.SetLength(count)
	withRowFlags := metadata&(selectedRowsHasNull|selectedRowsHasGrouping) != 0
	for row := 0; row < count; row++ {
		rowFlags := byte(0)
		if withRowFlags {
			rowFlags, err = types.ReadByte(r)
			if err != nil {
				return err
			}
			if rowFlags&^(selectedRowsHasNull|selectedRowsHasGrouping) != 0 ||
				rowFlags&selectedRowsHasNull != 0 && metadata&selectedRowsHasNull == 0 ||
				rowFlags&selectedRowsHasGrouping != 0 && metadata&selectedRowsHasGrouping == 0 {
				return moerr.NewInvalidInputNoCtx("invalid selected vector row metadata")
			}
		}
		if rowFlags&selectedRowsHasGrouping != 0 {
			v.gsp.Set(uint64(row))
		}
		if rowFlags&selectedRowsHasNull != 0 {
			v.SetNull(uint64(row))
			continue
		}
		valueSize, err := types.ReadInt32AsInt(r)
		if err != nil {
			return err
		}
		if valueSize < 0 || !v.typ.IsVarlen() && valueSize != v.typ.TypeSize() {
			return moerr.NewInvalidInputNoCtx("invalid selected vector value size")
		}
		if err := v.readRawBytesAt(r, row, valueSize, mp); err != nil {
			return err
		}
	}
	switch kindMode {
	case selectedRowsKindUniform:
		v.SetPrepareParamKind(uniformKind)
	case selectedRowsKindRows:
		if err := v.SetPrepareParamKindsFromReader(r, count, mp); err != nil {
			return err
		}
	}
	return nil
}

func (v *Vector) readRawBytesAt(
	r io.Reader,
	row int,
	size int,
	mp *mpool.MPool,
) error {
	if !v.typ.IsVarlen() {
		start := row * v.typ.TypeSize()
		_, err := io.ReadFull(r, v.data[start:start+size])
		return err
	}
	if uint64(size) > math.MaxUint32 {
		return moerr.NewInvalidInputNoCtx("selected vector value exceeds varlena format")
	}
	var value types.Varlena
	if size <= types.VarlenaInlineSize {
		value[0] = byte(size)
		if _, err := io.ReadFull(r, value[1:1+size]); err != nil {
			return err
		}
		return SetFixedAtWithTypeCheck(v, row, value)
	}
	oldAreaLength := len(v.area)
	if uint64(oldAreaLength)+uint64(size) > math.MaxUint32 ||
		uint64(oldAreaLength)+uint64(size) > uint64(math.MaxInt) {
		return moerr.NewInvalidInputNoCtx("selected vector area exceeds varlena format")
	}
	newAreaLength := oldAreaLength + size
	area, err := v.growArea(mp, newAreaLength)
	if err != nil {
		return err
	}
	if _, err = io.ReadFull(r, area[oldAreaLength:newAreaLength]); err != nil {
		v.area = area[:oldAreaLength]
		return err
	}
	value.SetOffsetLen(uint32(oldAreaLength), uint32(size))
	if err = SetFixedAtWithTypeCheck(v, row, value); err != nil {
		v.area = area[:oldAreaLength]
		return err
	}
	v.area = area
	v.areaDisjoint = true
	return nil
}
