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

func writeSelectedRowsInt32(w io.Writer, value int32, encoded *[4]byte) error {
	binary.LittleEndian.PutUint32(encoded[:], uint32(value))
	return types.WriteFull(w, encoded[:])
}

func writeSelectedRowsByte(w io.Writer, value byte, encoded *[4]byte) error {
	if typed, ok := w.(vectorPrimitiveWriter); ok {
		return typed.WriteByte(value)
	}
	encoded[0] = value
	return types.WriteFull(w, encoded[:1])
}

// selectedRowsHasNull is the only metadata bit: in the header it says the
// selection has a NULL, so every row carries a one-byte flag; in a row flag it
// marks that row NULL. The codec only backs query-scoped spill, which the same
// process writes and reads, so there is no older writer to accept.
const selectedRowsHasNull = byte(1 << 0)

// selectedFixedRowsWriter lets an execution-owned buffered writer gather a
// sparse fixed-width selection without paying one io.Writer call per value.
// The ordinary io.Writer path remains the wire-format reference and fallback.
type selectedFixedRowsWriter interface {
	WriteSelectedFixedRows(data []byte, width int, rows []int32) (int, error)
}

type selectedVarlenaRowsWriter interface {
	WriteSelectedVarlenaRows(values []types.Varlena, area []byte, rows []int32) (int, error)
}

// MarshalSelectedRowsTo writes a bounded, private execution codec for the
// selected rows. Unlike MarshalBinaryTo it does not first materialize a
// selection Vector, which lets spill make progress when retained state has
// reached its allocation-account capacity.
func (v *Vector) MarshalSelectedRowsTo(w io.Writer, rows []int32) error {
	return v.marshalSelectedRowsTo(w, len(rows), rows, func(i int) int {
		return int(rows[i])
	})
}

// MarshalRowRangeTo writes one contiguous half-open row range without first
// allocating a row-index slice. Large spill batches use it to split records
// while keeping the split path bounded by the encoded output buffer alone.
func (v *Vector) MarshalRowRangeTo(w io.Writer, start, end int) error {
	if v == nil || start < 0 || end < start || end > v.Length() {
		return moerr.NewInvalidInputNoCtx("invalid selected vector row range")
	}
	return v.marshalSelectedRowsTo(w, end-start, nil, func(i int) int {
		return start + i
	})
}

func (v *Vector) marshalSelectedRowsTo(
	w io.Writer,
	count int,
	rows []int32,
	rowAt func(int) int,
) error {
	if v == nil || w == nil || count < 0 || count > math.MaxInt32 {
		return moerr.NewInvalidInputNoCtx("invalid selected vector rows")
	}
	isVarlen := v.typ.IsVarlen()
	// Reuse one framing word for counts, sizes and metadata bytes. In
	// particular, row flags must not allocate a fresh byte slice per row.
	var encoded [4]byte
	if err := writeSelectedRowsInt32(w, int32(count), &encoded); err != nil {
		return err
	}

	metadata := byte(0)
	if rows != nil && count == len(rows) && !v.IsConst() && v.nsp.IsEmpty() {
		// Every selected row has uniform metadata, but selection bounds still
		// need validation before passing rows to a specialized writer.
		for _, row := range rows {
			if row < 0 || int(row) >= v.length {
				return moerr.NewInvalidInputNoCtx("selected vector row out of range")
			}
		}
	} else {
		for i := 0; i < count; i++ {
			row := rowAt(i)
			if row < 0 || row >= v.length {
				return moerr.NewInvalidInputNoCtx("selected vector row out of range")
			}
			if v.IsNull(uint64(row)) {
				metadata |= selectedRowsHasNull
			}
		}
	}
	if err := writeSelectedRowsByte(w, metadata, &encoded); err != nil {
		return err
	}
	if !isVarlen {
		fixedWidth := v.typ.TypeSize()
		if fixedWidth < 0 || fixedWidth > math.MaxInt32 {
			return moerr.NewInvalidInputNoCtx("invalid selected vector fixed-width type")
		}
		if err := writeSelectedRowsInt32(w, int32(fixedWidth), &encoded); err != nil {
			return err
		}
	}

	withRowFlags := metadata&selectedRowsHasNull != 0
	if !v.IsConst() && isVarlen && !withRowFlags && rows != nil {
		if fastWriter, ok := w.(selectedVarlenaRowsWriter); ok {
			values := MustFixedColNoTypeCheck[types.Varlena](v)
			expected := 0
			for _, row := range rows {
				value := &values[row]
				length := int(value[0])
				if !value.IsSmall() {
					_, size := value.OffsetLen()
					length = int(size)
				}
				if length > math.MaxInt32 || expected > math.MaxInt-4-length {
					return moerr.NewInvalidInputNoCtx("selected vector value exceeds wire format")
				}
				expected += 4 + length
			}
			written, err := fastWriter.WriteSelectedVarlenaRows(values, v.area, rows)
			if err != nil {
				return err
			}
			if written != expected {
				return io.ErrShortWrite
			}
			goto metadataTrailers
		}
	}
	if !v.IsConst() && !isVarlen && !withRowFlags && rows != nil {
		fixedWidth := v.typ.TypeSize()
		if fixedWidth != 0 && count > math.MaxInt/fixedWidth {
			return moerr.NewInvalidInputNoCtx("selected vector value exceeds wire format")
		}
		if fastWriter, ok := w.(selectedFixedRowsWriter); ok {
			expected := count * fixedWidth
			written, err := fastWriter.WriteSelectedFixedRows(v.data, fixedWidth, rows)
			if err != nil {
				return err
			}
			if written != expected {
				return io.ErrShortWrite
			}
			goto metadataTrailers
		}
	}
	for i := 0; i < count; i++ {
		row := rowAt(i)
		nullValue := v.IsNull(uint64(row))
		if withRowFlags {
			rowFlags := byte(0)
			if nullValue {
				rowFlags |= selectedRowsHasNull
			}
			if err := writeSelectedRowsByte(w, rowFlags, &encoded); err != nil {
				return err
			}
		}
		if nullValue {
			continue
		}
		value := v.GetRawBytesAt(row)
		if isVarlen {
			if len(value) > math.MaxInt32 {
				return moerr.NewInvalidInputNoCtx("selected vector value exceeds wire format")
			}
			if err := writeSelectedRowsInt32(w, int32(len(value)), &encoded); err != nil {
				return err
			}
		}
		if err := types.WriteFull(w, value); err != nil {
			return err
		}
	}

metadataTrailers:
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
	isVarlen := v.typ.IsVarlen()
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
	if metadata&^selectedRowsHasNull != 0 {
		return moerr.NewInvalidInputNoCtx("invalid selected vector metadata")
	}
	fixedWidth := v.typ.TypeSize()
	if !isVarlen {
		encodedWidth, err := types.ReadInt32AsInt(r)
		if err != nil {
			return err
		}
		if encodedWidth != fixedWidth || fixedWidth < 0 ||
			(fixedWidth > 0 && count > math.MaxInt/fixedWidth) {
			return moerr.NewInvalidInputNoCtx("invalid selected vector value size")
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
	v.SetLength(count)
	withRowFlags := metadata&selectedRowsHasNull != 0
	if !isVarlen && !withRowFlags {
		// Fixed-width, non-null data is encoded as one dense byte stream. Decode
		// it in one operation instead of one length read and one value read per
		// row. PreExtend and SetLength above have already reserved and published
		// exactly this type-derived extent.
		valueBytes := count * fixedWidth
		if _, err = io.ReadFull(r, v.data[:valueBytes]); err != nil {
			return err
		}
	} else {
		// Passing a local array through io.Reader makes it escape to the heap.
		// Reuse one header scratch instead of allocating one for every row.
		var encoded [4]byte
		for row := 0; row < count; row++ {
			rowFlags := byte(0)
			if withRowFlags {
				if _, err = io.ReadFull(r, encoded[:1]); err != nil {
					return err
				}
				rowFlags = encoded[0]
				if rowFlags&^selectedRowsHasNull != 0 {
					return moerr.NewInvalidInputNoCtx("invalid selected vector row metadata")
				}
			}
			if rowFlags&selectedRowsHasNull != 0 {
				v.SetNull(uint64(row))
				continue
			}
			valueSize := fixedWidth
			if isVarlen {
				if _, err = io.ReadFull(r, encoded[:]); err != nil {
					return err
				}
				valueSize = int(int32(binary.LittleEndian.Uint32(encoded[:])))
				if valueSize < 0 {
					return moerr.NewInvalidInputNoCtx("invalid selected vector value size")
				}
			}
			if err := v.readRawBytesAt(r, row, valueSize, mp); err != nil {
				return err
			}
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
	// UnmarshalSelectedRowsFrom has admitted owned descriptor storage for all
	// rows. Area growth cannot move it; read inline bytes there directly instead
	// of passing a temporary descriptor through io.Reader on every row.
	value := &MustFixedColNoTypeCheck[types.Varlena](v)[row]
	if size <= types.VarlenaInlineSize {
		*value = types.Varlena{}
		value[0] = byte(size)
		_, err := io.ReadFull(r, value[1:1+size])
		return err
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
	*value = types.Varlena{}
	value.SetOffsetLen(uint32(oldAreaLength), uint32(size))
	v.area = area
	return nil
}
