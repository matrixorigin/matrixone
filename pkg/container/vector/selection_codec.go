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
	return writeVectorMarshalBytes(w, encoded[:])
}

const (
	selectedRowsHasNull     = byte(1 << 0)
	selectedRowsHasGrouping = byte(1 << 1)
	selectedRowsKindShift   = 2
	selectedRowsKindMask    = byte(3 << selectedRowsKindShift)
	selectedRowsBinaryShift = 4
	selectedRowsBinaryMask  = byte(3 << selectedRowsBinaryShift)
	selectedRowsSourceShift = 6
	selectedRowsSourceMask  = byte(3 << selectedRowsSourceShift)

	selectedRowsRowBinary = byte(1 << 2)
	selectedRowsRowText   = byte(1 << 3)

	selectedRowsKindNone    = byte(0)
	selectedRowsKindUniform = byte(1)
	selectedRowsKindRows    = byte(2)

	selectedRowsBinaryNone    = byte(0)
	selectedRowsBinaryUniform = byte(1)
	selectedRowsBinaryRows    = byte(2)
	selectedRowsBinaryText    = byte(3)

	selectedRowsSourceNone    = byte(0)
	selectedRowsSourceUniform = byte(1)
	selectedRowsSourceRows    = byte(2)
)

// selectedFixedRowsWriter lets an execution-owned buffered writer gather a
// sparse fixed-width selection without paying one io.Writer call per value.
// The ordinary io.Writer path remains the wire-format reference and fallback.
type selectedFixedRowsWriter interface {
	WriteSelectedFixedRows(data []byte, width int, rows []int32) (int, error)
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
	err := v.marshalSelectedRowsTo(w, count, nil, func(i int) int {
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
	rows []int32,
	rowAt func(int) int,
) error {
	if v == nil || w == nil || count < 0 || count > math.MaxInt32 {
		return moerr.NewInvalidInputNoCtx("invalid selected vector rows")
	}
	isVarlen := v.typ.IsVarlen()
	// Reuse one framing word for the row count, the fixed width recorded once
	// per vector, and variable-length value sizes. Repeating a fixed width for
	// every row only adds bytes and codec work.
	var encodedInt32 [4]byte
	if err := writeSelectedRowsInt32(w, int32(count), &encodedInt32); err != nil {
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
		} else {
			if v.prepareParamKindSeen || len(v.prepareParamKinds) != 0 {
				kind := v.GetPrepareParamKindAt(row)
				if !kindSeen {
					firstKind, kindSeen = kind, true
				} else if kind != firstKind {
					kindMixed = true
				}
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
	var firstDomain types.RuntimeStringDomain
	binarySeen := false
	binaryMixed := false
	if v.HasBinaryStringMetadata() {
		// Dynamic binary provenance is uncommon. Keep its extra scan entirely
		// off the ordinary spill path instead of adding work to every row in the
		// mandatory NULL/grouping/parameter-kind pass above.
		for i := 0; i < count; i++ {
			row := rowAt(i)
			if v.IsNull(uint64(row)) {
				continue
			}
			domain := v.GetRuntimeStringDomainAt(row)
			if !binarySeen {
				firstDomain, binarySeen = domain, true
			} else if domain != firstDomain {
				binaryMixed = true
			}
		}
	}
	binaryMode := selectedRowsBinaryNone
	if binarySeen && firstDomain == types.RuntimeStringBinary {
		binaryMode = selectedRowsBinaryUniform
	} else if binarySeen && firstDomain == types.RuntimeStringText {
		binaryMode = selectedRowsBinaryText
	}
	if binaryMixed {
		binaryMode = selectedRowsBinaryRows
	}
	metadata |= binaryMode << selectedRowsBinaryShift
	var firstSource types.StringSource
	sourceSeen := false
	sourceMixed := false
	if v.HasStringSourceMetadata() {
		for i := 0; i < count; i++ {
			source := v.GetStringSourceAt(rowAt(i))
			if !source.Valid() {
				return moerr.NewInvalidInputNoCtx("invalid selected vector string source")
			}
			if !sourceSeen {
				firstSource, sourceSeen = source, true
			} else if source != firstSource {
				sourceMixed = true
			}
		}
	}
	sourceMode := selectedRowsSourceNone
	if sourceSeen && firstSource != types.StringSourceExpression {
		sourceMode = selectedRowsSourceUniform
	}
	if sourceMixed {
		sourceMode = selectedRowsSourceRows
	}
	metadata |= sourceMode << selectedRowsSourceShift
	if err := writeVectorMarshalByte(w, metadata); err != nil {
		return err
	}
	if kindMode == selectedRowsKindUniform {
		if err := writeVectorMarshalByte(w, byte(firstKind)); err != nil {
			return err
		}
	}
	if sourceMode == selectedRowsSourceUniform {
		if err := writeVectorMarshalByte(w, byte(firstSource)); err != nil {
			return err
		}
	}
	if !isVarlen {
		fixedWidth := v.typ.TypeSize()
		if fixedWidth < 0 || fixedWidth > math.MaxInt32 {
			return moerr.NewInvalidInputNoCtx("invalid selected vector fixed-width type")
		}
		if err := writeSelectedRowsInt32(w, int32(fixedWidth), &encodedInt32); err != nil {
			return err
		}
	}

	withRowFlags := metadata&(selectedRowsHasNull|selectedRowsHasGrouping) != 0 ||
		binaryMode == selectedRowsBinaryRows
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
			if v.gsp.Contains(uint64(row)) {
				rowFlags |= selectedRowsHasGrouping
			}
			if binaryMode == selectedRowsBinaryRows &&
				v.GetRuntimeStringDomainAt(row) == types.RuntimeStringBinary {
				rowFlags |= selectedRowsRowBinary
			}
			if binaryMode == selectedRowsBinaryRows &&
				v.GetRuntimeStringDomainAt(row) == types.RuntimeStringText {
				rowFlags |= selectedRowsRowText
			}
			if err := writeVectorMarshalByte(w, rowFlags); err != nil {
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
			if err := writeSelectedRowsInt32(w, int32(len(value)), &encodedInt32); err != nil {
				return err
			}
		}
		if err := writeVectorMarshalBytes(w, value); err != nil {
			return err
		}
	}

metadataTrailers:
	if kindMode == selectedRowsKindRows {
		for i := 0; i < count; i++ {
			if err := writeVectorMarshalByte(w, byte(v.GetPrepareParamKindAt(rowAt(i)))); err != nil {
				return err
			}
		}
	}
	if sourceMode == selectedRowsSourceRows {
		for i := 0; i < count; i++ {
			if err := writeVectorMarshalByte(w, byte(v.GetStringSourceAt(rowAt(i)))); err != nil {
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
	kindMode := (metadata & selectedRowsKindMask) >> selectedRowsKindShift
	binaryMode := (metadata & selectedRowsBinaryMask) >> selectedRowsBinaryShift
	sourceMode := (metadata & selectedRowsSourceMask) >> selectedRowsSourceShift
	if kindMode > selectedRowsKindRows || binaryMode > selectedRowsBinaryText ||
		sourceMode > selectedRowsSourceRows {
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
	var uniformSource types.StringSource
	if sourceMode == selectedRowsSourceUniform {
		encoded, err := types.ReadByte(r)
		if err != nil {
			return err
		}
		uniformSource = types.StringSource(encoded)
		if !uniformSource.Valid() || uniformSource == types.StringSourceExpression {
			return moerr.NewInvalidInputNoCtx("invalid selected vector string source")
		}
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
	if metadata&selectedRowsHasGrouping != 0 {
		if err := v.PreExtendGrouping(count, mp); err != nil {
			return err
		}
	}
	if binaryMode == selectedRowsBinaryRows || binaryMode == selectedRowsBinaryText {
		if err := v.ensureBinaryStringCapacity(count, mp); err != nil {
			return err
		}
		v.binaryStringRows.InitWithSize(int64(count))
		v.textStringRows.InitWithSize(int64(count))
	}
	v.SetLength(count)
	withRowFlags := metadata&(selectedRowsHasNull|selectedRowsHasGrouping) != 0 ||
		binaryMode == selectedRowsBinaryRows
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
		for row := 0; row < count; row++ {
			rowFlags := byte(0)
			if withRowFlags {
				rowFlags, err = types.ReadByte(r)
				if err != nil {
					return err
				}
				if rowFlags&^(selectedRowsHasNull|selectedRowsHasGrouping|
					selectedRowsRowBinary|selectedRowsRowText) != 0 ||
					rowFlags&selectedRowsHasNull != 0 && metadata&selectedRowsHasNull == 0 ||
					rowFlags&selectedRowsHasGrouping != 0 && metadata&selectedRowsHasGrouping == 0 ||
					rowFlags&selectedRowsRowBinary != 0 && binaryMode != selectedRowsBinaryRows ||
					rowFlags&selectedRowsRowText != 0 && binaryMode != selectedRowsBinaryRows ||
					rowFlags&selectedRowsRowBinary != 0 && rowFlags&selectedRowsRowText != 0 ||
					rowFlags&selectedRowsHasNull != 0 && rowFlags&(selectedRowsRowBinary|selectedRowsRowText) != 0 {
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
			if rowFlags&selectedRowsRowBinary != 0 {
				v.binaryStringRows.Add(uint64(row))
			}
			if rowFlags&selectedRowsRowText != 0 {
				v.textStringRows.Add(uint64(row))
			}
			valueSize := fixedWidth
			if isVarlen {
				valueSize, err = types.ReadInt32AsInt(r)
				if err != nil {
					return err
				}
				if valueSize < 0 {
					return moerr.NewInvalidInputNoCtx("invalid selected vector value size")
				}
			}
			if err := v.readRawBytesAt(r, row, valueSize, mp); err != nil {
				return err
			}
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
	switch sourceMode {
	case selectedRowsSourceUniform:
		if err := v.SetStringSource(uniformSource); err != nil {
			return err
		}
	case selectedRowsSourceRows:
		if err := v.SetStringSourcesFromReader(r, count, mp); err != nil {
			return err
		}
	}
	switch binaryMode {
	case selectedRowsBinaryUniform:
		v.setBinaryStringScalar(true)
	case selectedRowsBinaryText:
		if err := v.SetRuntimeStringDomainWithMP(types.RuntimeStringText, mp); err != nil {
			return err
		}
	case selectedRowsBinaryRows:
		v.binaryString = true
		v.binaryStringRowsActive = true
		v.normalizeBinaryStringRows()
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
