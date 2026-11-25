// Copyright 2021 Matrix Origin
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

package batch

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	batchMetadataMagic0      = byte('P')
	batchMetadataMagic1      = byte('P')
	batchMetadataMagic2      = byte('B')
	batchMetadataVersionV1   = byte(1)
	batchMetadataVersion     = byte(2)
	batchMetadataModeNone    = byte(0)
	batchMetadataModeUniform = byte(1)
	batchMetadataModeRows    = byte(2)
	batchMetadataBinaryFlag  = byte(0x80)
	batchMetadataTextFlag    = byte(0x40)
	batchMetadataMaxRows     = int32(1 << 24)
	// Older writers put a per-row kind (0..4) in the low bits; it is retired
	// and dropped on read.
	batchMetadataRetiredKindMax = byte(4)
)

type batchMetadataRecord struct {
	mode        byte
	encodedRows []byte
}

// discardBatchMetadataRows validates and drops one vector's per-row
// binary/text string flags written by an older peer. Vectors no longer carry
// a runtime string domain: the static type is authoritative.
func discardBatchMetadataRows(r io.Reader, n int) error {
	var buf [256]byte
	for n > 0 {
		chunk := buf[:min(n, len(buf))]
		if _, err := io.ReadFull(r, chunk); err != nil {
			return err
		}
		for _, b := range chunk {
			if b&batchMetadataBinaryFlag != 0 && b&batchMetadataTextFlag != 0 {
				return moerr.NewInvalidInputNoCtx("binary and text row flags are mutually exclusive")
			}
			// The retired kind field held values 0..4.
			if b&^(batchMetadataBinaryFlag|batchMetadataTextFlag) > batchMetadataRetiredKindMax {
				return moerr.NewInvalidInputNoCtxf("invalid row metadata byte %d", b)
			}
		}
		n -= len(chunk)
	}
	return nil
}

func NewWithSize(n int) *Batch {
	return &Batch{
		Vecs:     make([]*vector.Vector, n),
		rowCount: 0,
	}
}

// NewWithTypes returns a batch of empty vectors of attTypes, all from mp.
func NewWithTypes(attTypes []types.Type, mp *mpool.MPool) *Batch {
	bat := NewWithSize(len(attTypes))
	for i, t := range attTypes {
		bat.Vecs[i] = vector.NewVec(t, mp)
	}
	return bat
}

func SetLength(bat *Batch, n int) {
	for _, vec := range bat.Vecs {
		vec.SetLength(n)
	}
	bat.rowCount = n
}

func (bat *Batch) CheckLength() error {
	for _, vec := range bat.Vecs {
		if vec.Length() != bat.rowCount {
			return moerr.NewInternalErrorNoCtx("vec.Length() != bat.rowCount")
		}
	}
	return nil
}

// MarshalBinary encodes bat in the stable Batch wire format. The format has
// a column-name section; it is written empty. Use MarshalBinaryWithNames for a
// message whose receiver identifies its columns by name.
func (bat *Batch) MarshalBinary() ([]byte, error) {
	var w bytes.Buffer
	return bat.MarshalBinaryWithBufferAndNames(&w, false, nil)
}

// MarshalBinaryWithNames is MarshalBinary with names written into the
// format's column-name section. names belong to the message, not to bat:
// WireNames reads them back from the encoded bytes.
func (bat *Batch) MarshalBinaryWithNames(names []string) ([]byte, error) {
	var w bytes.Buffer
	return bat.MarshalBinaryWithBufferAndNames(&w, false, names)
}

func (bat *Batch) MarshalBinaryWithBuffer(w *bytes.Buffer, reset bool) ([]byte, error) {
	return bat.MarshalBinaryWithBufferAndNames(w, reset, nil)
}

// MarshalBinaryWithBufferAndNames is MarshalBinaryWithBuffer with names
// written into the column-name section (see MarshalBinaryWithNames).
func (bat *Batch) MarshalBinaryWithBufferAndNames(
	w *bytes.Buffer, reset bool, names []string,
) ([]byte, error) {
	if reset {
		w.Reset()
	}
	if err := bat.marshalBinaryTo(w, names); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (bat *Batch) MarshalBinarySize() (int, error) {
	return bat.prepareMarshalBinary(nil, nil)
}

func (bat *Batch) prepareMarshalBinary(
	plans []vector.MarshalBinaryPlan,
	names []string,
) (int, error) {
	if bat == nil {
		return 0, moerr.NewInvalidInputNoCtx("invalid batch for marshal")
	}
	if bat.selection != nil {
		return 0, errSelectedBatch()
	}
	const fixedSize = uint64(8 + 4 + 4 + 4 + 4 + 4)
	total := fixedSize
	add := func(value uint64) bool {
		if value > uint64(^uint(0)>>1)-total {
			return false
		}
		total += value
		return true
	}
	if uint64(len(bat.Vecs)) > uint64(^uint32(0)>>1) ||
		uint64(len(names)) > uint64(^uint32(0)>>1) ||
		uint64(len(bat.ExtraBuf)) > uint64(^uint32(0)>>1) {
		return 0, moerr.NewInvalidInputNoCtx(
			"batch field exceeds marshal format",
		)
	}
	if plans != nil && len(plans) < len(bat.Vecs) {
		return 0, moerr.NewInvalidInputNoCtx("short batch marshal plan")
	}
	for i, vec := range bat.Vecs {
		if vec == nil {
			return 0, moerr.NewInvalidInputNoCtx(
				"cannot marshal a nil batch vector",
			)
		}
		plan, err := vec.PrepareMarshalBinary()
		if err != nil {
			return 0, err
		}
		size := plan.Size()
		if uint64(size) > uint64(^uint32(0)) ||
			!add(4+uint64(size)) {
			return 0, moerr.NewInvalidInputNoCtx(
				"batch vector exceeds marshal format",
			)
		}
		if plans != nil {
			plans[i] = plan
		}
	}
	for _, attr := range names {
		if uint64(len(attr)) > uint64(^uint32(0)>>1) ||
			!add(4+uint64(len(attr))) {
			return 0, moerr.NewInvalidInputNoCtx(
				"batch attribute exceeds marshal format",
			)
		}
	}
	if !add(uint64(len(bat.ExtraBuf))) {
		return 0, moerr.NewInvalidInputNoCtx(
			"batch marshal size exceeds platform limit",
		)
	}
	return int(total), nil
}

func (bat *Batch) MarshalBinaryTo(w io.Writer) error {
	return bat.marshalBinaryTo(w, nil)
}

func (bat *Batch) marshalBinaryTo(w io.Writer, names []string) error {
	if bat == nil || w == nil {
		return io.ErrClosedPipe
	}
	if bat.selection != nil {
		return errSelectedBatch()
	}
	var inlinePlans [64]vector.MarshalBinaryPlan
	var plans []vector.MarshalBinaryPlan
	if len(bat.Vecs) <= len(inlinePlans) {
		plans = inlinePlans[:len(bat.Vecs)]
	} else {
		plans = make([]vector.MarshalBinaryPlan, len(bat.Vecs))
	}
	size, err := bat.prepareMarshalBinary(plans, names)
	if err != nil {
		return err
	}
	if sized, ok := w.(interface {
		Len() int
		EnsureCapacity(int) error
	}); ok {
		if sized.Len() > math.MaxInt-size {
			return moerr.NewInvalidInputNoCtx("batch marshal size exceeds platform limit")
		}
		if err := sized.EnsureCapacity(sized.Len() + size); err != nil {
			return err
		}
	}
	if err := writeBatchMarshalInt64(w, int64(bat.rowCount)); err != nil {
		return err
	}

	l := int32(len(bat.Vecs))
	if err := writeBatchMarshalInt32(w, l); err != nil {
		return err
	}
	for i := 0; i < int(l); i++ {
		if err := writeBatchMarshalUint32(w, uint32(plans[i].Size())); err != nil {
			return err
		}
		if err := plans[i].MarshalTo(w); err != nil {
			return err
		}
	}

	l = int32(len(names))
	if err := writeBatchMarshalInt32(w, l); err != nil {
		return err
	}
	for i := 0; i < int(l); i++ {
		size := int32(len(names[i]))
		if err := writeBatchMarshalInt32(w, size); err != nil {
			return err
		}
		n, err := io.WriteString(w, names[i])
		if err != nil {
			return err
		}
		if int32(n) != size {
			return io.ErrShortWrite
		}
	}

	extraSize := int32(len(bat.ExtraBuf))
	if err := writeBatchMarshalInt32(w, extraSize); err != nil {
		return err
	}
	if err := types.WriteFull(w, bat.ExtraBuf); err != nil {
		return err
	}

	if err := writeBatchMarshalInt32(w, bat.Recursive); err != nil {
		return err
	}
	return writeBatchMarshalInt32(w, bat.ShuffleIDX)
}

type batchPrimitiveWriter interface {
	WriteUint32(uint32) error
	WriteInt32(int32) error
	WriteInt64(int64) error
}

func writeBatchMarshalUint32(w io.Writer, value uint32) error {
	if typed, ok := w.(batchPrimitiveWriter); ok {
		return typed.WriteUint32(value)
	}
	var data [4]byte
	binary.NativeEndian.PutUint32(data[:], value)
	return types.WriteFull(w, data[:])
}

func writeBatchMarshalInt32(w io.Writer, value int32) error {
	if typed, ok := w.(batchPrimitiveWriter); ok {
		return typed.WriteInt32(value)
	}
	var data [4]byte
	binary.NativeEndian.PutUint32(data[:], uint32(value))
	return types.WriteFull(w, data[:])
}

func writeBatchMarshalInt64(w io.Writer, value int64) error {
	if typed, ok := w.(batchPrimitiveWriter); ok {
		return typed.WriteInt64(value)
	}
	var data [8]byte
	binary.NativeEndian.PutUint64(data[:], uint64(value))
	return types.WriteFull(w, data[:])
}

func (bat *Batch) UnmarshalBinary(data []byte) (err error) {
	return bat.UnmarshalBinaryWithAnyMp(data, nil)
}

// UnmarshalBinaryWithMetadata decodes the stable Batch prefix and an
// optional pipeline-only provenance trailer.  The stable public decoder keeps
// its historical trailing-byte behavior for persisted/legacy callers.
func (bat *Batch) UnmarshalBinaryWithMetadata(data []byte, mp *mpool.MPool) error {
	prefixLen, err := stableBatchPayloadLength(data)
	if err != nil {
		return err
	}
	return bat.unmarshalBinaryWithMetadataAtPrefix(data, prefixLen, mp)
}

// unmarshalBinaryWithMetadataAtPrefix uses a previously validated
// stable boundary; callers must ensure it lies within data.
func (bat *Batch) unmarshalBinaryWithMetadataAtPrefix(data []byte, prefixLen int, mp *mpool.MPool) error {
	if err := bat.UnmarshalBinaryWithAnyMp(data[:prefixLen], mp); err != nil {
		return err
	}
	if prefixLen == len(data) {
		return nil
	}
	records, rowCount, err := parseBatchMetadataTrailer(
		data[prefixLen:], bat.Vecs, int64(bat.RowCount()))
	if err != nil {
		return err
	}
	if rowCount != int64(bat.RowCount()) {
		return moerr.NewInvalidInputNoCtx("prepared parameter metadata batch row count mismatch")
	}
	for i, record := range records {
		if i >= len(bat.Vecs) {
			return moerr.NewInvalidInputNoCtx("prepared parameter metadata vector count mismatch")
		}
		// The trailer carries only retired metadata from older writers; it is
		// validated and dropped.
		switch record.mode {
		case batchMetadataModeNone, batchMetadataModeUniform:
		case batchMetadataModeRows:
			if len(record.encodedRows) != bat.Vecs[i].Length() {
				return moerr.NewInvalidInputNoCtx("prepared parameter metadata row count mismatch")
			}
			if err := discardBatchMetadataRows(
				bytes.NewReader(record.encodedRows), len(record.encodedRows)); err != nil {
				return err
			}
		default:
			return moerr.NewInvalidInputNoCtx("invalid prepared parameter metadata mode")
		}
	}
	return nil
}

// UnmarshalFromReaderWithMetadata decodes a framed local-spill
// payload without copying the stable batch or its trailer to a Go-heap
// buffer. payloadSize is the exact size of this batch section; any remaining
// bytes after the stable Batch fields are parsed as the transient provenance
// trailer. The stable reader remains the ownership-aware MPool path.
func (bat *Batch) UnmarshalFromReaderWithMetadata(
	r io.Reader,
	payloadSize int64,
	mp *mpool.MPool,
) error {
	return bat.unmarshalFromReaderWithMetadata(
		r,
		payloadSize,
		mp,
		true,
	)
}

// UnmarshalFromReaderWithMetadataForSpill is the metadata-free local
// spill variant. It rejects column names and ExtraBuf before allocating their
// payloads, while retaining the prepared-parameter trailer used by expression
// results.
func (bat *Batch) UnmarshalFromReaderWithMetadataForSpill(
	r io.Reader,
	payloadSize int64,
	mp *mpool.MPool,
) error {
	return bat.unmarshalFromReaderWithMetadata(
		r,
		payloadSize,
		mp,
		false,
	)
}

func (bat *Batch) unmarshalFromReaderWithMetadata(
	r io.Reader,
	payloadSize int64,
	mp *mpool.MPool,
	allowMetadata bool,
) error {
	if bat == nil || r == nil {
		return io.ErrClosedPipe
	}
	if payloadSize < 0 {
		return moerr.NewInvalidInputNoCtx("negative batch payload size")
	}
	limited := &io.LimitedReader{R: r, N: payloadSize}
	if err := bat.unmarshalFromReader(limited, mp, allowMetadata); err != nil {
		return err
	}
	if limited.N == 0 {
		return nil
	}
	trailerSize := limited.N
	if trailerSize < 0 || trailerSize > int64(^uint32(0)) {
		return moerr.NewInvalidInputNoCtx("prepared parameter metadata trailer exceeds wire limit")
	}
	startRemaining := trailerSize
	readByte := func() (byte, error) {
		return types.ReadByte(limited)
	}
	magic0, err := readByte()
	if err != nil {
		return err
	}
	magic1, err := readByte()
	if err != nil {
		return err
	}
	magic2, err := readByte()
	if err != nil {
		return err
	}
	version, err := readByte()
	if err != nil {
		return err
	}
	if magic0 != batchMetadataMagic0 ||
		magic1 != batchMetadataMagic1 ||
		magic2 != batchMetadataMagic2 ||
		version != batchMetadataVersionV1 && version != batchMetadataVersion {
		return moerr.NewInvalidInputNoCtx("invalid prepared parameter batch trailer")
	}
	nVecs, err := types.ReadInt32(limited)
	if err != nil || nVecs < 0 || nVecs > 1<<20 {
		if err == nil {
			err = moerr.NewInvalidInputNoCtx("invalid prepared parameter metadata vector count")
		}
		return err
	}
	if int(nVecs) != len(bat.Vecs) {
		return moerr.NewInvalidInputNoCtx("prepared parameter metadata vector count mismatch")
	}
	rowCount, err := types.ReadInt64(limited)
	if err != nil || rowCount < 0 || int64(int(rowCount)) != rowCount {
		if err == nil {
			err = moerr.NewInvalidInputNoCtx("invalid prepared parameter metadata row count")
		}
		return err
	}
	if rowCount != int64(bat.RowCount()) {
		return moerr.NewInvalidInputNoCtx("prepared parameter metadata batch row count mismatch")
	}
	for i := range bat.Vecs {
		mode, err := readByte()
		if err != nil {
			return err
		}
		binaryString := mode&batchMetadataBinaryFlag != 0
		textString := mode&batchMetadataTextFlag != 0
		if binaryString && textString {
			return moerr.NewInvalidInputNoCtx("binary and text vector flags are mutually exclusive")
		}
		mode &^= batchMetadataBinaryFlag | batchMetadataTextFlag
		switch mode {
		case batchMetadataModeNone:
		case batchMetadataModeUniform:
			kind, err := readByte()
			if err != nil {
				return err
			}
			// Retired kind byte from an older writer.
			if kind == 0 || kind > batchMetadataRetiredKindMax {
				return moerr.NewInvalidInputNoCtx("invalid uniform vector metadata kind")
			}
		case batchMetadataModeRows:
			count, err := types.ReadInt32(limited)
			if err != nil {
				return err
			}
			if count < 0 || count > batchMetadataMaxRows || count != int32(bat.Vecs[i].Length()) {
				return moerr.NewInvalidInputNoCtx("prepared parameter metadata row count mismatch")
			}
			// Before the vector allocates count bytes, prove that the frame can
			// contain this row payload, at least one mode byte for every
			// remaining vector record, and the four-byte trailer footer.
			// nVecs is capped above, so this int64 sum cannot overflow.
			minimumPerVector := int64(1)
			if version >= batchMetadataVersion {
				minimumPerVector = 2
			}
			minimumRemaining := int64(len(bat.Vecs)-i-1)*minimumPerVector + 4
			if limited.N < minimumRemaining || int64(count) > limited.N-minimumRemaining {
				return io.ErrUnexpectedEOF
			}
			if err := discardBatchMetadataRows(limited, int(count)); err != nil {
				return err
			}
		default:
			return moerr.NewInvalidInputNoCtx("invalid prepared parameter metadata mode")
		}
		if version >= batchMetadataVersion {
			sourceMode, err := readByte()
			if err != nil {
				return err
			}
			// The string-source section is retired: validate an older writer's
			// section and drop it.
			switch sourceMode {
			case batchMetadataModeNone:
			case batchMetadataModeUniform:
				encoded, err := readByte()
				source := types.StringSource(encoded)
				if err != nil || !source.Valid() || source == types.StringSourceExpression {
					return moerr.NewInvalidInputNoCtx("invalid uniform string source metadata")
				}
			case batchMetadataModeRows:
				count, err := types.ReadInt32(limited)
				if err != nil || count < 0 || count != int32(bat.Vecs[i].Length()) ||
					count > batchMetadataMaxRows || int64(count) > limited.N-4 {
					return moerr.NewInvalidInputNoCtx("invalid string source metadata row count")
				}
				for row := int32(0); row < count; row++ {
					encoded, err := readByte()
					if err != nil {
						return err
					}
					if !types.StringSource(encoded).Valid() {
						return moerr.NewInvalidInputNoCtx("invalid string source metadata")
					}
				}
			default:
				return moerr.NewInvalidInputNoCtx("invalid string source metadata mode")
			}
		}
	}
	trailerLen, err := types.ReadUint32(limited)
	if err != nil {
		return err
	}
	if trailerLen != uint32(startRemaining) || limited.N != 0 {
		return moerr.NewInvalidInputNoCtx("prepared parameter metadata length mismatch")
	}
	return nil
}

// stableBatchPayloadLength parses only the stable Batch framing and returns
// the exact byte offset at which any pipeline-only extension begins.  Keeping
// this parser separate from UnmarshalBinaryWithAnyMp preserves the historical
// public decoder while making the extension boundary unambiguous: a random
// "PPB" sequence inside a vector or attribute can never be mistaken for a
// trailer.
func stableBatchPayloadLength(data []byte) (int, error) {
	cursor := batchUnmarshalCursor{data: data}
	rowCount, err := cursor.readInt64()
	if err != nil {
		return 0, err
	}
	if rowCount < 0 || int64(int(rowCount)) != rowCount {
		return 0, moerr.NewInvalidInputNoCtx("invalid batch row count")
	}
	vecs, err := cursor.readInt32()
	if err != nil {
		return 0, err
	}
	if vecs < 0 || int64(vecs) > int64(len(cursor.data)/4) {
		return 0, moerr.NewInvalidInputNoCtx("invalid batch vector count")
	}
	for range vecs {
		size, err := cursor.readUint32()
		if err != nil {
			return 0, err
		}
		if uint64(size) > uint64(len(cursor.data)) {
			return 0, io.ErrUnexpectedEOF
		}
		if _, err := cursor.read(int(size)); err != nil {
			return 0, err
		}
	}
	attrs, err := cursor.readInt32()
	if err != nil {
		return 0, err
	}
	if attrs < 0 || int64(attrs) > int64(len(cursor.data)/4) {
		return 0, moerr.NewInvalidInputNoCtx("invalid batch attribute count")
	}
	for range attrs {
		size, err := cursor.readInt32()
		if err != nil {
			return 0, err
		}
		if size < 0 {
			return 0, moerr.NewInvalidInputNoCtx("invalid batch attribute size")
		}
		if _, err := cursor.read(int(size)); err != nil {
			return 0, err
		}
	}
	extra, err := cursor.readInt32()
	if err != nil {
		return 0, err
	}
	if extra < 0 {
		return 0, moerr.NewInvalidInputNoCtx("invalid batch extra buffer size")
	}
	if _, err := cursor.read(int(extra)); err != nil {
		return 0, err
	}
	if _, err := cursor.readInt32(); err != nil {
		return 0, err
	}
	if _, err := cursor.readInt32(); err != nil {
		return 0, err
	}
	return len(data) - len(cursor.data), nil
}

func parseBatchMetadataTrailer(
	ext []byte,
	vecs []*vector.Vector,
	expectedRowCount int64,
) ([]batchMetadataRecord, int64, error) {
	if len(ext) < 4+4 {
		return nil, 0, moerr.NewInvalidInputNoCtx("truncated prepared parameter batch trailer")
	}
	reader := bytes.NewReader(ext)
	magic0, _ := types.ReadByte(reader)
	magic1, _ := types.ReadByte(reader)
	magic2, _ := types.ReadByte(reader)
	version, err := types.ReadByte(reader)
	if err != nil || magic0 != batchMetadataMagic0 ||
		magic1 != batchMetadataMagic1 || magic2 != batchMetadataMagic2 ||
		version != batchMetadataVersionV1 && version != batchMetadataVersion {
		return nil, 0, moerr.NewInvalidInputNoCtx("invalid prepared parameter batch trailer")
	}
	nVecs, err := types.ReadInt32(reader)
	if err != nil || nVecs < 0 || nVecs > 1<<20 {
		return nil, 0, moerr.NewInvalidInputNoCtx("invalid prepared parameter metadata vector count")
	}
	if int(nVecs) != len(vecs) {
		return nil, 0, moerr.NewInvalidInputNoCtx("prepared parameter metadata vector count mismatch")
	}
	rowCount, err := types.ReadInt64(reader)
	if err != nil || rowCount < 0 || int64(int(rowCount)) != rowCount {
		return nil, 0, moerr.NewInvalidInputNoCtx("invalid prepared parameter metadata row count")
	}
	if rowCount != expectedRowCount {
		return nil, 0, moerr.NewInvalidInputNoCtx("prepared parameter metadata batch row count mismatch")
	}
	records := make([]batchMetadataRecord, len(vecs))
	for i := range records {
		mode, err := types.ReadByte(reader)
		if err != nil {
			return nil, 0, err
		}
		if mode&batchMetadataBinaryFlag != 0 && mode&batchMetadataTextFlag != 0 {
			return nil, 0, moerr.NewInvalidInputNoCtx("binary and text vector flags are mutually exclusive")
		}
		mode &^= batchMetadataBinaryFlag | batchMetadataTextFlag
		records[i].mode = mode
		switch mode {
		case batchMetadataModeNone:
		case batchMetadataModeUniform:
			kind, err := types.ReadByte(reader)
			if err != nil || kind == 0 || kind > batchMetadataRetiredKindMax {
				return nil, 0, moerr.NewInvalidInputNoCtx("invalid uniform vector metadata kind")
			}
		case batchMetadataModeRows:
			count, err := types.ReadInt32(reader)
			if err != nil || count < 0 || count > batchMetadataMaxRows {
				return nil, 0, moerr.NewInvalidInputNoCtx("invalid prepared parameter metadata row count")
			}
			if vecs[i] == nil || count != int32(vecs[i].Length()) {
				return nil, 0, moerr.NewInvalidInputNoCtx("prepared parameter metadata row count mismatch")
			}
			// Reserve at least the footer before allocating a row payload.  The
			// vector-length check above bounds this allocation by the decoded
			// batch, while this check rejects truncated amplification payloads.
			if reader.Len() < 4 || int64(count) > int64(reader.Len()-4) {
				return nil, 0, io.ErrUnexpectedEOF
			}
			rowStart := len(ext) - reader.Len()
			rowEnd := rowStart + int(count)
			records[i].encodedRows = ext[rowStart:rowEnd]
			for _, encoded := range records[i].encodedRows {
				if encoded&batchMetadataBinaryFlag != 0 && encoded&batchMetadataTextFlag != 0 {
					return nil, 0, moerr.NewInvalidInputNoCtx("binary and text row flags are mutually exclusive")
				}
				kind := encoded &^ (batchMetadataBinaryFlag | batchMetadataTextFlag)
				if kind > batchMetadataRetiredKindMax {
					return nil, 0, moerr.NewInvalidInputNoCtx("invalid vector metadata kind")
				}
			}
			if _, err := reader.Seek(int64(count), io.SeekCurrent); err != nil {
				return nil, 0, err
			}
		default:
			return nil, 0, moerr.NewInvalidInputNoCtx("invalid prepared parameter metadata mode")
		}
		if version >= batchMetadataVersion {
			sourceMode, err := types.ReadByte(reader)
			if err != nil {
				return nil, 0, err
			}
			switch sourceMode {
			case batchMetadataModeNone:
			case batchMetadataModeUniform:
				encoded, err := types.ReadByte(reader)
				source := types.StringSource(encoded)
				if err != nil || !source.Valid() || source == types.StringSourceExpression {
					return nil, 0, moerr.NewInvalidInputNoCtx("invalid uniform string source metadata")
				}
			case batchMetadataModeRows:
				count, err := types.ReadInt32(reader)
				if err != nil || count < 0 || vecs[i] == nil || count != int32(vecs[i].Length()) ||
					count > batchMetadataMaxRows || int64(count) > int64(reader.Len()-4) {
					return nil, 0, moerr.NewInvalidInputNoCtx("invalid string source metadata row count")
				}
				rowStart := len(ext) - reader.Len()
				rowEnd := rowStart + int(count)
				for _, encoded := range ext[rowStart:rowEnd] {
					if !types.StringSource(encoded).Valid() {
						return nil, 0, moerr.NewInvalidInputNoCtx("invalid string source metadata")
					}
				}
				if _, err := reader.Seek(int64(count), io.SeekCurrent); err != nil {
					return nil, 0, err
				}
			default:
				return nil, 0, moerr.NewInvalidInputNoCtx("invalid string source metadata mode")
			}
		}
	}
	if reader.Len() != 4 {
		return nil, 0, moerr.NewInvalidInputNoCtx("unexpected prepared parameter metadata bytes")
	}
	trailerLen, err := types.ReadUint32(reader)
	if err != nil || trailerLen != uint32(len(ext)) {
		return nil, 0, moerr.NewInvalidInputNoCtx("prepared parameter metadata length mismatch")
	}
	return records, rowCount, nil
}

type batchUnmarshalCursor struct {
	data []byte
}

func (c *batchUnmarshalCursor) read(size int) ([]byte, error) {
	if size < 0 || size > len(c.data) {
		return nil, io.ErrUnexpectedEOF
	}
	data := c.data[:size]
	c.data = c.data[size:]
	return data, nil
}

func (c *batchUnmarshalCursor) readInt32() (int32, error) {
	data, err := c.read(4)
	if err != nil {
		return 0, err
	}
	return types.DecodeInt32(data), nil
}

func (c *batchUnmarshalCursor) readInt64() (int64, error) {
	data, err := c.read(8)
	if err != nil {
		return 0, err
	}
	return types.DecodeInt64(data), nil
}

func (c *batchUnmarshalCursor) readUint32() (uint32, error) {
	data, err := c.read(4)
	if err != nil {
		return 0, err
	}
	return types.DecodeUint32(data), nil
}

// WireNames returns the column-name section of a batch encoded in the stable
// Batch wire format, without decoding the vectors. It is empty unless the
// writer used MarshalBinaryWithNames (or is an older version, which always
// wrote names).
func WireNames(data []byte) ([]string, error) {
	cursor := batchUnmarshalCursor{data: data}
	if _, err := cursor.readInt64(); err != nil {
		return nil, err
	}
	l, err := cursor.readInt32()
	if err != nil {
		return nil, err
	}
	if l < 0 || int64(l) > int64(len(cursor.data)/4) {
		return nil, moerr.NewInvalidInputNoCtx("invalid batch vector count")
	}
	for i := 0; i < int(l); i++ {
		size, err := cursor.readUint32()
		if err != nil {
			return nil, err
		}
		if uint64(size) > uint64(len(cursor.data)) {
			return nil, io.ErrUnexpectedEOF
		}
		if _, err := cursor.read(int(size)); err != nil {
			return nil, err
		}
	}
	l, err = cursor.readInt32()
	if err != nil {
		return nil, err
	}
	if l < 0 || int64(l) > int64(len(cursor.data)/4) {
		return nil, moerr.NewInvalidInputNoCtx("invalid batch attribute count")
	}
	if l == 0 {
		return nil, nil
	}
	names := make([]string, l)
	for i := range names {
		size, err := cursor.readInt32()
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, moerr.NewInvalidInputNoCtx("invalid batch attribute size")
		}
		name, err := cursor.read(int(size))
		if err != nil {
			return nil, err
		}
		names[i] = string(name)
	}
	return names, nil
}

func (bat *Batch) UnmarshalBinaryWithAnyMp(data []byte, mp *mpool.MPool) (err error) {
	// Decoding replaces every column; no previous lent backing stays aliased.
	bat.leases.Release()
	cursor := batchUnmarshalCursor{data: data}
	rowCount, err := cursor.readInt64()
	if err != nil {
		return err
	}
	if rowCount < 0 || int64(int(rowCount)) != rowCount {
		return moerr.NewInvalidInputNoCtx("invalid batch row count")
	}
	decodedRowCount := int(rowCount)

	l, err := cursor.readInt32()
	if err != nil {
		return err
	}
	if l < 0 || int64(l) > int64(len(cursor.data)/4) {
		return moerr.NewInvalidInputNoCtx("invalid batch vector count")
	}
	// Fix for bug #23156: Handle Vecs length changes (from d4b79f12) while maintaining revert version's firstTime logic
	firstTime := bat.Vecs == nil
	vecsLen := int(l)
	vecsLenChanged := !firstTime && vecsLen != len(bat.Vecs)

	// CRITICAL FIX: When batch is reused (not firstTime), always reallocate Vecs if length changed
	// This ensures Vecs are properly reset and prevents stale data from previous unmarshal operations
	if firstTime || vecsLenChanged {
		if vecsLenChanged && len(bat.Vecs) > 0 {
			if err := bat.freeReceivers(mp); err != nil {
				return err
			}
		}
		bat.Vecs = make([]*vector.Vector, vecsLen)
	}

	vecs := bat.Vecs
	// SelectColumns and ReplaceVector can leave multiple slots pointing to the
	// same Vector. Reuse each receiver for at most one decoded column.
	// Most batches are narrow, so avoid allocating a map on every decode. The
	// prefix scan is bounded; wide batches keep the linear-time map path.
	const linearReceiverScanLimit = 64
	var usedReceivers map[*vector.Vector]struct{}
	for i := 0; i < vecsLen; i++ {
		size, err := cursor.readUint32()
		if err != nil {
			return err
		}
		if uint64(size) > uint64(len(cursor.data)) {
			return io.ErrUnexpectedEOF
		}
		vecData, err := cursor.read(int(size))
		if err != nil {
			return err
		}
		if vecs[i] != nil {
			used := false
			if vecsLen <= linearReceiverScanLimit {
				for j := 0; j < i; j++ {
					if vecs[j] == vecs[i] {
						used = true
						break
					}
				}
			} else {
				_, used = usedReceivers[vecs[i]]
			}
			if used {
				vecs[i] = nil
			} else if vecsLen > linearReceiverScanLimit {
				if usedReceivers == nil {
					usedReceivers = make(map[*vector.Vector]struct{}, vecsLen)
				}
				usedReceivers[vecs[i]] = struct{}{}
			}
		}
		// A receiver is reused only when it belongs to mp; with a nil mp the
		// decoded vectors are read-only references to data. A read-only
		// receiver is dropped; one owned by another pool is not ours to free.
		if vecs[i] != nil && vecs[i].MPool() != mp {
			if err := dropReadOnlyReceiver(vecs[i]); err != nil {
				return err
			}
			vecs[i] = nil
		}
		if vecs[i] == nil {
			vecs[i] = vector.NewVec(types.T_any.ToType(), mp)
		} else {
			vecs[i].Free(mp)
		}
		if err := vecs[i].UnmarshalBinary(vecData); err != nil {
			return err
		}
	}

	l, err = cursor.readInt32()
	if err != nil {
		return err
	}
	if l < 0 || int64(l) > int64(len(cursor.data)/4) {
		return moerr.NewInvalidInputNoCtx("invalid batch attribute count")
	}
	// The column-name section belongs to the message, not to the batch:
	// WireNames reads it for a receiver that needs it. Skip it here.
	for i := 0; i < int(l); i++ {
		size, err := cursor.readInt32()
		if err != nil {
			return err
		}
		if size < 0 {
			return moerr.NewInvalidInputNoCtx("invalid batch attribute size")
		}
		if _, err := cursor.read(int(size)); err != nil {
			return err
		}
	}

	// ExtraBuf
	l, err = cursor.readInt32()
	if err != nil {
		return err
	}
	if l < 0 {
		return moerr.NewInvalidInputNoCtx("invalid batch extra buffer size")
	}
	extraBuf, err := cursor.read(int(l))
	if err != nil {
		return err
	}
	bat.releaseExtraBuf()
	bat.ExtraBuf = append(bat.ExtraBuf, extraBuf...)

	bat.Recursive, err = cursor.readInt32()
	if err != nil {
		return err
	}
	bat.ShuffleIDX, err = cursor.readInt32()
	if err != nil {
		return err
	}
	bat.rowCount = decodedRowCount
	return nil
}

func (bat *Batch) UnmarshalFromReader(r io.Reader, mp *mpool.MPool) (err error) {
	return bat.unmarshalFromReader(r, mp, true)
}

func (bat *Batch) unmarshalFromReader(
	r io.Reader,
	mp *mpool.MPool,
	allowMetadata bool,
) (err error) {
	if bat == nil || r == nil {
		return io.ErrClosedPipe
	}
	// Decoding replaces every column; no previous lent backing stays aliased.
	bat.leases.Release()
	i64, err := types.ReadInt64(r)
	if err != nil {
		return err
	}
	if i64 < 0 || int64(int(i64)) != i64 {
		return moerr.NewInvalidInputNoCtx("invalid batch row count")
	}
	decodedRowCount := int(i64)

	l, err := types.ReadInt32AsInt(r)
	if err != nil {
		return err
	}
	if err = validateReaderElementCount(r, l, 4, "vector"); err != nil {
		return err
	}
	if err = bat.prepareOwnedDecodeVectors(l, mp); err != nil {
		return err
	}
	vecs := bat.Vecs

	for i := 0; i < l; i++ {
		vecL, err := types.ReadUint32(r)
		if err != nil {
			return err
		}
		limitedReader := io.LimitReader(r, int64(vecL))
		if err := vecs[i].UnmarshalWithReader(limitedReader, mp); err != nil {
			return err
		}
		// Ensure the vector consumed exactly the bytes allocated by its length prefix.
		// Any leftover bytes indicate a serialization mismatch and would corrupt
		// subsequent reads from the underlying reader.
		if n, _ := io.Copy(io.Discard, limitedReader); n > 0 {
			return moerr.NewInternalErrorNoCtxf("vector unmarshal did not consume all bytes: %d remaining", n)
		}
	}

	l, err = types.ReadInt32AsInt(r)
	if err != nil {
		return err
	}
	if err = validateReaderElementCount(r, l, 4, "attribute"); err != nil {
		return err
	}
	if !allowMetadata && l != 0 {
		return moerr.NewInvalidInputNoCtx("spill batch attributes are not allowed")
	}
	for i := 0; i < int(l); i++ {
		if _, err := readBatchSizedBytes(r); err != nil {
			return err
		}
	}

	// ExtraBuf is a data-scaled Go-heap field in the stable Batch codec. Spill
	// records do not use it and reject it before allocating its payload.
	if allowMetadata {
		bat.releaseExtraBuf()
		if bat.ExtraBuf, err = readBatchSizedBytes(r); err != nil {
			return err
		}
	} else {
		extraSize, readErr := types.ReadInt32AsInt(r)
		if readErr != nil {
			return readErr
		}
		if extraSize != 0 {
			return moerr.NewInvalidInputNoCtx("spill batch extra buffer is not allowed")
		}
		bat.releaseExtraBuf()
	}

	if bat.Recursive, err = types.ReadInt32(r); err != nil {
		return err
	}
	if bat.ShuffleIDX, err = types.ReadInt32(r); err != nil {
		return err
	}
	bat.rowCount = decodedRowCount
	return nil
}

func readBatchSizedBytes(r io.Reader) ([]byte, error) {
	size, err := types.ReadInt32AsInt(r)
	if err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, moerr.NewInvalidInputNoCtx("negative batch buffer size")
	}
	if limited, ok := r.(*io.LimitedReader); ok && int64(size) > limited.N {
		return nil, io.ErrUnexpectedEOF
	}
	if lengthAware, ok := r.(interface{ Len() int }); ok && size > lengthAware.Len() {
		return nil, io.ErrUnexpectedEOF
	}
	if size == 0 {
		return nil, nil
	}
	value := make([]byte, size)
	if _, err = io.ReadFull(r, value); err != nil {
		return nil, err
	}
	return value, nil
}

func validateReaderElementCount(
	r io.Reader,
	count int,
	minimumWireBytes int64,
	field string,
) error {
	const maxBatchWireFields = 1 << 20
	if count < 0 || count > maxBatchWireFields || minimumWireBytes <= 0 {
		return moerr.NewInvalidInputNoCtx("invalid batch " + field + " count")
	}
	var remaining int64 = -1
	switch reader := r.(type) {
	case *io.LimitedReader:
		remaining = reader.N
	case interface{ Len() int }:
		remaining = int64(reader.Len())
	}
	if remaining >= 0 && int64(count) > remaining/minimumWireBytes {
		return moerr.NewInvalidInputNoCtx("invalid batch " + field + " count")
	}
	return nil
}

// prepareOwnedDecodeVectors makes every destination an independent vector of
// mp for the streaming decoder.
func (bat *Batch) prepareOwnedDecodeVectors(count int, mp *mpool.MPool) error {
	if count < 0 {
		return moerr.NewInvalidInputNoCtx("invalid batch vector count")
	}
	if count != len(bat.Vecs) {
		if len(bat.Vecs) > 0 {
			if err := bat.freeReceivers(mp); err != nil {
				return err
			}
		}
		bat.Vecs = make([]*vector.Vector, count)
	}

	const inlineReceivers = 16
	var inline [inlineReceivers]*vector.Vector
	var used map[*vector.Vector]struct{}
	for i, vec := range bat.Vecs {
		if vec != nil {
			exists := false
			if i < inlineReceivers {
				for j := 0; j < i; j++ {
					if inline[j] == vec {
						exists = true
						break
					}
				}
				inline[i] = vec
			} else {
				if used == nil {
					used = make(map[*vector.Vector]struct{}, count)
					for _, prior := range inline {
						if prior != nil {
							used[prior] = struct{}{}
						}
					}
				}
				_, exists = used[vec]
				used[vec] = struct{}{}
			}
			if exists {
				vec = nil
			}
		}
		if vec != nil && vec.MPool() != mp {
			if err := dropReadOnlyReceiver(vec); err != nil {
				return err
			}
			vec = nil
		}
		if vec == nil {
			vec = vector.NewVec(types.T_any.ToType(), mp)
		}
		bat.Vecs[i] = vec
	}
	return nil
}

func (bat *Batch) ShrinkByMask(sels *bitmap.Bitmap, negate bool, offset uint64, mp *mpool.MPool) error {
	if !negate {
		if sels.Count() == bat.rowCount {
			return nil
		}
	}

	for _, vec := range bat.Vecs {
		if err := vec.ShrinkByMask(sels, negate, offset, mp); err != nil {
			return err
		}
	}

	if negate {
		bat.rowCount -= sels.Count()
		return nil
	}
	bat.rowCount = sels.Count()
	return nil
}

func (bat *Batch) Shrink(sels []int64, negate bool, mp *mpool.MPool) error {
	if !negate {
		if len(sels) == bat.rowCount {
			return nil
		}
	}
	for _, vec := range bat.Vecs {
		if err := vec.Shrink(sels, negate, mp); err != nil {
			return err
		}
	}
	if negate {
		bat.rowCount -= len(sels)
		return nil
	}
	bat.rowCount = len(sels)
	return nil
}

func (bat *Batch) Shuffle(sels []int64, m *mpool.MPool) error {
	if len(sels) > 0 {
		mp := make(map[*vector.Vector]uint8)
		for _, vec := range bat.Vecs {
			if _, ok := mp[vec]; ok {
				continue
			}
			mp[vec]++
			if err := vec.Shuffle(sels, m); err != nil {
				return err
			}
		}
		bat.rowCount = len(sels)
	}
	return nil
}

func (bat *Batch) Size() int {
	var size int

	for _, vec := range bat.Vecs {
		size += vec.Size()
	}
	return size
}

func (bat *Batch) RowCount() int {
	return bat.rowCount
}

func (bat *Batch) VectorCount() int {
	return len(bat.Vecs)
}

// SetAccountedExtraBuffer transfers one accounted byte buffer into the Batch.
// Clean releases it exactly once; MoveExtraBufferFrom preserves ownership when
// a pipeline spool moves the payload to another Batch object.
func (bat *Batch) SetAccountedExtraBuffer(buffer *mpool.AccountedBuffer) error {
	if bat == nil || buffer == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	data, pool, err := buffer.Detach()
	if err != nil {
		return err
	}
	bat.releaseExtraBuf()
	bat.ExtraBuf = data
	bat.extraBufMP = pool
	return nil
}

func (bat *Batch) HasAccountedExtraBuffer() bool {
	return bat != nil && bat.extraBufMP != nil
}

func (bat *Batch) MoveExtraBufferFrom(source *Batch) {
	if bat == nil || source == nil || bat == source {
		return
	}
	bat.releaseExtraBuf()
	bat.ExtraBuf = source.ExtraBuf
	bat.extraBufMP = source.extraBufMP
	source.ExtraBuf = nil
	source.extraBufMP = nil
}

// DropExtraBuffer releases an accounted payload or drops an ordinary Go-heap
// payload. It is used by reusable pipeline batches before entering a cache.
func (bat *Batch) DropExtraBuffer() {
	if bat != nil {
		bat.releaseExtraBuf()
	}
}

func (bat *Batch) releaseExtraBuf() {
	if bat.extraBufMP != nil && cap(bat.ExtraBuf) != 0 {
		bat.extraBufMP.Free(bat.ExtraBuf)
	}
	bat.ExtraBuf = nil
	bat.extraBufMP = nil
}

// OwnReadOnlyVectors replaces every read-only (reference) vector of bat with
// a copy in mp, so the batch can be mutated in place. A vector shared by
// several columns is copied once.
func (bat *Batch) OwnReadOnlyVectors(mp *mpool.MPool) error {
	var copied map[*vector.Vector]*vector.Vector
	for i, vec := range bat.Vecs {
		if vec == nil || !vec.IsReadOnly() {
			continue
		}
		if owned, ok := copied[vec]; ok {
			bat.Vecs[i] = owned
			continue
		}
		owned, err := vec.Dup(mp)
		if err != nil {
			return err
		}
		if copied == nil {
			copied = make(map[*vector.Vector]*vector.Vector)
		}
		copied[vec] = owned
		bat.Vecs[i] = owned
	}
	for vec := range copied {
		vec.Free(nil)
	}
	return nil
}

// errForeignReceiver reports a decode receiver vector owned by a pool other
// than the decode pool: the batch's owner does not own it, so it must not be
// freed or reused here.
func errForeignReceiver() error {
	return moerr.NewInternalErrorNoCtx("batch decode receiver vector belongs to another pool")
}

// dropReadOnlyReceiver drops a read-only receiver's references; a receiver
// owned by any pool is an error.
func dropReadOnlyReceiver(vec *vector.Vector) error {
	if !vec.IsReadOnly() {
		return errForeignReceiver()
	}
	vec.Free(nil)
	return nil
}

// freeReceivers releases the decode receivers before the vector count
// changes: receivers of mp are freed, read-only ones dropped, and a receiver
// of another pool is an error.
func (bat *Batch) freeReceivers(mp *mpool.MPool) error {
	for _, vec := range bat.Vecs {
		if vec != nil && !vec.IsReadOnly() && vec.MPool() != mp {
			return errForeignReceiver()
		}
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.Free(mp)
		}
	}
	bat.Vecs = nil
	return nil
}

// FreeWindow releases a batch returned by Window. Its vectors are read-only
// references, so this only drops them; it panics on an owned vector.
func (bat *Batch) FreeWindow() {
	if bat == nil {
		return
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.Free(nil)
		}
	}
	bat.Vecs = nil
	bat.rowCount = 0
}

// CleanOwned frees each vector with the pool it belongs to. It is for test
// fixtures that assemble batches from several pools. Production code must not
// use it: an owner frees only the vectors it created, with its own pool.
func (bat *Batch) CleanOwned() {
	if bat == nil {
		return
	}
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.Free(vec.MPool())
		}
	}
	bat.Vecs = nil
	bat.rowCount = 0
}

func (bat *Batch) InsertVector(pos int32, vec *vector.Vector) {
	bat.Vecs = append(bat.Vecs, nil)
	copy(bat.Vecs[pos+1:], bat.Vecs[pos:])
	bat.Vecs[pos] = vec
}

func (bat *Batch) SetVector(pos int32, vec *vector.Vector) {
	bat.Vecs[pos] = vec
}

func (bat *Batch) GetVector(pos int32) *vector.Vector {
	return bat.Vecs[pos]
}

func (bat *Batch) CloneSelectedColumns(
	selectCols []int,
	mp *mpool.MPool,
) (cloned *Batch, err error) {
	cloned = NewWithSize(len(selectCols))
	var typ types.Type
	for idx := range selectCols {
		cloned.Vecs[idx] = vector.NewVec(typ, mp)
	}
	if err = bat.CloneSelectedColumnsTo(selectCols, cloned, mp); err != nil {
		cloned.Clean(mp)
		cloned = nil
		return
	}
	return
}

func (bat *Batch) CloneSelectedColumnsTo(
	selectCols []int,
	toBat *Batch,
	mp *mpool.MPool,
) (err error) {
	for idx, sourceIdx := range selectCols {
		toVec := toBat.Vecs[idx]
		toVec.ResetWithNewType(bat.Vecs[sourceIdx].GetType())
		if err = toVec.UnionBatch(
			bat.Vecs[sourceIdx],
			0,
			bat.Vecs[sourceIdx].Length(),
			nil,
			mp,
		); err != nil {
			return
		}
		toVec.SetSorted(bat.Vecs[sourceIdx].GetSorted())

		if toVec.Length() != bat.rowCount {
			return moerr.NewInternalErrorNoCtx("toVec.Length() != bat.rowCount")
		}
	}
	toBat.rowCount = bat.rowCount
	return nil
}

// ViewInto makes dst a view of bat: the same vectors, attributes, row count
// and selection. dst owns nothing it references (no vectors, leases or
// ExtraBuf); it is valid while bat is, and is released by dropping it.
func (bat *Batch) ViewInto(dst *Batch) {
	dst.Recursive = bat.Recursive
	dst.ShuffleIDX = bat.ShuffleIDX
	dst.Vecs = append(dst.Vecs[:0], bat.Vecs...)
	dst.ExtraBuf = bat.ExtraBuf
	dst.rowCount = bat.rowCount
	dst.selection = bat.selection
	dst.physicalRows = bat.physicalRows
}

func (bat *Batch) SelectColumns(cols []int) *Batch {
	rbat := NewWithSize(len(cols))
	for i, col := range cols {
		rbat.Vecs[i] = bat.Vecs[col]
	}
	rbat.rowCount = bat.rowCount
	return rbat
}

func (bat *Batch) Clean(m *mpool.MPool) {
	// situations that batch was still in use.
	if bat == EmptyBatch || bat == CteEndBatch || bat == EmptyForConstFoldBatch {
		return
	}

	for i, vec := range bat.Vecs {
		if vec != nil {
			bat.SetVector(int32(i), nil)
			vec.Free(m)
		}
	}
	bat.leases.Release()

	bat.Vecs = nil
	bat.releaseExtraBuf()
	bat.SetRowCount(0)
	bat.selection, bat.physicalRows = nil, 0
}

func (bat *Batch) Last() bool {
	return bat.Recursive > 0
}

func (bat *Batch) SetEnd() {
	bat.Recursive = 2
}

func (bat *Batch) SetLast() {
	bat.Recursive = 1
}

func (bat *Batch) End() bool {
	return bat.Recursive == 2
}

func (bat *Batch) CleanOnlyData() {
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.CleanOnlyData()
		}
	}
	bat.leases.Release()
	bat.rowCount = 0
	bat.selection, bat.physicalRows = nil, 0
}

func (bat *Batch) FreeColumns(m *mpool.MPool) {
	for _, vec := range bat.Vecs {
		if vec != nil {
			vec.Free(m)
		}
	}
	bat.leases.Release()
}

// HoldLease makes the batch the lender of lease: it keeps one reference until
// the batch next drops or reuses its column data. Vectors installed into this
// batch may alias lease's bytes until then.
func (bat *Batch) HoldLease(lease bufferlease.BufferLease) error {
	if bat == nil {
		return moerr.NewInternalErrorNoCtx("cannot hold a buffer lease in a nil batch")
	}
	return bat.leases.Hold(lease)
}

// MoveLeasesFrom transfers source's lease references to bat, together with
// the vectors (or aliases of them) the caller moves from source to bat. bat's
// previous references are released first: the caller has already dropped or
// replaced the columns that aliased them.
func (bat *Batch) MoveLeasesFrom(source *Batch) {
	if bat == nil || source == nil || bat == source {
		return
	}
	bat.leases.Release()
	bat.leases, source.leases = source.leases, bat.leases
}

// HeldLeases reports how many lease references the batch holds.
func (bat *Batch) HeldLeases() int {
	if bat == nil {
		return 0
	}
	return bat.leases.Len()
}

// ReleaseLeases drops the batch's lease references. The caller guarantees no
// vector of the batch still aliases a lent backing (they were reset, freed or
// materialized).
func (bat *Batch) ReleaseLeases() {
	if bat != nil {
		bat.leases.Release()
	}
}

func (bat *Batch) String() string {
	var buf bytes.Buffer

	for i, vec := range bat.Vecs {
		buf.WriteString(fmt.Sprintf("%d : %s\n", i, vec.String()))
	}
	return buf.String()
}

// Types returns a copy of the types of bat's vectors.
func (bat *Batch) Types() []types.Type {
	attrTypes := make([]types.Type, len(bat.Vecs))
	for i, vec := range bat.Vecs {
		attrTypes[i] = *vec.GetType()
	}
	return attrTypes
}

// Clone copies bat into a new batch whose vectors all come from mp.
func (bat *Batch) Clone(mp *mpool.MPool) (*Batch, error) {
	cloned := NewWithTypes(bat.Types(), mp)
	cloned.Recursive = bat.Recursive
	err := bat.CloneTo(cloned, mp)
	if err != nil {
		return nil, err
	}
	return cloned, nil
}

func (bat *Batch) CloneTo(toBat *Batch, mp *mpool.MPool) (err error) {
	for i, srcVec := range bat.Vecs {
		toVec := toBat.Vecs[i]
		toVec.ResetWithNewType(srcVec.GetType())
		if err = toVec.UnionBatch(srcVec, 0, srcVec.Length(), nil, mp); err != nil {
			toBat.Clean(mp)
			return
		}
		toVec.SetSorted(srcVec.GetSorted())
	}
	toBat.rowCount = bat.rowCount
	toBat.ShuffleIDX = bat.ShuffleIDX

	return
}

// Dup used to copy a Batch object, this method will create a new batch
// and copy all vectors (Vecs) of the current batch to the new batch.
func (bat *Batch) Dup(mp *mpool.MPool) (*Batch, error) {
	return bat.Clone(mp)
}

func (bat *Batch) Union(bat2 *Batch, sels []int64, m *mpool.MPool) error {
	for i, vec := range bat.Vecs {
		if err := vec.Union(bat2.Vecs[i], sels, m); err != nil {
			return err
		}
	}
	if len(bat.Vecs) > 0 {
		bat.rowCount = bat.Vecs[0].Length()
	}
	return nil
}

func (bat *Batch) UnionWindow(bat2 *Batch, offset, cnt int, m *mpool.MPool) error {
	for i, vec := range bat.Vecs {
		if err := vec.UnionBatch(bat2.Vecs[i], int64(offset), cnt, nil, m); err != nil {
			return err
		}
	}
	bat.rowCount += cnt
	return nil
}

func (bat *Batch) UnionOne(bat2 *Batch, pos int64, m *mpool.MPool) error {
	for i, vec := range bat.Vecs {
		if err := vec.UnionOne(bat2.Vecs[i], pos, m); err != nil {
			return err
		}
	}
	bat.rowCount++
	return nil
}

func (bat *Batch) PreExtend(mp *mpool.MPool, rows int) error {
	for i := range bat.Vecs {
		if err := bat.Vecs[i].PreExtend(rows, mp); err != nil {
			return err
		}
	}
	return nil
}

// AppendWithCopy is used to append data from batch `b` to another batch `bat`. The function
// ensures that the batch structure is consistent and copies all vector data to the target batch.
// WARING: this function will cause a memory allocation.
func (bat *Batch) AppendWithCopy(ctx context.Context, mp *mpool.MPool, b *Batch) (*Batch, error) {
	if bat == nil {
		return b.Dup(mp)
	}
	if len(bat.Vecs) != len(b.Vecs) {
		return nil, moerr.NewInternalError(ctx, "unexpected error happens in batch append")
	}
	if len(bat.Vecs) == 0 {
		return bat, nil
	}

	for i := range bat.Vecs {
		if err := bat.Vecs[i].UnionBatch(b.Vecs[i], 0, b.Vecs[i].Length(), nil, mp); err != nil {
			return bat, err
		}
		bat.Vecs[i].SetSorted(false)
	}
	bat.rowCount += b.rowCount
	return bat, nil
}

func (bat *Batch) Append(ctx context.Context, mp *mpool.MPool, b *Batch) (*Batch, error) {
	if bat == nil {
		return b, nil
	}
	if len(bat.Vecs) != len(b.Vecs) {
		return nil, moerr.NewInternalError(ctx, "unexpected error happens in batch append")
	}
	if len(bat.Vecs) == 0 {
		return bat, nil
	}

	for i := range bat.Vecs {
		if err := bat.Vecs[i].UnionBatch(b.Vecs[i], 0, b.Vecs[i].Length(), nil, mp); err != nil {
			return bat, err
		}
		bat.Vecs[i].SetSorted(false)
	}
	bat.rowCount += b.rowCount
	return bat, nil
}

func (bat *Batch) AddRowCount(rowCount int) {
	bat.rowCount += rowCount
}

func (bat *Batch) SetRowCount(rowCount int) {
	bat.rowCount = rowCount
	bat.selection, bat.physicalRows = nil, 0
}

func (bat *Batch) ReplaceVector(oldVec *vector.Vector, newVec *vector.Vector, startIndex int) {
	for i := startIndex; i < len(bat.Vecs); i++ {
		if bat.Vecs[i] == oldVec {
			bat.SetVector(int32(i), newVec)
		}
	}
}

func (bat *Batch) IsEmpty() bool {
	return bat.rowCount == 0
}

func (bat *Batch) IsDone() bool {
	if bat == nil {
		return true
	}
	return bat.IsEmpty() || bat.Last()
}

func (bat *Batch) Allocated() int {
	if bat == nil {
		return 0
	}
	ret := 0
	for i := range bat.Vecs {
		if bat.Vecs[i] != nil {
			ret += bat.Vecs[i].Allocated()
		}
	}
	return ret
}

func (bat *Batch) Window(start, end int) (*Batch, error) {
	if bat == nil || start < 0 || end < start || end > bat.RowCount() {
		return nil, moerr.NewInvalidInputNoCtx("invalid batch window")
	}
	b := NewWithSize(len(bat.Vecs))
	var err error
	for i, vec := range bat.Vecs {
		if vec == nil {
			b.FreeWindow()
			return nil, moerr.NewInvalidInputNoCtx("invalid batch vector")
		}
		// Each window is a read-only reference to its source vector.
		b.Vecs[i], err = vec.WindowByLogicalRows(start, end)
		if err != nil {
			b.FreeWindow()
			return nil, err
		}
	}
	b.rowCount = end - start
	return b, nil
}
