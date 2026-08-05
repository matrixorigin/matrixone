// Copyright 2026 Matrix Origin
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

package objectio

import (
	"bufio"
	"context"
	"errors"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

const legacyColumnStreamBufferSize = 64 << 10

// ColumnWindowSpill is a caller-owned temporary file used to range-materialize
// legacy whole-column extents without retaining their complete decoded bytes.
type ColumnWindowSpill interface {
	io.ReaderAt
	io.Writer
	io.Closer
	Grow(uint64) error
}

type ColumnWindowSpillFactory func(context.Context) (ColumnWindowSpill, error)

type legacyColumnLayout struct {
	version                  uint16
	class                    byte
	typ                      types.Type
	rows                     int
	dataOffset, dataLength   int64
	areaOffset, areaLength   int64
	nullsOffset, nullsLength int64
}

type reservedSpillWriter struct {
	ctx   context.Context
	spill ColumnWindowSpill
	bytes uint64
}

func (w *reservedSpillWriter) Write(data []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	if err := w.spill.Grow(uint64(len(data))); err != nil {
		return 0, err
	}
	n, err := w.spill.Write(data)
	w.bytes += uint64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}

func streamLegacyColumnToSpill(
	ctx context.Context,
	name string,
	ext Extent,
	fs fileservice.FileService,
	spill ColumnWindowSpill,
) error {
	var source io.ReadCloser
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries: []fileservice.IOEntry{{
			Offset: int64(ext.Offset()), Size: int64(ext.Length()),
			ReadCloserForRead: &source,
		}},
		Policy: fileservice.SkipAllCache,
	}
	if err := fs.Read(ctx, ioVec); err != nil {
		ioVec.ReleaseReadResultOnError()
		return err
	}
	if source == nil {
		return moerr.NewInternalErrorNoCtx("legacy object column stream is unavailable")
	}
	defer source.Close()

	reserved := &reservedSpillWriter{ctx: ctx, spill: spill}
	output := bufio.NewWriterSize(reserved, legacyColumnStreamBufferSize)
	var err error
	switch ext.Alg() {
	case compress.None:
		_, err = io.CopyBuffer(output, source, make([]byte, legacyColumnStreamBufferSize))
	case compress.Lz4:
		err = decodeLegacyLZ4Block(ctx, bufio.NewReaderSize(source, legacyColumnStreamBufferSize), output, int64(ext.OriginSize()))
	default:
		err = moerr.NewInvalidInputNoCtx("unsupported legacy object column compression")
	}
	if err != nil {
		return err
	}
	if err = output.Flush(); err != nil {
		return err
	}
	if reserved.bytes != uint64(ext.OriginSize()) {
		return moerr.NewInvalidInputNoCtx("legacy object column decoded size mismatch")
	}
	return nil
}

func decodeLegacyLZ4Block(
	ctx context.Context,
	source *bufio.Reader,
	destination io.Writer,
	expected int64,
) error {
	if expected < 0 {
		return moerr.NewInvalidInputNoCtx("invalid legacy object column size")
	}
	history := make([]byte, 1<<16)
	produced := int64(0)
	emit := func(data []byte) error {
		if int64(len(data)) > expected-produced {
			return moerr.NewInvalidInputNoCtx("legacy LZ4 output exceeds declared size")
		}
		for _, value := range data {
			history[produced&0xffff] = value
			produced++
		}
		_, err := destination.Write(data)
		return err
	}
	readLength := func(base int) (int, error) {
		length := base
		if base != 15 {
			return length, nil
		}
		for {
			value, err := source.ReadByte()
			if err != nil {
				return 0, err
			}
			if length > int(expected)-int(value) {
				return 0, moerr.NewInvalidInputNoCtx("legacy LZ4 length exceeds declared size")
			}
			length += int(value)
			if value != 255 {
				return length, nil
			}
		}
	}
	literals := make([]byte, legacyColumnStreamBufferSize)
	for produced < expected {
		if err := ctx.Err(); err != nil {
			return err
		}
		token, err := source.ReadByte()
		if err != nil {
			return err
		}
		literalLength, err := readLength(int(token >> 4))
		if err != nil {
			return err
		}
		for literalLength > 0 {
			count := min(literalLength, len(literals))
			if _, err = io.ReadFull(source, literals[:count]); err != nil {
				return err
			}
			if err = emit(literals[:count]); err != nil {
				return err
			}
			literalLength -= count
		}
		if produced == expected {
			break
		}
		var offsetBytes [2]byte
		if _, err = io.ReadFull(source, offsetBytes[:]); err != nil {
			return err
		}
		offset := int64(offsetBytes[0]) | int64(offsetBytes[1])<<8
		if offset == 0 || offset > produced || offset > int64(len(history)) {
			return moerr.NewInvalidInputNoCtx("invalid legacy LZ4 match offset")
		}
		matchLength, err := readLength(int(token & 0x0f))
		if err != nil {
			return err
		}
		matchLength += 4
		for matchLength > 0 {
			count := min(matchLength, len(literals))
			for i := 0; i < count; i++ {
				literals[i] = history[(produced-offset)&0xffff]
				history[produced&0xffff] = literals[i]
				produced++
			}
			if produced > expected {
				return moerr.NewInvalidInputNoCtx("legacy LZ4 output exceeds declared size")
			}
			if _, err = destination.Write(literals[:count]); err != nil {
				return err
			}
			matchLength -= count
		}
	}
	if _, err := source.ReadByte(); !errors.Is(err, io.EOF) {
		if err == nil {
			return moerr.NewInvalidInputNoCtx("legacy LZ4 input has trailing bytes")
		}
		return err
	}
	return nil
}

func readLegacyColumnLayout(source io.ReaderAt, originSize int64) (legacyColumnLayout, error) {
	const prefixSize = IOEntryHeaderSize + 1 + types.TSize + 4 + 4
	var layout legacyColumnLayout
	prefix := make([]byte, prefixSize)
	if _, err := source.ReadAt(prefix, 0); err != nil {
		return layout, err
	}
	header := DecodeIOEntryHeader(prefix)
	if header.Type != IOET_ColData ||
		(header.Version != IOET_ColumnData_V1 && header.Version != IOET_ColumnData_V2) {
		return layout, moerr.NewInvalidInputNoCtx("invalid legacy object column header")
	}
	offset := IOEntryHeaderSize
	layout.version = header.Version
	layout.class = prefix[offset]
	offset++
	layout.typ = types.DecodeType(prefix[offset : offset+types.TSize])
	offset += types.TSize
	layout.rows = int(types.DecodeUint32(prefix[offset : offset+4]))
	offset += 4
	layout.dataLength = int64(types.DecodeUint32(prefix[offset : offset+4]))
	layout.dataOffset = int64(prefixSize)
	areaLengthOffset := layout.dataOffset + layout.dataLength
	areaLengthBytes := make([]byte, 4)
	if _, err := source.ReadAt(areaLengthBytes, areaLengthOffset); err != nil {
		return layout, err
	}
	layout.areaLength = int64(types.DecodeUint32(areaLengthBytes))
	layout.areaOffset = areaLengthOffset + 4
	nullsLengthOffset := layout.areaOffset + layout.areaLength
	nullsLengthBytes := make([]byte, 4)
	if _, err := source.ReadAt(nullsLengthBytes, nullsLengthOffset); err != nil {
		return layout, err
	}
	layout.nullsLength = int64(types.DecodeUint32(nullsLengthBytes))
	layout.nullsOffset = nullsLengthOffset + 4
	if layout.rows < 0 || layout.class > vector.DIST || layout.typ.TypeSize() <= 0 ||
		layout.nullsOffset+layout.nullsLength+1 != originSize {
		return layout, moerr.NewInvalidInputNoCtx("invalid legacy object column layout")
	}
	expectedData := int64(layout.typ.TypeSize())
	if layout.class != vector.CONSTANT {
		expectedData *= int64(layout.rows)
	} else if layout.dataLength == 0 {
		expectedData = 0
	}
	if layout.dataLength != expectedData ||
		layout.nullsLength > int64(max(1024, layout.rows*32+1024)) {
		return layout, moerr.NewInvalidInputNoCtx("invalid legacy object column buffers")
	}
	return layout, nil
}

func readLegacyColumnNulls(source io.ReaderAt, layout legacyColumnLayout) (nulls.Nulls, error) {
	var result nulls.Nulls
	if layout.nullsLength == 0 {
		return result, nil
	}
	data := make([]byte, layout.nullsLength)
	if _, err := source.ReadAt(data, layout.nullsOffset); err != nil {
		return result, err
	}
	var err error
	if layout.version == IOET_ColumnData_V1 {
		err = result.ReadNoCopyV1(data)
	} else {
		err = result.ReadNoCopy(data)
	}
	return result, err
}

func materializeLegacyColumnWindow(
	source io.ReaderAt,
	originSize int64,
	offset, length int,
	mp *mpool.MPool,
) (*vector.Vector, error) {
	layout, err := readLegacyColumnLayout(source, originSize)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length <= 0 || offset > layout.rows-length {
		return nil, moerr.NewInvalidInputNoCtx("legacy object column window is out of range")
	}
	sourceNulls, err := readLegacyColumnNulls(source, layout)
	if err != nil {
		return nil, err
	}
	isNull := func(row int) bool {
		if layout.class == vector.CONSTANT {
			row = 0
		}
		return sourceNulls.Contains(uint64(row))
	}
	dst := vector.NewVec(layout.typ)
	failed := true
	defer func() {
		if failed {
			dst.Free(mp)
		}
	}()
	if layout.typ.IsVarlen() {
		descriptorSize := layout.typ.TypeSize()
		for row := offset; row < offset+length; row++ {
			sourceRow := row
			if layout.class == vector.CONSTANT {
				sourceRow = 0
			}
			descriptor := make([]byte, descriptorSize)
			if _, err = source.ReadAt(descriptor, layout.dataOffset+int64(sourceRow*descriptorSize)); err != nil {
				return nil, err
			}
			if isNull(row) {
				if err = vector.AppendBytes(dst, nil, true, mp); err != nil {
					return nil, err
				}
				continue
			}
			value := types.DecodeSlice[types.Varlena](descriptor)[0]
			var payload []byte
			if value.IsSmall() {
				payload = value.GetByteSlice(nil)
			} else {
				areaOffset, areaLength := value.OffsetLen()
				if uint64(areaOffset)+uint64(areaLength) > uint64(layout.areaLength) {
					return nil, moerr.NewInvalidInputNoCtx("invalid legacy object column varlen offset")
				}
				payload = make([]byte, areaLength)
				if _, err = source.ReadAt(payload, layout.areaOffset+int64(areaOffset)); err != nil {
					return nil, err
				}
			}
			if err = vector.AppendBytes(dst, payload, false, mp); err != nil {
				return nil, err
			}
		}
	} else {
		rowsToRead := length
		sourceRow := offset
		if layout.class == vector.CONSTANT {
			rowsToRead = 1
			sourceRow = 0
		}
		if err = dst.PreExtend(rowsToRead, mp); err != nil {
			return nil, err
		}
		dataSize := rowsToRead * layout.typ.TypeSize()
		if dataSize > 0 {
			if _, err = source.ReadAt(dst.GetData()[:dataSize], layout.dataOffset+int64(sourceRow*layout.typ.TypeSize())); err != nil {
				return nil, err
			}
		}
		dst.SetLength(length)
		dst.SetClass(int(layout.class))
		for row := 0; row < length; row++ {
			if isNull(offset + row) {
				if layout.class == vector.CONSTANT {
					dst.SetNull(0)
					break
				}
				dst.SetNull(uint64(row))
			}
		}
	}
	failed = false
	return dst, nil
}

func readLegacyColumnWindow(
	ctx context.Context,
	name string,
	ext Extent,
	offset, length int,
	fs fileservice.FileService,
	mp *mpool.MPool,
	spillFactory ColumnWindowSpillFactory,
) (*vector.Vector, error) {
	if spillFactory == nil {
		return nil, moerr.NewInternalErrorNoCtx("legacy object column spill is unavailable")
	}
	spill, err := spillFactory(ctx)
	if err != nil {
		return nil, err
	}
	if spill == nil {
		return nil, moerr.NewInternalErrorNoCtx("legacy object column spill is nil")
	}
	defer spill.Close()
	if err = streamLegacyColumnToSpill(ctx, name, ext, fs, spill); err != nil {
		return nil, err
	}
	return materializeLegacyColumnWindow(spill, int64(ext.OriginSize()), offset, length, mp)
}
