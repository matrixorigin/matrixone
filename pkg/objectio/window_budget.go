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
	"context"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

type blockWindowPart struct {
	firstRow, rows int
	originSize     int64
	data           fscache.Data
	spill          io.ReaderAt
}

func (p *blockWindowPart) close() {
	if p.data != nil {
		p.data.Release()
		p.data = nil
	}
	p.spill = nil
}

func (p *blockWindowPart) addCosts(offset int, costs []uint64) error {
	if p.spill != nil {
		return addLegacyColumnWindowCosts(p.spill, p.originSize, offset, costs)
	}
	return addCachedColumnWindowCosts(p.data, offset, costs)
}

func (p *blockWindowPart) materialize(offset, length int, mp *mpool.MPool) (*vector.Vector, error) {
	if p.spill != nil {
		return materializeLegacyColumnWindow(p.spill, p.originSize, offset, length, mp)
	}
	return MaterializeCachedVectorWindow(p.data, offset, length, mp)
}

type blockWindowColumn struct {
	parts []blockWindowPart
}

// One file owns all spilled chunks of the current column. The file is closed
// before loading the next column, so wide tables use at most one spill FD.
type blockWindowSpill struct {
	file  ColumnWindowSpill
	bytes int64
}

type blockWindowSpillWriter struct {
	owner   *blockWindowSpill
	written int64
}

func (w *blockWindowSpillWriter) Write(data []byte) (int, error) {
	n, err := w.owner.file.Write(data)
	w.written += int64(n)
	return n, err
}

func (w *blockWindowSpillWriter) Grow(size uint64) error { return w.owner.file.Grow(size) }
func (w *blockWindowSpillWriter) ReadAt(data []byte, offset int64) (int, error) {
	return w.owner.file.ReadAt(data, offset)
}
func (w *blockWindowSpillWriter) Close() error { return nil }

func (s *blockWindowSpill) appendColumn(
	ctx context.Context, name string, ext Extent, fs fileservice.FileService,
) (io.ReaderAt, error) {
	writer := &blockWindowSpillWriter{owner: s}
	if err := streamLegacyColumnToSpill(ctx, name, ext, fs, writer); err != nil {
		return nil, err
	}
	section := io.NewSectionReader(s.file, s.bytes, writer.written)
	s.bytes += writer.written
	return section, nil
}

func (c *blockWindowColumn) close() {
	for i := range c.parts {
		c.parts[i].close()
	}
}

func (c *blockWindowColumn) addCosts(offset int, costs []uint64) error {
	for i := range c.parts {
		part := &c.parts[i]
		start := max(offset, part.firstRow)
		end := min(offset+len(costs), part.firstRow+part.rows)
		if start < end {
			if err := part.addCosts(start-part.firstRow, costs[start-offset:end-offset]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *blockWindowColumn) materialize(offset, length int, mp *mpool.MPool) (*vector.Vector, error) {
	var dst *vector.Vector
	for i := range c.parts {
		part := &c.parts[i]
		start := max(offset, part.firstRow)
		end := min(offset+length, part.firstRow+part.rows)
		if start >= end {
			continue
		}
		vec, err := part.materialize(start-part.firstRow, end-start, mp)
		if err != nil {
			if dst != nil {
				dst.Free(mp)
			}
			return nil, err
		}
		if dst == nil {
			dst = vec
		} else {
			if *vec.GetType() != *dst.GetType() {
				vec.Free(mp)
				dst.Free(mp)
				return nil, moerr.NewInvalidInputNoCtx("chunked object column payload type mismatch")
			}
			if err = dst.UnionBatch(vec, 0, vec.Length(), nil, mp); err != nil {
				vec.Free(mp)
				dst.Free(mp)
				return nil, err
			}
			vec.Free(mp)
		}
	}
	if dst == nil || dst.Length() != length {
		if dst != nil {
			dst.Free(mp)
		}
		return nil, moerr.NewInvalidInputNoCtx("object column window row count mismatch")
	}
	return dst, nil
}

// ReadOneBlockAllColumnsBudgetedWindow sizes and materializes each column from
// one decoded source. The admitted prefix only shrinks as columns are added;
// previous output vectors are compacted if a later column narrows it. This
// keeps at most one column source and one spill file live at a time. One
// oversized row is admitted to make progress.
func ReadOneBlockAllColumnsBudgetedWindow(
	ctx context.Context, meta *ObjectDataMeta, name string, id uint32,
	cols []uint16, offset, candidateRows, maxBytes int,
	policy fileservice.Policy, fs fileservice.FileService, mp *mpool.MPool,
	spillFactory ColumnWindowSpillFactory,
) (*batch.Batch, int, error) {
	if candidateRows <= 0 || maxBytes <= 0 {
		return nil, 0, moerr.NewInvalidInputNoCtx("object block window budget must be positive")
	}
	if mp == nil {
		return nil, 0, moerr.NewInvalidInputNoCtx("nil mpool for object block window")
	}
	block := meta.GetBlockMeta(id)
	if offset < 0 || offset > int(block.GetRows())-candidateRows {
		return nil, 0, moerr.NewInvalidInputNoCtx("object block window is out of range")
	}
	costs := make([]uint64, candidateRows)
	rows := candidateRows
	bat := batch.NewWithSize(len(cols))
	for i, seqnum := range cols {
		if err := ctx.Err(); err != nil {
			bat.Clean(mp)
			return nil, 0, err
		}
		ext := block.ColumnMeta(seqnum).Location()
		var retained uint64
		var spill *blockWindowSpill
		source, err := loadBlockWindowColumn(
			ctx, name, ext, int(block.GetRows()), offset, rows,
			uint64(maxBytes), &retained, &spill, policy, fs, spillFactory,
		)
		if err != nil {
			if spill != nil {
				_ = spill.file.Close()
			}
			bat.Clean(mp)
			return nil, 0, err
		}
		var vec *vector.Vector
		if err = source.addCosts(offset, costs[:rows]); err == nil {
			newRows, used := rows, uint64(0)
			for row, cost := range costs[:rows] {
				if row > 0 && (used >= uint64(maxBytes) || cost > uint64(maxBytes)-used) {
					newRows = row
					break
				}
				used += cost
			}
			if err = ctx.Err(); err == nil {
				vec, err = source.materialize(offset, newRows, mp)
			}
			if err == nil {
				for j := 0; j < i && err == nil; j++ {
					if bat.Vecs[j].Length() > newRows {
						var compact *vector.Vector
						compact, err = bat.Vecs[j].CloneWindow(0, newRows, mp)
						if err == nil {
							bat.Vecs[j].Free(mp)
							bat.Vecs[j] = compact
						}
					}
				}
				if err == nil {
					bat.Vecs[i] = vec
					rows = newRows
				}
			}
		}
		source.close()
		if spill != nil {
			_ = spill.file.Close()
		}
		if err != nil {
			if vec != nil {
				vec.Free(mp)
			}
			bat.Clean(mp)
			return nil, 0, err
		}
	}
	bat.SetRowCount(rows)
	return bat, rows, nil
}

func loadBlockWindowColumn(
	ctx context.Context, name string, ext Extent, totalRows, offset, candidateRows int,
	maxSourceBytes uint64, retained *uint64, spill **blockWindowSpill, policy fileservice.Policy,
	fs fileservice.FileService, spillFactory ColumnWindowSpillFactory,
) (_ *blockWindowColumn, err error) {
	column := &blockWindowColumn{}
	defer func() {
		if err != nil {
			column.close()
		}
	}()
	if ext.Alg() != compress.Lz4Chunked {
		part, loadErr := loadBlockWindowPart(ctx, name, ext, 0, totalRows,
			maxSourceBytes, retained, spill, policy, fs, spillFactory, false)
		if loadErr != nil {
			return nil, loadErr
		}
		column.parts = append(column.parts, part)
		return column, nil
	}
	prefixExtent := NewExtent(compress.None, ext.Offset(), columnChunkHeaderSize, columnChunkHeaderSize)
	prefix, err := readCachedColumnRange(ctx, name, prefixExtent, policy, fs)
	if err != nil {
		return nil, err
	}
	headerSize, err := chunkedColumnHeaderReadSize(prefix.Bytes(), ext.Length())
	prefix.Release()
	if err != nil {
		return nil, err
	}
	headerExtent := NewExtent(compress.None, ext.Offset(), uint32(headerSize), uint32(headerSize))
	header, err := readCachedColumnRange(ctx, name, headerExtent, policy, fs)
	if err != nil {
		return nil, err
	}
	rows, metas, err := parseColumnChunkHeader(header.Bytes(), ext.Length())
	header.Release()
	if err != nil {
		return nil, err
	}
	if int(rows) != totalRows {
		return nil, moerr.NewInvalidInputNoCtx("chunked object column row count mismatch")
	}
	for _, meta := range metas {
		first, last := int(meta.rowStart), int(meta.rowStart+meta.rowCount)
		if last <= offset || first >= offset+candidateRows {
			continue
		}
		chunkExtent := NewExtent(meta.algorithm, ext.Offset()+meta.offset,
			meta.length, meta.originSize)
		part, loadErr := loadBlockWindowPart(ctx, name, chunkExtent, first,
			int(meta.rowCount), maxSourceBytes, retained, spill, policy, fs, spillFactory, true)
		if loadErr != nil {
			return nil, loadErr
		}
		column.parts = append(column.parts, part)
	}
	return column, nil
}

func loadBlockWindowPart(
	ctx context.Context, name string, ext Extent, firstRow, rows int,
	maxSourceBytes uint64, retained *uint64, spillStore **blockWindowSpill, policy fileservice.Policy,
	fs fileservice.FileService, spillFactory ColumnWindowSpillFactory, chunked bool,
) (blockWindowPart, error) {
	part := blockWindowPart{firstRow: firstRow, rows: rows, originSize: int64(ext.OriginSize())}
	size := uint64(ext.OriginSize())
	spill := size > maxSourceBytes || (spillFactory != nil && *retained+size > maxSourceBytes)
	if chunked && spillFactory == nil {
		// Preserve the existing chunked reader's source allowance for callers
		// without a spill factory, but never retain an unbounded number of
		// decoded chunks while sizing a single column.
		if *retained+size > max(maxSourceBytes, uint64(2*columnChunkTargetBytes)) {
			return part, moerr.NewInvalidInputNoCtx("chunked object column window requires spill")
		}
		spill = false
	}
	if spill {
		if spillFactory == nil {
			return part, moerr.NewInternalErrorNoCtx("legacy object column spill is unavailable")
		}
		if *spillStore == nil {
			file, err := spillFactory(ctx)
			if err != nil {
				return part, err
			}
			if file == nil {
				return part, moerr.NewInternalErrorNoCtx("legacy object column spill is nil")
			}
			*spillStore = &blockWindowSpill{file: file}
		}
		reader, err := (*spillStore).appendColumn(ctx, name, ext, fs)
		if err != nil {
			return part, err
		}
		part.spill = reader
		return part, nil
	}
	data, err := readCachedColumnRange(ctx, name, ext, policy, fs)
	if err != nil {
		return part, err
	}
	part.data = data
	*retained += size
	return part, nil
}

func readCachedColumnRange(
	ctx context.Context, name string, ext Extent, policy fileservice.Policy,
	fs fileservice.FileService,
) (fscache.Data, error) {
	ioVec := &fileservice.IOVector{
		FilePath: name,
		Entries: []fileservice.IOEntry{{
			Offset: int64(ext.Offset()), Size: int64(ext.Length()),
			CachedDataSize: int64(ext.OriginSize()),
			ToCacheData:    constructorFactory(int64(ext.OriginSize()), ext.Alg()),
		}},
		Policy: policy,
	}
	if err := fs.Read(ctx, ioVec); err != nil {
		ioVec.ReleaseReadResultOnError()
		return nil, err
	}
	data := ioVec.Entries[0].CachedData
	if data == nil {
		ioVec.Release()
		return nil, moerr.NewInternalErrorNoCtx("object column cache data is unavailable")
	}
	data.Retain()
	ioVec.Release()
	return data, nil
}

func addCachedColumnWindowCosts(data fscache.Data, offset int, costs []uint64) error {
	var source vector.Vector
	if err := bindCachedVectorForScope(&source, data); err != nil {
		return err
	}
	defer source.Free(nil)
	if offset < 0 || offset > source.Length()-len(costs) {
		return moerr.NewInvalidInputNoCtx("object column window is out of range")
	}
	for i := range costs {
		costs[i] += uint64(source.GetType().TypeSize())
		if source.GetType().IsVarlen() && !source.IsNull(uint64(offset+i)) {
			// Charging every selected value is conservative for shared area
			// references and bounds the compact materialization as well.
			costs[i] += uint64(len(source.GetBytesAt(offset + i)))
		}
	}
	return nil
}
