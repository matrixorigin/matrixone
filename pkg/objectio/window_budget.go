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
	spill          ColumnWindowSpill
}

func (p *blockWindowPart) close() {
	if p.data != nil {
		p.data.Release()
		p.data = nil
	}
	if p.spill != nil {
		_ = p.spill.Close()
		p.spill = nil
	}
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

func appendBlockWindowVector(dst **vector.Vector, part *vector.Vector, mp *mpool.MPool) error {
	if *dst == nil {
		*dst = part
		return nil
	}
	defer part.Free(mp)
	if *part.GetType() != *(*dst).GetType() {
		return moerr.NewInvalidInputNoCtx("chunked object column payload type mismatch")
	}
	return (*dst).UnionBatch(part, 0, part.Length(), nil, mp)
}

// ReadOneBlockAllColumnsBudgetedWindow sizes and materializes one extent at a
// time. The admitted prefix only shrinks as columns are added;
// previous output vectors are compacted if a later column narrows it. This
// keeps at most one decoded chunk source and one spill file live at a time. One
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
		var vec *vector.Vector
		newRows := rows
		err := forEachBlockWindowPart(
			ctx, name, ext, int(block.GetRows()), offset, rows,
			uint64(maxBytes), policy, fs, spillFactory,
			func(part *blockWindowPart) (bool, error) {
				start := max(offset, part.firstRow)
				end := min(offset+newRows, part.firstRow+part.rows)
				if start >= end {
					return true, nil
				}
				if err := part.addCosts(start-part.firstRow, costs[start-offset:end-offset]); err != nil {
					return false, err
				}
				used := uint64(0)
				for row, cost := range costs[:newRows] {
					if row > 0 && (used >= uint64(maxBytes) || cost > uint64(maxBytes)-used) {
						newRows = row
						break
					}
					used += cost
				}
				if err := ctx.Err(); err != nil {
					return false, err
				}
				if materializeEnd := min(end, offset+newRows); start < materializeEnd {
					partVec, err := part.materialize(start-part.firstRow, materializeEnd-start, mp)
					if err != nil {
						return false, err
					}
					if err = appendBlockWindowVector(&vec, partVec, mp); err != nil {
						return false, err
					}
				}
				return newRows < rows, nil
			},
		)
		if err == nil && (vec == nil || vec.Length() != newRows) {
			err = moerr.NewInvalidInputNoCtx("object column window row count mismatch")
		}
		if err != nil {
			if vec != nil {
				vec.Free(mp)
			}
			bat.Clean(mp)
			return nil, 0, err
		}
		for j := 0; j < i; j++ {
			if bat.Vecs[j].Length() > newRows {
				compact, cloneErr := bat.Vecs[j].CloneWindow(0, newRows, mp)
				if cloneErr != nil {
					vec.Free(mp)
					bat.Clean(mp)
					return nil, 0, cloneErr
				}
				bat.Vecs[j].Free(mp)
				bat.Vecs[j] = compact
			}
		}
		bat.Vecs[i] = vec
		rows = newRows
	}
	bat.SetRowCount(rows)
	return bat, rows, nil
}

// Each part is released before the next chunk is loaded. Chunk metadata caps
// each decoded source at columnChunkTargetBytes, so chunked columns need no
// spill even when several chunks overlap the requested window.
func forEachBlockWindowPart(
	ctx context.Context, name string, ext Extent, totalRows, offset, candidateRows int,
	maxSourceBytes uint64, policy fileservice.Policy, fs fileservice.FileService,
	spillFactory ColumnWindowSpillFactory,
	visit func(*blockWindowPart) (bool, error),
) error {
	loadAndVisit := func(ext Extent, firstRow, rows int, chunked bool) (bool, error) {
		part, err := loadBlockWindowPart(ctx, name, ext, firstRow, rows,
			maxSourceBytes, policy, fs, spillFactory, chunked)
		if err != nil {
			part.close()
			return false, err
		}
		defer part.close()
		return visit(&part)
	}
	if ext.Alg() != compress.Lz4Chunked {
		_, err := loadAndVisit(ext, 0, totalRows, false)
		return err
	}
	prefixExtent := NewExtent(compress.None, ext.Offset(), columnChunkHeaderSize, columnChunkHeaderSize)
	prefix, err := readCachedColumnRange(ctx, name, prefixExtent, policy, fs)
	if err != nil {
		return err
	}
	headerSize, err := chunkedColumnHeaderReadSize(prefix.Bytes(), ext.Length())
	prefix.Release()
	if err != nil {
		return err
	}
	headerExtent := NewExtent(compress.None, ext.Offset(), uint32(headerSize), uint32(headerSize))
	header, err := readCachedColumnRange(ctx, name, headerExtent, policy, fs)
	if err != nil {
		return err
	}
	rows, metas, err := parseColumnChunkHeader(header.Bytes(), ext.Length())
	header.Release()
	if err != nil {
		return err
	}
	if int(rows) != totalRows {
		return moerr.NewInvalidInputNoCtx("chunked object column row count mismatch")
	}
	for _, meta := range metas {
		first, last := int(meta.rowStart), int(meta.rowStart+meta.rowCount)
		if last <= offset || first >= offset+candidateRows {
			continue
		}
		chunkExtent := NewExtent(meta.algorithm, ext.Offset()+meta.offset,
			meta.length, meta.originSize)
		stop, err := loadAndVisit(chunkExtent, first, int(meta.rowCount), true)
		if err != nil {
			return err
		}
		if stop {
			break
		}
	}
	return nil
}

func loadBlockWindowPart(
	ctx context.Context, name string, ext Extent, firstRow, rows int,
	maxSourceBytes uint64, policy fileservice.Policy,
	fs fileservice.FileService, spillFactory ColumnWindowSpillFactory, chunked bool,
) (blockWindowPart, error) {
	part := blockWindowPart{firstRow: firstRow, rows: rows, originSize: int64(ext.OriginSize())}
	if !chunked && uint64(ext.OriginSize()) > maxSourceBytes {
		if spillFactory == nil {
			return part, moerr.NewInternalErrorNoCtx("legacy object column spill is unavailable")
		}
		file, err := spillFactory(ctx)
		if err != nil {
			return part, err
		}
		if file == nil {
			return part, moerr.NewInternalErrorNoCtx("legacy object column spill is nil")
		}
		part.spill = file
		if err := streamLegacyColumnToSpill(ctx, name, ext, fs, file); err != nil {
			return part, err
		}
		return part, nil
	}
	data, err := readCachedColumnRange(ctx, name, ext, policy, fs)
	if err != nil {
		return part, err
	}
	part.data = data
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
