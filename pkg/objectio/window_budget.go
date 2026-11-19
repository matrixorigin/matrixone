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
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// FitOneBlockAllColumnsWindow counts the decoded bytes of the candidate rows
// before materializing them. A single oversized row is returned by itself so
// callers can still make progress and spill that row.
func FitOneBlockAllColumnsWindow(
	ctx context.Context, meta *ObjectDataMeta, name string, id uint32,
	cols []uint16, offset, maxRows, maxBytes int,
	policy fileservice.Policy, fs fileservice.FileService,
	spillFactory ColumnWindowSpillFactory,
) (int, error) {
	if maxRows <= 0 || maxBytes <= 0 {
		return 0, moerr.NewInvalidInputNoCtx("object block window budget must be positive")
	}
	costs := make([]uint64, maxRows)
	for _, seqnum := range cols {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		ext := meta.GetBlockMeta(id).ColumnMeta(seqnum).Location()
		var err error
		if ext.Alg() == compress.Lz4Chunked {
			err = addChunkedColumnWindowCosts(ctx, name, ext, offset, costs, maxBytes, policy, fs, spillFactory)
		} else {
			err = addColumnWindowCosts(ctx, name, ext, offset, costs, maxBytes, policy, fs, spillFactory)
		}
		if err != nil {
			return 0, err
		}
	}
	used := uint64(0)
	for i, cost := range costs {
		if i > 0 && cost > uint64(maxBytes)-min(used, uint64(maxBytes)) {
			return i, nil
		}
		used += cost
	}
	return maxRows, nil
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

func addColumnWindowCosts(
	ctx context.Context, name string, ext Extent, offset int, costs []uint64,
	maxSourceBytes int, policy fileservice.Policy, fs fileservice.FileService,
	spillFactory ColumnWindowSpillFactory,
) error {
	if int64(ext.OriginSize()) > int64(maxSourceBytes) {
		if spillFactory == nil {
			return moerr.NewInternalErrorNoCtx("legacy object column spill is unavailable")
		}
		spill, err := spillFactory(ctx)
		if err != nil {
			return err
		}
		if spill == nil {
			return moerr.NewInternalErrorNoCtx("legacy object column spill is nil")
		}
		defer spill.Close()
		if err := streamLegacyColumnToSpill(ctx, name, ext, fs, spill); err != nil {
			return err
		}
		return addLegacyColumnWindowCosts(spill, int64(ext.OriginSize()), offset, costs)
	}
	data, err := readCachedColumnRange(ctx, name, ext, policy, fs)
	if err != nil {
		return err
	}
	defer data.Release()
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
			costs[i] += uint64(len(source.GetBytesAt(offset + i)))
		}
	}
	return nil
}

func addChunkedColumnWindowCosts(
	ctx context.Context, name string, ext Extent, offset int, costs []uint64,
	maxSourceBytes int, policy fileservice.Policy, fs fileservice.FileService,
	spillFactory ColumnWindowSpillFactory,
) error {
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
	totalRows, metas, err := parseColumnChunkHeader(header.Bytes(), ext.Length())
	header.Release()
	if err != nil {
		return err
	}
	if offset < 0 || offset > int(totalRows)-len(costs) {
		return moerr.NewInvalidInputNoCtx("chunked object column window is out of range")
	}
	for _, chunk := range metas {
		chunkStart, chunkEnd := int(chunk.rowStart), int(chunk.rowStart+chunk.rowCount)
		start, end := max(offset, chunkStart), min(offset+len(costs), chunkEnd)
		if start >= end {
			continue
		}
		chunkExtent := NewExtent(chunk.algorithm, ext.Offset()+chunk.offset, chunk.length, chunk.originSize)
		// Chunked readers already bound their decoded source to one chunk.
		// Inspect that same chunk here without requiring a legacy spill.
		if err := addColumnWindowCosts(ctx, name, chunkExtent, start-chunkStart,
			costs[start-offset:end-offset], max(maxSourceBytes, int(chunk.originSize)), policy, fs, spillFactory); err != nil {
			return err
		}
	}
	return nil
}
