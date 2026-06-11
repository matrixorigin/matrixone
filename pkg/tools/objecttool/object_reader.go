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

package objecttool

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
)

// ObjectInfo contains object metadata
type ObjectInfo struct {
	Path       string
	ObjectName objectio.ObjectName

	BlockCount uint32
	RowCount   uint64
	ColCount   uint16

	Size     uint32
	OrigSize uint32

	IsSorted     bool
	IsAppendable bool
	IsCNCreated  bool
}

// ColInfo contains column information
type ColInfo struct {
	Idx    uint16
	SeqNum uint16
	Type   types.Type
}

// ObjectReader reads object files
type ObjectReader struct {
	fs        fileservice.FileService
	reader    *ioutil.BlockReader
	objReader *objectio.ObjectReader
	meta      objectio.ObjectDataMeta
	info      *ObjectInfo
	cols      []ColInfo
	mp        *mpool.MPool
}

// Open opens an object file
func Open(ctx context.Context, path string) (*ObjectReader, error) {
	// 1. Parse path: directory and filename
	dir := "/"
	filename := path
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		dir = path[:idx]
		if dir == "" {
			dir = "/"
		}
		filename = path[idx+1:]
	}

	// 2. Create local file service
	fs, err := fileservice.NewLocalFS(ctx, "local", dir, fileservice.DisabledCacheConfig, nil)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "create file service: %v", err)
	}

	return OpenWithFS(ctx, fs, filename, path)
}

// OpenWithFS opens an object file from an existing file service.
func OpenWithFS(ctx context.Context, fs fileservice.FileService, fileName string, displayPath string) (*ObjectReader, error) {
	objReader, err := objectio.NewObjectReaderWithStr(fileName, fs,
		objectio.WithMetaCachePolicyOption(fileservice.SkipMemoryCache|fileservice.SkipFullFilePreloads))
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "create object reader: %v", err)
	}

	reader := &ioutil.BlockReader{}
	// Use reflection or direct creation, simplified here

	// 4. Read meta (ReadAllMeta automatically reads header to get extent)
	meta, err := objReader.ReadAllMeta(ctx, nil)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "load meta: %v", err)
	}
	dataMeta := meta.MustDataMeta()

	// 5. Build info
	info := buildObjectInfo(displayPath, dataMeta)
	cols := buildColInfo(dataMeta)

	return &ObjectReader{
		fs:        fs,
		reader:    reader,
		objReader: objReader,
		meta:      dataMeta,
		info:      info,
		cols:      cols,
		mp:        mpool.MustNewZero(),
	}, nil
}

func buildObjectInfo(path string, meta objectio.ObjectDataMeta) *ObjectInfo {
	info := &ObjectInfo{
		Path:       path,
		BlockCount: meta.BlockCount(),
		ColCount:   meta.BlockHeader().ColumnCount(),
	}

	// Calculate total row count
	for i := uint32(0); i < info.BlockCount; i++ {
		info.RowCount += uint64(meta.GetBlockMeta(i).GetRows())
	}

	return info
}

func buildColInfo(meta objectio.ObjectDataMeta) []ColInfo {
	colCount := meta.BlockHeader().ColumnCount()
	cols := make([]ColInfo, colCount)

	// Get column types from first block
	if meta.BlockCount() > 0 {
		blockMeta := meta.GetBlockMeta(0)
		for i := uint16(0); i < colCount; i++ {
			colMeta := blockMeta.ColumnMeta(i)
			cols[i] = ColInfo{
				Idx:    i,
				SeqNum: i,
				Type:   types.T(colMeta.DataType()).ToType(),
			}
		}
	}

	return cols
}

// Info returns object information
func (r *ObjectReader) Info() *ObjectInfo {
	return r.info
}

// Columns returns column information
func (r *ObjectReader) Columns() []ColInfo {
	return r.cols
}

// Meta returns the object data meta
func (r *ObjectReader) Meta() objectio.ObjectDataMeta {
	return r.meta
}

// ReadBlock reads data from specified block
func (r *ObjectReader) ReadBlock(ctx context.Context, blockIdx uint32) (*batch.Batch, func(), error) {
	if blockIdx >= r.info.BlockCount {
		return nil, nil, moerr.NewInternalErrorf(ctx, "block index %d out of range [0, %d)", blockIdx, r.info.BlockCount)
	}

	// Read all columns
	colIdxs := make([]uint16, len(r.cols))
	colTypes := make([]types.Type, len(r.cols))
	for i := range r.cols {
		colIdxs[i] = r.cols[i].SeqNum
		colTypes[i] = r.cols[i].Type
	}

	// Use objReader to read directly
	ioVectors, err := r.objReader.ReadOneBlock(ctx, colIdxs, colTypes, uint16(blockIdx), r.mp)
	if err != nil {
		return nil, nil, err
	}

	release := func() {
		objectio.ReleaseIOVector(&ioVectors)
	}

	// Decode to batch
	bat := batch.NewWithSize(len(colIdxs))
	for i := range colIdxs {
		obj, err := objectio.Decode(ioVectors.Entries[i].CachedData.Bytes())
		if err != nil {
			release()
			return nil, nil, err
		}
		bat.Vecs[i] = obj.(*vector.Vector)
		bat.SetRowCount(bat.Vecs[i].Length())
	}

	return bat, release, nil
}

// ReadBlockCommitTS reads the hidden commit timestamp column for a block.
// Objects that do not carry commit timestamps return a nil vector.
func (r *ObjectReader) ReadBlockCommitTS(ctx context.Context, blockIdx uint32) (*vector.Vector, func(), error) {
	if blockIdx >= r.info.BlockCount {
		return nil, nil, moerr.NewInternalErrorf(ctx, "block index %d out of range [0, %d)", blockIdx, r.info.BlockCount)
	}

	ioVectors, err := r.objReader.ReadOneBlock(
		ctx,
		[]uint16{objectio.SEQNUM_COMMITTS},
		[]types.Type{types.T_TS.ToType()},
		uint16(blockIdx),
		r.mp,
	)
	if err != nil {
		return nil, nil, err
	}

	release := func() {
		objectio.ReleaseIOVector(&ioVectors)
	}

	obj, err := objectio.Decode(ioVectors.Entries[0].CachedData.Bytes())
	if err != nil {
		release()
		return nil, nil, err
	}
	vec := obj.(*vector.Vector)
	if vec.GetType().Oid != types.T_TS {
		release()
		return nil, nil, moerr.NewInternalErrorf(ctx, "commit TS column type mismatch: expected TS, got %s", vec.GetType().String())
	}
	if vec.GetNulls().GetCardinality() == vec.Length() {
		release()
		return nil, func() {}, nil
	}
	return vec, release, nil
}

// BlockCount returns block count
func (r *ObjectReader) BlockCount() uint32 {
	return r.info.BlockCount
}

// Close closes the reader
func (r *ObjectReader) Close() error {
	return nil
}
