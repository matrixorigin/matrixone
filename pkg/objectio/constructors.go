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

package objectio

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

var eventVectorDestinationNotEmpty = logutil.Event{
	Name:    "objectio.vector.destination-not-empty",
	Message: "ObjectIO destination vector must be readonly or empty",
}

type CacheConstructor = func(ctx context.Context, r io.Reader, buf []byte, allocator fileservice.CacheDataAllocator) (fscache.Data, error)
type CacheConstructorFactory = func(size int64, algo uint8) CacheConstructor

func newColumnIOEntry(ext Extent, factory CacheConstructorFactory) fileservice.IOEntry {
	return fileservice.IOEntry{
		Offset:            int64(ext.Offset()),
		Size:              int64(ext.Length()),
		CachedDataSize:    int64(ext.OriginSize()),
		ToCacheData:       factory(int64(ext.OriginSize()), ext.Alg()),
		ValidateCacheData: validateVectorCacheData,
	}
}

// validatedVectorCacheData owns bytes that passed the full V2 vector validator
// after decompression. The backing Data is deliberately not embedded: the
// public FileService interface receives snapshots, while objectio's private
// method is the only zero-copy access to the sealed representation.
type validatedVectorCacheData struct {
	data fscache.Data
}

var _ fscache.Data = (*validatedVectorCacheData)(nil)

func (d *validatedVectorCacheData) Bytes() []byte {
	return bytes.Clone(d.validatedVectorBytes())
}

func (d *validatedVectorCacheData) Size() int64 {
	return int64(len(d.validatedVectorBytes()))
}

func (d *validatedVectorCacheData) Slice(length int) fscache.Data {
	buf := d.validatedVectorBytes()
	if length == len(buf) {
		return d
	}
	// A changed range gets independent storage and no validation capability.
	// In particular, do not call Data.Slice: fileservice.Bytes slices in place,
	// which would mutate this sealed owner and every existing alias.
	return fileservice.NewBytes(bytes.Clone(buf[:length]))
}

func (d *validatedVectorCacheData) Retain() {
	d.data.Retain()
}

func (d *validatedVectorCacheData) Release() {
	d.data.Release()
}

func (d *validatedVectorCacheData) validatedVectorBytes() []byte {
	return d.data.Bytes()
}

type validatedVectorCacheDataMarker interface {
	validatedVectorBytes() []byte
}

func isValidatedVectorCacheData(data fscache.Data) bool {
	_, ok := data.(validatedVectorCacheDataMarker)
	return ok
}

func vectorCacheDataBytes(data fscache.Data) (buf []byte, trusted bool) {
	if marked, ok := data.(validatedVectorCacheDataMarker); ok {
		return marked.validatedVectorBytes(), true
	}
	return data.Bytes(), false
}

// use this to replace all other constructors
func constructorFactory(size int64, algo uint8) CacheConstructor {
	return func(ctx context.Context, reader io.Reader, data []byte, allocator fileservice.CacheDataAllocator) (cacheData fscache.Data, err error) {
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return
			}
		}

		// no compress
		if algo == compress.None {
			cacheData = allocator.CopyToCacheData(ctx, data)
			return cacheData, nil
		}

		// lz4 compress
		decompressedData := allocator.AllocateCacheDataWithHint(ctx, int(size), malloc.NoClear)
		bs, err := compress.Decompress(data, decompressedData.Bytes(), compress.Lz4)
		if err != nil {
			decompressedData.Release()
			return
		}
		decompressedData = decompressedData.Slice(len(bs))
		return decompressedData, nil
	}
}

// columnCacheConstructorFactory validates V2 column data once, after
// decompression and before it can enter the memory cache. Cache hits can then
// bind the already-validated encoding without repeating linear value scans.
func columnCacheConstructorFactory(size int64, algo uint8) CacheConstructor {
	construct := constructorFactory(size, algo)
	return func(
		ctx context.Context,
		reader io.Reader,
		data []byte,
		allocator fileservice.CacheDataAllocator,
	) (fscache.Data, error) {
		cacheData, err := construct(ctx, reader, data, allocator)
		if err != nil {
			return nil, err
		}
		validated, err := validateVectorCacheData(cacheData)
		if err != nil {
			if cacheData != nil {
				cacheData.Release()
			}
			return nil, err
		}
		return validated, nil
	}
}

func validateVectorCacheData(data fscache.Data) (fscache.Data, error) {
	if data == nil {
		return nil, moerr.NewInvalidInputNoCtx("nil object column cache data")
	}
	if isValidatedVectorCacheData(data) {
		return data, nil
	}
	buf := data.Bytes()
	if len(buf) < IOEntryHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	header := DecodeIOEntryHeader(buf)
	if header.Type != IOET_ColData {
		return nil, moerr.NewInvalidInputNoCtx("invalid object column data type")
	}
	if header.Version == IOET_ColumnData_V1 {
		// V1 null bitmaps compute their count while decoding, so V1 cannot
		// provide the constant-time trusted contract. Keep it on the legacy
		// path without granting the marker.
		return data, nil
	}
	if header.Version != IOET_ColumnData_V2 {
		return nil, moerr.NewInvalidInputNoCtx("invalid object column data version")
	}
	var vec vector.Vector
	if err := vec.UnmarshalBinary(buf[IOEntryHeaderSize:]); err != nil {
		return nil, err
	}
	return &validatedVectorCacheData{data: data}, nil
}

func Decode(buf []byte) (any, error) {
	return decode(buf, false)
}

// DecodeCached uses the trusted V2 bind only for FileService cache data that
// objectio itself validated before cache admission. Unmarked data uses the
// normal versioned decoder; V2 therefore keeps its full validation.
func DecodeCached(data fscache.Data) (any, error) {
	if data == nil {
		return nil, moerr.NewInvalidInputNoCtx("nil object cache data")
	}
	buf, trusted := vectorCacheDataBytes(data)
	return decode(buf, trusted)
}

func decode(buf []byte, trusted bool) (any, error) {
	if len(buf) < IOEntryHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	header := DecodeIOEntryHeader(buf)
	codec := GetIOEntryCodec(*header)
	if codec.NoUnmarshal() {
		return buf[IOEntryHeaderSize:], nil
	}
	if trusted && header.Type == IOET_ColData && header.Version == IOET_ColumnData_V2 {
		vec := vector.NewVec(types.Type{})
		if err := vec.UnmarshalBinaryTrusted(buf[IOEntryHeaderSize:]); err != nil {
			return nil, err
		}
		return vec, nil
	}
	v, err := codec.Decode(buf[IOEntryHeaderSize:])
	if err != nil {
		return nil, err
	}
	return v, nil
}

// NOTE: hack way to get vector
func MustVectorTo(toVec *vector.Vector, buf []byte) (err error) {
	return mustVectorTo(toVec, buf, false)
}

// MustVectorToCached binds cache-backed column data to toVec. Only data with
// objectio's private validation marker uses the trusted path.
func MustVectorToCached(toVec *vector.Vector, data fscache.Data) error {
	if data == nil {
		return moerr.NewInvalidInputNoCtx("nil object cache data")
	}
	buf, trusted := vectorCacheDataBytes(data)
	return mustVectorTo(toVec, buf, trusted)
}

func mustVectorTo(toVec *vector.Vector, buf []byte, trusted bool) (err error) {
	// check if vector cannot be freed
	if !toVec.NeedDup() && toVec.Allocated() > 0 {
		eventVectorDestinationNotEmpty.WarnLazy(func() []zap.Field {
			return []zap.Field{
				zap.Bool("need-dup", toVec.NeedDup()),
				zap.Int("allocated-bytes", toVec.Allocated()),
				zap.Int("input-bytes", len(buf)),
			}
		})
	}
	if len(buf) < IOEntryHeaderSize {
		return io.ErrUnexpectedEOF
	}
	header := DecodeIOEntryHeader(buf)
	if header.Type != IOET_ColData {
		return moerr.NewInternalError(context.Background(), fmt.Sprintf("invalid object meta: %s", header.String()))
	}
	if header.Version == IOET_ColumnData_V2 {
		if trusted {
			err = toVec.UnmarshalBinaryTrusted(buf[IOEntryHeaderSize:])
		} else {
			err = toVec.UnmarshalBinary(buf[IOEntryHeaderSize:])
		}
		return
	} else if header.Version == IOET_ColumnData_V1 {
		err = toVec.UnmarshalBinaryV1(buf[IOEntryHeaderSize:])
		return
	}
	panic(fmt.Sprintf("invalid column data: %s", header.String()))
}

func MustObjectMeta(buffer []byte) ObjectMeta {
	header := DecodeIOEntryHeader(buffer)
	if header.Type != IOET_ObjMeta {
		panic(fmt.Sprintf("invalid object meta: %s", header.String()))
	}
	return ObjectMeta(buffer)
}
