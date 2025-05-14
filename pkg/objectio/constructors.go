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
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type CacheConstructor = func(ctx context.Context, r io.Reader, buf []byte, allocator fileservice.CacheDataAllocator) (fscache.Data, error)
type CacheConstructorFactory = func(size int64, algo uint8) CacheConstructor

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
			return
		}
		decompressedData = decompressedData.Slice(len(bs))
		return decompressedData, nil
	}
}

func Decode(buf []byte) (any, error) {
	header := DecodeIOEntryHeader(buf)
	codec := GetIOEntryCodec(*header)
	if codec.NoUnmarshal() {
		return buf[IOEntryHeaderSize:], nil
	}
	v, err := codec.Decode(buf[IOEntryHeaderSize:])
	if err != nil {
		return nil, err
	}
	return v, nil
}

// NOTE: hack way to get vector
func MustVectorTo(toVec *vector.Vector, buf []byte) (err error) {
	// check if vector cannot be freed
	if !toVec.NeedDup() && toVec.Allocated() > 0 {
		logutil.Warn("input vector should be readonly or empty")
	}
	header := DecodeIOEntryHeader(buf)
	if header.Type != IOET_ColData {
		return moerr.NewInternalError(context.Background(), fmt.Sprintf("invalid object meta: %s", header.String()))
	}
	if header.Version == IOET_ColumnData_V2 {
		err = toVec.UnmarshalBinary(buf[IOEntryHeaderSize:])
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
