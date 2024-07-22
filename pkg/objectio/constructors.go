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
	"io"

	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache"
)

type CacheConstructor = func(r io.Reader, buf []byte, allocator fileservice.CacheDataAllocator) (memorycache.CacheData, error)
type CacheConstructorFactory = func(size int64, algo uint8) CacheConstructor

// use this to replace all other constructors
func constructorFactory(size int64, algo uint8) CacheConstructor {
	return func(reader io.Reader, data []byte, allocator fileservice.CacheDataAllocator) (cacheData memorycache.CacheData, err error) {
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return
			}
		}

		// no compress
		if algo == compress.None {
			cacheData = allocator.Alloc(len(data))
			copy(cacheData.Bytes(), data)
			return cacheData, nil
		}

		// lz4 compress
		decompressed := allocator.Alloc(int(size))
		bs, err := compress.Decompress(data, decompressed.Bytes(), compress.Lz4)
		if err != nil {
			return
		}
		decompressed = decompressed.Slice(len(bs))
		return decompressed, nil
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
