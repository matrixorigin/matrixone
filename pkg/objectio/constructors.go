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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type CacheConstructor = func(r io.Reader, buf []byte) (any, int64, error)
type CacheConstructorFactory = func(size int64) CacheConstructor

func objectMetaConstructorFactory(size int64) CacheConstructor {
	return func(reader io.Reader, data []byte) (any, int64, error) {
		// decompress
		var err error
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return nil, 0, err
			}
		}
		decompressed := make([]byte, size)
		decompressed, err = compress.Decompress(data, decompressed, compress.Lz4)
		if err != nil {
			return nil, 0, err
		}
		objectMeta := ObjectMeta(decompressed)
		objectMeta.BlockHeader().MetaLocation().SetLength(uint32(len(data)))
		return decompressed, int64(len(decompressed)), nil
	}
}

func noDecompressConstructorFactory(size int64) CacheConstructor {
	return func(reader io.Reader, data []byte) (any, int64, error) {
		if len(data) == 0 {
			var err error
			data, err = io.ReadAll(reader)
			if err != nil {
				return nil, 0, err
			}
		}
		return data, int64(len(data)), nil
	}
}

// decompressConstructorFactory the decompression function passed to fileservice
func decompressConstructorFactory(size int64) CacheConstructor {
	return func(reader io.Reader, data []byte) (any, int64, error) {
		// decompress
		var err error
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return nil, 0, err
			}
		}
		decompressed := make([]byte, size)
		decompressed, err = compress.Decompress(data, decompressed, compress.Lz4)
		if err != nil {
			return nil, 0, err
		}
		return decompressed, int64(len(decompressed)), nil
	}
}

func BloomFilterConstructorFactory(size int64) CacheConstructor {
	return func(reader io.Reader, data []byte) (any, int64, error) {
		// decompress
		var err error
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return nil, 0, err
			}
		}
		decompressed := make([]byte, size)
		decompressed, err = compress.Decompress(data, decompressed, compress.Lz4)
		if err != nil {
			return nil, 0, err
		}
		indexes := make([]StaticFilter, 0)
		bf := BloomFilter(decompressed)
		count := bf.BlockCount()
		for i := uint32(0); i < count; i++ {
			buf := bf.GetBloomFilter(i)
			if len(buf) == 0 {
				indexes = append(indexes, nil)
				continue
			}
			index, err := index.DecodeBloomFilter(bf.GetBloomFilter(i))
			if err != nil {
				return nil, 0, err
			}
			indexes = append(indexes, index)
		}
		return indexes, int64(len(decompressed)), nil
	}
}

func ColumnConstructorFactory(size int64) CacheConstructor {
	return func(reader io.Reader, data []byte) (any, int64, error) {
		// decompress
		var err error
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return nil, 0, err
			}
		}
		decompressed := make([]byte, size)
		decompressed, err = compress.Decompress(data, decompressed, compress.Lz4)
		if err != nil {
			return nil, 0, err
		}
		vec := vector.NewVec(types.Type{})
		if err = vec.UnmarshalBinary(decompressed); err != nil {
			return nil, 0, err
		}
		return vec, int64(len(decompressed)), nil
	}
}
