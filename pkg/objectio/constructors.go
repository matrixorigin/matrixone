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
)

type CacheConstructor = func(r io.Reader, buf []byte) (any, int64, error)
type CacheConstructorFactory = func(size int64, algo uint8, noHeaderHint bool) CacheConstructor

// use this to replace all other constructors
func constructorFactory(size int64, algo uint8, noHeaderHint bool) CacheConstructor {
	return func(reader io.Reader, data []byte) (any, int64, error) {
		fn := func() ([]byte, int64, error) {
			var err error
			if len(data) == 0 {
				data, err = io.ReadAll(reader)
				if err != nil {
					return nil, 0, err
				}
			}

			// no compress
			if algo == 0 {
				return data, int64(len(data)), nil
			}

			// lz4 compress
			decompressed := make([]byte, size)
			decompressed, err = compress.Decompress(data, decompressed, compress.Lz4)
			if err != nil {
				return nil, 0, err
			}
			return decompressed, int64(len(decompressed)), nil
		}
		buf, size, err := fn()
		if noHeaderHint || err != nil {
			return buf, size, err
		}

		header := DecodeIOEntryHeader(buf)
		codec := GetIOEntryCodec(*header)
		if codec.NoUnmarshal() {
			return buf[IOEntryHeaderSize:], size, err
		}
		v, err := codec.Decode(buf[IOEntryHeaderSize:])
		if err != nil {
			return nil, 0, err
		}
		return v, size, nil
	}
}
