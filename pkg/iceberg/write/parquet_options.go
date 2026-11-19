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

package write

import (
	"strings"
	"sync"

	kzstd "github.com/klauspost/compress/zstd"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/compress/brotli"
	"github.com/parquet-go/parquet-go/compress/gzip"
	"github.com/parquet-go/parquet-go/compress/lz4"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/compress/uncompressed"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

const parquetCompressionProperty = "write.parquet.compression-codec"

// ParquetOptions is a validated snapshot of table data-file options. Its zero value uses Iceberg defaults.
type ParquetOptions struct{ codec string }

func ParseParquetOptions(properties map[string]string) (ParquetOptions, error) {
	value, ok := properties[parquetCompressionProperty]
	if !ok {
		return ParquetOptions{}, nil
	}
	name := strings.ToLower(strings.TrimSpace(value))
	switch name {
	case "zstd", "snappy", "gzip", "brotli", "lz4", "uncompressed":
		return ParquetOptions{codec: name}, nil
	default:
		return ParquetOptions{}, api.NewError(api.ErrConfigInvalid, "invalid Iceberg Parquet compression codec", map[string]string{"property": parquetCompressionProperty, "value": value})
	}
}

func (o ParquetOptions) compression() compress.Codec {
	switch o.codec {
	case "snappy":
		return &snappy.Codec{}
	case "gzip":
		return &gzip.Codec{}
	case "brotli":
		return &brotli.Codec{}
	case "lz4":
		return &lz4.Codec{}
	case "uncompressed":
		return &uncompressed.Codec{}
	default:
		return &limitedZstd{}
	}
}

// limitedZstd holds one lazily initialized encoder per writer owner. EncodeAll
// permits concurrent calls and the embedded codec supplies standard decoding.
type limitedZstd struct {
	zstd.Codec
	once sync.Once
	enc  *kzstd.Encoder
	err  error
}

func (c *limitedZstd) Encode(dst, src []byte) ([]byte, error) {
	c.once.Do(func() {
		c.enc, c.err = kzstd.NewWriter(nil, kzstd.WithEncoderLevel(kzstd.SpeedDefault),
			kzstd.WithWindowSize(128<<10), kzstd.WithLowerEncoderMem(true),
			kzstd.WithEncoderConcurrency(1), kzstd.WithZeroFrames(true), kzstd.WithEncoderCRC(false))
	})
	if c.err != nil {
		return dst[:0], c.err
	}
	return c.enc.EncodeAll(src, dst[:0]), nil
}
