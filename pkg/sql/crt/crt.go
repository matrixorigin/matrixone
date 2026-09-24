// Copyright 2022 - 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package crt implements common runtime for colexec.
// Utilities, IO, and other common functions.
package crt

import (
	"archive/tar"
	"bytes"
	"compress/bzip2"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"io"
	"math"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/pierrec/lz4/v4"
)

// GetIOReadCloserSimple is a simple function to get an io.ReadCloser from a string.
// We construct an ExternParam that has only info related to reader.
func GetIOReadCloserSimple(proc *process.Process, inline bool, data string) (io.ReadCloser, error) {
	var param tree.ExternParam
	if inline {
		param.ScanType = tree.INLINE
		param.Data = data
	} else {
		param.ScanType = tree.INFILE
		param.FileService = proc.Base.FileService
		// FileStartOff is 0
		param.Filepath = data
	}

	// is this filepath thing duplicated unnecessarily?
	return GetIOReadCloser(proc, &param, data, nil, math.MaxInt64)
}

func GetIOReadCloser(proc *process.Process, param *tree.ExternParam, data string,
	fileOffsets []int64, fileSizeMax int64) (io.ReadCloser, error) {
	// inline data
	if param.ScanType == tree.INLINE {
		return io.NopCloser(bytes.NewReader([]byte(data))), nil
	}

	// local data, only used in load local ...,
	if param.Local {
		return io.NopCloser(proc.GetLoadLocalReader()), nil
	}

	fs, readPath, err := plan2.GetForETLWithType(param, data)
	if err != nil {
		return nil, err
	}

	var r io.ReadCloser
	vec := fileservice.IOVector{
		FilePath: readPath,
		// LOAD DATA reads each source file exactly once; caching its blocks
		// only wastes memory cache space and adds GC pressure.
		Policy: fileservice.SkipAllCache,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            param.FileStartOff,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}

	// adjust read offset for parallel load.
	if param.Parallel {
		vec.Entries[0].Offset = fileOffsets[0]
		vec.Entries[0].Size = fileOffsets[1] - fileOffsets[0]
	}
	if vec.Entries[0].Size == 0 || vec.Entries[0].Offset >= fileSizeMax {
		return nil, nil
	}

	// XXX: read the file, old code uses param.Ctx, but WHY do we need a
	// context in param?   Use proc.Ctx instead.
	err = fs.Read(proc.Ctx, &vec)
	return r, err
}

// GetCompressType reports how a load source is compressed; see
// plan.GetCompressType, which planning uses too.
func GetCompressType(compressType string, filepath string) string {
	return plan2.GetCompressType(compressType, filepath)
}

func getTarReader(ctx context.Context, r io.Reader) (io.ReadCloser, error) {
	tarReader := tar.NewReader(r)
	// move to first file
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil, moerr.NewInternalError(ctx, "failed to decompress the file, no available files found")
		}
		if err != nil {
			return nil, err
		}
		if !header.FileInfo().IsDir() && !strings.HasPrefix(header.FileInfo().Name(), ".") {
			break
		}
	}
	return io.NopCloser(tarReader), nil
}

// decompressReader reads decompressed bytes and owns its source: Close
// releases the decoder and then the source.  Without it the file or object
// stream under a compressed load stayed open until the garbage collector ran
// its finalizer, because the decoders' own Close does not close their input.
type decompressReader struct {
	io.Reader
	closeDecoder func() error
	src          io.Closer
}

func (d *decompressReader) Close() error {
	var err error
	if d.closeDecoder != nil {
		err = d.closeDecoder()
	}
	if cerr := d.src.Close(); err == nil {
		err = cerr
	}
	return err
}

func decompressed(r io.Reader, closeDecoder func() error, src io.Closer) io.ReadCloser {
	return &decompressReader{Reader: r, closeDecoder: closeDecoder, src: src}
}

// getUnCompressReader wraps r in the decompressor for its compression type.
// On success the returned reader owns r; on error r is still the caller's.
func getUnCompressReader(ctx context.Context, compType string, filepath string, r io.ReadCloser) (io.ReadCloser, error) {
	switch GetCompressType(compType, filepath) {
	case tree.NOCOMPRESS:
		return r, nil
	case tree.GZIP, tree.GZ:
		gz, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return decompressed(gz, gz.Close, r), nil
	case tree.BZIP2, tree.BZ2:
		return decompressed(bzip2.NewReader(r), nil, r), nil
	case tree.FLATE:
		fl := flate.NewReader(r)
		return decompressed(fl, fl.Close, r), nil
	case tree.ZLIB:
		zl, err := zlib.NewReader(r)
		if err != nil {
			return nil, err
		}
		return decompressed(zl, zl.Close, r), nil
	case tree.LZ4:
		return decompressed(lz4.NewReader(r), nil, r), nil
	case tree.ZSTD:
		// One decoding goroutine per reader, like lz4's default: a parallel
		// load already runs one reader per file, and the default would start
		// GOMAXPROCS decoders for each of them.  Close stops the decoder.
		zd, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}
		return decompressed(zd, func() error { zd.Close(); return nil }, r), nil
	case tree.ZIP:
		zr, closeDecoder, err := getZipReader(ctx, r)
		if err != nil {
			return nil, err
		}
		return decompressed(zr, closeDecoder, r), nil
	case tree.LZW:
		return nil, moerr.NewInternalErrorf(ctx, "the compress type '%s' is not support now", compType)
	case tree.TAR_GZ:
		gzipReader, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		tr, err := getTarReader(ctx, gzipReader)
		if err != nil {
			return nil, err
		}
		return decompressed(tr, gzipReader.Close, r), nil
	case tree.TAR_BZ2:
		tr, err := getTarReader(ctx, bzip2.NewReader(r))
		if err != nil {
			return nil, err
		}
		return decompressed(tr, nil, r), nil
	default:
		return nil, moerr.NewInternalErrorf(ctx, "the compress type '%s' is not support now", compType)
	}
}

func GetUnCompressReader(proc *process.Process, compType string, filepath string, r io.ReadCloser) (io.ReadCloser, error) {
	r, err := getUnCompressReader(proc.Ctx, compType, filepath, r)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	return r, nil
}
