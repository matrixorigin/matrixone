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

func GetCompressType(compressType string, filepath string) string {
	if compressType != "" && compressType != tree.AUTO {
		return compressType
	}

	filepath = strings.ToLower(filepath)

	switch {
	case strings.HasSuffix(filepath, ".tar.gz") || strings.HasSuffix(filepath, ".tar.gzip"):
		return tree.TAR_GZ
	case strings.HasSuffix(filepath, ".tar.bz2") || strings.HasSuffix(filepath, ".tar.bzip2"):
		return tree.TAR_BZ2
	case strings.HasSuffix(filepath, ".gz") || strings.HasSuffix(filepath, ".gzip"):
		return tree.GZIP
	case strings.HasSuffix(filepath, ".bz2") || strings.HasSuffix(filepath, ".bzip2"):
		return tree.BZIP2
	case strings.HasSuffix(filepath, ".lz4"):
		return tree.LZ4
	default:
		return tree.NOCOMPRESS
	}
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

func getUnCompressReader(ctx context.Context, compType string, filepath string, r io.ReadCloser) (io.ReadCloser, error) {
	switch strings.ToLower(GetCompressType(compType, filepath)) {
	case tree.NOCOMPRESS:
		return r, nil
	case tree.GZIP, tree.GZ:
		return gzip.NewReader(r)
	case tree.BZIP2, tree.BZ2:
		return io.NopCloser(bzip2.NewReader(r)), nil
	case tree.FLATE:
		return flate.NewReader(r), nil
	case tree.ZLIB:
		return zlib.NewReader(r)
	case tree.LZ4:
		return io.NopCloser(lz4.NewReader(r)), nil
	case tree.LZW:
		return nil, moerr.NewInternalErrorf(ctx, "the compress type '%s' is not support now", compType)
	case tree.TAR_GZ:
		gzipReader, err := gzip.NewReader(r)
		if err != nil {
			return nil, err
		}
		return getTarReader(ctx, gzipReader)
	case tree.TAR_BZ2:
		return getTarReader(ctx, bzip2.NewReader(r))
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
