// Copyright 2026 Matrix Origin
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

package external

import (
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/crt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
)

// MemoryRecordBytes bounds a record by its input, when that input has a known
// uncompressed size. LOCAL streams and compressed files retain the configured
// limit. CsvReader enforces the same bound before growing its record buffer,
// including when a file changes after planning.
func (external *External) MemoryRecordBytes() int64 {
	param, ok := external.memoryInputParam()
	if !ok {
		return int64(config.LargestEntryLimit)
	}
	return csvRecordBytes(&external.Es.ExParamConst, param)
}

func csvRecordBytes(source *ExParamConst, param *tree.ExternParam) int64 {
	limit := int64(config.LargestEntryLimit)
	if param.Local {
		return limit
	}
	if param.ScanType == tree.INLINE {
		if crt.GetCompressType(param.CompressType, param.Filepath) != tree.NOCOMPRESS {
			return limit
		}
		return min(limit, max(1, int64(len(param.Data))))
	}
	if len(source.FileList) == 0 || len(source.FileList) != len(source.FileSize) {
		return limit
	}
	var size int64
	for i, path := range source.FileList {
		if source.FileSize[i] < 0 || crt.GetCompressType(param.CompressType, path) != tree.NOCOMPRESS {
			return limit
		}
		size = max(size, source.FileSize[i])
	}
	return min(limit, max(1, size))
}

func recordByteLimit(inputBytes int64) int64 {
	if inputBytes <= 0 {
		return int64(config.LargestEntryLimit)
	}
	return min(inputBytes, int64(config.LargestEntryLimit))
}

// SourceEnvelopeForFields describes allocations that can coexist while one CSV
// row is converted. recordBytes is a proven input bound, capped by the configured
// entry limit; zero keeps that limit. retainedFields matches CSVParser.RetainFields.
// The factors below are allocation-table bounds, not runtime tuning controls:
// append old+new (3), byte parser scratch (10), and JSON tree nodes (64).
func SourceEnvelopeForFields(schema []types.Type, unit, recordBytes int64, retainedFields int) (int64, error) {
	if unit <= 0 || retainedFields < 0 || config.LargestEntryLimit <= 0 {
		return 0, moerr.NewInternalErrorNoCtx("invalid CSV memory envelope")
	}
	limit := recordByteLimit(recordBytes)
	row, err := MaxRowBytes(schema, limit)
	if err != nil {
		return 0, err
	}
	var jsonScratch int64
	for _, typ := range schema {
		if typ.Oid == types.T_json {
			jsonScratch = 64 * limit
		}
	}
	q := max(unit, row)
	// Parser record/readUntil buffers may each have old+new backing storage;
	// escape/starting-by buffers and the fixed read blocks coexist with them.
	raw := 10*limit + 4*csvparser.ReadBlockSize*csvparser.BufferSizeScale
	// Field metadata: indexes, quoted flags and LastRow Field. Each append may
	// have old+new backing arrays. Extra ignored fields have no stored metadata.
	fieldWidth := int64(unsafe.Sizeof(int(0)) + unsafe.Sizeof(bool(false)) + unsafe.Sizeof(csvparser.Field{}))
	fields := 3 * int64(retainedFields) * fieldWidth
	// One typed batch and complete new backing stores; array parsing retains
	// one numeric array plus its split-string views while the typed append runs.
	parts := []int64{3 * q, 2 * row, raw, fields, jsonScratch, 16 * limit}
	var total int64
	for _, part := range parts {
		if part < 0 || total > math.MaxInt64-part {
			return 0, moerr.NewInternalErrorNoCtx("CSV memory envelope overflow")
		}
		total += part
	}
	return total, nil
}

// MaxRowBytes is the typed payload bound for one bounded raw record.
func MaxRowBytes(schema []types.Type, recordBytes int64) (int64, error) {
	limit := recordByteLimit(recordBytes)
	if limit <= 0 {
		return 0, moerr.NewInternalErrorNoCtx("invalid CSV raw record limit")
	}
	var fixed, factor int64
	for _, typ := range schema {
		if int64(typ.TypeSize()) > math.MaxInt64-fixed {
			return 0, moerr.NewInternalErrorNoCtx("CSV row memory envelope overflow")
		}
		fixed += int64(typ.TypeSize())
		switch typ.Oid {
		case types.T_array_float64:
			factor = max(factor, 4)
		case types.T_array_float32:
			factor = max(factor, 2)
		case types.T_json:
			factor = max(factor, 16)
		default:
			if typ.IsVarlen() {
				factor = max(factor, 1)
			}
		}
	}
	if factor > 0 && limit > (math.MaxInt64-fixed)/factor {
		return 0, moerr.NewInternalErrorNoCtx("CSV row memory envelope overflow")
	}
	return fixed + factor*limit, nil
}
