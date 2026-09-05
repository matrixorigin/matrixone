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

package arrowbridge

import (
	"context"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// MaxOutputRows selects the largest non-empty prefix that fits the logical MO
// batch budget. A single oversized row is selected to guarantee planning
// progress; the canonical wire-size admission step still rejects it before
// execution if that row exceeds the hard statement budget. The estimate is
// deliberately conservative for materialized varlen dictionaries and exact
// for borrowed Arrow varlen areas.
func (p *Plan) MaxOutputRows(
	ctx context.Context,
	record arrow.RecordBatch,
	start int64,
	maxRows int,
	maxBytes uint64,
) (int, error) {
	if p == nil || record == nil || start < 0 || start >= record.NumRows() || maxRows <= 0 {
		return 0, moerr.NewInvalidInput(ctx, "invalid Arrow output budget input")
	}
	recordSchema := record.Schema()
	if recordSchema == nil {
		return 0, moerr.NewInvalidInput(ctx, "Arrow record schema is nil")
	}
	if schemaFingerprint(recordSchema) != p.schemaFingerprint {
		return 0, moerr.NewInvalidInput(ctx, "Arrow record schema does not match the bound schema")
	}
	// ReadBatch validates a newly received immutable record once before it is
	// split into output windows.  Keep this hot path to structural checks: a
	// full validity-bitmap scan here for every window turns one large record
	// into quadratic work. Convert validates each selected window instead.
	if err := validateRecordShape(ctx, record, recordSchema, p.columns); err != nil {
		return 0, err
	}
	available := record.NumRows() - start
	if int64(maxRows) > available {
		maxRows = int(available)
	}
	if maxBytes == 0 {
		return maxRows, nil
	}

	var used uint64
	for relativeRow := 0; relativeRow < maxRows; relativeRow++ {
		if err := checkConvertContext(ctx, relativeRow); err != nil {
			return 0, err
		}
		row := int(start) + relativeRow
		var rowBytes uint64
		for _, binding := range p.columns {
			column := record.Column(binding.source)
			bytes, err := estimateColumnRowBytes(ctx, column, binding, row)
			if err != nil {
				return 0, err
			}
			if rowBytes > math.MaxUint64-bytes {
				return 0, moerr.NewInvalidInput(ctx, "Arrow output row size overflows")
			}
			rowBytes += bytes
		}
		if used > maxBytes || rowBytes > maxBytes-used {
			if relativeRow == 0 {
				return 1, nil
			}
			return relativeRow, nil
		}
		used += rowBytes
	}
	return maxRows, nil
}

func validateRecordColumns(
	ctx context.Context,
	record arrow.RecordBatch,
	schema *arrow.Schema,
	columns []columnPlan,
) error {
	if err := validateRecordShape(ctx, record, schema, columns); err != nil {
		return err
	}
	for _, binding := range columns {
		column := record.Column(binding.source)
		if _, err := validateArrowArrayValidity(ctx, column); err != nil {
			return err
		}
	}
	return nil
}

// ValidateRecord validates an immutable record once before a caller splits it
// into output windows. It includes validity metadata; window conversion still
// validates its own sliced arrays so a standalone Convert remains safe.
func (p *Plan) ValidateRecord(ctx context.Context, record arrow.RecordBatch) error {
	if p == nil || record == nil {
		return moerr.NewInvalidInput(ctx, "invalid Arrow record")
	}
	recordSchema := record.Schema()
	if recordSchema == nil {
		return moerr.NewInvalidInput(ctx, "Arrow record schema is nil")
	}
	if schemaFingerprint(recordSchema) != p.schemaFingerprint {
		return moerr.NewInvalidInput(ctx, "Arrow record schema does not match the bound schema")
	}
	return validateRecordColumns(ctx, record, recordSchema, p.columns)
}

func validateRecordShape(
	ctx context.Context,
	record arrow.RecordBatch,
	schema *arrow.Schema,
	columns []columnPlan,
) error {
	if record.NumCols() != int64(len(columns)) {
		return moerr.NewInvalidInputf(ctx, "Arrow record has %d columns, expected %d", record.NumCols(), len(columns))
	}
	for _, binding := range columns {
		column := record.Column(binding.source)
		if column == nil {
			return moerr.NewInvalidInputf(ctx, "Arrow column %q is nil", binding.target.Name)
		}
		if int64(column.Len()) != record.NumRows() {
			return moerr.NewInvalidInputf(ctx,
				"Arrow column %q has %d rows, expected %d",
				binding.target.Name, column.Len(), record.NumRows())
		}
		if binding.source < 0 || binding.source >= schema.NumFields() || schema.Field(binding.source).Type == nil ||
			column.DataType() == nil || column.DataType().Fingerprint() != schema.Field(binding.source).Type.Fingerprint() {
			return moerr.NewInvalidInputf(ctx,
				"Arrow column %q data type does not match the bound schema", binding.target.Name)
		}
	}
	return nil
}

func estimateColumnRowBytes(
	ctx context.Context,
	column arrow.Array,
	binding columnPlan,
	row int,
) (uint64, error) {
	fixed := binding.target.Type.TypeSize()
	if binding.kind != conversionBorrowVarlen && binding.kind != conversionMaterializeDictionary {
		if fixed < 0 {
			return 0, moerr.NewInvalidInput(ctx, "invalid MatrixOne target width")
		}
		return uint64(fixed), nil
	}
	if binding.kind == conversionMaterializeDictionary {
		dictionary, ok := column.(*array.Dictionary)
		if !ok {
			return 0, moerr.NewInvalidInput(ctx, "invalid Arrow Dictionary array")
		}
		valueKind, err := selectLoadConversion(dictionary.Dictionary().DataType(), binding.target.Type)
		if err != nil {
			return 0, err
		}
		if valueKind != conversionBorrowVarlen {
			if fixed < 0 {
				return 0, moerr.NewInvalidInput(ctx, "invalid MatrixOne target width")
			}
			return uint64(fixed), nil
		}
		if dictionary.IsNull(row) {
			return uint64(fixed), nil
		}
		index, err := checkedDictionaryIndex(ctx, dictionary, row, dictionary.Dictionary().Len())
		if err != nil {
			return 0, err
		}
		if dictionary.Dictionary().IsNull(index) {
			return uint64(fixed), nil
		}
		length, err := varlenValueLength(ctx, dictionary.Dictionary(), index)
		if err != nil {
			return 0, err
		}
		return checkedRowBytes(ctx, fixed, length)
	}

	length, err := varlenValueLength(ctx, column, row)
	if err != nil {
		return 0, err
	}
	// Borrowed varlen keeps the whole Arrow values window, including physical
	// bytes associated with a logical null, until canonical serialization.
	return checkedRowBytes(ctx, fixed, length)
}

func checkedRowBytes(ctx context.Context, fixed int, variable int) (uint64, error) {
	if fixed < 0 || variable < 0 || uint64(fixed) > math.MaxUint64-uint64(variable) {
		return 0, moerr.NewInvalidInput(ctx, "Arrow output row size overflows")
	}
	return uint64(fixed) + uint64(variable), nil
}

func varlenValueLength(ctx context.Context, values arrow.Array, row int) (length int, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			length = 0
			err = moerr.NewInvalidInputf(ctx, "invalid Arrow varlen value at row %d: %v", row, recovered)
		}
	}()
	switch typed := values.(type) {
	case *array.String:
		return len(typed.Value(row)), nil
	case *array.LargeString:
		return len(typed.Value(row)), nil
	case *array.Binary:
		return len(typed.Value(row)), nil
	case *array.LargeBinary:
		return len(typed.Value(row)), nil
	case *array.FixedSizeBinary:
		return len(typed.Value(row)), nil
	default:
		return 0, moerr.NewInvalidInputNoCtxf("invalid Arrow varlen array %T", values)
	}
}
