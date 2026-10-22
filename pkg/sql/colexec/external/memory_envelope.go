// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
package external

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
)

func SourceEnvelope(schema []types.Type, unit int64) (int64, error) {
	return SourceEnvelopeForFields(schema, unit, len(schema)+2)
}

// SourceEnvelopeForFields describes allocations that can coexist while one CSV
// row is converted. The row limit is the existing configured entry limit, not
// a customer-specific threshold. retainedFields must match CSVParser.RetainFields.
// The factors below are allocation-table bounds, not runtime tuning controls:
// append old+new (3), byte parser scratch (10), and JSON tree nodes (64).
func SourceEnvelopeForFields(schema []types.Type, unit int64, retainedFields int) (int64, error) {
	if unit <= 0 || retainedFields < 0 || config.LargestEntryLimit <= 0 {
		return 0, fmt.Errorf("invalid CSV memory envelope")
	}
	limit := int64(config.LargestEntryLimit)
	row, err := MaxRowBytes(schema)
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
			return 0, fmt.Errorf("CSV memory envelope overflow")
		}
		total += part
	}
	return total, nil
}

// MaxRowBytes is the typed payload bound for one configured raw record.
func MaxRowBytes(schema []types.Type) (int64, error) {
	limit := int64(config.LargestEntryLimit)
	if limit <= 0 {
		return 0, fmt.Errorf("invalid CSV raw record limit")
	}
	var fixed, factor int64
	for _, typ := range schema {
		if int64(typ.TypeSize()) > math.MaxInt64-fixed {
			return 0, fmt.Errorf("CSV row memory envelope overflow")
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
		return 0, fmt.Errorf("CSV row memory envelope overflow")
	}
	return fixed + factor*limit, nil
}
