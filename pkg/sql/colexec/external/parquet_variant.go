// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/util/parquetvariant"
	"github.com/parquet-go/parquet-go"
)

// Validate even schemas whose rows happen to be SQL NULL. A residual payload
// alone is not the complete value of a shredded VARIANT.
func validateParquetVariantSchema(ctx context.Context, col *parquet.Column) error {
	count := 0
	var visit func(*parquet.Column, int) error
	visit = func(c *parquet.Column, depth int) error {
		count++
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if depth > 256 || count > 65536 {
			return moerr.NewInvalidInput(ctx, "Parquet schema exceeds VARIANT validation limit")
		}
		if c.Leaf() {
			if lt := parquetLogicalType(ctx, c); lt != nil && lt.Variant != nil {
				return variantSchemaError(ctx, "VARIANT must annotate a group")
			}
			return nil
		}
		if lt := parquetLogicalType(ctx, c); lt != nil && lt.Variant != nil {
			return validateVariantGroup(ctx, c, true, 0, &count)
		}
		for _, child := range c.Columns() {
			if err := visit(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(col, 0)
}

func variantChildren(col *parquet.Column) (metadata, value, typed *parquet.Column) {
	for _, c := range col.Columns() {
		switch c.Name() {
		case "metadata":
			metadata = c
		case "value":
			value = c
		case "typed_value":
			typed = c
		}
	}
	return
}

func variantSchemaError(ctx context.Context, reason string) error {
	return moerr.NewInvalidInputf(ctx, "invalid Parquet VARIANT schema: %s", reason)
}

func validateVariantGroup(ctx context.Context, col *parquet.Column, root bool, depth int, count *int) error {
	*count += len(col.Columns())
	if *count > 65536 {
		return variantSchemaError(ctx, "schema exceeds 65536 nodes")
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if depth > bytejson.JSONDocumentMaxNestingDepth {
		return variantSchemaError(ctx, "nesting exceeds 100 containers")
	}
	meta, value, typed := variantChildren(col)
	expected := 0
	if root {
		expected++
		if meta == nil || !meta.Required() || !variantBinary(meta) {
			return variantSchemaError(ctx, "metadata must be required unannotated BINARY")
		}
	} else if meta != nil {
		return variantSchemaError(ctx, "nested metadata is forbidden")
	}
	if value != nil {
		expected++
		if !variantBinary(value) || value.Repeated() || (!root || typed != nil) && !value.Optional() || root && typed == nil && !value.Required() {
			return variantSchemaError(ctx, "invalid residual value field")
		}
	}
	if typed != nil {
		expected++
	}
	if expected != len(col.Columns()) || value == nil && typed == nil {
		return variantSchemaError(ctx, "unexpected or missing fields")
	}
	if typed == nil {
		return nil
	}
	if !typed.Optional() {
		return variantSchemaError(ctx, "typed_value must be optional")
	}
	*count += len(typed.Columns())
	if *count > 65536 {
		return variantSchemaError(ctx, "schema exceeds 65536 nodes")
	}
	if typed.Leaf() {
		return validateVariantScalar(ctx, typed)
	}
	if depth >= bytejson.JSONDocumentMaxNestingDepth {
		return variantSchemaError(ctx, "nesting exceeds 100 containers")
	}
	lt := parquetLogicalType(ctx, typed)
	if lt != nil {
		if lt.List == nil || len(typed.Columns()) != 1 {
			return variantSchemaError(ctx, "typed group must be an object or three-level LIST")
		}
		list := typed.Columns()[0]
		if list.Name() != "list" || !list.Repeated() || list.Leaf() || parquetLogicalType(ctx, list) != nil || len(list.Columns()) != 1 {
			return variantSchemaError(ctx, "invalid LIST repeated group")
		}
		elem := list.Columns()[0]
		if elem.Name() != "element" || !elem.Required() || elem.Leaf() || parquetLogicalType(ctx, elem) != nil {
			return variantSchemaError(ctx, "LIST element must be a required VARIANT field group")
		}
		return validateVariantGroup(ctx, elem, false, depth+1, count)
	}
	if len(typed.Columns()) == 0 || len(typed.Columns()) > 65536 {
		return variantSchemaError(ctx, "invalid typed object field count")
	}
	for _, field := range typed.Columns() {
		if !field.Required() || field.Leaf() || parquetLogicalType(ctx, field) != nil || !utf8.ValidString(field.Name()) || len(field.Name()) > math.MaxUint16 {
			return variantSchemaError(ctx, fmt.Sprintf("invalid typed object field %s required=%v leaf=%v logical=%v", field.Name(), field.Required(), field.Leaf(), parquetLogicalType(ctx, field)))
		}
		if err := validateVariantGroup(ctx, field, false, depth+1, count); err != nil {
			return err
		}
	}
	return nil
}

func validateVariantScalar(ctx context.Context, c *parquet.Column) error {
	k := c.Type().Kind()
	lt := parquetLogicalType(ctx, c)
	if lt == nil {
		switch k {
		case parquet.Boolean, parquet.Int32, parquet.Int64, parquet.Float, parquet.Double, parquet.ByteArray:
			return nil
		}
	} else {
		switch {
		case lt.UTF8 != nil && k == parquet.ByteArray:
			return nil
		case lt.Integer != nil:
			w := lt.Integer.BitWidth
			if lt.Integer.IsSigned && ((w == 8 || w == 16 || w == 32) && k == parquet.Int32 || w == 64 && k == parquet.Int64) {
				return nil
			}
		case lt.Decimal != nil:
			p, s := lt.Decimal.Precision, lt.Decimal.Scale
			if k == parquet.FixedLenByteArray {
				// floor(log10(2^(8*n-1)-1)) from the Parquet DECIMAL contract.
				capacity := [...]int32{0, 2, 4, 6, 9, 11, 14, 16, 18, 21, 23, 26, 28, 31, 33, 35, 38}
				n := c.Type().Length()
				if n < 1 || n >= len(capacity) || p > capacity[n] {
					return variantSchemaError(ctx, "DECIMAL precision exceeds fixed physical width")
				}
			}
			if p >= 1 && p <= 38 && s >= 0 && s <= p && (k == parquet.Int32 && p <= 9 || k == parquet.Int64 && p <= 18 || k == parquet.ByteArray || k == parquet.FixedLenByteArray && c.Type().Length() <= 16) {
				return nil
			}
		case lt.Date != nil && k == parquet.Int32:
			return nil
		case lt.Time != nil && k == parquet.Int64 && !lt.Time.IsAdjustedToUTC && lt.Time.Unit.Micros != nil:
			return nil
		case lt.Timestamp != nil && k == parquet.Int64 && !lt.Timestamp.IsAdjustedToUTC && lt.Timestamp.Unit.Micros != nil:
			return nil
		}
	}
	return moerr.NewNYIf(ctx, "Parquet VARIANT typed scalar %s has no lossless supported JSON mapping", c.Type())
}

// A field is absent only when both representations are NULL. Object fields
// omit it; a missing root is VARIANT null; array elements must never be absent.
func reconstructParquetVariant(ctx context.Context, col *parquet.Column, values []parquet.Value) (any, error) {
	if err := validateParquetVariantSchema(ctx, col); err != nil {
		return nil, err
	}
	if len(values) > (64<<20)/32 {
		return nil, moerr.NewInvalidInput(ctx, "Parquet VARIANT row exceeds working limit")
	}
	if err := validateParquetNestedValues(ctx, col, values); err != nil {
		return nil, err
	}
	present, err := variantGroupPresent(ctx, col, values)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, moerr.NewInvalidInputf(ctx, "malformed Parquet VARIANT row: %v", err)
	}
	if !present {
		return nil, nil
	}
	meta, _, _ := variantChildren(col)
	v, err := variantLeafValue(meta, values)
	if err != nil || v.IsNull() {
		return nil, moerr.NewInvalidInput(ctx, "missing or malformed Parquet VARIANT metadata")
	}
	d, err := parquetvariant.NewDocument(ctx, v.ByteArray())
	if err == nil {
		err = d.Reserve(len(values) * 32)
	}
	if err == nil {
		err = validateVariantDictionary(ctx, d, col)
	}
	var bj bytejson.ByteJson
	if err == nil {
		bj, _, err = readVariantGroup(ctx, d, col, values, 0)
	}
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return nil, cause
		}
		return nil, moerr.NewInvalidInputf(ctx, "Parquet VARIANT %s: %v", col.Name(), err)
	}
	return bj, nil
}

func variantLeafValue(col *parquet.Column, values []parquet.Value) (parquet.Value, error) {
	var result parquet.Value
	count := 0
	for _, v := range values {
		if v.Column() == col.Index() {
			result = v
			count++
		}
	}
	if count != 1 {
		return result, fmt.Errorf("missing or duplicate VARIANT leaf %s", col.Name())
	}
	if !result.IsNull() && (result.Kind() != col.Type().Kind() || int(result.DefinitionLevel()) != col.MaxDefinitionLevel()) {
		return result, fmt.Errorf("invalid VARIANT leaf %s", col.Name())
	}
	if result.IsNull() && int(result.DefinitionLevel()) != col.MaxDefinitionLevel()-1 {
		return result, fmt.Errorf("NULL VARIANT leaf at a defined level")
	}
	return result, nil
}

func validateVariantDictionary(ctx context.Context, d *parquetvariant.Document, col *parquet.Column) error {
	if err := d.Reserve(0); err != nil {
		return err
	}
	_, _, typed := variantChildren(col)
	if typed == nil || typed.Leaf() {
		return nil
	}
	if lt := parquetLogicalType(ctx, typed); lt != nil && lt.List != nil {
		return validateVariantDictionary(ctx, d, typed.Columns()[0].Columns()[0])
	}
	for _, field := range typed.Columns() {
		ok, err := d.HasField(field.Name())
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("typed field %q missing from metadata dictionary", field.Name())
		}
		if err := validateVariantDictionary(ctx, d, field); err != nil {
			return err
		}
	}
	return nil
}

// Check presence consistently across every descendant, including placeholders
// for empty arrays. Consulting only the first leaf would hide malformed rows.
func variantGroupPresent(ctx context.Context, col *parquet.Column, values []parquet.Value) (bool, error) {
	present := -1
	for _, leaf := range collectLeafColumns(col) {
		if err := context.Cause(ctx); err != nil {
			return false, err
		}
		found := false
		count := 0
		for i, v := range values {
			if i%1024 == 0 {
				if err := context.Cause(ctx); err != nil {
					return false, err
				}
			}
			if v.Column() != leaf.Index() {
				continue
			}
			if !found && int(v.RepetitionLevel()) > col.MaxRepetitionLevel() {
				return false, fmt.Errorf("invalid initial VARIANT repetition level")
			}
			found = true
			count++
			p := 0
			if int(v.DefinitionLevel()) >= col.MaxDefinitionLevel() {
				p = 1
			}
			if present != -1 && present != p {
				return false, fmt.Errorf("inconsistent VARIANT group definition levels")
			}
			if p == 0 && (!v.IsNull() || int(v.DefinitionLevel()) != col.MaxDefinitionLevel()-1) {
				return false, fmt.Errorf("invalid absent VARIANT group placeholder")
			}
			present = p
		}
		if present == 0 && count != 1 {
			return false, fmt.Errorf("duplicate absent VARIANT group placeholder")
		}
		if !found {
			return false, fmt.Errorf("missing VARIANT descendant %s", leaf.Name())
		}
	}
	return present == 1, nil
}

func readVariantGroup(ctx context.Context, d *parquetvariant.Document, col *parquet.Column, values []parquet.Value, depth int) (bytejson.ByteJson, bool, error) {
	if err := context.Cause(ctx); err != nil {
		return bytejson.ByteJson{}, false, err
	}
	present, presenceErr := variantGroupPresent(ctx, col, values)
	if presenceErr != nil {
		return bytejson.ByteJson{}, false, presenceErr
	}
	if !present {
		return bytejson.ByteJson{}, false, fmt.Errorf("undefined VARIANT field group")
	}
	_, rawCol, typed := variantChildren(col)
	var raw parquet.Value
	var err error
	if rawCol != nil {
		raw, err = variantLeafValue(rawCol, values)
		if err != nil {
			return bytejson.ByteJson{}, false, err
		}
	}
	rawPresent := rawCol != nil && !raw.IsNull()
	typedPresent := false
	if typed != nil {
		typedPresent, err = variantGroupPresent(ctx, typed, values)
		if err != nil {
			return bytejson.ByteJson{}, false, err
		}
	}
	if !typedPresent {
		if !rawPresent {
			return bytejson.Null, false, nil
		}
		bj, err := d.DecodeValue(raw.ByteArray(), depth)
		if err == nil && bj.Type == bytejson.TpCodeObject && typed != nil && !typed.Leaf() && parquetLogicalType(ctx, typed) == nil {
			err = fmt.Errorf("residual object requires a present typed object")
		}
		return bj, true, err
	}
	object := !typed.Leaf() && parquetLogicalType(ctx, typed) == nil
	if rawPresent && !object {
		return bytejson.ByteJson{}, false, fmt.Errorf("both value and typed_value populated for a non-object")
	}
	if typed.Leaf() {
		v, err := variantLeafValue(typed, values)
		if err != nil {
			return bytejson.ByteJson{}, false, err
		}
		bj, err := readVariantScalar(ctx, d, typed, v, depth)
		return bj, true, err
	}
	if depth >= bytejson.JSONDocumentMaxNestingDepth {
		return bytejson.ByteJson{}, false, fmt.Errorf("VARIANT nesting exceeds 100 containers")
	}
	if !object {
		bj, err := readVariantArray(ctx, d, typed, values, depth)
		return bj, true, err
	}
	if err := d.Reserve(len(typed.Columns()) * 64); err != nil {
		return bytejson.ByteJson{}, false, err
	}
	fields := make(map[string]any)
	if rawPresent {
		residual, err := d.DecodeValue(raw.ByteArray(), depth)
		if err != nil {
			return bytejson.ByteJson{}, false, err
		}
		if residual.Type != bytejson.TpCodeObject {
			return bytejson.ByteJson{}, false, fmt.Errorf("typed object has a non-object residual")
		}
		for i := 0; i < residual.GetElemCnt(); i++ {
			fields[string(residual.GetObjectKey(i))] = residual.GetObjectVal(i)
		}
	}
	for _, field := range typed.Columns() {
		if _, ok := fields[field.Name()]; ok {
			return bytejson.ByteJson{}, false, fmt.Errorf("residual duplicates typed schema field %q", field.Name())
		}
		bj, present, err := readVariantGroup(ctx, d, field, extractNestedColumnValues(values, field), depth+1)
		if err != nil {
			return bytejson.ByteJson{}, false, err
		}
		if present {
			fields[field.Name()] = bj
		}
	}
	bj, err := d.Object(fields, depth)
	return bj, true, err
}

func readVariantArray(ctx context.Context, d *parquetvariant.Document, col *parquet.Column, values []parquet.Value, depth int) (bytejson.ByteJson, error) {
	elem := col.Columns()[0].Columns()[0]
	local := extractNestedColumnValues(values, elem)
	leaves := collectLeafColumns(elem)
	// Count outer elements separately in every leaf. Nested arrays may have
	// different numbers of physical values, but their outer boundaries must agree.
	expected := -1
	for _, leaf := range leaves {
		nested, _ := repeatedDepthToLeaf(elem, leaf)
		outer := leaf.MaxRepetitionLevel() - nested
		count := 0
		for _, v := range local {
			if v.Column() == leaf.Index() && (count == 0 || int(v.RepetitionLevel()) <= outer) {
				count++
			}
		}
		if expected != -1 && expected != count {
			return bytejson.ByteJson{}, fmt.Errorf("inconsistent VARIANT array element counts")
		}
		expected = count
	}
	if expected <= 0 {
		return bytejson.ByteJson{}, fmt.Errorf("missing VARIANT array placeholders")
	}
	if err := d.CheckElementCount(expected); err != nil {
		return bytejson.ByteJson{}, err
	}
	if isEmptyListValues(elem, local) {
		if len(local) != len(leaves) {
			return bytejson.ByteJson{}, fmt.Errorf("duplicate empty VARIANT array placeholders")
		}
		return d.Array([]any{}, depth)
	}
	groups := groupNestedValuesByOuterRepetition(elem, leaves, local)
	result := make([]any, 0, len(groups))
	for _, group := range groups {
		present, err := variantGroupPresent(ctx, elem, group)
		if err != nil {
			return bytejson.ByteJson{}, err
		}
		if !present {
			return bytejson.ByteJson{}, fmt.Errorf("undefined VARIANT array element")
		}
		bj, present, err := readVariantGroup(ctx, d, elem, group, depth+1)
		if err != nil {
			return bytejson.ByteJson{}, err
		}
		if !present {
			return bytejson.ByteJson{}, fmt.Errorf("missing VARIANT array element")
		}
		result = append(result, bj)
	}
	return d.Array(result, depth)
}

// Convert physical typed leaves to the same primitive wire representation as
// residual leaves, so both paths use identical range/UTF-8/finite/type checks.
func readVariantScalar(ctx context.Context, d *parquetvariant.Document, col *parquet.Column, v parquet.Value, depth int) (bytejson.ByteJson, error) {
	lt := parquetLogicalType(ctx, col)
	kind := col.Type().Kind()
	var wire []byte
	switch {
	case lt != nil && lt.Decimal != nil:
		if kind == parquet.ByteArray || kind == parquet.FixedLenByteArray {
			size := len(v.ByteArray())
			if size < 1 || size > 16 || kind == parquet.FixedLenByteArray && size != col.Type().Length() {
				return bytejson.ByteJson{}, fmt.Errorf("invalid typed decimal byte width")
			}
		}
		n, err := parquetDecimalValueToBigInt(ctx, col.Type(), v)
		if err != nil {
			return bytejson.ByteJson{}, err
		}
		digits := n.Text(10)
		if n.Sign() < 0 {
			digits = digits[1:]
		}
		if len(digits) > int(lt.Decimal.Precision) {
			return bytejson.ByteJson{}, fmt.Errorf("typed decimal exceeds declared precision")
		}
		width, tag := 16, 10
		if lt.Decimal.Precision <= 9 {
			width, tag = 4, 8
		} else if lt.Decimal.Precision <= 18 {
			width, tag = 8, 9
		}
		wire = make([]byte, 2+width)
		wire[0] = byte(tag << 2)
		wire[1] = byte(lt.Decimal.Scale)
		magnitude := n.Bytes()
		for i := range magnitude {
			wire[2+i] = magnitude[len(magnitude)-1-i]
		}
		if n.Sign() < 0 {
			carry := uint16(1)
			for i := 2; i < len(wire); i++ {
				carry += uint16(^wire[i])
				wire[i] = byte(carry)
				carry >>= 8
			}
		}
	case lt != nil && lt.Date != nil:
		wire = binary.LittleEndian.AppendUint32([]byte{11 << 2}, uint32(v.Int32()))
	case lt != nil && lt.Time != nil:
		wire = binary.LittleEndian.AppendUint64([]byte{17 << 2}, uint64(v.Int64()))
	case lt != nil && lt.Timestamp != nil:
		wire = binary.LittleEndian.AppendUint64([]byte{13 << 2}, uint64(v.Int64()))
	case lt != nil && lt.UTF8 != nil:
		if err := d.Reserve(len(v.ByteArray())); err != nil {
			return bytejson.ByteJson{}, err
		}
		wire = binary.LittleEndian.AppendUint32([]byte{16 << 2}, uint32(len(v.ByteArray())))
		wire = append(wire, v.ByteArray()...)
	case kind == parquet.ByteArray:
		if err := d.Reserve(len(v.ByteArray())); err != nil {
			return bytejson.ByteJson{}, err
		}
		wire = binary.LittleEndian.AppendUint32([]byte{15 << 2}, uint32(len(v.ByteArray())))
		wire = append(wire, v.ByteArray()...)
	case kind == parquet.Boolean:
		wire = []byte{2 << 2}
		if v.Boolean() {
			wire[0] = 1 << 2
		}
	case kind == parquet.Int32 || kind == parquet.Int64:
		n := v.Int64()
		if kind == parquet.Int32 {
			n = int64(v.Int32())
		}
		if lt != nil && lt.Integer != nil {
			w := lt.Integer.BitWidth
			if w < 64 && (n < -(int64(1)<<(w-1)) || n >= int64(1)<<(w-1)) {
				return bytejson.ByteJson{}, fmt.Errorf("typed integer exceeds declared width")
			}
		}
		wire = binary.LittleEndian.AppendUint64([]byte{6 << 2}, uint64(n))
	case kind == parquet.Float:
		wire = binary.LittleEndian.AppendUint32([]byte{14 << 2}, math.Float32bits(v.Float()))
	case kind == parquet.Double:
		wire = binary.LittleEndian.AppendUint64([]byte{7 << 2}, math.Float64bits(v.Double()))
	default:
		return bytejson.ByteJson{}, fmt.Errorf("unsupported typed VARIANT leaf")
	}
	return d.DecodeValue(wire, depth)
}

func variantBinary(c *parquet.Column) bool {
	return c.Leaf() && c.Type().Kind() == parquet.ByteArray && c.Type().LogicalType() == nil
}
