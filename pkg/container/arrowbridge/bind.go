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
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	// MaxFields and MaxNestingDepth bound the already-decoded Arrow schema.
	// Wire-level FlatBuffers vectors have separate, stricter validation in
	// container/arrowipc before Arrow-Go is allowed to construct this schema.
	MaxFields       = 4096
	MaxNestingDepth = 32
	// ConversionPlanVersion fences distributed LOAD plans from binaries that
	// implement a different binding/conversion contract.
	ConversionPlanVersion = uint32(1)
)

// MatchMode controls how source fields are bound to target columns.
type MatchMode uint8

const (
	// MatchByName binds case-insensitively and rejects missing or ambiguous
	// source names.
	MatchByName MatchMode = iota
	// MatchByPosition ignores source names but still fingerprints both schemas
	// so execution cannot accept a drifted record.
	MatchByPosition
)

// TargetColumn is the stable table-side contract used by the bridge. MOIndex
// and AttrName let a caller keep output ordering independent of source order.
type TargetColumn struct {
	Name     string
	Type     types.Type
	NotNull  bool
	MOIndex  int
	AttrName string
}

type conversionKind uint8

const (
	conversionBorrowFixed conversionKind = iota
	conversionBorrowVarlen
	conversionMaterializeBool
	conversionMaterializeDate32
	conversionMaterializeDate64
	conversionMaterializeTimestamp
	conversionMaterializeDictionary
	conversionMaterializeWiden
	conversionMaterializeTime
	conversionMaterializeNull
)

type columnPlan struct {
	source int
	target TargetColumn
	kind   conversionKind
}

// Plan is immutable after BindLoad and may be reused for records carrying the
// exact same Arrow schema.
type Plan struct {
	schemaFingerprint     [sha256.Size]byte
	conversionFingerprint [sha256.Size]byte
	columns               []columnPlan
	attrs                 []string
}

// Bind is the compatibility spelling for BindLoad.
//
// Deprecated: new callers must use BindLoad so the LOAD conversion policy is
// visible at the call site. Other Arrow consumers, especially Python UDF, must
// not treat this policy as their wire ABI.
func Bind(
	ctx context.Context,
	schema *arrow.Schema,
	targets []TargetColumn,
	mode MatchMode,
) (*Plan, error) {
	return BindLoad(ctx, schema, targets, mode)
}

// BindLoad validates cardinality, mapping ambiguity, and the supported LOAD
// conversion matrix before any record data is acquired. The matrix permits a
// small set of checked widening and temporal conversions that are useful for
// ingestion but are intentionally forbidden by exact result protocols.
func BindLoad(
	ctx context.Context,
	schema *arrow.Schema,
	targets []TargetColumn,
	mode MatchMode,
) (*Plan, error) {
	if schema == nil {
		return nil, moerr.NewInvalidInput(ctx, "Arrow schema is nil")
	}
	if err := validateSchemaShape(ctx, schema); err != nil {
		return nil, err
	}
	if len(targets) != schema.NumFields() {
		return nil, moerr.NewInvalidInputf(ctx, "Arrow source has %d fields but target has %d columns", schema.NumFields(), len(targets))
	}
	if mode != MatchByName && mode != MatchByPosition {
		return nil, moerr.NewInvalidInputf(ctx, "unknown Arrow column match mode %d", mode)
	}

	plan := &Plan{
		schemaFingerprint: schemaFingerprint(schema),
		columns:           make([]columnPlan, len(targets)),
		attrs:             make([]string, len(targets)),
	}
	explicitOutputOrder := false
	for _, target := range targets {
		explicitOutputOrder = explicitOutputOrder || target.MOIndex != 0
	}
	used := make([]bool, schema.NumFields())
	outputUsed := make([]bool, len(targets))
	for targetIndex, target := range targets {
		if !explicitOutputOrder {
			target.MOIndex = targetIndex
		}
		if target.MOIndex < 0 || target.MOIndex >= len(targets) {
			return nil, moerr.NewInvalidInputf(ctx, "invalid MatrixOne output column index %d", target.MOIndex)
		}
		// AttrName is allowed to be empty at this transport-neutral boundary,
		// so it cannot double as an occupancy sentinel for output ordering.
		if outputUsed[target.MOIndex] {
			return nil, moerr.NewInvalidInputf(ctx, "duplicate MatrixOne output column index %d", target.MOIndex)
		}
		outputUsed[target.MOIndex] = true
		if target.AttrName == "" {
			target.AttrName = target.Name
		}

		sourceIndex := targetIndex
		if mode == MatchByName {
			matches := fieldIndicesFold(schema, target.Name)
			if len(matches) == 0 {
				return nil, moerr.NewInvalidInputf(ctx, "Arrow field %q is missing", target.Name)
			}
			if len(matches) != 1 {
				return nil, moerr.NewInvalidInputf(ctx, "Arrow field %q is ambiguous", target.Name)
			}
			sourceIndex = matches[0]
		}
		if used[sourceIndex] {
			return nil, moerr.NewInvalidInputf(ctx, "Arrow source field %q is bound more than once", schema.Field(sourceIndex).Name)
		}
		used[sourceIndex] = true

		kind, err := selectLoadConversion(schema.Field(sourceIndex).Type, target.Type)
		if err != nil {
			return nil, moerr.NewNotSupportedf(ctx, "Arrow field %q (%s) to MatrixOne column %q (%s): %v",
				schema.Field(sourceIndex).Name, schema.Field(sourceIndex).Type, target.Name, target.Type, err)
		}
		if err := validateLoadTargetType(ctx, target.Type); err != nil {
			return nil, err
		}
		plan.columns[target.MOIndex] = columnPlan{source: sourceIndex, target: target, kind: kind}
		plan.attrs[target.MOIndex] = target.AttrName
	}
	plan.conversionFingerprint = planFingerprint(plan, mode)
	return plan, nil
}

func validateSchemaShape(ctx context.Context, schema *arrow.Schema) error {
	if schema.NumFields() == 0 {
		return moerr.NewInvalidInput(ctx, "Arrow schema does not contain fields")
	}
	type pendingField struct {
		field arrow.Field
		depth int
	}
	pending := make([]pendingField, 0, schema.NumFields())
	for _, field := range schema.Fields() {
		pending = append(pending, pendingField{field: field, depth: 1})
	}
	total := 0
	for len(pending) > 0 {
		last := len(pending) - 1
		current := pending[last]
		pending = pending[:last]
		total++
		if total > MaxFields {
			return moerr.NewInvalidInputf(ctx, "Arrow total field count exceeds %d", MaxFields)
		}
		// BindLoad is also a public in-process boundary; callers may construct an
		// Arrow schema directly rather than through the validated IPC decoder.
		if current.field.Type == nil {
			return moerr.NewInvalidInputf(ctx, "Arrow field %q type is nil", current.field.Name)
		}
		if err := validateArrowTypeContract(ctx, current.field.Name, current.field.Type); err != nil {
			return err
		}
		if current.depth > MaxNestingDepth {
			return moerr.NewInvalidInputf(ctx,
				"Arrow field %q nesting depth exceeds %d", current.field.Name, MaxNestingDepth)
		}
		if nested, ok := current.field.Type.(arrow.NestedType); ok {
			children := nested.Fields()
			for _, child := range children {
				pending = append(pending, pendingField{field: child, depth: current.depth + 1})
			}
		}
	}
	return nil
}

// validateArrowTypeContract checks type metadata that later conversion code
// relies on for bounded arithmetic. Dictionary value types are not exposed as
// NestedType children by Arrow, so they need an explicit recursive check here.
func validateArrowTypeContract(ctx context.Context, fieldName string, typ arrow.DataType) error {
	switch typed := typ.(type) {
	case *arrow.Decimal128Type:
		if typed.Precision < 1 || typed.Precision > decimal128.MaxPrecision {
			return moerr.NewInvalidInputf(ctx,
				"Arrow field %q has invalid Decimal128 precision %d", fieldName, typed.Precision)
		}
	case *arrow.TimestampType:
		if !validArrowTimeUnit(typed.Unit) {
			return moerr.NewInvalidInputf(ctx,
				"Arrow field %q has invalid timestamp time unit %d", fieldName, typed.Unit)
		}
		// GetZone validates both IANA names and fixed offsets. Do this while
		// binding the schema so an invalid timezone cannot survive until the
		// first record happens to exercise the timestamp conversion.
		if _, err := typed.GetZone(); err != nil {
			return moerr.NewInvalidInputf(ctx,
				"Arrow field %q has invalid timestamp timezone %q: %v", fieldName, typed.TimeZone, err)
		}
	case *arrow.Time32Type:
		if typed.Unit != arrow.Second && typed.Unit != arrow.Millisecond {
			return moerr.NewInvalidInputf(ctx,
				"Arrow field %q has invalid time32 time unit %d", fieldName, typed.Unit)
		}
	case *arrow.Time64Type:
		if typed.Unit != arrow.Microsecond && typed.Unit != arrow.Nanosecond {
			return moerr.NewInvalidInputf(ctx,
				"Arrow field %q has invalid time64 time unit %d", fieldName, typed.Unit)
		}
	case *arrow.DictionaryType:
		if typed.ValueType == nil {
			return moerr.NewInvalidInputf(ctx, "Arrow field %q has a dictionary with nil value type", fieldName)
		}
		return validateArrowTypeContract(ctx, fieldName, typed.ValueType)
	}
	return nil
}

func validArrowTimeUnit(unit arrow.TimeUnit) bool {
	return unit == arrow.Second || unit == arrow.Millisecond ||
		unit == arrow.Microsecond || unit == arrow.Nanosecond
}

func validateLoadTargetType(ctx context.Context, typ types.Type) error {
	var expected int32
	switch typ.Oid {
	case types.T_bool, types.T_int8, types.T_uint8:
		expected = 1
	case types.T_int16, types.T_uint16, types.T_year:
		expected = 2
	case types.T_int32, types.T_uint32, types.T_date, types.T_float32:
		expected = 4
	case types.T_int64, types.T_uint64, types.T_datetime, types.T_time, types.T_timestamp,
		types.T_float64, types.T_decimal64:
		expected = 8
	case types.T_decimal128:
		expected = 16
	case types.T_char, types.T_varchar, types.T_text, types.T_binary, types.T_varbinary, types.T_blob:
		expected = int32(types.VarlenaSize)
	default:
		return nil
	}
	if typ.Size != expected {
		return moerr.NewInvalidInputf(ctx,
			"invalid MatrixOne target type size %d for %s, expected %d",
			typ.Size, typ.Oid, expected)
	}
	return nil
}

// Fingerprint returns the immutable logical source-schema and conversion
// contract identity. It is safe to serialize into a distributed scan plan.
func (p *Plan) Fingerprint() [sha256.Size]byte {
	if p == nil {
		return [sha256.Size]byte{}
	}
	return p.conversionFingerprint
}

func schemaFingerprint(schema *arrow.Schema) [sha256.Size]byte {
	h := sha256.New()
	writeFingerprintString(h, "matrixone-arrow-schema-v1")
	writeFingerprintUint64(h, uint64(schema.Endianness()))
	writeFingerprintMetadata(h, schema.Metadata())
	for _, field := range schema.Fields() {
		writeFingerprintField(h, field)
	}
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], h.Sum(nil))
	return fingerprint
}

func writeFingerprintField(h hash.Hash, field arrow.Field) {
	writeFingerprintString(h, field.Name)
	writeFingerprintBool(h, field.Nullable)
	writeFingerprintString(h, field.Type.Fingerprint())
	writeFingerprintMetadata(h, field.Metadata)
	if nested, ok := field.Type.(arrow.NestedType); ok {
		for _, child := range nested.Fields() {
			writeFingerprintField(h, child)
		}
	}
}

func writeFingerprintMetadata(h hash.Hash, metadata arrow.Metadata) {
	keys := metadata.Keys()
	values := metadata.Values()
	indices := make([]int, len(keys))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		if keys[indices[i]] == keys[indices[j]] {
			return values[indices[i]] < values[indices[j]]
		}
		return keys[indices[i]] < keys[indices[j]]
	})
	writeFingerprintUint64(h, uint64(len(indices)))
	for _, index := range indices {
		writeFingerprintString(h, keys[index])
		writeFingerprintString(h, values[index])
	}
}

func planFingerprint(plan *Plan, mode MatchMode) [sha256.Size]byte {
	h := sha256.New()
	writeFingerprintString(h, "matrixone-arrow-conversion-plan")
	writeFingerprintUint64(h, uint64(ConversionPlanVersion))
	_, _ = h.Write(plan.schemaFingerprint[:])
	writeFingerprintUint64(h, uint64(mode))
	for _, column := range plan.columns {
		writeFingerprintUint64(h, uint64(column.source))
		writeFingerprintString(h, column.target.Name)
		writeFingerprintString(h, column.target.AttrName)
		writeFingerprintUint64(h, uint64(column.target.MOIndex))
		writeFingerprintBool(h, column.target.NotNull)
		writeFingerprintUint64(h, uint64(column.target.Type.Oid))
		writeFingerprintUint64(h, uint64(column.target.Type.Charset))
		writeFingerprintUint64(h, uint64(uint32(column.target.Type.Size)))
		writeFingerprintUint64(h, uint64(uint32(column.target.Type.Width)))
		writeFingerprintUint64(h, uint64(uint32(column.target.Type.Scale)))
		writeFingerprintUint64(h, uint64(column.kind))
	}
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], h.Sum(nil))
	return fingerprint
}

func writeFingerprintString(h hash.Hash, value string) {
	writeFingerprintUint64(h, uint64(len(value)))
	_, _ = h.Write([]byte(value))
}

func writeFingerprintUint64(h hash.Hash, value uint64) {
	var buffer [8]byte
	binary.LittleEndian.PutUint64(buffer[:], value)
	_, _ = h.Write(buffer[:])
}

func writeFingerprintBool(h hash.Hash, value bool) {
	if value {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}
}

func fieldIndicesFold(schema *arrow.Schema, name string) []int {
	indices := make([]int, 0, 1)
	for i, field := range schema.Fields() {
		if strings.EqualFold(field.Name, name) {
			indices = append(indices, i)
		}
	}
	return indices
}

// selectLoadConversion is intentionally private: its result is an execution
// kernel choice, not a reusable Arrow ABI declaration. Consumers with an
// exact protocol must first validate their own versioned logical descriptor.
func selectLoadConversion(source arrow.DataType, target types.Type) (conversionKind, error) {
	if source.ID() == arrow.DICTIONARY {
		dictionary, ok := source.(*arrow.DictionaryType)
		if !ok || dictionary.IndexType == nil || dictionary.ValueType == nil {
			return 0, fmt.Errorf("invalid Arrow dictionary type")
		}
		switch dictionary.IndexType.ID() {
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
			arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		default:
			return 0, fmt.Errorf("dictionary index type %s is not an integer", dictionary.IndexType)
		}
		if dictionary.ValueType.ID() == arrow.DICTIONARY {
			return 0, fmt.Errorf("nested Arrow dictionaries are not supported")
		}
		if _, err := selectLoadConversion(dictionary.ValueType, target); err != nil {
			return 0, err
		}
		return conversionMaterializeDictionary, nil
	}
	if exactFixedLayout(source, target) {
		return conversionBorrowFixed, nil
	}
	switch source.ID() {
	case arrow.STRING, arrow.LARGE_STRING:
		if target.Oid == types.T_char || target.Oid == types.T_varchar || target.Oid == types.T_text {
			return conversionBorrowVarlen, nil
		}
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		if target.Oid == types.T_binary || target.Oid == types.T_varbinary || target.Oid == types.T_blob {
			return conversionBorrowVarlen, nil
		}
	case arrow.BOOL:
		if target.Oid == types.T_bool {
			return conversionMaterializeBool, nil
		}
	case arrow.DATE32:
		if target.Oid == types.T_date || target.Oid == types.T_datetime {
			return conversionMaterializeDate32, nil
		}
	case arrow.DATE64:
		if target.Oid == types.T_date || target.Oid == types.T_datetime {
			return conversionMaterializeDate64, nil
		}
	case arrow.TIMESTAMP:
		if (target.Oid == types.T_timestamp || target.Oid == types.T_datetime) &&
			target.Scale >= 0 && target.Scale <= 6 {
			return conversionMaterializeTimestamp, nil
		}
	case arrow.TIME32, arrow.TIME64:
		if target.Oid == types.T_time && target.Scale >= 0 && target.Scale <= 6 {
			return conversionMaterializeTime, nil
		}
	case arrow.NULL:
		return conversionMaterializeNull, nil
	}
	if isCheckedWidening(source.ID(), target.Oid) {
		return conversionMaterializeWiden, nil
	}
	return 0, fmt.Errorf("no exact long-term conversion")
}

func isCheckedWidening(source arrow.Type, target types.T) bool {
	switch source {
	case arrow.INT8:
		return target == types.T_int16 || target == types.T_int32 || target == types.T_int64
	case arrow.INT16:
		return target == types.T_int32 || target == types.T_int64
	case arrow.INT32:
		return target == types.T_int64
	case arrow.UINT8:
		return target == types.T_uint16 || target == types.T_uint32 || target == types.T_uint64
	case arrow.UINT16:
		return target == types.T_uint32 || target == types.T_uint64
	case arrow.UINT32:
		return target == types.T_uint64
	case arrow.FLOAT32:
		return target == types.T_float64
	default:
		return false
	}
}

func exactFixedLayout(source arrow.DataType, target types.Type) bool {
	matched := false
	switch source.ID() {
	case arrow.INT8:
		matched = target.Oid == types.T_int8
	case arrow.INT16:
		matched = target.Oid == types.T_int16
	case arrow.INT32:
		matched = target.Oid == types.T_int32
	case arrow.INT64:
		matched = target.Oid == types.T_int64
	case arrow.UINT8:
		matched = target.Oid == types.T_uint8
	case arrow.UINT16:
		matched = target.Oid == types.T_uint16
	case arrow.UINT32:
		matched = target.Oid == types.T_uint32
	case arrow.UINT64:
		matched = target.Oid == types.T_uint64
	case arrow.FLOAT32:
		matched = target.Oid == types.T_float32
	case arrow.FLOAT64:
		matched = target.Oid == types.T_float64
	case arrow.DECIMAL128:
		decimal, ok := source.(*arrow.Decimal128Type)
		matched = ok && target.Oid == types.T_decimal128 &&
			target.Width == decimal.Precision && target.Scale == decimal.Scale
	case arrow.TIME64:
		timeType, ok := source.(*arrow.Time64Type)
		matched = ok && timeType.Unit == arrow.Microsecond && target.Oid == types.T_time && target.Scale == 6
	}
	return matched && source.ID() != arrow.BOOL && target.TypeSize() > 0
}
