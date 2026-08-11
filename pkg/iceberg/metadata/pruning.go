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

package metadata

import (
	"encoding/binary"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type scanPruner struct {
	predicates []compiledPrunePredicate
	specs      map[int]api.PartitionSpec
}

type compiledPrunePredicate struct {
	field api.SchemaField
	op    api.PruneOp
	lit   api.PruneLiteral
}

type pruneValueKind int

const (
	pruneValueInvalid pruneValueKind = iota
	pruneValueInt64
	pruneValueFloat64
	pruneValueString
)

type pruneValue struct {
	kind pruneValueKind
	i64  int64
	f64  float64
	str  string
}

func newScanPruner(meta *api.TableMetadata, schema api.Schema, predicates []api.PrunePredicate) scanPruner {
	fields := make(map[int]api.SchemaField, len(schema.Fields))
	for _, field := range schema.Fields {
		fields[field.ID] = field
	}
	compiled := make([]compiledPrunePredicate, 0, len(predicates))
	for _, predicate := range predicates {
		field, ok := fields[predicate.FieldID]
		if !ok || predicate.Literal.IsNull {
			continue
		}
		op := normalizePruneOp(predicate.Op)
		if op == "" {
			continue
		}
		compiled = append(compiled, compiledPrunePredicate{
			field: field,
			op:    op,
			lit:   predicate.Literal,
		})
	}
	specs := make(map[int]api.PartitionSpec)
	if meta != nil {
		for _, spec := range meta.PartitionSpecs {
			specs[spec.SpecID] = spec
		}
	}
	return scanPruner{predicates: compiled, specs: specs}
}

func (p scanPruner) empty() bool {
	return len(p.predicates) == 0
}

func (p scanPruner) shouldPruneManifest(manifest api.ManifestFile) bool {
	if p.empty() || len(manifest.Partitions) == 0 {
		return false
	}
	spec, ok := p.specs[manifest.PartitionSpecID]
	if !ok {
		return false
	}
	for _, predicate := range p.predicates {
		if p.manifestPredicatePrunes(spec, manifest.Partitions, predicate) {
			return true
		}
	}
	return false
}

func (p scanPruner) manifestPredicatePrunes(spec api.PartitionSpec, summaries []api.PartitionFieldSummary, predicate compiledPrunePredicate) bool {
	for idx, field := range spec.Fields {
		if field.SourceID != predicate.field.ID || idx >= len(summaries) {
			continue
		}
		literal, boundType, exactTransform, ok := partitionPredicateValue(predicate.field, field.Transform, predicate.lit)
		if !ok {
			return false
		}
		summary := summaries[idx]
		lower, hasLower := decodePruneBound(boundType, summary.LowerBound)
		upper, hasUpper := decodePruneBound(boundType, summary.UpperBound)
		if !hasLower && !hasUpper {
			return false
		}
		return partitionRangePrunes(predicate.op, literal, optionalPruneValue(lower, hasLower), optionalPruneValue(upper, hasUpper), exactTransform)
	}
	return false
}

func (p scanPruner) shouldPruneDataFile(file api.DataFile) bool {
	if p.empty() {
		return false
	}
	for _, predicate := range p.predicates {
		if p.dataFilePredicatePrunes(file, predicate) {
			return true
		}
	}
	return false
}

func (p scanPruner) dataFilePredicatePrunes(file api.DataFile, predicate compiledPrunePredicate) bool {
	if dataFileAllNull(file, predicate.field.ID) {
		return true
	}
	if dataFileAllNaN(file, predicate.field.ID, predicate.field.Type, predicate.lit) {
		return true
	}
	if p.dataFilePartitionTuplePrunes(file, predicate) {
		return true
	}
	literal, ok := literalPruneValue(predicate.field.Type, predicate.lit)
	if !ok {
		return false
	}
	lower, hasLower := decodePruneBound(predicate.field.Type, file.LowerBounds[predicate.field.ID])
	upper, hasUpper := decodePruneBound(predicate.field.Type, file.UpperBounds[predicate.field.ID])
	if !hasLower && !hasUpper {
		return false
	}
	return rangePrunes(predicate.op, literal, optionalPruneValue(lower, hasLower), optionalPruneValue(upper, hasUpper))
}

func (p scanPruner) dataFilePartitionTuplePrunes(file api.DataFile, predicate compiledPrunePredicate) bool {
	if len(file.Partition) == 0 {
		return false
	}
	spec, ok := p.specs[file.SpecID]
	if !ok {
		return false
	}
	for _, field := range spec.Fields {
		if field.SourceID != predicate.field.ID {
			continue
		}
		literal, boundType, exactTransform, ok := partitionPredicateValue(predicate.field, field.Transform, predicate.lit)
		if !ok {
			return false
		}
		raw, ok := lookupPartitionValue(file.Partition, field.Name)
		if !ok || raw == nil {
			return false
		}
		value, ok := pruneValueFromAny(boundType, raw)
		if !ok {
			return false
		}
		return partitionRangePrunes(predicate.op, literal, &value, &value, exactTransform)
	}
	return false
}

func dataFileAllNull(file api.DataFile, fieldID int) bool {
	valueCount, ok := file.ValueCounts[fieldID]
	if !ok || valueCount <= 0 {
		return false
	}
	nullCount, ok := file.NullValueCounts[fieldID]
	return ok && nullCount >= valueCount
}

func dataFileAllNaN(file api.DataFile, fieldID int, fieldType api.IcebergType, literal api.PruneLiteral) bool {
	if fieldType.Kind != api.TypeFloat && fieldType.Kind != api.TypeDouble {
		return false
	}
	if literal.IsNull || math.IsNaN(literal.Float64) {
		return false
	}
	valueCount, ok := file.ValueCounts[fieldID]
	if !ok || valueCount <= 0 {
		return false
	}
	nanCount, ok := file.NaNValueCounts[fieldID]
	return ok && nanCount >= valueCount
}

func normalizePruneOp(op api.PruneOp) api.PruneOp {
	switch strings.ToLower(strings.TrimSpace(string(op))) {
	case string(api.PruneOpEQ), "=", "==":
		return api.PruneOpEQ
	case string(api.PruneOpLT), "<":
		return api.PruneOpLT
	case string(api.PruneOpLTE), "<=":
		return api.PruneOpLTE
	case string(api.PruneOpGT), ">":
		return api.PruneOpGT
	case string(api.PruneOpGTE), ">=":
		return api.PruneOpGTE
	default:
		return ""
	}
}

func rangePrunes(op api.PruneOp, literal pruneValue, lower, upper *pruneValue) bool {
	switch op {
	case api.PruneOpEQ:
		return (upper != nil && comparePruneValue(*upper, literal) < 0) ||
			(lower != nil && comparePruneValue(*lower, literal) > 0)
	case api.PruneOpGT:
		return upper != nil && comparePruneValue(*upper, literal) <= 0
	case api.PruneOpGTE:
		return upper != nil && comparePruneValue(*upper, literal) < 0
	case api.PruneOpLT:
		return lower != nil && comparePruneValue(*lower, literal) >= 0
	case api.PruneOpLTE:
		return lower != nil && comparePruneValue(*lower, literal) > 0
	default:
		return false
	}
}

func partitionRangePrunes(op api.PruneOp, literal pruneValue, lower, upper *pruneValue, exactTransform bool) bool {
	if !exactTransform {
		switch op {
		case api.PruneOpGT:
			op = api.PruneOpGTE
		case api.PruneOpLT:
			op = api.PruneOpLTE
		}
	}
	return rangePrunes(op, literal, lower, upper)
}

func optionalPruneValue(value pruneValue, ok bool) *pruneValue {
	if !ok {
		return nil
	}
	return &value
}

func comparePruneValue(left, right pruneValue) int {
	if left.kind != right.kind {
		return 0
	}
	switch left.kind {
	case pruneValueInt64:
		if left.i64 < right.i64 {
			return -1
		}
		if left.i64 > right.i64 {
			return 1
		}
		return 0
	case pruneValueFloat64:
		if math.IsNaN(left.f64) || math.IsNaN(right.f64) {
			return 0
		}
		if left.f64 < right.f64 {
			return -1
		}
		if left.f64 > right.f64 {
			return 1
		}
		return 0
	case pruneValueString:
		return strings.Compare(left.str, right.str)
	default:
		return 0
	}
}

func literalPruneValue(fieldType api.IcebergType, literal api.PruneLiteral) (pruneValue, bool) {
	if literal.IsNull {
		return pruneValue{}, false
	}
	switch fieldType.Kind {
	case api.TypeInt, api.TypeLong, api.TypeDate:
		if literal.Kind != "" && literal.Kind != api.TypeInt && literal.Kind != api.TypeLong && literal.Kind != api.TypeDate {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueInt64, i64: literal.Int64}, true
	case api.TypeFloat, api.TypeDouble:
		if literal.Kind != "" && literal.Kind != api.TypeFloat && literal.Kind != api.TypeDouble {
			return pruneValue{}, false
		}
		if math.IsNaN(literal.Float64) {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueFloat64, f64: literal.Float64}, true
	case api.TypeString:
		if literal.Kind != "" && literal.Kind != api.TypeString {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueString, str: literal.String}, true
	case api.TypeTimestamp, api.TypeTimestampTZ:
		if !literal.Normalized || (literal.Kind != "" && literal.Kind != fieldType.Kind) {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueInt64, i64: literal.Int64}, true
	case api.TypeTimestampNS:
		return pruneValue{}, false
	default:
		return pruneValue{}, false
	}
}

func partitionPredicateValue(field api.SchemaField, transform string, literal api.PruneLiteral) (pruneValue, api.IcebergType, bool, bool) {
	transform = strings.ToLower(strings.TrimSpace(transform))
	if transform == "" || transform == "identity" {
		value, ok := literalPruneValue(field.Type, literal)
		return value, field.Type, true, ok
	}
	days, ok := dateLiteralDays(literal)
	if ok && field.Type.Kind == api.TypeDate {
		switch transform {
		case "year":
			return pruneValue{kind: pruneValueInt64, i64: int64(dateTransformYear(days))}, api.IcebergType{Kind: api.TypeInt}, false, true
		case "month":
			return pruneValue{kind: pruneValueInt64, i64: int64(dateTransformMonth(days))}, api.IcebergType{Kind: api.TypeInt}, false, true
		case "day":
			return pruneValue{kind: pruneValueInt64, i64: days}, api.IcebergType{Kind: api.TypeInt}, true, true
		default:
			return pruneValue{}, api.IcebergType{}, false, false
		}
	}

	micros, ok := timestampLiteralMicros(field.Type, literal)
	if !ok {
		return pruneValue{}, api.IcebergType{}, false, false
	}
	switch transform {
	case "year":
		return pruneValue{kind: pruneValueInt64, i64: int64(timestampTransformYear(micros))}, api.IcebergType{Kind: api.TypeInt}, false, true
	case "month":
		return pruneValue{kind: pruneValueInt64, i64: int64(timestampTransformMonth(micros))}, api.IcebergType{Kind: api.TypeInt}, false, true
	case "day":
		return pruneValue{kind: pruneValueInt64, i64: timestampTransformDay(micros)}, api.IcebergType{Kind: api.TypeInt}, false, true
	case "hour":
		return pruneValue{kind: pruneValueInt64, i64: floorDiv(micros, int64(time.Hour/time.Microsecond))}, api.IcebergType{Kind: api.TypeInt}, false, true
	default:
		return pruneValue{}, api.IcebergType{}, false, false
	}
}

func lookupPartitionValue(partition map[string]any, fieldName string) (any, bool) {
	if len(partition) == 0 {
		return nil, false
	}
	if value, ok := partition[fieldName]; ok {
		return value, true
	}
	lowerName := strings.ToLower(strings.TrimSpace(fieldName))
	for key, value := range partition {
		if strings.ToLower(strings.TrimSpace(key)) == lowerName {
			return value, true
		}
	}
	return nil, false
}

func pruneValueFromAny(fieldType api.IcebergType, raw any) (pruneValue, bool) {
	if raw == nil {
		return pruneValue{}, false
	}
	switch fieldType.Kind {
	case api.TypeInt, api.TypeLong, api.TypeDate:
		value, ok := pruneInt64FromAny(raw)
		if !ok {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueInt64, i64: value}, true
	case api.TypeFloat, api.TypeDouble:
		value, ok := pruneFloat64FromAny(raw)
		if !ok || math.IsNaN(value) {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueFloat64, f64: value}, true
	case api.TypeString:
		value, ok := raw.(string)
		if !ok {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueString, str: value}, true
	case api.TypeTimestamp, api.TypeTimestampTZ:
		value, ok := pruneInt64FromAny(raw)
		if !ok {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueInt64, i64: value}, true
	default:
		return pruneValue{}, false
	}
}

func pruneInt64FromAny(raw any) (int64, bool) {
	switch v := raw.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), uint64(v) <= math.MaxInt64
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		return int64(v), v <= math.MaxInt64
	default:
		rv := reflect.ValueOf(raw)
		if !rv.IsValid() {
			return 0, false
		}
		switch rv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int(), true
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			value := rv.Uint()
			if value > math.MaxInt64 {
				return 0, false
			}
			return int64(value), true
		default:
			return 0, false
		}
	}
}

func pruneFloat64FromAny(raw any) (float64, bool) {
	switch v := raw.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		rv := reflect.ValueOf(raw)
		if !rv.IsValid() {
			return 0, false
		}
		switch rv.Kind() {
		case reflect.Float32, reflect.Float64:
			return rv.Float(), true
		default:
			return 0, false
		}
	}
}

func dateLiteralDays(literal api.PruneLiteral) (int64, bool) {
	if literal.IsNull {
		return 0, false
	}
	if literal.Kind != "" && literal.Kind != api.TypeDate && literal.Kind != api.TypeInt && literal.Kind != api.TypeLong {
		return 0, false
	}
	return literal.Int64, true
}

func timestampLiteralMicros(fieldType api.IcebergType, literal api.PruneLiteral) (int64, bool) {
	if literal.IsNull || !literal.Normalized {
		return 0, false
	}
	if fieldType.Kind != api.TypeTimestamp && fieldType.Kind != api.TypeTimestampTZ {
		return 0, false
	}
	if literal.Kind != fieldType.Kind {
		return 0, false
	}
	return literal.Int64, true
}

func dateTransformYear(days int64) int {
	t := time.Unix(0, 0).UTC().AddDate(0, 0, int(days))
	return t.Year() - 1970
}

func dateTransformMonth(days int64) int {
	t := time.Unix(0, 0).UTC().AddDate(0, 0, int(days))
	return (t.Year()-1970)*12 + int(t.Month()) - 1
}

func timestampTransformYear(micros int64) int {
	t := time.UnixMicro(micros).UTC()
	return t.Year() - 1970
}

func timestampTransformMonth(micros int64) int {
	t := time.UnixMicro(micros).UTC()
	return (t.Year()-1970)*12 + int(t.Month()) - 1
}

func timestampTransformDay(micros int64) int64 {
	return floorDiv(micros, int64((24*time.Hour)/time.Microsecond))
}

func floorDiv(value, divisor int64) int64 {
	if divisor <= 0 {
		return 0
	}
	quotient := value / divisor
	remainder := value % divisor
	if remainder != 0 && ((remainder < 0) != (divisor < 0)) {
		quotient--
	}
	return quotient
}

func decodePruneBound(fieldType api.IcebergType, data []byte) (pruneValue, bool) {
	if len(data) == 0 {
		return pruneValue{}, false
	}
	switch fieldType.Kind {
	case api.TypeInt, api.TypeDate:
		if len(data) < 4 {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueInt64, i64: int64(int32(binary.LittleEndian.Uint32(data[:4])))}, true
	case api.TypeLong:
		if len(data) < 8 {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueInt64, i64: int64(binary.LittleEndian.Uint64(data[:8]))}, true
	case api.TypeTimestamp, api.TypeTimestampTZ:
		if len(data) < 8 {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueInt64, i64: int64(binary.LittleEndian.Uint64(data[:8]))}, true
	case api.TypeFloat:
		if len(data) < 4 {
			return pruneValue{}, false
		}
		value := float64(math.Float32frombits(binary.LittleEndian.Uint32(data[:4])))
		if math.IsNaN(value) {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueFloat64, f64: value}, true
	case api.TypeDouble:
		if len(data) < 8 {
			return pruneValue{}, false
		}
		value := math.Float64frombits(binary.LittleEndian.Uint64(data[:8]))
		if math.IsNaN(value) {
			return pruneValue{}, false
		}
		return pruneValue{kind: pruneValueFloat64, f64: value}, true
	case api.TypeString:
		return pruneValue{kind: pruneValueString, str: string(data)}, true
	default:
		return pruneValue{}, false
	}
}
