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

package mongodb

import (
	"context"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ProjectColumnsByName returns mappings in the compact MO scan-column order.
// Column pruning rewrites filter ColPos values against that compact order, so
// a connector must convert every retained scan column (including residual-only
// filter columns), not only the columns projected to its parent operator.
func ProjectColumnsByName(ctx context.Context, columns []ColumnMapping, names []string) ([]ColumnMapping, error) {
	byName := make(map[string]ColumnMapping, len(columns))
	for _, column := range columns {
		key := strings.ToLower(column.Name)
		if _, exists := byName[key]; exists {
			return nil, moerr.NewInternalErrorf(ctx, "duplicate MongoDB mapping column %s", column.Name)
		}
		byName[key] = column
	}
	projected := make([]ColumnMapping, 0, len(names))
	for _, name := range names {
		column, ok := byName[strings.ToLower(name)]
		if !ok {
			return nil, moerr.NewInternalErrorf(ctx, "MongoDB scan column %s has no mapping", name)
		}
		projected = append(projected, column)
	}
	return projected, nil
}

// MappingDefinitionMatchesPlan validates the non-secret definition captured
// from rel_createsql against the authoritative mapping row during compile.
func MappingDefinitionMatchesPlan(mapping TableMapping, scan *plan.MongoScan) bool {
	if scan == nil || mapping.Database != scan.Database || mapping.Collection != scan.Collection ||
		mapping.MaxParallelism != scan.MaxParallelism {
		return false
	}
	return slices.Equal(mapping.Columns, ColumnsFromPlan(scan.Columns))
}

// MappingSnapshotMatchesPlan additionally pins catalog identity and version;
// execution CNs use this after resolving the exact mapping generation.
func MappingSnapshotMatchesPlan(mapping TableMapping, scan *plan.MongoScan) bool {
	if scan == nil || mapping.Database != scan.Database || mapping.Collection != scan.Collection ||
		mapping.MaxParallelism != scan.MaxParallelism ||
		mapping.TableID != scan.TableId ||
		mapping.MappingID != scan.MappingId ||
		mapping.Version != scan.MappingVersion ||
		mapping.ConnectionID != scan.ConnectionId {
		return false
	}
	planned := ColumnsFromPlan(scan.Columns)
	if len(planned) == 0 {
		return false
	}
	for _, projected := range planned {
		matched := false
		for _, catalogColumn := range mapping.Columns {
			if projected == catalogColumn {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func ColumnsFromPlan(columns []*plan.MongoColumnMapping) []ColumnMapping {
	result := make([]ColumnMapping, 0, len(columns))
	for _, column := range columns {
		if column == nil {
			continue
		}
		result = append(result, ColumnMapping{
			Name:        column.Name,
			Path:        column.Path,
			TypeID:      column.MoType.Id,
			Width:       column.MoType.Width,
			Scale:       column.MoType.Scale,
			NotNullable: column.MoType.NotNullable,
			Conversion:  column.ConversionMode,
		})
	}
	return result
}

func ColumnsToPlan(columns []ColumnMapping) []*plan.MongoColumnMapping {
	result := make([]*plan.MongoColumnMapping, 0, len(columns))
	for _, column := range columns {
		result = append(result, &plan.MongoColumnMapping{
			Name: column.Name,
			Path: column.Path,
			MoType: plan.Type{
				Id:          column.TypeID,
				Width:       column.Width,
				Scale:       column.Scale,
				NotNullable: column.NotNullable,
			},
			ConversionMode: column.Conversion,
		})
	}
	return result
}

// MarshalPredicateValue encodes a scalar in a one-field BSON document. BSON
// has no standalone-value wire format, so the stable field name is part of the
// connector protocol.
func MarshalPredicateValue(value any) ([]byte, error) {
	return bson.Marshal(bson.D{{Key: "v", Value: value}})
}

func predicateValue(ctx context.Context, data []byte) (any, error) {
	if len(data) == 0 {
		return nil, nil
	}
	raw := bson.Raw(data)
	if err := raw.Validate(); err != nil {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB predicate contains invalid BSON")
	}
	value, err := raw.LookupErr("v")
	if err != nil {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB predicate BSON is missing its scalar value")
	}
	var decoded any
	if err := value.Unmarshal(&decoded); err != nil {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB predicate BSON scalar cannot be decoded")
	}
	return decoded, nil
}

func PredicateFromPlan(ctx context.Context, input *plan.MongoPredicate) (*Predicate, error) {
	if input == nil {
		return nil, nil
	}
	result := &Predicate{Op: PredicateOp(input.Op), Path: input.Path}
	var err error
	if len(input.ValueBson) > 0 {
		result.Value, err = predicateValue(ctx, input.ValueBson)
		if err != nil {
			return nil, err
		}
	}
	result.Values = make([]any, 0, len(input.ValuesBson))
	for _, raw := range input.ValuesBson {
		value, valueErr := predicateValue(ctx, raw)
		if valueErr != nil {
			return nil, valueErr
		}
		result.Values = append(result.Values, value)
	}
	result.Children = make([]*Predicate, 0, len(input.Children))
	for _, child := range input.Children {
		converted, childErr := PredicateFromPlan(ctx, child)
		if childErr != nil {
			return nil, childErr
		}
		result.Children = append(result.Children, converted)
	}
	if err = result.Validate(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func PredicateToPlan(ctx context.Context, input *Predicate) (*plan.MongoPredicate, error) {
	if input == nil {
		return nil, nil
	}
	if err := input.Validate(ctx); err != nil {
		return nil, err
	}
	result := &plan.MongoPredicate{Op: plan.MongoPredicateOp(input.Op), Path: input.Path}
	var err error
	if input.Op != PredicateAnd && input.Op != PredicateIsNull && input.Op != PredicateIsNotNull {
		result.ValueBson, err = MarshalPredicateValue(input.Value)
		if err != nil {
			return nil, err
		}
	}
	for _, value := range input.Values {
		encoded, encodeErr := MarshalPredicateValue(value)
		if encodeErr != nil {
			return nil, encodeErr
		}
		result.ValuesBson = append(result.ValuesBson, encoded)
	}
	for _, child := range input.Children {
		converted, childErr := PredicateToPlan(ctx, child)
		if childErr != nil {
			return nil, childErr
		}
		result.Children = append(result.Children, converted)
	}
	return result, nil
}
