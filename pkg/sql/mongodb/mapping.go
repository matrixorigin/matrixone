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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type TableMappingSpec struct {
	Mapping TableMapping
}

func ParseTableMappingSpec(
	ctx context.Context,
	param *tree.MongoDBTableParam,
	defs tree.TableDefs,
	tableDef *plan.TableDef,
) (TableMappingSpec, error) {
	if param == nil || tableDef == nil {
		return TableMappingSpec{}, moerr.NewInvalidInput(ctx, "MongoDB external table mapping is missing")
	}
	options := make(map[string]string, len(param.Options))
	for _, option := range param.Options {
		if option == nil {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(string(option.Key)))
		if key == "" || strings.TrimSpace(option.Val) == "" {
			return TableMappingSpec{}, moerr.NewInvalidInput(ctx, "MongoDB table option key and value cannot be empty")
		}
		if _, exists := options[key]; exists {
			return TableMappingSpec{}, moerr.NewInvalidInputf(ctx, "duplicate MongoDB table option %s", key)
		}
		switch key {
		case "connection", "database", "collection", "schema_mode", "conversion_mode", "split_key", "max_parallelism":
		default:
			return TableMappingSpec{}, moerr.NewInvalidInputf(ctx, "unsupported MongoDB table option %s", key)
		}
		options[key] = strings.TrimSpace(option.Val)
	}
	mapping := TableMapping{
		Connection:     options["connection"],
		Database:       options["database"],
		Collection:     options["collection"],
		SchemaMode:     defaultString(options["schema_mode"], SchemaExplicit),
		Conversion:     defaultString(options["conversion_mode"], ConversionStrict),
		SplitKey:       options["split_key"],
		MaxParallelism: 1,
	}
	if mapping.Connection == "" || mapping.Database == "" || mapping.Collection == "" {
		return TableMappingSpec{}, moerr.NewInvalidInput(ctx, "MongoDB external table requires connection, database, and collection options")
	}
	if strings.ContainsAny(mapping.Database+mapping.Collection, "\x00") {
		return TableMappingSpec{}, moerr.NewInvalidInput(ctx, "MongoDB namespace contains an invalid character")
	}
	if mapping.SchemaMode != SchemaExplicit {
		return TableMappingSpec{}, moerr.NewNotSupported(ctx, "MongoDB MVP requires schema_mode=explicit")
	}
	if mapping.Conversion != ConversionStrict && mapping.Conversion != ConversionTryNull {
		return TableMappingSpec{}, moerr.NewInvalidInput(ctx, "MongoDB conversion_mode must be strict or try_null")
	}
	if value := options["max_parallelism"]; value != "" {
		parsed, err := strconv.ParseInt(value, 10, 32)
		if err != nil || parsed != 1 {
			return TableMappingSpec{}, moerr.NewNotSupported(ctx, "MongoDB MVP requires max_parallelism=1")
		}
	}

	columnDefs := make([]*tree.ColumnTableDef, 0, len(defs))
	for _, def := range defs {
		if column, ok := def.(*tree.ColumnTableDef); ok {
			columnDefs = append(columnDefs, column)
		}
	}
	// buildTableDefs may append MO-internal hidden columns before the external
	// engine is attached to the TableDef. They are storage metadata, not BSON
	// mappings, and MongoScan must neither require nor emit them.
	plannedColumns := make([]*plan.ColDef, 0, len(tableDef.Cols))
	for _, column := range tableDef.Cols {
		if column != nil && !column.Hidden {
			plannedColumns = append(plannedColumns, column)
		}
	}
	if len(columnDefs) == 0 || len(columnDefs) != len(plannedColumns) {
		return TableMappingSpec{}, moerr.NewInvalidInput(ctx, "MongoDB external table requires an explicit scalar column schema")
	}
	mapping.Columns = make([]ColumnMapping, 0, len(columnDefs))
	for i, column := range columnDefs {
		planned := plannedColumns[i]
		if !strings.EqualFold(column.Name.ColName(), planned.Name) {
			return TableMappingSpec{}, moerr.NewInternalError(ctx, "MongoDB parsed and planned column order diverged")
		}
		path := planned.Name
		conversion := mapping.Conversion
		seenPath := false
		seenConversion := false
		for _, attribute := range column.Attributes {
			switch typed := attribute.(type) {
			case *tree.AttributeMongoDBPath:
				if seenPath {
					return TableMappingSpec{}, moerr.NewInvalidInputf(ctx, "duplicate MONGODB_PATH for column %s", planned.Name)
				}
				seenPath = true
				path = typed.Path
			case *tree.AttributeMongoDBConvert:
				if seenConversion {
					return TableMappingSpec{}, moerr.NewInvalidInputf(ctx, "duplicate MONGODB_CONVERT for column %s", planned.Name)
				}
				seenConversion = true
				conversion = strings.ToLower(strings.TrimSpace(typed.Mode))
			}
		}
		if conversion != ConversionStrict && conversion != ConversionTryNull {
			return TableMappingSpec{}, moerr.NewInvalidInputf(ctx, "invalid MONGODB_CONVERT for column %s", planned.Name)
		}
		if err := ValidateBSONPath(ctx, path); err != nil {
			return TableMappingSpec{}, err
		}
		notNullable := planned.Typ.NotNullable
		// CREATE TABLE records declared nullability in Default.NullAbility,
		// while resolved catalog TableDefs also expose it on Typ.NotNullable.
		// Honor both representations so the mapping cannot weaken the schema.
		if planned.Default != nil && !planned.Default.NullAbility {
			notNullable = true
		}
		mapping.Columns = append(mapping.Columns, ColumnMapping{
			Name:        planned.Name,
			Path:        path,
			TypeID:      planned.Typ.Id,
			Width:       planned.Typ.Width,
			Scale:       planned.Typ.Scale,
			NotNullable: notNullable,
			Conversion:  conversion,
		})
	}
	if _, err := NewConverter(ctx, mapping.Columns, DefaultRuntimeConfig().MaxValueBytes); err != nil {
		return TableMappingSpec{}, err
	}
	return TableMappingSpec{Mapping: mapping}, nil
}
