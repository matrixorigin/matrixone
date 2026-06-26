// Copyright 2023 Matrix Origin
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

package plan

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// checkConstraintNames Check whether the name of the constraint(index,unqiue etc) is legal, and handle constraints without a name
func checkConstraintNames(uniqueConstraints []*tree.UniqueIndex, indexConstraints []*tree.Index, ctx context.Context) error {
	constrNames := map[string]bool{}
	// Check not empty constraint name whether is duplicated.
	for _, constr := range indexConstraints {
		err := checkDuplicateConstraint(constrNames, constr.Name, false, ctx)
		if err != nil {
			return err
		}
	}
	for _, constr := range uniqueConstraints {
		err := checkDuplicateConstraint(constrNames, constr.Name, false, ctx)
		if err != nil {
			return err
		}
	}
	// set empty constraint names(index and unique index)
	for _, constr := range indexConstraints {
		setEmptyIndexName(constrNames, constr)
	}
	for _, constr := range uniqueConstraints {
		setEmptyUniqueIndexName(constrNames, constr)
	}
	return nil
}

// checkDuplicateConstraint Check whether the constraint name is duplicate
func checkDuplicateConstraint(namesMap map[string]bool, name string, foreign bool, ctx context.Context) error {
	if name == "" {
		return nil
	}
	nameLower := strings.ToLower(name)
	if namesMap[nameLower] {
		if foreign {
			return moerr.NewInvalidInputf(ctx, "Duplicate foreign key constraint name '%s'", name)
		}
		return moerr.NewDuplicateKey(ctx, name)
	}
	namesMap[nameLower] = true
	return nil
}

// setEmptyUniqueIndexName Set name for unqiue index constraint with an empty name
func setEmptyUniqueIndexName(namesMap map[string]bool, indexConstr *tree.UniqueIndex) {
	if indexConstr.Name == "" && len(indexConstr.KeyParts) > 0 {
		colName := indexConstr.KeyParts[0].ColName.ColName()
		constrName := colName
		i := 2
		if strings.EqualFold(constrName, "PRIMARY") {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[constrName] {
			// loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		indexConstr.Name = constrName
		namesMap[constrName] = true
	}
}

// setEmptyIndexName Set name for index constraint with an empty name
func setEmptyIndexName(namesMap map[string]bool, indexConstr *tree.Index) {
	if indexConstr.Name == "" && len(indexConstr.KeyParts) > 0 {
		colName := indexConstr.KeyParts[0].ColName.ColName()
		constrName := colName
		i := 2
		if strings.EqualFold(constrName, "PRIMARY") {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[constrName] {
			//  loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		indexConstr.Name = constrName
		namesMap[constrName] = true
	}
}

// setEmptyFullTextIndexName Set name for index constraint with an empty name
func setEmptyFullTextIndexName(namesMap map[string]bool, indexConstr *tree.FullTextIndex) {
	if indexConstr.Name == "" && len(indexConstr.KeyParts) > 0 {
		colName := indexConstr.KeyParts[0].ColName.ColName()
		constrName := colName
		i := 2
		if strings.EqualFold(constrName, "PRIMARY") {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[constrName] {
			//  loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		indexConstr.Name = constrName
		namesMap[constrName] = true
	}
}

// TODO
// Currently, using expression as index keyparts are not supported in matrixone
func checkIndexKeypartSupportability(context context.Context, keyParts []*tree.KeyPart) error {
	for _, key := range keyParts {
		if key.Expr != nil {
			return moerr.NewInternalError(context, "unsupported index which using expression as keypart")
		}
	}
	return nil
}

func indexTableKeyTypeForSinglePart(col *ColDef, keyPart *tree.KeyPart) Type {
	if col == nil {
		return Type{}
	}
	if keyPart != nil && keyPart.Length > 0 {
		if prefixType, ok := indexTableKeyTypeForPrefix(col.Typ); ok {
			return prefixType
		}
	}
	return Type{
		Id:    col.Typ.Id,
		Width: col.Typ.Width,
		Scale: col.Typ.Scale,
	}
}

func indexTableKeyTypeForPrefix(colType Type) (Type, bool) {
	switch colType.Id {
	case int32(types.T_text):
		return Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		}, true
	case int32(types.T_blob):
		return Type{
			Id:    int32(types.T_varbinary),
			Width: types.MaxVarBinaryLen,
		}, true
	default:
		return Type{}, false
	}
}

func indexColumnCheckKind(indexType tree.IndexType) string {
	switch indexType {
	case tree.INDEX_TYPE_IVFFLAT:
		return "ivfflat"
	case tree.INDEX_TYPE_HNSW:
		return "hnsw"
	case tree.INDEX_TYPE_CAGRA:
		return "cagra"
	case tree.INDEX_TYPE_IVFPQ:
		return "ivfpq"
	case tree.INDEX_TYPE_RTREE:
		return "rtree"
	default:
		return "secondary"
	}
}

func checkIndexColumnSupportability(ctx context.Context, col *ColDef, keyPart *tree.KeyPart, indexKind string) error {
	if col == nil || keyPart == nil || keyPart.ColName == nil {
		return moerr.NewInternalError(ctx, "index column definition is nil")
	}

	colName := keyPart.ColName.ColNameOrigin()

	switch col.Typ.Id {
	case int32(types.T_blob):
		if keyPart.Length > 0 && indexKind != "primary" {
			return nil
		}
		return moerr.NewNotSupported(ctx, fmt.Sprintf("BLOB column '%s' cannot be in index", colName))
	case int32(types.T_text):
		if keyPart.Length > 0 && indexKind != "primary" {
			return nil
		}
		return moerr.NewNotSupported(ctx, fmt.Sprintf("TEXT column '%s' cannot be in index", colName))
	case int32(types.T_datalink):
		return moerr.NewNotSupported(ctx, fmt.Sprintf("DATALINK column '%s' cannot be in index", colName))
	case int32(types.T_json):
		return moerr.NewNotSupported(ctx, fmt.Sprintf("JSON column '%s' cannot be in index", colName))
	case int32(types.T_array_float32), int32(types.T_array_float64),
		int32(types.T_array_float16), int32(types.T_array_bf16),
		int32(types.T_array_int8), int32(types.T_array_uint8):
		// A vector column is valid only as the key of a vector index, AND only if
		// that algorithm supports this element type. Delegate to the plugin's
		// catalog hook (SupportedVectorTypes) rather than hardcoding — each algo
		// differs (ivfflat: f32/f64/f16/bf16/int8/uint8; cagra/ivfpq: f32/f16 only;
		// hnsw: f32/f64). Non-vector index kinds (secondary/primary/unique/rtree)
		// have no plugin, so the vector column is rejected.
		if p, ok := indexplugin.Get(indexKind); ok &&
			catalogplugin.SupportsVectorType(p.Catalog(), types.T(col.Typ.Id)) {
			return nil
		}
		return moerr.NewNotSupported(ctx, fmt.Sprintf("VECTOR column '%s' cannot be in index", colName))
	}

	if isEnumPlanType(&col.Typ) && indexKind == "primary" {
		return moerr.NewNotSupported(ctx, fmt.Sprintf("ENUM column '%s' cannot be in primary key", colName))
	}
	if isSetPlanType(&col.Typ) {
		switch indexKind {
		case "primary":
			return moerr.NewNotSupported(ctx, fmt.Sprintf("SET column '%s' cannot be in primary key", colName))
		case "unique":
			return moerr.NewNotSupported(ctx, fmt.Sprintf("SET column '%s' cannot be in unique index", colName))
		}
	}
	if isGeometryPlanType(&col.Typ) && indexKind != "rtree" {
		switch indexKind {
		case "primary":
			return moerr.NewNotSupported(ctx, fmt.Sprintf("GEOMETRY column '%s' cannot be in primary key", colName))
		case "unique":
			return moerr.NewNotSupported(ctx, fmt.Sprintf("GEOMETRY column '%s' cannot be in unique index", colName))
		default:
			return moerr.NewNotSupported(ctx, fmt.Sprintf("GEOMETRY column '%s' cannot be in index", colName))
		}
	}
	return nil
}
