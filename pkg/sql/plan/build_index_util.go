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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// indexNameKey defines the comparison contract shared by index creation,
// duplicate detection, lookup, and sequential ALTER state. It deliberately
// uses the parser's identifier normalization instead of strings.EqualFold:
// Unicode simple folding can equate distinct MySQL identifiers such as Greek
// sigma (Σ) and final sigma (ς).
func indexNameKey(name string) string {
	return tree.NewCStr(name, 1).Compare()
}

// IndexNamesEqual compares index identifiers using the canonical parser
// normalization shared by index creation, duplicate detection, and lookup.
func IndexNamesEqual(left, right string) bool {
	return indexNameKey(left) == indexNameKey(right)
}

// resolveIndexName matches index identifiers while returning the catalog
// spelling for later execution and metadata updates.
func resolveIndexName(indexes []*planpb.IndexDef, name string) (string, bool) {
	key := indexNameKey(name)
	for _, index := range indexes {
		if index != nil && indexNameKey(index.IndexName) == key {
			return index.IndexName, true
		}
	}
	return "", false
}

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
	nameKey := indexNameKey(name)
	if namesMap[nameKey] {
		if foreign {
			return moerr.NewInvalidInputf(ctx, "Duplicate foreign key constraint name '%s'", name)
		}
		return moerr.NewDuplicateKey(ctx, name)
	}
	namesMap[nameKey] = true
	return nil
}

// setEmptyUniqueIndexName Set name for unqiue index constraint with an empty name
func setEmptyUniqueIndexName(namesMap map[string]bool, indexConstr *tree.UniqueIndex) {
	if indexConstr.Name == "" && len(indexConstr.KeyParts) > 0 {
		colName := indexConstr.KeyParts[0].ColName.ColName()
		constrName := colName
		i := 2
		if IndexNamesEqual(constrName, "PRIMARY") {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[indexNameKey(constrName)] {
			// loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		indexConstr.Name = constrName
		namesMap[indexNameKey(constrName)] = true
	}
}

// setEmptyIndexName Set name for index constraint with an empty name
func setEmptyIndexName(namesMap map[string]bool, indexConstr *tree.Index) {
	if indexConstr.Name == "" && len(indexConstr.KeyParts) > 0 {
		colName := indexConstr.KeyParts[0].ColName.ColName()
		constrName := colName
		i := 2
		if IndexNamesEqual(constrName, "PRIMARY") {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[indexNameKey(constrName)] {
			//  loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		indexConstr.Name = constrName
		namesMap[indexNameKey(constrName)] = true
	}
}

// setEmptyFullTextIndexName Set name for index constraint with an empty name
func setEmptyFullTextIndexName(namesMap map[string]bool, indexConstr *tree.FullTextIndex) {
	if indexConstr.Name == "" && len(indexConstr.KeyParts) > 0 {
		colName := indexConstr.KeyParts[0].ColName.ColName()
		constrName := colName
		i := 2
		if IndexNamesEqual(constrName, "PRIMARY") {
			constrName = fmt.Sprintf("%s_%d", constrName, 2)
			i = 3
		}
		for namesMap[indexNameKey(constrName)] {
			//  loop forever until we find constrName that haven't been used.
			constrName = fmt.Sprintf("%s_%d", colName, i)
			i++
		}
		indexConstr.Name = constrName
		namesMap[indexNameKey(constrName)] = true
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
	if isVersionedCollationType(col.Typ) {
		// Native 0900 keys are opaque comparison weights. Keep the source
		// column's value in the base table, but make the hidden index column
		// binary so storage and hash consumers never apply the transform twice.
		return Type{
			Id:      int32(types.T_varbinary),
			Width:   types.MaxVarBinaryLen,
			Charset: uint32(types.CharsetBinary),
		}
	}
	if keyPart != nil && keyPart.Length > 0 {
		if prefixType, ok := indexTableKeyTypeForPrefix(col.Typ); ok {
			return prefixType
		}
	}
	// Preserve Enumvalues so a single-column UNIQUE index on an ENUM/SET
	// column stores the typed key with the same enum metadata as the base
	// column. DML index-maintenance joins compare the source value against
	// this index-table column; without matching Enumvalues the equality bind
	// either fails (nil-pointer panic) or compares values in incompatible
	// representations.
	return Type{
		Id:               col.Typ.Id,
		Width:            col.Typ.Width,
		Scale:            col.Typ.Scale,
		Enumvalues:       col.Typ.Enumvalues,
		Charset:          col.Typ.Charset,
		CollationVersion: col.Typ.CollationVersion,
	}
}

func hasNative0900KeyParts(colMap map[string]*ColDef, parts []*tree.KeyPart) bool {
	for _, part := range parts {
		if part == nil || part.ColName == nil {
			continue
		}
		col, ok := colMap[part.ColName.ColName()]
		if ok && isVersionedCollationType(col.Typ) {
			return true
		}
	}
	return false
}

func hasNative0900Columns(colMap map[string]*ColDef, names []string) bool {
	for _, name := range names {
		col, ok := colMap[name]
		if ok && isVersionedCollationType(col.Typ) {
			return true
		}
	}
	return false
}

func isNative0900Type(typ Type) bool {
	// Keep the historical helper name for callers, but make the decision from
	// the explicit semantic version as well. General-ci and utf8mb4_bin use the
	// same physical hidden-key layout once a rebuilt schema opts into V1.
	return isVersionedCollationType(typ)
}

func setPhysicalKeyFormat(tableDef *TableDef, indexDef *IndexDef, native0900 bool) {
	if !native0900 {
		return
	}
	format := uint32(types.PADSpaceKeyV1)
	if tableDef != nil {
		tableDef.KeyFormat = format
	}
	if indexDef != nil {
		indexDef.KeyFormat = format
	}
}

// recomputeTableCollationMetadata derives the physical key format from the
// final schema. The table CollationVersion field records the default semantic
// domain and is deliberately preserved here; a column-level collation must not
// become the default for unrelated columns. ALTER COPY mutates the copied
// schema in several independent steps (column changes, primary-key changes and
// index removal), so stale key formats must be cleared and rebuilt from the
// final PK/index references.
func recomputeTableCollationMetadata(ctx context.Context, tableDef *TableDef) error {
	if tableDef == nil {
		return nil
	}
	if err := validateTableCollationMetadata(ctx, tableDef); err != nil {
		return err
	}
	byName := make(map[string]*ColDef, len(tableDef.Cols)*2)
	for _, col := range tableDef.Cols {
		if col == nil {
			continue
		}
		byName[col.Name] = col
		byName[catalog.ResolveAlias(col.Name)] = col
	}
	lookup := func(name string) (*ColDef, bool) {
		col, ok := byName[name]
		if ok {
			return col, true
		}
		col, ok = byName[catalog.ResolveAlias(name)]
		return col, ok
	}
	containsVersioned := func(names []string) (bool, error) {
		for _, name := range names {
			col, ok := lookup(name)
			if !ok {
				return false, moerr.NewInvalidInputf(ctx,
					"key references unknown column '%s'", name)
			}
			if isVersionedCollationType(col.Typ) {
				return true, nil
			}
		}
		return false, nil
	}

	tableDef.KeyFormat = uint32(types.LegacyKeyFormat)
	if tableDef.Pkey != nil {
		versioned, err := containsVersioned(tableDef.Pkey.Names)
		if err != nil {
			return err
		}
		if versioned {
			tableDef.KeyFormat = uint32(types.PADSpaceKeyV1)
		}
	}
	for _, index := range tableDef.Indexes {
		if index == nil {
			continue
		}
		versioned, err := containsVersioned(index.Parts)
		if err != nil {
			return moerr.NewInvalidInputf(ctx, "index %q: %s", index.IndexName, err.Error())
		}
		if versioned {
			index.KeyFormat = uint32(types.PADSpaceKeyV1)
			tableDef.KeyFormat = uint32(types.PADSpaceKeyV1)
		} else {
			index.KeyFormat = uint32(types.LegacyKeyFormat)
		}
	}
	return nil
}

// validateTableCollationMetadata is the schema boundary for the versioned
// collation contract. Protobuf keeps unknown integers when decoding, so a
// caller must reject them before recomputing formats or narrowing them into
// the in-memory Type/KeyFormat representation. In particular, recompute must
// not turn an unknown persisted format into legacy merely because the final
// schema happens to have no transformed key parts.
func validateTableCollationMetadata(ctx context.Context, tableDef *TableDef) error {
	if tableDef == nil {
		return nil
	}
	if err := types.ValidateKeyFormat(tableDef.KeyFormat); err != nil {
		return moerr.NewInvalidInputf(ctx, "table %q: %s", tableDef.Name, err.Error())
	}
	if err := types.ValidateCollationVersion(tableDef.CollationVersion); err != nil {
		return moerr.NewInvalidInputf(ctx, "table %q: %s", tableDef.Name, err.Error())
	}
	for _, col := range tableDef.Cols {
		if col == nil {
			continue
		}
		if col.Typ.Id < 0 || col.Typ.Id > 255 {
			return moerr.NewInvalidInputf(ctx, "table %q column %q has unsupported type id %d", tableDef.Name, col.Name, col.Typ.Id)
		}
		if err := types.ValidateCollationTypeMetadata(
			types.T(col.Typ.Id), col.Typ.Charset, col.Typ.CollationVersion,
		); err != nil {
			return moerr.NewInvalidInputf(ctx, "table %q column %q: %s", tableDef.Name, col.Name, err.Error())
		}
	}
	for _, index := range tableDef.Indexes {
		if index == nil {
			continue
		}
		if err := types.ValidateKeyFormat(index.KeyFormat); err != nil {
			return moerr.NewInvalidInputf(ctx, "index %q: %s", index.IndexName, err.Error())
		}
	}
	return nil
}

func indexTableKeyTypeForPrefix(colType Type) (Type, bool) {
	switch colType.Id {
	case int32(types.T_text):
		return Type{
			Id:               int32(types.T_varchar),
			Width:            types.MaxVarcharLen,
			Charset:          colType.Charset,
			CollationVersion: colType.CollationVersion,
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
