// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"reflect"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func (t Type) ValidateCollation() error {
	if t.Id < 0 || t.Id > 255 || t.Charset > 255 || t.CollationVersion > uint32(collation.RevisionV1) {
		return moerr.NewNotSupportedNoCtx("unknown type collation metadata")
	}
	if t.CollationCoercibility > 6 || (!t.CollationCoercibilitySet && t.CollationCoercibility != 0) {
		return moerr.NewInvalidInputNoCtx("invalid collation coercibility")
	}
	if isPlanMySQLStringType(t.Id) {
		return collation.ValidateMetadata(t.Charset, t.CollationVersion)
	}
	if t.CollationVersion != 0 {
		return moerr.NewInvalidInputNoCtx("collation revision on a non-string type")
	}
	if t.Charset == 255 {
		return nil
	} // Existing numeric CAST marker.
	return collation.ValidateMetadata(t.Charset, 0)
}

// SameCollation includes expression provenance, which can change binding even
// when the current physical vector types happen to match.
func (t Type) SameCollation(other Type) bool {
	return t.Charset == other.Charset && t.CollationVersion == other.CollationVersion &&
		t.CollationCoercibility == other.CollationCoercibility &&
		t.CollationCoercibilitySet == other.CollationCoercibilitySet &&
		t.CollationMergeConflict == other.CollationMergeConflict
}

func (t *TableDef) ValidateCollation() error {
	if err := collation.ValidateMetadata(t.DefaultCharset, t.CollationVersion); err != nil {
		return err
	}
	if err := collation.ValidateKeyFormat(t.KeyFormat); err != nil {
		return err
	}
	for _, index := range t.Indexes {
		if index == nil {
			continue
		}
		if err := collation.ValidateKeyFormatPair(t.KeyFormat, index.KeyFormat); err != nil {
			// Keep the existing execution-boundary contract explicit: a
			// partially upgraded or mismatched index must be rejected as a
			// disabled physical-key domain, before any plan is admitted.
			return moerr.NewInvalidInputNoCtxf("disabled collation key format: %v", err)
		}
	}
	return nil
}

func (t *IndexDef) ValidateCollation() error {
	return collation.ValidateKeyFormat(t.KeyFormat)
}

func requireLegacyType(value Type) error {
	if err := value.ValidateCollation(); err != nil {
		return err
	}
	if isPlanMySQLStringType(value.Id) {
		return collation.RequireLegacy(value.Charset, value.CollationVersion, 0)
	}
	return nil
}

func requireLegacySchema(charset, revision, keyFormat uint32) error {
	return collation.RequireLegacy(charset, revision, keyFormat)
}

// Native UCA unique keys are persisted with the versioned physical key format
// only after the planner has selected the complete new-table contract.  Keep
// this admission rule in the plan package so old or partially upgraded plans
// cannot smuggle KeyFormatV1 through a generic metadata check.
func isNativeUnicodeUniqueType(typ Type) bool {
	if !isPlanMySQLStringType(typ.Id) ||
		(typ.Charset != uint32(collation.UTF8UnicodeCIIdentity) &&
			typ.Charset != uint32(collation.UTF8MB4UnicodeCIIdentity)) {
		return false
	}
	return typ.CollationVersion == uint32(collation.RevisionV1)
}

func isPhysicalIndexTable(value *TableDef) bool {
	if value == nil || value.IsTemporary || value.TableType == SystemExternalRel ||
		(value.TableType != "" && value.TableType != "i") ||
		!strings.HasPrefix(value.Name, "__mo_index_unique_") || tableRelKind(value) != "i" {
		return false
	}
	if value.Pkey == nil || len(value.Cols) == 0 {
		return false
	}
	// A physical unique-index relation is identified by the exact primary
	// key column emitted by buildUniqueIndexTable.  Name-prefix matching alone
	// would let an arbitrary internal-looking relation bypass the legacy
	// admission fence.
	if value.Pkey.PkeyColName != "__mo_index_idx_col" ||
		len(value.Pkey.Names) != 1 || value.Pkey.Names[0] != "__mo_index_idx_col" ||
		len(value.Indexes) != 0 {
		return false
	}
	hasKeyColumn := false
	for _, col := range value.Cols {
		if col != nil && col.Name == "__mo_index_idx_col" {
			hasKeyColumn = true
		}
		if col != nil && isNativeUnicodeUniqueType(col.Typ) {
			return false
		}
	}
	return hasKeyColumn
}

func tableRelKind(value *TableDef) string {
	if value == nil {
		return ""
	}
	for _, def := range value.Defs {
		if def == nil {
			continue
		}
		properties, ok := def.GetDef().(*TableDef_DefType_Properties)
		if !ok || properties.Properties == nil {
			continue
		}
		for _, property := range properties.Properties.Properties {
			if property != nil && strings.EqualFold(property.Key, "relkind") {
				return strings.ToLower(property.Value)
			}
		}
	}
	return ""
}

func hasNativeUnicodeUniqueIndex(value *TableDef) bool {
	if value == nil {
		return false
	}
	columns := make(map[string]Type, len(value.Cols))
	for _, col := range value.Cols {
		if col != nil {
			columns[strings.ToLower(col.Name)] = col.Typ
		}
	}
	for _, index := range value.Indexes {
		if isNativeUnicodeUniqueIndex(columns, index) {
			return true
		}
	}
	return false
}

func hasNativeUnicodePrimaryKey(value *TableDef) bool {
	if value == nil || value.Pkey == nil {
		return false
	}
	columns := make(map[string]Type, len(value.Cols))
	for _, col := range value.Cols {
		if col != nil {
			columns[strings.ToLower(col.Name)] = col.Typ
		}
	}
	for _, name := range value.Pkey.Names {
		if typ, ok := columns[strings.ToLower(name)]; ok && isNativeUnicodeUniqueType(typ) {
			return true
		}
	}
	return false
}

func isNativeUnicodeUniqueTable(value *TableDef) bool {
	relKind := tableRelKind(value)
	if value == nil || value.KeyFormat != collation.KeyFormatV1 || value.IsTemporary ||
		value.TableType == SystemExternalRel || value.TableType == "cluster" ||
		(value.TableType != "" && value.TableType != "r") ||
		relKind == SystemExternalRel || relKind == "cluster" ||
		(relKind != "" && relKind != "r") ||
		hasNativeUnicodePrimaryKey(value) {
		return false
	}
	// A versioned table marker is meaningful only for a table that actually
	// owns a versioned unique-key definition.  Without this guard, an
	// arbitrary TableDef carrying KeyFormatV1/CollationVersionV1 but no index
	// could bypass the legacy admission fence merely because the all-index
	// validation below is vacuously true.  Optimizer-pruned shells retain the
	// index definitions, so requiring one here does not discard the durable
	// native-unique proof.
	if len(value.Indexes) == 0 {
		return false
	}
	// Optimizer pruning intentionally removes unrelated columns from scan and
	// DML table shells.  The durable table-level revision marker therefore
	// carries the native-unique proof after the native column itself is no
	// longer present in the plan.  A complete catalog definition still proves
	// the marker from the indexed column identity directly.
	if !hasNativeUnicodeUniqueIndex(value) &&
		value.CollationVersion != uint32(collation.RevisionV1) {
		return false
	}
	for _, index := range value.Indexes {
		if index == nil || !index.Unique || index.KeyFormat != collation.KeyFormatV1 {
			return false
		}
	}
	return true
}

func isNativeUnicodeUniqueIndex(columns map[string]Type, index *IndexDef) bool {
	if index == nil || !index.Unique || index.KeyFormat != collation.KeyFormatV1 {
		return false
	}
	for _, part := range index.Parts {
		if typ, ok := columns[strings.ToLower(part)]; ok && isNativeUnicodeUniqueType(typ) {
			return true
		}
	}
	return false
}

func requireLegacyTable(value *TableDef) error {
	if value == nil {
		return nil
	}
	if err := value.ValidateCollation(); err != nil {
		return err
	}
	if value.KeyFormat == collation.KeyFormatV1 &&
		(isPhysicalIndexTable(value) || isNativeUnicodeUniqueTable(value)) {
		return nil
	}
	return requireLegacySchema(value.DefaultCharset, value.CollationVersion, value.KeyFormat)
}

func requireLegacyTableIndex(table *TableDef, index *IndexDef) error {
	if index == nil {
		return nil
	}
	if err := index.ValidateCollation(); err != nil {
		return err
	}
	if index.KeyFormat == collation.KeyFormatV1 && table != nil {
		// A V1 base table owns the physical format for every index relation
		// created with it.  The planner currently limits this path to unique
		// indexes, but legacy-collated unique parts are still valid members of
		// the same table-level format contract.
		if isPhysicalIndexTable(table) || isNativeUnicodeUniqueTable(table) {
			return nil
		}
	}
	return requireLegacySchema(0, 0, index.KeyFormat)
}

// RequireLegacyCollations admits mutable plans and foreign pipeline owners.
// Generated paths visit only metadata-bearing protobuf fields; foreign wrappers
// retain a reflective fallback. Each call owns its cycle state, so mutation
// between planner publication and direct compilation is checked again.
// Byte payloads remain opaque and are admitted by their actual decode owner.
func RequireLegacyCollations(owner any) error {
	visitor := legacyCollationVisitor{}
	if handled, err := visitor.typed(owner); handled {
		return err
	}
	seen := make(map[uintptr]struct{})
	var walk func(reflect.Value) error
	walk = func(v reflect.Value) error {
		if !v.IsValid() {
			return nil
		}
		if v.Kind() == reflect.Interface {
			if v.IsNil() {
				return nil
			}
			return walk(v.Elem())
		}
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return nil
			}
			if v.CanInterface() {
				if handled, err := visitor.typed(v.Interface()); handled {
					return err
				}
			}
			if _, ok := seen[v.Pointer()]; ok {
				return nil
			}
			seen[v.Pointer()] = struct{}{}
			return walk(v.Elem())
		}
		// Foreign wrappers retain their admission boundary. Only metadata
		// structs are boxed here; scalar fields and opaque bytes are not.
		if v.Kind() == reflect.Struct && v.CanInterface() &&
			(v.Type() == reflect.TypeOf(Type{}) || v.Type() == reflect.TypeOf(TableDef{}) || v.Type() == reflect.TypeOf(IndexDef{})) {
			switch value := v.Interface().(type) {
			case Type:
				return requireLegacyType(value)
			case TableDef:
				return visitor.walkTableDef(&value)
			case IndexDef:
				if err := collation.RequireLegacy(0, 0, value.KeyFormat); err != nil {
					return err
				}
			}
		}
		switch v.Kind() {
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if v.Type().Field(i).PkgPath == "" {
					if err := walk(v.Field(i)); err != nil {
						return err
					}
				}
			}
		case reflect.Slice, reflect.Array:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return nil
			}
			for i := 0; i < v.Len(); i++ {
				if err := walk(v.Index(i)); err != nil {
					return err
				}
			}
		case reflect.Map:
			iter := v.MapRange()
			for iter.Next() {
				if err := walk(iter.Value()); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(reflect.ValueOf(owner))
}
