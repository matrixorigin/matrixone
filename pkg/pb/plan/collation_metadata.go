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
				if err := collation.RequireLegacy(value.DefaultCharset, value.CollationVersion, value.KeyFormat); err != nil {
					return err
				}
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
