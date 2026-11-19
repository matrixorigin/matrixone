// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// Numeric presentation/domain attributes are not represented in the bounded
// witness. Decline them rather than authorize a rewritten covering projection
// after dropping part of its type contract.
func indexPlanningColumnValue(col *plan.ColDef) (indexPlanningColumn, bool) {
	var out indexPlanningColumn
	if col == nil || col.Name == catalog.Row_ID || col.Typ.Id == int32(types.T_Rowid) || col.GeneratedCol != nil || col.Typ.Id < 0 || col.Typ.Id > 255 || col.Typ.Enumvalues != "" || col.Typ.NumericAttributes != 0 {
		return out, false
	}
	name, ok := makeIndexPlanningName(col.Name)
	if !ok || col.Seqnum > 65535 {
		return out, false
	}
	out = indexPlanningColumn{Name: name, ID: col.ColId, Sequence: col.Seqnum, Type: col.Typ.Id, Width: col.Typ.Width, Scale: col.Typ.Scale, Charset: col.Typ.Charset, CollationVersion: col.Typ.CollationVersion, NotNull: col.NotNull, TypeNotNull: col.Typ.NotNullable, AutoIncrement: col.Typ.AutoIncr, PadSpace: col.Typ.PadSpace, CollationCoercibility: col.Typ.CollationCoercibility, CollationCoercibilitySet: col.Typ.CollationCoercibilitySet, CollationMergeConflict: col.Typ.CollationMergeConflict, Primary: col.Primary}
	return out, true
}

func indexPlanningOptionalName(value string) (indexPlanningName, bool) {
	if value == "" {
		return indexPlanningName{}, true
	}
	return makeIndexPlanningName(value)
}

func findIndexPlanningColumn(table *plan.TableDef, name string, budget *int) (indexPlanningColumn, bool) {
	if table == nil || len(name) == 0 || len(name) > engine.MaxIndexPlanningNameBytes || len(table.Cols) == 0 || len(table.Cols) > engine.MaxIndexPlanningColumns {
		return indexPlanningColumn{}, false
	}
	var found *plan.ColDef
	rowIDSeen := false
	for _, col := range table.Cols {
		if budget == nil || *budget <= 0 {
			return indexPlanningColumn{}, false
		}
		*budget--
		if col == nil {
			return indexPlanningColumn{}, false
		}
		// GetTableDef may append a virtual rowid with the default sequence 0.
		// It has no physical column position and cannot participate in a binding.
		if col.Name == catalog.Row_ID {
			if rowIDSeen || !indexPlanningVirtualRowIDSupported(col) {
				return indexPlanningColumn{}, false
			}
			rowIDSeen = true
			continue
		}
		if col.Typ.Id == int32(types.T_Rowid) {
			return indexPlanningColumn{}, false
		}
		if col.Name == name {
			if found != nil {
				return indexPlanningColumn{}, false
			}
			found = col
		}
	}
	if found == nil {
		return indexPlanningColumn{}, false
	}
	for _, col := range table.Cols {
		if budget == nil || *budget <= 0 {
			return indexPlanningColumn{}, false
		}
		*budget--
		if col.Name != catalog.Row_ID && col != found && col.Seqnum == found.Seqnum {
			return indexPlanningColumn{}, false
		}
	}
	return indexPlanningColumnValue(found)
}

func indexPlanningVirtualRowIDSupported(col *plan.ColDef) bool {
	return col != nil && col.Name == catalog.Row_ID && col.Hidden && col.Typ.Id == int32(types.T_Rowid) && !col.Primary && !col.ClusterBy && !col.Typ.AutoIncr && col.GeneratedCol == nil
}

// Projection pruning follows index selection, so a scan definition may still
// describe an unused virtual rowid. Validate that metadata without assigning it
// a physical position; callers separately prove which columns the plan reads.
func matchIndexPlanningVirtualRowID(full *plan.TableDef, projected *plan.ColDef, budget *int) bool {
	if !indexPlanningVirtualRowIDSupported(projected) {
		return false
	}
	var found *plan.ColDef
	for _, col := range full.Cols {
		if budget == nil || *budget <= 0 {
			return false
		}
		*budget--
		if col == nil {
			return false
		}
		if col.Name == catalog.Row_ID {
			if found != nil || !indexPlanningVirtualRowIDSupported(col) {
				return false
			}
			found = col
		}
	}
	return found != nil && found.ColId == projected.ColId && found.Seqnum == projected.Seqnum
}

// Ordinary DDL persists a visibility marker even without user options.
// Only modeled, visible/default options can share this physical binding.
func indexPlanningOptionSupported(option *plan.IndexOption) bool {
	return option == nil || !option.CreateExtraTable && option.ParserName == "" && option.NgramTokenSize == 0 && len(option.XXX_unrecognized) == 0 && (option.Visibility == plan.IndexOption_VISIBILITY_UNSPECIFIED || option.Visibility == plan.IndexOption_VISIBILITY_VISIBLE)
}

// copyIndexBaseSchemaWithBudget shares one bounded work allowance across a
// statement's candidates; exhausting it declines the optional observation.
func copyIndexBaseSchemaWithBudget(base *plan.TableDef, hiddenName string, samples []string, budget *int) (indexBaseSchema, bool) {
	var out indexBaseSchema
	if budget == nil || *budget <= 0 {
		return out, false
	}
	if *budget > engine.MaxIndexPlanningWork {
		*budget = engine.MaxIndexPlanningWork
	}
	if base == nil || base.Pkey == nil || base.TblId == 0 || len(hiddenName) == 0 || len(hiddenName) > engine.MaxIndexPlanningNameBytes || len(samples) == 0 || len(samples) > maxIndexPlanningSamples || len(base.Indexes) > engine.MaxIndexPlanningDefinitions || len(base.Cols) > engine.MaxIndexPlanningColumns || len(base.Pkey.Names) > engine.MaxIndexPlanningParts || base.IsTemporary || base.IsDynamic || base.FeatureFlag != 0 || base.KeyFormat != 0 {
		return out, false
	}
	var definition *plan.IndexDef
	for _, candidate := range base.Indexes {
		if *budget <= 0 {
			return out, false
		}
		*budget--
		if candidate == nil {
			return out, false
		}
		if candidate.IndexTableName == hiddenName {
			if definition != nil {
				return out, false
			}
			definition = candidate
		}
	}
	if definition == nil || !definition.TableExist || !catalog.IsRegularIndexAlgo(definition.IndexAlgo) || catalog.IsRTreeIndexAlgo(definition.IndexAlgo) || len(definition.Parts) == 0 || len(definition.Parts) > engine.MaxIndexPlanningParts || len(definition.IncludedColumns) > engine.MaxIndexPlanningParts || !indexPlanningOptionSupported(definition.Option) || definition.KeyFormat != 0 || definition.IndexAlgoParams != "" {
		return out, false
	}
	var ok bool
	idx := &out.Index
	if idx.Name, ok = makeIndexPlanningName(definition.IndexName); !ok {
		return indexBaseSchema{}, false
	}
	if idx.HiddenName, ok = makeIndexPlanningName(definition.IndexTableName); !ok {
		return indexBaseSchema{}, false
	}
	if idx.Algorithm, ok = indexPlanningOptionalName(definition.IndexAlgo); !ok {
		return indexBaseSchema{}, false
	}
	if idx.AlgorithmType, ok = indexPlanningOptionalName(definition.IndexAlgoTableType); !ok {
		return indexBaseSchema{}, false
	}
	if idx.Parameters, ok = indexPlanningOptionalName(definition.IndexAlgoParams); !ok {
		return indexBaseSchema{}, false
	}
	idx.Unique, idx.Visible, idx.TableExist, idx.KeyFormat = definition.Unique, definition.Visible, definition.TableExist, definition.KeyFormat
	for i, part := range definition.Parts {
		if idx.Parts[i].Name, ok = makeIndexPlanningName(part); !ok {
			return indexBaseSchema{}, false
		}
		if idx.Parts[i].Column, ok = findIndexPlanningColumn(base, catalog.ResolveAlias(part), budget); !ok {
			return indexBaseSchema{}, false
		}
	}
	idx.PartCount = uint8(len(definition.Parts))
	for i, name := range definition.IncludedColumns {
		if idx.Includes[i].Name, ok = makeIndexPlanningName(name); !ok {
			return indexBaseSchema{}, false
		}
		if idx.Includes[i].Column, ok = findIndexPlanningColumn(base, name, budget); !ok {
			return indexBaseSchema{}, false
		}
	}
	idx.IncludeCount = uint8(len(definition.IncludedColumns))
	for i, name := range samples {
		for _, previous := range samples[:i] {
			if previous == name {
				return indexBaseSchema{}, false
			}
		}
		if out.Samples[i], ok = findIndexPlanningColumn(base, name, budget); !ok {
			return indexBaseSchema{}, false
		}
	}
	out.SampleCount = uint8(len(samples))
	if out.BasePrimary, ok = findIndexPlanningColumn(base, base.Pkey.PkeyColName, budget); !ok {
		return indexBaseSchema{}, false
	}
	for i, name := range base.Pkey.Names {
		if out.BasePrimaryParts[i], ok = findIndexPlanningColumn(base, name, budget); !ok {
			return indexBaseSchema{}, false
		}
	}
	out.BasePrimaryPartCount = uint8(len(base.Pkey.Names))
	out.BaseID, out.BaseVersion = base.TblId, base.Version
	out.BaseDefaultCharset, out.BaseCollationVersion = base.DefaultCharset, base.CollationVersion
	return out, true
}

// copyIndexSchemaPairWithBudget compares already-resolved definitions without
// catalog access, sharing the statement's allowance across all candidates.
// A pair alone proves neither source visibility nor authorization.
func copyIndexSchemaPairWithBudget(base, hidden *plan.TableDef, hiddenName string, samples []string, budget *int) (indexSchemaPair, bool) {
	var out indexSchemaPair
	if budget == nil || *budget <= 0 || !indexPlanningHiddenSchemaSupported(hidden) {
		return out, false
	}
	var ok bool
	out.BaseSchema, ok = copyIndexBaseSchemaWithBudget(base, hiddenName, samples, budget)
	if !ok {
		return indexSchemaPair{}, false
	}
	if out.HiddenPrimary, ok = findIndexPlanningColumn(hidden, catalog.IndexTableIndexColName, budget); !ok {
		return indexSchemaPair{}, false
	}
	if out.HiddenPayload, ok = findIndexPlanningColumn(hidden, catalog.IndexTablePrimaryColName, budget); !ok {
		return indexSchemaPair{}, false
	}
	if !sameIndexPhysicalType(out.BaseSchema.BasePrimary, out.HiddenPayload) {
		return indexSchemaPair{}, false
	}
	if out.BaseSchema.Index.PartCount == 1 {
		if !sameIndexPhysicalType(out.BaseSchema.Index.Parts[0].Column, out.HiddenPrimary) {
			return indexSchemaPair{}, false
		}
	} else if types.T(out.HiddenPrimary.Type) != types.T_varchar {
		return indexSchemaPair{}, false
	}
	out.HiddenID, out.HiddenVersion = hidden.TblId, hidden.Version
	out.HiddenDefaultCharset, out.HiddenCollationVersion = hidden.DefaultCharset, hidden.CollationVersion
	return out, true
}

func sameIndexPhysicalType(a, b indexPlanningColumn) bool {
	return a.Type == b.Type && a.Width == b.Width && a.Scale == b.Scale && a.Charset == b.Charset && a.CollationVersion == b.CollationVersion
}

// indexPlanningHiddenSchemaSupported admits only index layouts the reader can prove.
func indexPlanningHiddenSchemaSupported(hidden *plan.TableDef) bool {
	if hidden == nil || hidden.Pkey == nil || hidden.TblId == 0 || len(hidden.Cols) < 2 || len(hidden.Cols) > 3 || hidden.IsTemporary || hidden.IsDynamic || hidden.FeatureFlag != features.IndexTable || hidden.KeyFormat != 0 || hidden.ClusterBy != nil || hidden.Pkey.PkeyColName != catalog.IndexTableIndexColName {
		return false
	}
	// Composite or differently named primary-key metadata changes the reader's
	// pruning semantics and is not represented by this bounded schema pair.
	if hidden.Pkey.CompPkeyCol != nil || len(hidden.Pkey.Names) > 1 || len(hidden.Pkey.Names) == 1 && hidden.Pkey.Names[0] != catalog.IndexTableIndexColName {
		return false
	}
	for _, col := range hidden.Cols {
		if col == nil || col.Typ.AutoIncr || col.Name == catalog.Row_ID && !indexPlanningVirtualRowIDSupported(col) || (col.Name != catalog.IndexTableIndexColName && col.Name != catalog.IndexTablePrimaryColName && col.Name != catalog.Row_ID) {
			return false
		}
	}
	return true
}

// Validate projected columns and table attributes against the complete binding.
func matchIndexProjectionWithBudget(full, projection *plan.TableDef, expected *indexBaseSchema, budget *int) bool {
	if full == nil || projection == nil || expected == nil || budget == nil || *budget <= 0 || len(projection.Cols) == 0 || len(projection.Cols) > engine.MaxIndexPlanningColumns || len(full.Cols) > engine.MaxIndexPlanningColumns || full.TblId != projection.TblId || full.Version != projection.Version || len(full.Name) > engine.MaxIndexPlanningNameBytes || full.Name != projection.Name || full.DbId != projection.DbId || len(full.DbName) > engine.MaxIndexPlanningNameBytes || full.DbName != projection.DbName || len(full.TableType) > engine.MaxIndexPlanningNameBytes || full.TableType != projection.TableType || full.Hidden != projection.Hidden || full.LogicalId != projection.LogicalId || full.KeyFormat != projection.KeyFormat || full.FeatureFlag != projection.FeatureFlag || full.IsTemporary || projection.IsTemporary || full.IsDynamic || projection.IsDynamic || full.Partition != nil || projection.Partition != nil || full.TblFunc != nil || projection.TblFunc != nil || full.ViewSql != nil || projection.ViewSql != nil || full.ClusterBy != nil || projection.ClusterBy != nil || full.Pkey == nil || projection.Pkey == nil || full.Pkey.Option != nil || projection.Pkey.Option != nil {
		return false
	}
	if *budget > engine.MaxIndexPlanningWork {
		*budget = engine.MaxIndexPlanningWork
	}

	if full.Pkey.PkeyColId != projection.Pkey.PkeyColId || len(full.Pkey.Cols) > engine.MaxIndexPlanningParts || len(projection.Pkey.Cols) != len(full.Pkey.Cols) {
		return false
	}
	for i, col := range full.Pkey.Cols {
		if col != projection.Pkey.Cols[i] {
			return false
		}
	}
	if (full.Pkey.CompPkeyCol == nil) != (projection.Pkey.CompPkeyCol == nil) {
		return false
	}
	if full.Pkey.CompPkeyCol != nil {
		a, aOK := indexPlanningColumnValue(full.Pkey.CompPkeyCol)
		b, bOK := indexPlanningColumnValue(projection.Pkey.CompPkeyCol)
		if !aOK || !bOK || a != b {
			return false
		}
	}
	for i, col := range projection.Cols {
		if *budget <= 0 {
			return false
		}
		*budget--
		if col == nil {
			return false
		}
		if col.Name == catalog.Row_ID {
			if !matchIndexPlanningVirtualRowID(full, col, budget) {
				return false
			}
		} else {
			value, ok := indexPlanningColumnValue(col)
			if !ok {
				return false
			}
			native, ok := findIndexPlanningColumn(full, col.Name, budget)
			if !ok || value != native {
				return false
			}
		}
		for _, previous := range projection.Cols[:i] {
			if *budget <= 0 {
				return false
			}
			*budget--
			if previous.Name == col.Name || previous.Name != catalog.Row_ID && col.Name != catalog.Row_ID && previous.Seqnum == col.Seqnum {
				return false
			}
		}
	}
	// Use complete columns only after proving that every remaining projected
	// column belongs to them; copy just the small definition header, not a table.
	merged := *projection
	merged.Cols = full.Cols
	var samples [maxIndexPlanningSamples]string
	if expected.SampleCount == 0 || expected.SampleCount > maxIndexPlanningSamples {
		return false
	}
	for i := range int(expected.SampleCount) {
		samples[i] = expected.Samples[i].Name.String()
	}
	observed, ok := copyIndexBaseSchemaWithBudget(&merged, expected.Index.HiddenName.String(), samples[:expected.SampleCount], budget)
	if !ok || observed != *expected {
		return false
	}
	native, ok := copyIndexBaseSchemaWithBudget(full, expected.Index.HiddenName.String(), samples[:expected.SampleCount], budget)
	return ok && native == *expected
}

const maxIndexPlanningSamples = 4

// indexPlanningName is an owned, bounded value, used only for bounded relation-definition comparison.
// It cannot retain the backing storage of a query or catalog string.
type indexPlanningName struct {
	Length uint16
	Bytes  [engine.MaxIndexPlanningNameBytes]byte
}

func makeIndexPlanningName(value string) (indexPlanningName, bool) {
	var out indexPlanningName
	if len(value) == 0 || len(value) > len(out.Bytes) {
		return out, false
	}
	out.Length = uint16(len(value))
	copy(out.Bytes[:], value)
	return out, true
}

func (n indexPlanningName) String() string {
	if n.Length > engine.MaxIndexPlanningNameBytes {
		return ""
	}
	return string(n.Bytes[:n.Length])
}

// indexPlanningColumn contains only the bounded physical and semantic type fields
// needed to compare a bound plan with a catalog observation.
type indexPlanningColumn struct {
	Name                     indexPlanningName
	ID                       uint64
	Sequence                 uint32
	Type                     int32
	Width                    int32
	Scale                    int32
	Charset                  uint32
	CollationVersion         uint32
	NotNull                  bool
	TypeNotNull              bool
	AutoIncrement            bool
	PadSpace                 bool
	CollationCoercibility    uint32
	CollationCoercibilitySet bool
	CollationMergeConflict   bool
	Primary                  bool
}

// indexPlanningPart retains the original alias as well as the resolved base
// column. The alias is part of the physical key format and must not be lost.
type indexPlanningPart struct {
	Name   indexPlanningName
	Column indexPlanningColumn
}

type indexPlanningDefinition struct {
	Name          indexPlanningName
	HiddenName    indexPlanningName
	Algorithm     indexPlanningName
	AlgorithmType indexPlanningName
	Parameters    indexPlanningName
	PartCount     uint8
	Parts         [engine.MaxIndexPlanningParts]indexPlanningPart
	IncludeCount  uint8
	Includes      [engine.MaxIndexPlanningParts]indexPlanningPart
	Unique        bool
	Visible       bool
	TableExist    bool
	KeyFormat     uint32
}

// indexBaseSchema is a bounded copy of the authorized final-plan binding.
// It contains no plan pointers and is used synchronously by the planning owner.
type indexBaseSchema struct {
	BaseID                                                uint64
	BaseVersion, BaseDefaultCharset, BaseCollationVersion uint32
	BasePrimary                                           indexPlanningColumn
	BasePrimaryPartCount                                  uint8
	BasePrimaryParts                                      [engine.MaxIndexPlanningParts]indexPlanningColumn
	SampleCount                                           uint8
	Samples                                               [maxIndexPlanningSamples]indexPlanningColumn
	Index                                                 indexPlanningDefinition
}

// indexSchemaPair contains no pointers into catalog entries or plan objects.
// A caller must still establish its ordinary transaction and statement owner.
type indexSchemaPair struct {
	BaseSchema                                                  indexBaseSchema
	HiddenID                                                    uint64
	HiddenVersion, HiddenDefaultCharset, HiddenCollationVersion uint32
	HiddenPrimary, HiddenPayload                                indexPlanningColumn
}
