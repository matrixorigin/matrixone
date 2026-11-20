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

package cache

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// BuildEngineDefs reconstructs a private engine view for a transaction table.
// An uninitialized item retains the existing nil-defs behavior.
func (item *TableItem) BuildEngineDefs() ([]engine.TableDef, error) {
	if item == nil {
		return nil, nil
	}
	s := item.schema
	if s == nil {
		s = newCandidateSchema(item)
	}
	if s == nil {
		return nil, nil
	}
	return s.engineDefs(item)
}

// ConstraintBytes returns a private copy of the original constraint stream.
func (item *TableItem) ConstraintBytes() []byte {
	if item.schema != nil {
		return append([]byte(nil), item.schema.constraint.raw...)
	}
	return append([]byte(nil), item.Constraint...)
}

// SchemaSource holds only the immutable schema of one catalog version.
// Its projection methods return independently owned values.
type SchemaSource struct{ schema *candidateSchema }

// SchemaSource returns a handle only for published canonical schemas. Temporary
// items must continue through the validating fallback path.
func (item *TableItem) SchemaSource() SchemaSource {
	if item == nil {
		return SchemaSource{}
	}
	return SchemaSource{schema: item.schema}
}

func (s SchemaSource) Valid() bool { return s.schema != nil }

// Columns projects either all columns or only primary columns, including rowid
// when selected. No mutable child is shared with the source or other calls.
func (s SchemaSource) Columns(primaryOnly bool) []*engine.Attribute {
	if s.schema == nil {
		return nil
	}
	attrs := s.schema.attributes(primaryOnly)
	out := make([]*engine.Attribute, len(attrs))
	for i := range attrs {
		out[i] = &attrs[i].Attr
	}
	return out
}

// EngineDefs reconstructs the complete internal view for a transaction.
func (s SchemaSource) EngineDefs(item *TableItem) ([]engine.TableDef, error) {
	return s.schema.engineDefs(item)
}

// ConstraintBytes returns the original wire stream in private storage.
func (s SchemaSource) ConstraintBytes() []byte {
	if s.schema == nil {
		return nil
	}
	return append([]byte(nil), s.schema.constraint.raw...)
}

// PlanTableDef returns a private planner projection.
func (s SchemaSource) PlanTableDef() *plan.TableDef { return s.schema.projection() }

// BorrowPlanTableDef returns the published planner for read-only use by catalog consumers.
// Callers must copy before modifying any part of the returned graph.
func (s SchemaSource) BorrowPlanTableDef() *plan.TableDef {
	if s.schema == nil {
		return nil
	}
	return s.schema.planner
}
