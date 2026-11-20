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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// catalogColumn stores only fields populated by the catalog column producer.
// The complete engine type is retained independently of the planner type.
type catalogColumn struct {
	typ                               types.Type
	plannerType                       plan.Type
	name, originName, comment         string
	colID                             uint64
	seqnum                            uint32
	alg                               compress.T
	defaultValue                      *plan.Default
	onUpdate                          *plan.OnUpdate
	generated                         *plan.GeneratedCol
	rowID, hidden, primary, clusterBy bool
}

type prototypeSchema struct {
	candidate  *candidateSchema
	planner    *plan.TableDef
	columns    []catalogColumn
	constraint schemaConstraint
}

func newPrototypeSchema(item *TableItem) *prototypeSchema {
	c := newCandidateSchema(item)
	if c == nil {
		return nil
	}
	return &prototypeSchema{candidate: c, constraint: c.constraint}
}

func (c catalogColumn) attribute() engine.Attribute {
	return engine.Attribute{IsHidden: c.hidden, IsRowId: c.rowID, ID: c.colID, Name: c.originName, Alg: c.alg, Type: c.typ,
		Default: plan.CopyDefaultInto(new(plan.Default), c.defaultValue), OnUpdate: cloneOnUpdate(c.onUpdate), Primary: c.primary,
		ClusterBy: c.clusterBy, Comment: c.comment, AutoIncrement: c.plannerType.AutoIncr, Seqnum: uint16(c.seqnum),
		EnumVlaues: c.plannerType.Enumvalues, GeneratedCol: cloneGeneratedCol(c.generated), NumericAttributes: uint8(c.plannerType.NumericAttributes)}
}

func (s *prototypeSchema) attributes() []engine.Attribute {
	if s.candidate != nil {
		return s.candidate.expanded().attributes()
	}
	out := make([]engine.Attribute, len(s.columns))
	for i, c := range s.columns {
		out[i] = c.attribute()
	}
	return out
}

// engineDefs reconstructs the internal engine view from the original metadata
// and a private decoding of the original constraint stream.
func (s *prototypeSchema) engineDefs(item *TableItem) ([]engine.TableDef, error) {
	columns := s.attributes()
	defs := make([]engine.TableDef, 0, len(columns)+4)
	if item.Comment != "" {
		defs = append(defs, &engine.CommentDef{Comment: item.Comment})
	}
	if item.ViewDef != "" {
		defs = append(defs, &engine.ViewDef{View: item.ViewDef})
	}
	if len(s.constraint.raw) != 0 {
		c, err := s.constraint.decode()
		if err != nil {
			return nil, err
		}
		defs = append(defs, c)
	}
	if item.Partitioned > 0 {
		defs = append(defs, &engine.PartitionDef{Partitioned: item.Partitioned, Partition: item.Partition})
	}
	props := &engine.PropertiesDef{Properties: []engine.Property{
		{Key: catalog.SystemRelAttr_Kind, Value: item.Kind},
		{Key: catalog.PropSchemaExtra, Value: string(api.MustMarshalTblExtra(item.ExtraInfo))},
	}}
	if item.CreateSql != "" {
		props.Properties = append(props.Properties, engine.Property{Key: catalog.SystemRelAttr_CreateSQL, Value: item.CreateSql})
	}
	defs = append(defs, props)
	for i := range columns {
		defs = append(defs, &engine.AttributeDef{Attr: columns[i]})
	}
	return defs, nil
}

func (s *prototypeSchema) projection() *plan.TableDef {
	if s.candidate != nil {
		return s.candidate.expanded().projection()
	}
	p := plan.CopyCatalogTableTemplate(s.planner)
	cols := make([]plan.ColDef, len(s.columns))
	p.Cols = make([]*plan.ColDef, len(cols))
	defaultCount := 0
	for _, c := range s.columns {
		if c.defaultValue != nil {
			defaultCount++
		}
	}
	defaults := make([]plan.Default, defaultCount)
	nextDefault := 0
	for i, c := range s.columns {
		var defaultValue *plan.Default
		if c.defaultValue != nil {
			defaultValue = plan.CopyDefaultInto(&defaults[nextDefault], c.defaultValue)
			nextDefault++
		}
		cols[i] = plan.ColDef{ColId: c.colID, Name: c.name, OriginName: c.originName, Typ: plan.CopyCatalogType(c.plannerType), Primary: c.primary, Default: defaultValue, OnUpdate: cloneOnUpdate(c.onUpdate), GeneratedCol: cloneGeneratedCol(c.generated), Comment: c.comment, ClusterBy: c.clusterBy, Hidden: c.hidden, Seqnum: c.seqnum}
		p.Cols[i] = &cols[i]
	}
	if p.Pkey != nil && p.Pkey.PkeyColName == catalog.CPrimaryKeyColName {
		p.Pkey.CompPkeyCol = plan2.GetColDefFromTable(p.Cols, catalog.CPrimaryKeyColName)
	}
	if p.ClusterBy != nil && util.JudgeIsCompositeClusterByColumn(p.ClusterBy.Name) {
		p.ClusterBy.CompCbkeyCol = plan2.GetColDefFromTable(p.Cols, p.ClusterBy.Name)
	}
	return p
}

// BuildEngineDefs reconstructs a private engine view for a transaction table.
// An uninitialized item retains the existing nil-defs behavior.
func (item *TableItem) BuildEngineDefs() ([]engine.TableDef, error) {
	if item == nil {
		return nil, nil
	}
	s := item.prototype
	if s == nil {
		s = newPrototypeSchema(item)
	}
	if s == nil {
		return nil, nil
	}
	return s.engineDefs(item)
}

// ConstraintBytes returns a private copy of the original constraint stream.
func (item *TableItem) ConstraintBytes() []byte {
	if item.prototype != nil {
		return append([]byte(nil), item.prototype.constraint.raw...)
	}
	return append([]byte(nil), item.Constraint...)
}

// SchemaSource holds only the immutable schema of one catalog version.
// Its projection methods return independently owned values.
type SchemaSource struct{ schema *prototypeSchema }

// SchemaSource returns a handle only for published canonical schemas. Temporary
// items must continue through the validating fallback path.
func (item *TableItem) SchemaSource() SchemaSource {
	if item == nil {
		return SchemaSource{}
	}
	return SchemaSource{schema: item.prototype}
}

func (s SchemaSource) Valid() bool { return s.schema != nil }

// Columns projects either all columns or only primary columns, including rowid
// when selected. No mutable child is shared with the source or other calls.
func (s SchemaSource) Columns(primaryOnly bool) []*engine.Attribute {
	if s.schema == nil {
		return nil
	}
	schema := s.schema
	if schema.candidate != nil {
		if primaryOnly {
			return schema.candidate.primaryColumns()
		}
		schema = schema.candidate.expanded()
	}
	n := 0
	for _, c := range schema.columns {
		if !primaryOnly || c.primary {
			n++
		}
	}
	values := make([]engine.Attribute, n)
	out := make([]*engine.Attribute, n)
	i := 0
	for _, c := range schema.columns {
		if primaryOnly && !c.primary {
			continue
		}
		values[i] = c.attribute()
		out[i] = &values[i]
		i++
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
	if s.schema == nil || s.schema.candidate == nil {
		return nil
	}
	return s.schema.candidate.planner
}
