// Copyright 2021 Matrix Origin
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

package build

import (
	"bytes"
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/colexec/transformer"
	"matrixone/pkg/vm/engine"
)

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

type Aggregation struct {
	Ref   int     // reference count
	Op    int     // opcode of aggregation function
	Type  types.T // return type of aggregation function
	Name  string  // name of attribute
	Alias string
}

type ProjectionExtend struct {
	Ref   int // reference count
	Alias string
	E     extend.Extend
}

type Attribute struct {
	Ref  int        // reference count
	Name string     // name of attribute
	Type types.Type // type of attribute
}

type Relation struct {
	Alias             string
	Name              string // table name
	Schema            string // schema name
	Query             *Query // relation may be a subquery
	Attrs             []string
	AttrsMap          map[string]*Attribute
	Aggregations      []*Aggregation
	RestrictConds     []extend.Extend
	ProjectionExtends []*ProjectionExtend
}

type Field struct {
	Attr string
	Type Direction
}

type JoinCondition struct {
	R     string
	S     string
	Rattr string
	Sattr string
}

type Query struct {
	Distinct          bool
	Limit             int64
	Offset            int64
	FreeAttrs         []string
	Rels              []string
	RelsMap           map[string]*Relation
	Fields            []*Field
	RestrictConds     []extend.Extend
	Conds             []*JoinCondition
	ProjectionExtends []*ProjectionExtend
}

type build struct {
	flg bool   // use for having clause
	db  string // name of schema
	sql string
	e   engine.Engine
}

func (qry *Query) String() string {
	var buf bytes.Buffer

	buf.WriteString(fmt.Sprintf("free attributes: %v\n", qry.FreeAttrs))
	buf.WriteString(fmt.Sprintf("relations: %v\n", qry.Rels))
	for _, rel := range qry.Rels {
		buf.WriteString(fmt.Sprintf("\t%s = %s\n", rel, qry.RelsMap[rel]))
	}
	switch {
	case qry.Limit != -1 && qry.Offset != -1:
		buf.WriteString(fmt.Sprintf("Limit %v, %v\n", qry.Offset, qry.Limit))
	case qry.Limit != -1 && qry.Offset == -1:
		buf.WriteString(fmt.Sprintf("Limit %v\n", qry.Limit))
	}
	buf.WriteString("join conditions\n")
	for _, cond := range qry.Conds {
		buf.WriteString(fmt.Sprintf("\t%s\n", cond))
	}
	buf.WriteString(fmt.Sprintf("\trestrict conditions\n"))
	for _, cond := range qry.RestrictConds {
		buf.WriteString(fmt.Sprintf("\t\t%s\n", cond))
	}
	buf.WriteString("\textend projection\n")
	for _, e := range qry.ProjectionExtends {
		buf.WriteString(fmt.Sprintf("\t\t%s\n", e))
	}
	buf.WriteString("\torder by\n")
	for _, f := range qry.Fields {
		buf.WriteString(fmt.Sprintf("\t\t%s\n", f))
	}
	return buf.String()
}

func (rel *Relation) ExistProjection(name string) int {
	for i, e := range rel.ProjectionExtends {
		if e.Alias == name {
			return i
		}
	}
	return -1
}

func (rel *Relation) ExistAggregation(name string) int {
	for i, agg := range rel.Aggregations {
		if agg.Alias == name {
			return i
		}
	}
	return -1
}

func (rel *Relation) AddRestrict(e extend.Extend) {
	rel.RestrictConds = append(rel.RestrictConds, e)
}

func (rel *Relation) AddProjection(e *ProjectionExtend) {
	rel.ProjectionExtends = append(rel.ProjectionExtends, e)
}

func (rel *Relation) AddAggregation(agg *Aggregation) {
	rel.Aggregations = append(rel.Aggregations, agg)
}

func (rel *Relation) GetAttributes() []*Attribute {
	attrs := make([]*Attribute, len(rel.Attrs))
	for i, attr := range rel.Attrs {
		attrs[i] = rel.AttrsMap[attr]
	}
	return attrs
}

func (rel *Relation) String() string {
	var buf bytes.Buffer

	if rel.Query != nil {
		buf.WriteString(fmt.Sprintf("%s -> %s\n", rel.Query, rel.Alias))
	} else {
		buf.WriteString(fmt.Sprintf("%s.%s -> %s\n", rel.Schema, rel.Name, rel.Alias))
	}
	buf.WriteString(fmt.Sprintf("\tattributes: %v\n", rel.Attrs))
	for _, attr := range rel.Attrs {
		buf.WriteString(fmt.Sprintf("\t\t%s = %s\n", attr, rel.AttrsMap[attr]))
	}
	buf.WriteString("\trestrict conditions\n")
	for _, cond := range rel.RestrictConds {
		buf.WriteString(fmt.Sprintf("\t\t%s\n", cond))
	}
	buf.WriteString("\textend projection\n")
	for _, e := range rel.ProjectionExtends {
		buf.WriteString(fmt.Sprintf("\t\t%s\n", e))
	}
	buf.WriteString("\tAggregation functions\n")
	for _, agg := range rel.Aggregations {
		buf.WriteString(fmt.Sprintf("\t\t%s\n", agg))
	}
	return buf.String()
}

func (attr *Attribute) IncRef() {
	attr.Ref++
}

func (attr *Attribute) DecDef() {
	attr.Ref--
}

func (attr *Attribute) String() string {
	return fmt.Sprintf("%s[%s] = %v", attr.Name, attr.Type, attr.Ref)
}

func (cond *JoinCondition) String() string {
	return fmt.Sprintf("%s.%s = %s.%s", cond.R, cond.Rattr, cond.S, cond.Sattr)
}

func (e *ProjectionExtend) IncRef() {
	e.Ref++
}

func (e *ProjectionExtend) DecDef() {
	e.Ref--
}

func (e *ProjectionExtend) String() string {
	return fmt.Sprintf("'%s as %s' = %v", e.E, e.Alias, e.Ref)
}

func (agg *Aggregation) IncRef() {
	agg.Ref++
}

func (agg *Aggregation) DecDef() {
	agg.Ref--
}

func (agg *Aggregation) String() string {
	return fmt.Sprintf("'%s(%s)' = %v", transformer.TransformerNames[agg.Op], agg.Name, agg.Ref)
}

func (n *Field) String() string {
	s := n.Attr
	if n.Type != DefaultDirection {
		s += " " + n.Type.String()
	}
	return s
}

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

func (i Direction) String() string {
	if i < 0 || i > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", i)
	}
	return directionName[i]
}
