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

package vtree

import (
	"bytes"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/colexec/extend/overload"
	"matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/ftree"
	"matrixone/pkg/sql/plan"
	"matrixone/pkg/sql/viewexec/transform"
	"matrixone/pkg/sql/viewexec/transformer"
)

func isBare(vs []*View) bool {
	for _, v := range vs {
		if v.Arg != nil && len(v.Arg.BoundVars) > 0 {
			return false
		}
		if len(v.Children) > 0 && !isBare(v.Children) {
			return false
		}
	}
	return true
}

func constructViews(ns []*node) []*View {
	if len(ns) == 0 {
		return nil
	}
	vs := make([]*View, len(ns))
	for i, n := range ns {
		switch cont := n.content.(type) {
		case *ftree.Variable:
			vs[i] = constructViewByVariable(n, cont)
		case *ftree.Relation:
			vs[i] = constructViewByRelation(n, cont)
		}
	}
	return vs
}

func constructViewByVariable(n *node, fvar *ftree.Variable) *View {
	return &View{
		Children: constructViews(n.children),
		Name:     constructViewRelationName(n.rns),
		Var:      &Variable{Ref: fvar.Ref, Name: fvar.Name},
	}
}

func constructViewByRelation(n *node, frel *ftree.Relation) *View {
	v := &View{
		FreeVars: n.freeVars,
		Rel:      constructViewRelation(frel),
		Name:     constructViewRelationName(n.rns),
	}
	arg := &transform.Argument{
		FreeVars: n.freeVars,
	}
	if len(frel.Rel.RestrictConds) > 0 {
		arg.Restrict = &restrict.Argument{
			E: constructRestrict(frel.Rel.RestrictConds),
		}
	}
	if len(frel.Rel.ProjectionExtends) > 0 {
		arg.Projection = constructProjection(frel.Rel.ProjectionExtends)
	}
	if len(frel.Rel.Aggregations) > 0 {
		arg.BoundVars = constructAggregation(frel.Rel.Aggregations)
	}
	v.Arg = arg
	return v
}

func constructViewRelationName(rns []string) string {
	var buf bytes.Buffer

	for i, rn := range rns {
		if i > 0 {
			buf.WriteString("-")
		}
		buf.WriteString(rn)
	}
	return buf.String()
}

func constructViewRelation(frel *ftree.Relation) *Relation {
	vars := make([]*Variable, len(frel.Rel.Attrs))
	for i, name := range frel.Rel.Attrs {
		attr := frel.Rel.AttrsMap[name]
		vars[i] = &Variable{
			Ref:  attr.Ref,
			Name: attr.Name,
		}
	}
	return &Relation{
		Vars:   vars,
		Alias:  frel.Rel.Alias,
		Name:   frel.Rel.Name,
		Schema: frel.Rel.Schema,
	}
}

func constructAggregation(aggs []*plan.Aggregation) []transformer.Transformer {
	bvars := make([]transformer.Transformer, len(aggs))
	for i := range aggs {
		bvars[i].Op = aggs[i].Op
		bvars[i].Ref = aggs[i].Ref
		bvars[i].Name = aggs[i].Name
		bvars[i].Alias = aggs[i].Alias
	}
	return bvars
}

func constructProjection(es []*plan.ProjectionExtend) *projection.Argument {
	arg := &projection.Argument{
		Rs: make([]uint64, len(es)),
		As: make([]string, len(es)),
		Es: make([]extend.Extend, len(es)),
	}
	for i := range es {
		arg.Es[i] = es[i].E
		arg.As[i] = es[i].Alias
		arg.Rs[i] = uint64(es[i].Ref)
	}
	return arg
}

func constructRestrict(es []extend.Extend) extend.Extend {
	if len(es) == 1 {
		return es[0]
	}
	return &extend.BinaryExtend{
		Op:    overload.And,
		Left:  es[0],
		Right: constructRestrict(es[1:]),
	}
}
