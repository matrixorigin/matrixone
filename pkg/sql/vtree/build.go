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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/ftree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func New() *build {
	return &build{}
}

func (b *build) Build(ft *ftree.FTree) *ViewTree {
	fvarsMap := make(map[string]uint8)
	for _, fv := range ft.FreeVars {
		fvarsMap[fv] = 0
	}
	ns, _ := buildPath(true, ft.Roots, ft.FreeVars, fvarsMap)
	return constructViewTree(constructViews(pruneNodes(0, ns)), ft, ft.Qry)
}

func constructViewTree(vs []*View, ft *ftree.FTree, qry *plan.Query) *ViewTree {
	if qry.Limit == 0 {
		return nil
	}
	vt := &ViewTree{Views: vs, FreeVars: ft.FreeVars}
	vt.ResultVariables = constructResultVariables(qry.ResultAttributes)
	if len(qry.RestrictConds) > 0 {
		vt.Restrict = &restrict.Argument{
			E: constructRestrict(qry.RestrictConds),
		}
	}
	if len(qry.ProjectionExtends) > 0 {
		vt.Projection = constructProjection(qry.ProjectionExtends)
	}
	if qry.Limit > 0 && qry.Offset == -1 && len(qry.Fields) > 0 { // top
		vt.Top = constructTop(qry.Fields, qry.Limit)
	} else {
		if len(qry.Fields) > 0 {
			vt.Order = constructOrder(qry.Fields)
		}
		if qry.Offset > 0 {
			vt.Offset = &offset.Argument{
				Offset: uint64(qry.Offset),
			}
		}
		if qry.Limit > 0 {
			vt.Limit = &limit.Argument{
				Limit: uint64(qry.Limit),
			}
		}
	}
	if qry.Distinct {
		vt.Dedup = &dedup.Argument{}
	}
	return vt
}

func constructOrder(fs []*plan.Field) *order.Argument {
	arg := &order.Argument{
		Fs: make([]order.Field, len(fs)),
	}
	for i, f := range fs {
		arg.Fs[i].Attr = f.Attr
		arg.Fs[i].Type = order.Direction(f.Type)
	}
	return arg
}

func constructTop(fs []*plan.Field, limit int64) *top.Argument {
	arg := &top.Argument{
		Limit: limit,
		Fs:    make([]top.Field, len(fs)),
	}
	for i, f := range fs {
		arg.Fs[i].Attr = f.Attr
		arg.Fs[i].Type = top.Direction(f.Type)
	}
	return arg
}

func constructResultVariables(attrs []*plan.Attribute) []*Variable {
	vars := make([]*Variable, len(attrs))
	for i, attr := range attrs {
		vars[i] = &Variable{
			Name: attr.Name,
			Type: attr.Type.Oid,
		}
	}
	return vars
}
