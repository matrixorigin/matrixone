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
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/colexec/aggregation"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/op"
	"matrixone/pkg/sql/op/group"
	"matrixone/pkg/sql/op/projection"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/sqlerror"
)

func (b *build) buildGroupBy(o op.OP, ns tree.SelectExprs, grs tree.GroupBy, where *tree.Where) (op.OP, error) {
	var err error
	var fs []*tree.FuncExpr
	var gs []*extend.Attribute
	var es []aggregation.Extend

	{
		var pes []*projection.Extend

		mp, mq := make(map[string]uint8), make(map[string]uint8)
		if where != nil {
			if err := b.extractExtend(o, where.Expr, &pes, mp); err != nil {
				return nil, err
			}
		}
		{
			for _, g := range grs {
				e, err := b.buildExtend(o, g)
				if err != nil {
					return nil, err
				}
				if _, ok := mp[e.String()]; !ok {
					mp[e.String()] = 0
					pes = append(pes, &projection.Extend{E: e})
				}
				gs = append(gs, &extend.Attribute{
					Name: e.String(),
					Type: e.ReturnType(),
				})
			}
		}
		for i, n := range ns {
			if ns[i].Expr, err = b.stripAggregate(o, n.Expr, &fs, &pes, mp, mq); err != nil {
				return nil, err
			}
		}
		if len(pes) > 0 {
			if o, err = projection.New(o, pes); err != nil {
				return nil, err
			}
		}
		if where != nil {
			if o, err = b.buildWhere(o, where); err != nil {
				return nil, err
			}
		}
	}
	for _, f := range fs {
		name, ok := f.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("illegal expression '%s'", f))
		}
		op, ok := AggFuncs[name.Parts[0]]
		if !ok {
			return nil, sqlerror.New(errno.UndefinedFunction, fmt.Sprintf("unimplemented aggregated functions '%s'", name.Parts[0]))
		}
		switch e := f.Exprs[0].(type) {
		case *tree.NumVal:
			alias := "count(*)"
			agg, err := newAggregate(op, types.Type{Oid: types.T_int64, Size: 8})
			if err != nil {
				return nil, err
			}
			es = append(es, aggregation.Extend{
				Agg:   agg,
				Alias: alias,
				Name:  o.Columns()[0],
				Op:    aggregation.StarCount,
			})
		case *tree.UnresolvedName:
			alias := fmt.Sprintf("%s(%s)", name.Parts[0], e.Parts[0])
			typ, ok := o.Attribute()[e.Parts[0]]
			if !ok {
				return nil, sqlerror.New(errno.UndefinedColumn, fmt.Sprintf("unknown column '%s' in aggregation", e.Parts[0]))
			}
			agg, err := newAggregate(op, typ)
			if err != nil {
				return nil, err
			}
			es = append(es, aggregation.Extend{
				Op:    op,
				Agg:   agg,
				Alias: alias,
				Name:  e.Parts[0],
			})
		}
	}
	if o, err = group.New(o, gs, es); err != nil {
		return nil, err
	}
	return b.buildProjection(o, ns)
}
