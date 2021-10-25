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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation/avg"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation/count"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation/max"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation/min"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation/starcount"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggregation/sum"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/op/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/op/summarize"
	"github.com/matrixorigin/matrixone/pkg/sql/tree"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
)

var AggFuncs map[string]int = map[string]int{
	"avg":       aggregation.Avg,
	"max":       aggregation.Max,
	"min":       aggregation.Min,
	"sum":       aggregation.Sum,
	"count":     aggregation.Count,
	"starcount": aggregation.StarCount,
}

func (b *build) hasSummarize(ns tree.SelectExprs) bool {
	for _, n := range ns {
		if b.hasAggregate(n.Expr) {
			return true
		}
	}
	return false
}

func (b *build) buildSummarize(o op.OP, ns tree.SelectExprs, where *tree.Where) (op.OP, error) {
	var err error
	var fs []*tree.FuncExpr
	var es []aggregation.Extend

	prev := o
	{
		for _, n := range ns {
			if !b.hasAggregate(n.Expr) {
				return nil, sqlerror.New(errno.SyntaxError, fmt.Sprintf("noaggregated column '%s'", n.Expr))
			}
		}
	}
	{
		var pes []*projection.Extend

		mp, mq := make(map[string]uint8), make(map[string]uint8)
		if where != nil {
			if err := b.extractExtend(o, where.Expr, &pes, mp); err != nil {
				return nil, err
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
			return nil, sqlerror.New(errno.SyntaxError, fmt.Sprintf("illegal expression '%s'", f))
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
				Name:  prev.ResultColumns()[0],
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
	if o, err = summarize.New(o, es); err != nil {
		return nil, err
	}
	return b.buildProjection(o, ns)
}

func (b *build) stripAggregate(o op.OP, n tree.Expr, fs *[]*tree.FuncExpr, es *[]*projection.Extend, mp, mq map[string]uint8) (tree.Expr, error) {
	var err error

	switch e := n.(type) {
	case *tree.ParenExpr:
		if e.Expr, err = b.stripAggregate(o, e.Expr, fs, es, mp, mq); err != nil {
			return nil, err
		}
		return e, nil
	case *tree.OrExpr:
		if e.Left, err = b.stripAggregate(o, e.Left, fs, es, mp, mq); err != nil {
			return nil, err
		}
		if e.Right, err = b.stripAggregate(o, e.Right, fs, es, mp, mq); err != nil {
			return nil, err
		}
		return e, nil
	case *tree.NotExpr:
		if e.Expr, err = b.stripAggregate(o, e.Expr, fs, es, mp, mq); err != nil {
			return nil, err
		}
		return e, nil
	case *tree.AndExpr:
		if e.Left, err = b.stripAggregate(o, e.Left, fs, es, mp, mq); err != nil {
			return nil, err
		}
		if e.Right, err = b.stripAggregate(o, e.Right, fs, es, mp, mq); err != nil {
			return nil, err
		}
		return e, nil
	case *tree.UnaryExpr:
		if e.Expr, err = b.stripAggregate(o, e.Expr, fs, es, mp, mq); err != nil {
			return nil, err
		}
		return e, nil
	case *tree.BinaryExpr:
		if e.Left, err = b.stripAggregate(o, e.Left, fs, es, mp, mq); err != nil {
			return nil, err
		}
		if e.Right, err = b.stripAggregate(o, e.Right, fs, es, mp, mq); err != nil {
			return nil, err
		}
		return e, nil
	case *tree.ComparisonExpr:
		if e.Left, err = b.stripAggregate(o, e.Left, fs, es, mp, mq); err != nil {
			return nil, err
		}
		if e.Right, err = b.stripAggregate(o, e.Right, fs, es, mp, mq); err != nil {
			return nil, err
		}
		return e, nil
	case *tree.FuncExpr:
		if name, ok := e.Func.FunctionReference.(*tree.UnresolvedName); ok {
			if _, ok = AggFuncs[name.Parts[0]]; ok {
				switch e.Exprs[0].(type) {
				case *tree.NumVal:
					ext, err := b.buildExtend(o, &tree.UnresolvedName{
						Parts: [4]string{o.ResultColumns()[0]},
					})
					if err != nil {
						return nil, err
					}
					if _, ok := mp[ext.String()]; !ok {
						mp[ext.String()] = 0
						*(es) = append((*es), &projection.Extend{E: ext})
					}
					if _, ok := mq["count(*)"]; !ok {
						*(fs) = append(*(fs), e)
						mq["count(*)"] = 0
					}
					return &tree.UnresolvedName{
						Parts: [4]string{"count(*)"},
					}, nil
				default:
					ext, err := b.buildExtend(o, e.Exprs[0])
					if err != nil {
						return nil, err
					}
					if _, ok := mp[ext.String()]; !ok {
						mp[ext.String()] = 0
						*(es) = append((*es), &projection.Extend{E: ext})
					}
					e.Exprs[0] = &tree.UnresolvedName{
						Parts: [4]string{ext.String()},
					}
					fn := fmt.Sprintf("%s(%s)", name.Parts[0], ext)
					if _, ok := mq[fn]; !ok {
						*(fs) = append(*(fs), e)
						mq[fn] = 0
					}
					return &tree.UnresolvedName{
						Parts: [4]string{fn},
					}, nil
				}
			}
		}
		return e, nil
	case *tree.RangeCond:
		if e.To, err = b.stripAggregate(o, e.To, fs, es, mp, mq); err != nil {
			return nil, err
		}
		if e.Left, err = b.stripAggregate(o, e.Left, fs, es, mp, mq); err != nil {
			return nil, err
		}
		if e.From, err = b.stripAggregate(o, e.From, fs, es, mp, mq); err != nil {
			return nil, err
		}
		return e, nil
	}
	return n, nil
}

func newAggregate(op int, typ types.Type) (aggregation.Aggregation, error) {
	switch op {
	case aggregation.Avg:
		switch typ.Oid {
		case types.T_float32, types.T_float64:
			return avg.NewFloat(typ), nil
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			return avg.NewInt(typ), nil
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			return avg.NewUint(typ), nil
		}
	case aggregation.Max:
		switch typ.Oid {
		case types.T_int8:
			return max.NewInt8(typ), nil
		case types.T_int16:
			return max.NewInt16(typ), nil
		case types.T_int32:
			return max.NewInt32(typ), nil
		case types.T_int64:
			return max.NewInt64(typ), nil
		case types.T_uint8:
			return max.NewUint8(typ), nil
		case types.T_uint16:
			return max.NewUint16(typ), nil
		case types.T_uint32:
			return max.NewUint32(typ), nil
		case types.T_uint64:
			return max.NewUint64(typ), nil
		case types.T_float32:
			return max.NewFloat32(typ), nil
		case types.T_float64:
			return max.NewFloat64(typ), nil
		case types.T_char, types.T_varchar:
			return max.NewStr(typ), nil
		}
	case aggregation.Min:
		switch typ.Oid {
		case types.T_int8:
			return min.NewInt8(typ), nil
		case types.T_int16:
			return min.NewInt16(typ), nil
		case types.T_int32:
			return min.NewInt32(typ), nil
		case types.T_int64:
			return min.NewInt64(typ), nil
		case types.T_uint8:
			return min.NewUint8(typ), nil
		case types.T_uint16:
			return min.NewUint16(typ), nil
		case types.T_uint32:
			return min.NewUint32(typ), nil
		case types.T_uint64:
			return min.NewUint64(typ), nil
		case types.T_float32:
			return min.NewFloat32(typ), nil
		case types.T_float64:
			return min.NewFloat64(typ), nil
		case types.T_char, types.T_varchar:
			return min.NewStr(typ), nil
		}
	case aggregation.Sum:
		switch typ.Oid {
		case types.T_float32, types.T_float64:
			return sum.NewFloat(typ), nil
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			return sum.NewInt(typ), nil
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			return sum.NewUint(typ), nil
		}
	case aggregation.Count:
		return count.New(typ), nil
	case aggregation.StarCount:
		return starcount.New(typ), nil
	}
	return nil, sqlerror.New(errno.UndefinedFunction, fmt.Sprintf("unimplemented aggregation '%v' for '%s'", op, typ))
}
