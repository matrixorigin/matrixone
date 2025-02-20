// Copyright 2022 Matrix Origin
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

package partitionprune

import (
	"context"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	p "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Filter(
	proc *process.Process,
	filters []*plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, error) {
	if len(filters) == 0 {
		res := make([]int, len(metadata.Partitions))
		for i, pt := range metadata.Partitions {
			res[i] = int(pt.Position)
		}
		return res, nil
	}
	switch metadata.Method {
	case partition.PartitionMethod_Range:
		return rangeFilter(proc, filters, metadata)
	case partition.PartitionMethod_Hash,
		partition.PartitionMethod_Key:
		return hashFilter(proc, filters, metadata)
	case partition.PartitionMethod_List:
		return listFilter(proc, filters, metadata)
	}
	return nil, nil
}

func hashFilter(
	proc *process.Process,
	filters []*plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, error) {
	colPosition := mustGetColPosition(metadata.Partitions[0].Expr)
	tm := map[int]struct{}{}
	for _, expr := range filters {
		targets, ok, err := hashFilterExpr(proc, colPosition, expr, metadata)
		if err != nil {
			return nil, err
		}
		if !ok {
			res := make([]int, len(metadata.Partitions))
			for i, pt := range metadata.Partitions {
				res[i] = int(pt.Position)
			}
			return res, nil
		}
		for _, target := range targets {
			tm[target] = struct{}{}
		}
	}
	res := make([]int, 0, len(tm))
	for target := range tm {
		res = append(res, target)
	}
	return res, nil
}

func hashFilterExpr(
	proc *process.Process,
	colPosition int32,
	expr *plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, bool, error) {
	exprs := make([]*plan.Expr, len(metadata.Partitions))
	for i, pt := range metadata.Partitions {
		exprs[i] = p.DeepCopyExpr(pt.Expr)
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "or":
			left, can, err := hashFilterExpr(proc, colPosition, exprImpl.F.Args[0], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			right, can, err := hashFilterExpr(proc, colPosition, exprImpl.F.Args[1], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			sort.Ints(left)
			sort.Ints(right)
			return mergeSortedSlices(left, right), true, nil

		case "and":
			left, can, err := hashFilterExpr(proc, colPosition, exprImpl.F.Args[0], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			right, can, err := hashFilterExpr(proc, colPosition, exprImpl.F.Args[1], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			sort.Ints(left)
			sort.Ints(right)
			return intersectSortedSlices(left, right), true, nil
		case "=":
			left, ok := exprImpl.F.Args[0].Expr.(*plan.Expr_Col)
			if !ok {
				return nil, false, nil
			}
			if left.Col.ColPos != colPosition {
				return nil, false, nil
			}
			for i := range exprs {
				mustReplaceCol(exprs[i], exprImpl.F.Args[1])
			}
			targets, err := filterResult(proc, exprs, metadata)
			if err != nil {
				return nil, false, err
			}
			return targets, true, nil
		}
	}
	return nil, false, nil
}

func rangeFilter(
	proc *process.Process,
	filters []*plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, error) {
	colPosition := mustGetColPosition(metadata.Partitions[0].Expr)
	tm := map[int]struct{}{}
	for _, expr := range filters {
		targets, ok, err := rangeFilterExpr(proc, colPosition, expr, metadata)
		if err != nil {
			return nil, err
		}
		if !ok {
			res := make([]int, len(metadata.Partitions))
			for i, pt := range metadata.Partitions {
				res[i] = int(pt.Position)
			}
			return res, nil
		}
		for _, target := range targets {
			tm[target] = struct{}{}
		}
	}
	res := make([]int, 0, len(tm))
	for target := range tm {
		res = append(res, target)
	}
	return res, nil
}

func rangeFilterExpr(
	proc *process.Process,
	colPosition int32,
	expr *plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, bool, error) {
	exprs := make([]*plan.Expr, len(metadata.Partitions))
	for i, pt := range metadata.Partitions {
		exprs[i] = p.DeepCopyExpr(pt.Expr)
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "or":
			left, can, err := rangeFilterExpr(proc, colPosition, exprImpl.F.Args[0], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			right, can, err := rangeFilterExpr(proc, colPosition, exprImpl.F.Args[1], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			sort.Ints(left)
			sort.Ints(right)
			return mergeSortedSlices(left, right), true, nil

		case "and":
			left, can, err := rangeFilterExpr(proc, colPosition, exprImpl.F.Args[0], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			right, can, err := rangeFilterExpr(proc, colPosition, exprImpl.F.Args[1], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			sort.Ints(left)
			sort.Ints(right)
			return intersectSortedSlices(left, right), true, nil

		case "=":
			left, ok := exprImpl.F.Args[0].Expr.(*plan.Expr_Col)
			if !ok {
				return nil, false, nil
			}
			if left.Col.ColPos != colPosition {
				return nil, false, nil
			}
			for i := range exprs {
				// a = 1 =>
				// p1 <= 1 < p2
				mustReplaceCol(exprs[i], exprImpl.F.Args[1])
			}
			targets, err := filterResult(proc, exprs, metadata)
			if err != nil {
				return nil, false, err
			}
			return targets, true, nil
		}
	}
	return nil, false, nil
}

func mergeSortedSlices(slice1, slice2 []int) []int {
	i, j := 0, 0
	result := make([]int, 0, len(slice1)+len(slice2))

	for i < len(slice1) && j < len(slice2) {
		var val int
		if slice1[i] < slice2[j] {
			val = slice1[i]
			i++
		} else if slice1[i] > slice2[j] {
			val = slice2[j]
			j++
		} else {
			val = slice1[i]
			i++
			j++
		}
		if len(result) == 0 || result[len(result)-1] != val {
			result = append(result, val)
		}
	}
	for i < len(slice1) {
		if len(result) == 0 || result[len(result)-1] != slice1[i] {
			result = append(result, slice1[i])
		}
		i++
	}
	for j < len(slice2) {
		if len(result) == 0 || result[len(result)-1] != slice2[j] {
			result = append(result, slice2[j])
		}
		j++
	}

	return result
}

func intersectSortedSlices(slice1, slice2 []int) []int {
	i, j := 0, 0
	result := []int{}
	for i < len(slice1) && j < len(slice2) {
		if slice1[i] == slice2[j] {
			if len(result) == 0 || result[len(result)-1] != slice1[i] {
				result = append(result, slice1[i])
			}
			for i < len(slice1) && slice1[i] == slice1[i] {
				i++
			}
			for j < len(slice2) && slice2[j] == slice2[j] {
				j++
			}
		} else if slice1[i] < slice2[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

func inPartition(proc *process.Process, expr *plan.Expr) (bool, error) {
	exec, err := colexec.NewExpressionExecutor(proc, expr)
	if err != nil {
		return false, err
	}
	defer exec.Free()
	vec, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	if err != nil {
		return false, err
	}
	return vector.MustFixedColNoTypeCheck[bool](vec)[0], nil
}

func filterResult(
	proc *process.Process,
	exprs []*plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, error) {
	var targets []int
	for i, expr := range exprs {
		ok, err := inPartition(proc, expr)
		if err != nil {
			return nil, err
		}
		if ok {
			targets = append(targets, int(metadata.Partitions[i].Position))
		}
	}
	return targets, nil
}

func mustReplaceCol(expr, value *plan.Expr) {
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		for i := range e.F.Args {
			switch e.F.Args[i].Expr.(type) {
			case *plan.Expr_Col:
				e.F.Args[i], _ = appendCastBeforeExpr(context.Background(), value, e.F.Args[i].Typ)
				return
			case *plan.Expr_F:
				mustReplaceCol(e.F.Args[i], value)
			}
		}
	}
}

func mustReplaceColPos(expr *plan.Expr, pos int32) {
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		for i := range e.F.Args {
			switch col := e.F.Args[i].Expr.(type) {
			case *plan.Expr_Col:
				col.Col.ColPos = pos
				return
			case *plan.Expr_F:
				mustReplaceColPos(e.F.Args[i], pos)
			}
		}
	}
}

func mustGetColPosition(expr *plan.Expr) int32 {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return e.Col.ColPos
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			position := mustGetColPosition(arg)
			if position != -1 {
				return position
			}
		}
	}
	return -1
}

func listFilter(
	proc *process.Process,
	filters []*plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, error) {
	colPosition := mustGetColPosition(metadata.Partitions[0].Expr)
	tm := map[int]struct{}{}
	for _, expr := range filters {
		targets, ok, err := listFilterExpr(proc, colPosition, expr, metadata)
		if err != nil {
			return nil, err
		}
		if !ok {
			res := make([]int, len(metadata.Partitions))
			for i, pt := range metadata.Partitions {
				res[i] = int(pt.Position)
			}
			return res, nil
		}
		for _, target := range targets {
			tm[target] = struct{}{}
		}
	}
	res := make([]int, 0, len(tm))
	for target := range tm {
		res = append(res, target)
	}
	return res, nil
}

func extractListValues(expr *plan.Expr) ([]*plan.Expr, error) {
	f, ok := expr.Expr.(*plan.Expr_F)
	if !ok {
		return nil, nil
	}
	list, ok := f.F.Args[1].Expr.(*plan.Expr_List)
	if !ok {
		return nil, nil
	}
	return list.List.List, nil
}

func constructVectorFromList(proc *process.Process, list []*plan.Expr) (*vector.Vector, error) {
	if len(list) == 0 {
		return nil, nil
	}

	vec := vector.NewVec(types.T(list[0].Typ.Id).ToType())
	if err := vec.PreExtend(len(list), proc.Mp()); err != nil {
		return nil, err
	}

	for _, expr := range list {
		exec, err := colexec.NewExpressionExecutor(proc, expr)
		if err != nil {
			exec.Free()
			vec.Free(proc.Mp())
			return nil, err
		}

		val, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
		if err != nil {
			exec.Free()
			vec.Free(proc.Mp())
			return nil, err
		}

		if err = vec.UnionOne(val, 0, proc.Mp()); err != nil {
			exec.Free()
			vec.Free(proc.Mp())
			return nil, err
		}
		exec.Free()
	}

	vec.SetLength(len(list))

	return vec, nil
}

func listFilterExpr(
	proc *process.Process,
	colPosition int32,
	expr *plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, bool, error) {
	var err error
	expr = p.DeepCopyExpr(expr)
	expr, err = ConvertFoldExprToNormal(expr)
	if err != nil {
		return nil, false, err
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "or":
			left, can, err := listFilterExpr(proc, colPosition, exprImpl.F.Args[0], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			right, can, err := listFilterExpr(proc, colPosition, exprImpl.F.Args[1], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			sort.Ints(left)
			sort.Ints(right)
			return mergeSortedSlices(left, right), true, nil

		case "and":
			left, can, err := listFilterExpr(proc, colPosition, exprImpl.F.Args[0], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			right, can, err := listFilterExpr(proc, colPosition, exprImpl.F.Args[1], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			sort.Ints(left)
			sort.Ints(right)
			return intersectSortedSlices(left, right), true, nil

		case "=", "<=", ">=", ">", "<", "prefix_eq", "prefix_between",
			"between", "prefix_in", "isnull", "is_null", "isnotnull", "is_not_null",
			"in":
			left, ok := exprImpl.F.Args[0].Expr.(*plan.Expr_Col)
			if !ok {
				return nil, false, nil
			}
			if left.Col.ColPos != colPosition {
				return nil, false, nil
			}
			left.Col.ColPos = 0

			bat := batch.NewWithSize(1)
			exec, err := colexec.NewExpressionExecutor(proc, expr)
			if err != nil {
				return nil, false, err
			}
			defer exec.Free()
			var targets []int

			for _, t := range metadata.Partitions {
				list, err := extractListValues(t.Expr)
				if err != nil {
					return nil, false, err
				}

				bat.Vecs[0], err = constructVectorFromList(proc, list)
				if err != nil {
					return nil, false, err
				}
				bat.SetRowCount(bat.Vecs[0].Length())
				vec, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
				if err != nil {
					return nil, false, err
				}
				chosen := vector.MustFixedColNoTypeCheck[bool](vec)
				for _, c := range chosen {
					if c {
						targets = append(targets, int(t.Position))
						break
					}
				}
			}
			return targets, true, nil
		}
	}
	return nil, false, nil
}

func makeTypeByPlan2Expr(expr *plan.Expr) types.Type {
	oid := types.T(expr.Typ.Id)
	return types.New(oid, expr.Typ.Width, expr.Typ.Scale)
}

func makeTypeByPlan2Type(typ plan.Type) types.Type {
	oid := types.T(typ.Id)
	return types.New(oid, typ.Width, typ.Scale)
}

func getFunctionObjRef(funcID int64, name string) *plan.ObjectRef {
	return &plan.ObjectRef{
		Obj:     funcID,
		ObjName: name,
	}
}

func appendCastBeforeExpr(ctx context.Context, expr *plan.Expr, typ plan.Type) (*plan.Expr, error) {
	typ.NotNullable = expr.Typ.NotNullable
	argsType := []types.Type{
		makeTypeByPlan2Expr(expr),
		makeTypeByPlan2Type(typ),
	}
	fGet, err := function.GetFunctionByName(ctx, "cast", argsType)
	if err != nil {
		return nil, err
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(fGet.GetEncodedOverloadID(), "cast"),
				Args: []*plan.Expr{
					expr,
					{
						Typ: typ,
						Expr: &plan.Expr_T{
							T: &plan.TargetType{},
						},
					},
				},
			},
		},
		Typ: typ,
	}, nil
}

func ConvertFoldExprToNormal(expr *plan.Expr) (*plan.Expr, error) {
	switch ef := expr.Expr.(type) {
	case *plan.Expr_Fold:
		if ef.Fold.IsConst {
			c, err := getConstantFromBytes(ef.Fold.Data, expr.Typ)
			if err != nil {
				return expr, err
			}
			return &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Lit{
					Lit: c,
				},
			}, nil
		} else {
			vec := vector.NewVec(types.T(expr.Typ.Id).ToType())
			err := vec.UnmarshalBinary(ef.Fold.Data)
			if err != nil {
				return nil, err
			}
			vec.InplaceSortAndCompact()
			data, err := vec.MarshalBinary()
			if err != nil {
				return nil, err
			}
			return &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Vec{
					Vec: &plan.LiteralVec{
						Len:  int32(vec.Length()),
						Data: data,
					},
				},
			}, nil
		}

	case *plan.Expr_F:
		for i := range ef.F.Args {
			newExpr, err := ConvertFoldExprToNormal(ef.F.Args[i])
			if err != nil {
				return nil, err
			}
			ef.F.Args[i] = newExpr
		}
		return expr, nil

	default:
		return expr, nil
	}
}

func getConstantFromBytes(data []byte, typ plan.Type) (*plan.Literal, error) {
	if len(data) == 0 {
		return nil, nil
	}

	switch types.T(typ.Id) {
	case types.T_bool:
		val := types.DecodeBool(data)
		return &plan.Literal{
			Value: &plan.Literal_Bval{Bval: val},
		}, nil

	case types.T_int8:
		val := types.DecodeInt8(data)
		return &plan.Literal{
			Value: &plan.Literal_I32Val{I32Val: int32(val)},
		}, nil

	case types.T_int16:
		val := types.DecodeInt16(data)
		return &plan.Literal{
			Value: &plan.Literal_I32Val{I32Val: int32(val)},
		}, nil

	case types.T_int32:
		val := types.DecodeInt32(data)
		return &plan.Literal{
			Value: &plan.Literal_I32Val{I32Val: val},
		}, nil

	case types.T_int64:
		val := types.DecodeInt64(data)
		return &plan.Literal{
			Value: &plan.Literal_I64Val{I64Val: val},
		}, nil

	case types.T_uint8:
		val := types.DecodeUint8(data)
		return &plan.Literal{
			Value: &plan.Literal_U32Val{U32Val: uint32(val)},
		}, nil

	case types.T_uint16:
		val := types.DecodeUint16(data)
		return &plan.Literal{
			Value: &plan.Literal_U32Val{U32Val: uint32(val)},
		}, nil

	case types.T_uint32:
		val := types.DecodeUint32(data)
		return &plan.Literal{
			Value: &plan.Literal_U32Val{U32Val: val},
		}, nil

	case types.T_uint64:
		val := types.DecodeUint64(data)
		return &plan.Literal{
			Value: &plan.Literal_U64Val{U64Val: val},
		}, nil

	case types.T_float32:
		val := types.DecodeFloat32(data)
		return &plan.Literal{
			Value: &plan.Literal_Fval{Fval: val},
		}, nil

	case types.T_float64:
		val := types.DecodeFloat64(data)
		return &plan.Literal{
			Value: &plan.Literal_Dval{Dval: val},
		}, nil

	case types.T_varchar, types.T_char, types.T_text:
		return &plan.Literal{
			Value: &plan.Literal_Sval{Sval: string(data)},
		}, nil

	case types.T_date:
		val := types.DecodeDate(data)
		return &plan.Literal{
			Value: &plan.Literal_Dateval{Dateval: int32(val)},
		}, nil

	case types.T_datetime:
		val := types.DecodeDatetime(data)
		return &plan.Literal{
			Value: &plan.Literal_Datetimeval{Datetimeval: int64(val)},
		}, nil

	case types.T_timestamp:
		val := types.DecodeTimestamp(data)
		return &plan.Literal{
			Value: &plan.Literal_Timestampval{Timestampval: int64(val)},
		}, nil

	case types.T_decimal64:
		val := types.DecodeDecimal64(data)
		return &plan.Literal{
			Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: int64(val)}},
		}, nil

	case types.T_decimal128:
		val := types.DecodeDecimal128(data)
		return &plan.Literal{
			Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{
				A: int64(val.B0_63),
				B: int64(val.B64_127),
			}},
		}, nil

	default:
		return nil, nil
	}
}
