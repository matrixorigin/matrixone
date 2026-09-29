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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	p "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Filter determines which partitions should be accessed based on the given filters and partition metadata.
// It returns a slice of partition tables that match the filter conditions.
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

// hashFilter handles partition pruning for hash-based partitioning.
// It evaluates the filters against hash partition expressions and returns matching partition tables.
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
		if ok {
			for _, target := range targets {
				tm[target] = struct{}{}
			}
		}
	}
	if len(tm) > 0 {
		res := make([]int, 0, len(tm))
		for target := range tm {
			res = append(res, target)
		}
		return res, nil
	}
	res := make([]int, len(metadata.Partitions))
	for i, pt := range metadata.Partitions {
		res[i] = int(pt.Position)
	}
	return res, nil
}

// hashFilterExpr evaluates a single filter expression against hash partitions.
// Returns the matching partition positions, whether the expression could be evaluated, and any error.
func hashFilterExpr(
	proc *process.Process,
	colPosition int32,
	expr *plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, bool, error) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "or":
			// For OR operator, recursively evaluate both left and right expressions
			// and merge their results to get all matching partitions
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
			// For AND operator, recursively evaluate both left and right expressions
			// and find the intersection of their results to get matching partitions
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
			value, canPrune := normalizePartitionValue(exprImpl.F.Args[1])
			if !canPrune {
				return nil, false, nil
			}
			exprs := make([]*plan.Expr, len(metadata.Partitions))
			for i, pt := range metadata.Partitions {
				// Deep copy partition expressions to avoid modifying the original expressions
				// when replacing column references with actual filter values.
				exprs[i] = p.DeepCopyExpr(pt.Expr)
			}
			for i := range exprs {
				mustReplaceCol(exprs[i], value)
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

// rangeFilter handles partition pruning for range-based partitioning.
// It evaluates the filters against range partition expressions and returns matching partition positions.
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
		if ok {
			for _, target := range targets {
				tm[target] = struct{}{}
			}
		}
	}
	if len(tm) > 0 {
		res := make([]int, 0, len(tm))
		for target := range tm {
			res = append(res, target)
		}
		return res, nil
	}
	res := make([]int, len(metadata.Partitions))
	for i, pt := range metadata.Partitions {
		res[i] = int(pt.Position)
	}
	return res, nil

}

// rangeFilterExpr evaluates a single filter expression against range partitions.
// Returns the matching partition positions, whether the expression could be evaluated, and any error.
func rangeFilterExpr(
	proc *process.Process,
	colPosition int32,
	expr *plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, bool, error) {
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
			value, canPrune := normalizePartitionValue(exprImpl.F.Args[1])
			if !canPrune {
				return nil, false, nil
			}
			exprs := make([]*plan.Expr, len(metadata.Partitions))
			for i, pt := range metadata.Partitions {
				// Deep copy partition expressions to avoid modifying the original expressions
				// when replacing column references with actual filter values.
				exprs[i] = p.DeepCopyExpr(pt.Expr)
			}
			for i := range exprs {
				// a = 1 =>
				// p1 <= 1 < p2
				mustReplaceCol(exprs[i], value)
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

// mergeSortedSlices merges two sorted integer slices while removing duplicates.
// Returns a new sorted slice containing all unique elements from both input slices.
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

// intersectSortedSlices finds the intersection of two sorted integer slices while removing duplicates.
// Returns a new sorted slice containing elements that appear in both input slices.
func intersectSortedSlices(slice1, slice2 []int) []int {
	i, j := 0, 0
	result := []int{}
	for i < len(slice1) && j < len(slice2) {
		if slice1[i] == slice2[j] {
			if len(result) == 0 || result[len(result)-1] != slice1[i] {
				result = append(result, slice1[i])
			}
			// Skip duplicates in slice1
			for i+1 < len(slice1) && slice1[i] == slice1[i+1] {
				i++
			}
			// Skip duplicates in slice2
			for j+1 < len(slice2) && slice2[j] == slice2[j+1] {
				j++
			}
			i++
			j++
		} else if slice1[i] < slice2[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

// inPartition evaluates whether a given expression is true for a partition.
// Returns true if the expression evaluates to true, false otherwise.
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

// filterResult evaluates partition expressions and returns positions of partitions that match.
// It processes each partition expression and collects matching partition positions.
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

// normalizePartitionValue converts the evaluated Fold representation used by
// remote scans before the value is substituted into partition expressions.
// The general expression executor intentionally does not own Fold values. A
// clone keeps this storage optimization from mutating the reusable scan filter.
// False means that pruning must conservatively scan every partition.
func normalizePartitionValue(value *plan.Expr) (*plan.Expr, bool) {
	if !p.HasFoldValExpr(value) {
		return value, true
	}
	return convertFoldExprToNormal(p.DeepCopyExpr(value))
}

// mustReplaceCol replaces column references in an expression with a given value expression.
// It recursively traverses the expression tree to find and replace column references.
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

// mustReplaceColPos recursively traverses the expression tree to find and replace
// the column position with the specified position. This is used in list partitioning
// to reset column positions when evaluating expressions against constructed batches.
func mustReplaceColPos(expr *plan.Expr, pos int32) {
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		for i := range e.F.Args {
			switch col := e.F.Args[i].Expr.(type) {
			case *plan.Expr_Col:
				if pos == -1 {
					pos = col.Col.ColPos + 1
				}
				col.Col.ColPos = pos
				return
			case *plan.Expr_F:
				mustReplaceColPos(e.F.Args[i], pos)
			}
		}
	}
}

// mustGetColPosition extracts the column position from an expression.
// Returns the column position if found, -1 otherwise.
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

// listFilter handles partition pruning for list-based partitioning.
// It evaluates the filters against list partition expressions and returns matching partition positions.
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
		if ok {
			for _, target := range targets {
				tm[target] = struct{}{}
			}
		}
	}
	if len(tm) > 0 {
		res := make([]int, 0, len(tm))
		for target := range tm {
			res = append(res, target)
		}
		return res, nil
	}
	res := make([]int, len(metadata.Partitions))
	for i, pt := range metadata.Partitions {
		res[i] = int(pt.Position)
	}
	return res, nil
}

// extractListValues extracts the list of values from a list expression.
// Returns the list of expressions if the input is a valid list expression, nil otherwise.
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

// constructVectorFromList creates a vector from a list of expressions.
// It evaluates each expression and combines the results into a single vector.
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

// listFilterExpr evaluates a single filter expression against list partitions.
// Returns the matching partition positions, whether the expression could be evaluated, and any error.
func listFilterExpr(
	proc *process.Process,
	colPosition int32,
	expr *plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, bool, error) {
	expr = p.DeepCopyExpr(expr)
	expr, canPrune := convertFoldExprToNormal(expr)
	if !canPrune {
		return nil, false, nil
	}
	return listFilterExprNormalized(proc, colPosition, expr, metadata)
}

// listFilterExprNormalized evaluates an expression that has already been
// cloned and normalized by listFilterExpr. Recursive calls must use this
// helper directly: cloning the remaining subtree at every OR/AND node turns a
// large CDC batch predicate into quadratic allocation growth.
func listFilterExprNormalized(
	proc *process.Process,
	colPosition int32,
	expr *plan.Expr,
	metadata partition.PartitionMetadata,
) ([]int, bool, error) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "or":
			left, can, err := listFilterExprNormalized(proc, colPosition, exprImpl.F.Args[0], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			right, can, err := listFilterExprNormalized(proc, colPosition, exprImpl.F.Args[1], metadata)
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
			left, can, err := listFilterExprNormalized(proc, colPosition, exprImpl.F.Args[0], metadata)
			if err != nil {
				return nil, false, err
			}
			if !can {
				return nil, false, nil
			}
			right, can, err := listFilterExprNormalized(proc, colPosition, exprImpl.F.Args[1], metadata)
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

// makeTypeByPlan2Expr converts a plan expression type to a MatrixOne type.
// Creates a new type based on the expression's type information.
func makeTypeByPlan2Expr(expr *plan.Expr) types.Type {
	oid := types.T(expr.Typ.Id)
	return types.New(oid, expr.Typ.Width, expr.Typ.Scale)
}

// makeTypeByPlan2Type converts a plan type to a MatrixOne type.
// Creates a new type based on the plan type information.
func makeTypeByPlan2Type(typ plan.Type) types.Type {
	oid := types.T(typ.Id)
	return types.New(oid, typ.Width, typ.Scale)
}

// getFunctionObjRef creates a function object reference.
// Returns a new ObjectRef with the given function ID and name.
func getFunctionObjRef(funcID int64, name string) *plan.ObjectRef {
	return &plan.ObjectRef{
		Obj:     funcID,
		ObjName: name,
	}
}

// appendCastBeforeExpr adds a cast operation before an expression.
// Creates a new expression that casts the input expression to the specified type.
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

// convertFoldExprToNormal converts Fold values into expressions owned by the
// general expression executor. The boolean is false when a Fold value cannot
// be represented losslessly. Partition pruning is optional, so callers must
// fail open instead of evaluating a partial or ambiguous conversion.
func convertFoldExprToNormal(expr *plan.Expr) (*plan.Expr, bool) {
	switch ef := expr.Expr.(type) {
	case *plan.Expr_Fold:
		if ef.Fold == nil {
			return nil, false
		}
		if ef.Fold.IsConst {
			c, ok := getConstantFromBytes(ef.Fold.Data, expr.Typ)
			if !ok {
				return nil, false
			}
			return &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Lit{
					Lit: c,
				},
			}, true
		} else {
			vec := vector.NewVec(types.T_any.ToType())
			err := vec.UnmarshalBinary(ef.Fold.Data)
			if err != nil {
				return nil, false
			}
			vecType := vec.GetType()
			if int32(vecType.Oid) != expr.Typ.Id || vecType.Scale != expr.Typ.Scale ||
				(expr.Typ.Width != 0 && vecType.Width != expr.Typ.Width) ||
				(expr.Typ.Charset != 0 && uint32(vecType.Charset) != expr.Typ.Charset) {
				return nil, false
			}
			// InplaceSortAndCompact's ordinary value-only path does not move the
			// null bitmap with the reordered payload. A nullable Fold could
			// therefore turn {1, NULL} into {0, NULL} and make pruning omit the
			// partition that contains 1. Nullable predicates retain their SQL
			// residual filter, so failing open is both safe and cheaper than
			// materializing a null-aware copy solely for optional pruning.
			if vec.IsConstNull() || vec.GetNulls().Any() {
				return nil, false
			}
			vec.InplaceSortAndCompact()
			data, err := vec.MarshalBinary()
			if err != nil {
				return nil, false
			}
			return &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Vec{
					Vec: &plan.LiteralVec{
						Len:  int32(vec.Length()),
						Data: data,
					},
				},
			}, true
		}

	case *plan.Expr_F:
		for i := range ef.F.Args {
			newExpr, ok := convertFoldExprToNormal(ef.F.Args[i])
			if !ok {
				return nil, false
			}
			ef.F.Args[i] = newExpr
		}
		return expr, true

	default:
		return expr, true
	}
}

// ConvertFoldExprToNormal preserves the package's strict conversion API for
// callers that require a normal expression. Partition-pruning callers use the
// boolean form above because an unrepresentable optimization input is a safe
// reason to scan all partitions, not a query error.
func ConvertFoldExprToNormal(expr *plan.Expr) (*plan.Expr, error) {
	normalized, ok := convertFoldExprToNormal(expr)
	if !ok {
		return nil, moerr.NewInvalidInputNoCtx("fold expression cannot be converted losslessly")
	}
	return normalized, nil
}

// getConstantFromBytes decodes the scalar encoding produced by
// plan.getConstantBytes. A nil payload means SQL NULL or a scalar family the
// producer could not encode; it never means an empty string. A non-nil empty
// payload is a valid empty variable-length value. Fixed-width decoders use
// unsafe loads, so validate the exact producer width before calling them.
func getConstantFromBytes(data []byte, typ plan.Type) (*plan.Literal, bool) {
	if data == nil {
		return nil, false
	}

	oid := types.T(typ.Id)
	switch oid {
	case types.T_varchar, types.T_char, types.T_binary, types.T_varbinary,
		types.T_text, types.T_blob, types.T_datalink:
		return &plan.Literal{
			Value: &plan.Literal_Sval{Sval: string(data)},
		}, true
	}

	switch oid {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_decimal64, types.T_decimal128:
		// These are the fixed-width scalar families represented by plan.Literal.
	default:
		return nil, false
	}
	if len(data) != oid.TypeLen() {
		return nil, false
	}

	switch oid {
	case types.T_bool:
		val := types.DecodeBool(data)
		return &plan.Literal{
			Value: &plan.Literal_Bval{Bval: val},
		}, true

	case types.T_bit:
		val := types.DecodeUint64(data)
		return &plan.Literal{
			Value: &plan.Literal_U64Val{U64Val: val},
		}, true

	case types.T_int8:
		val := types.DecodeInt8(data)
		return &plan.Literal{
			Value: &plan.Literal_I32Val{I32Val: int32(val)},
		}, true

	case types.T_int16:
		val := types.DecodeInt16(data)
		return &plan.Literal{
			Value: &plan.Literal_I32Val{I32Val: int32(val)},
		}, true

	case types.T_int32:
		val := types.DecodeInt32(data)
		return &plan.Literal{
			Value: &plan.Literal_I32Val{I32Val: val},
		}, true

	case types.T_int64:
		val := types.DecodeInt64(data)
		return &plan.Literal{
			Value: &plan.Literal_I64Val{I64Val: val},
		}, true

	case types.T_uint8:
		val := types.DecodeUint8(data)
		return &plan.Literal{
			Value: &plan.Literal_U32Val{U32Val: uint32(val)},
		}, true

	case types.T_uint16:
		val := types.DecodeUint16(data)
		return &plan.Literal{
			Value: &plan.Literal_U32Val{U32Val: uint32(val)},
		}, true

	case types.T_uint32:
		val := types.DecodeUint32(data)
		return &plan.Literal{
			Value: &plan.Literal_U32Val{U32Val: val},
		}, true

	case types.T_uint64:
		val := types.DecodeUint64(data)
		return &plan.Literal{
			Value: &plan.Literal_U64Val{U64Val: val},
		}, true

	case types.T_float32:
		val := types.DecodeFloat32(data)
		return &plan.Literal{
			Value: &plan.Literal_Fval{Fval: val},
		}, true

	case types.T_float64:
		val := types.DecodeFloat64(data)
		return &plan.Literal{
			Value: &plan.Literal_Dval{Dval: val},
		}, true

	case types.T_date:
		val := types.DecodeDate(data)
		return &plan.Literal{
			Value: &plan.Literal_Dateval{Dateval: int32(val)},
		}, true

	case types.T_time:
		val := types.DecodeTime(data)
		return &plan.Literal{
			Value: &plan.Literal_Timeval{Timeval: int64(val)},
		}, true

	case types.T_datetime:
		val := types.DecodeDatetime(data)
		return &plan.Literal{
			Value: &plan.Literal_Datetimeval{Datetimeval: int64(val)},
		}, true

	case types.T_timestamp:
		val := types.DecodeTimestamp(data)
		return &plan.Literal{
			Value: &plan.Literal_Timestampval{Timestampval: int64(val)},
		}, true

	case types.T_decimal64:
		val := types.DecodeDecimal64(data)
		return &plan.Literal{
			Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: int64(val)}},
		}, true

	case types.T_decimal128:
		val := types.DecodeDecimal128(data)
		return &plan.Literal{
			Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{
				A: int64(val.B0_63),
				B: int64(val.B64_127),
			}},
		}, true

	default:
		return nil, false
	}
}
