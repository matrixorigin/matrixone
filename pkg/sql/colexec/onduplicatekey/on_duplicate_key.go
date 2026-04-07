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

package onduplicatekey

import (
	"bytes"
	"fmt"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "on_duplicate_key"

func (onDuplicatekey *OnDuplicatekey) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(": processing on duplicate key before insert")
}

func (onDuplicatekey *OnDuplicatekey) OpType() vm.OpType {
	return vm.OnDuplicateKey
}

func (onDuplicatekey *OnDuplicatekey) Prepare(p *process.Process) (err error) {
	if onDuplicatekey.OpAnalyzer == nil {
		onDuplicatekey.OpAnalyzer = process.NewAnalyzer(onDuplicatekey.GetIdx(), onDuplicatekey.IsFirst, onDuplicatekey.IsLast, "on_duplicate_key")
	} else {
		onDuplicatekey.OpAnalyzer.Reset()
	}

	if len(onDuplicatekey.ctr.uniqueCheckExes) == 0 {
		onDuplicatekey.ctr.uniqueCheckExes, err = colexec.NewExpressionExecutorsFromPlanExpressions(p, onDuplicatekey.UniqueColCheckExpr)
		if err != nil {
			return
		}
	}
	return
}

func (onDuplicatekey *OnDuplicatekey) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := onDuplicatekey.OpAnalyzer

	result := vm.NewCallResult()
	for {
		switch onDuplicatekey.ctr.state {
		case Build:
			for {
				result, err := vm.ChildrenCall(onDuplicatekey.GetChildren(0), proc, analyzer)
				if err != nil {
					return result, err
				}

				if result.Batch == nil {
					break
				}
				bat := result.Batch
				err = resetInsertBatchForOnduplicateKey(proc, bat, onDuplicatekey)
				if err != nil {
					return result, err
				}

			}
			onDuplicatekey.ctr.state = Eval

		case Eval:
			result.Batch = onDuplicatekey.ctr.rbat
			onDuplicatekey.ctr.state = End
			return result, nil

		case End:
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func resetInsertBatchForOnduplicateKey(proc *process.Process, originBatch *batch.Batch, insertArg *OnDuplicatekey) error {
	//get rowid vec index
	rowIdIdx := int32(-1)
	for _, idx := range insertArg.OnDuplicateIdx {
		if originBatch.Vecs[idx].GetType().Oid == types.T_Rowid {
			rowIdIdx = idx
			break
		}
	}
	if rowIdIdx == -1 {
		return moerr.NewConstraintViolation(proc.Ctx, "can not find rowid when insert with on duplicate key")
	}

	insertColCount := int(insertArg.InsertColCount) //columns without hidden columns
	if insertArg.ctr.rbat == nil {
		insertArg.ctr.rbat = batch.NewWithSize(len(insertArg.Attrs))
		insertArg.ctr.rbat.Attrs = insertArg.Attrs

		for i, v := range originBatch.Vecs {
			newVec := vector.NewVec(*v.GetType())
			insertArg.ctr.rbat.SetVector(int32(i), newVec)
		}

		// Initialize hash-based conflict detection
		insertArg.ctr.uniqueKeyColIndices = make([][]int32, len(insertArg.UniqueColCheckExpr))
		insertArg.ctr.conflictMaps = make([]map[string]int, len(insertArg.UniqueColCheckExpr))
		for i, expr := range insertArg.UniqueColCheckExpr {
			insertArg.ctr.uniqueKeyColIndices[i] = extractColIndicesFromExpr(expr)
			if len(insertArg.ctr.uniqueKeyColIndices[i]) == 0 {
				return moerr.NewInternalErrorf(proc.Ctx, "failed to extract column indices from unique constraint expression %d", i)
			}
			insertArg.ctr.conflictMaps[i] = make(map[string]int)
		}
	} else {
		insertArg.ctr.rbat.CleanOnlyData()
		for i := range insertArg.ctr.conflictMaps {
			clear(insertArg.ctr.conflictMaps[i])
		}
	}

	insertBatch := insertArg.ctr.rbat
	attrs := make([]string, len(insertBatch.Attrs))
	copy(attrs, insertBatch.Attrs)

	updateExpr := insertArg.OnDuplicateExpr
	oldRowIdVec := vector.MustFixedColWithTypeCheck[types.Rowid](originBatch.Vecs[rowIdIdx])
	for i := 0; i < originBatch.RowCount(); i++ {
		newBatch, err := fetchOneRowAsBatch(i, originBatch, proc, attrs)
		if err != nil {
			return err
		}

		// O(1) hash map conflict check instead of O(N) linear scan
		oldConflictRowIdx, conflictMsg := findConflictByHashMap(
			&insertArg.ctr.keyBuf, newBatch.Vecs, insertArg.ctr.uniqueKeyColIndices,
			insertArg.ctr.conflictMaps, insertArg.UniqueCols, 0)
		if oldConflictRowIdx > -1 {

			if insertArg.IsIgnore {
				newBatch.Clean(proc.GetMPool())
				continue
			}

			// if conflict with origin row. and row_id is not equal row_id of insertBatch's inflict row. then throw error
			if !newBatch.Vecs[rowIdIdx].GetNulls().Contains(0) {
				oldRowId := vector.MustFixedColWithTypeCheck[types.Rowid](insertBatch.Vecs[rowIdIdx])[oldConflictRowIdx]
				newRowId := vector.MustFixedColWithTypeCheck[types.Rowid](newBatch.Vecs[rowIdIdx])[0]
				if !bytes.Equal(oldRowId[:], newRowId[:]) {
					newBatch.Clean(proc.GetMPool())
					return moerr.NewConstraintViolation(proc.Ctx, conflictMsg)
				}
			}

			for j := 0; j < insertColCount; j++ {
				fromVec := insertBatch.Vecs[j]
				toVec := newBatch.Vecs[j+insertColCount]
				err := toVec.Copy(fromVec, 0, int64(oldConflictRowIdx), proc.Mp())
				if err != nil {
					newBatch.Clean(proc.GetMPool())
					return err
				}
			}
			tmpBatch, err := updateOldBatch(newBatch, updateExpr, proc, insertColCount, attrs)
			if err != nil {
				newBatch.Clean(proc.GetMPool())
				return err
			}

			// Save old keys before in-place update (in case update changes unique columns)
			oldKeys := make([]string, len(insertArg.ctr.uniqueKeyColIndices))
			for k, colIndices := range insertArg.ctr.uniqueKeyColIndices {
				oldKeys[k] = serializeUniqueKey(&insertArg.ctr.keyBuf, insertBatch.Vecs, colIndices, oldConflictRowIdx)
			}

			// update the oldConflictRowIdx of insertBatch by newBatch
			for j := 0; j < insertColCount; j++ {
				fromVec := tmpBatch.Vecs[j]
				toVec := insertBatch.Vecs[j]
				err := toVec.Copy(fromVec, int64(oldConflictRowIdx), 0, proc.Mp())
				if err != nil {
					tmpBatch.Clean(proc.GetMPool())
					newBatch.Clean(proc.GetMPool())
					return err
				}
			}

			// Update hash maps after in-place modification
			for k, colIndices := range insertArg.ctr.uniqueKeyColIndices {
				if oldKeys[k] != "" {
					delete(insertArg.ctr.conflictMaps[k], oldKeys[k])
				}
				newKey := serializeUniqueKey(&insertArg.ctr.keyBuf, insertBatch.Vecs, colIndices, oldConflictRowIdx)
				if newKey != "" {
					insertArg.ctr.conflictMaps[k][newKey] = oldConflictRowIdx
				}
			}

			tmpBatch.Clean(proc.GetMPool())
		} else {
			// row id is null: means no uniqueness conflict found in origin rows
			if len(oldRowIdVec) == 0 || originBatch.Vecs[rowIdIdx].GetNulls().Contains(uint64(i)) {
				_, err := insertBatch.Append(proc.Ctx, proc.Mp(), newBatch)
				if err != nil {
					newBatch.Clean(proc.GetMPool())
					return err
				}
				addToConflictMaps(&insertArg.ctr.keyBuf, insertBatch.Vecs, insertArg.ctr.uniqueKeyColIndices,
					insertArg.ctr.conflictMaps, insertBatch.RowCount()-1)
			} else {

				if insertArg.IsIgnore {
					newBatch.Clean(proc.GetMPool())
					continue
				}

				tmpBatch, err := updateOldBatch(newBatch, updateExpr, proc, insertColCount, attrs)
				if err != nil {
					newBatch.Clean(proc.GetMPool())
					return err
				}
				conflictRowIdx, conflictMsg := findConflictByHashMap(
					&insertArg.ctr.keyBuf, tmpBatch.Vecs, insertArg.ctr.uniqueKeyColIndices,
					insertArg.ctr.conflictMaps, insertArg.UniqueCols, 0)
				if conflictRowIdx > -1 {
					tmpBatch.Clean(proc.GetMPool())
					newBatch.Clean(proc.GetMPool())
					return moerr.NewConstraintViolation(proc.Ctx, conflictMsg)
				} else {
					// append batch to insertBatch
					_, err = insertBatch.Append(proc.Ctx, proc.Mp(), tmpBatch)
					if err != nil {
						tmpBatch.Clean(proc.GetMPool())
						newBatch.Clean(proc.GetMPool())
						return err
					}
					addToConflictMaps(&insertArg.ctr.keyBuf, insertBatch.Vecs, insertArg.ctr.uniqueKeyColIndices,
						insertArg.ctr.conflictMaps, insertBatch.RowCount()-1)
				}
				tmpBatch.Clean(proc.GetMPool())
			}
		}
		newBatch.Clean(proc.GetMPool())
	}

	return nil
}

func resetColPos(e *plan.Expr, columnCount int) {
	switch tmpExpr := e.Expr.(type) {
	case *plan.Expr_Col:
		tmpExpr.Col.ColPos = tmpExpr.Col.ColPos + int32(columnCount)
	case *plan.Expr_F:
		if tmpExpr.F.Func.ObjName != "values" {
			for _, arg := range tmpExpr.F.Args {
				resetColPos(arg, columnCount)
			}
		}
	}
}

// extractColIndicesFromExpr extracts the left-side column indices from a unique check expression.
// For a single-column unique key: "col_i = col_j" → [i]
// For a composite unique key: "(col_i = col_j AND col_k = col_l)" → [i, k]
func extractColIndicesFromExpr(expr *plan.Expr) []int32 {
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		if e.F.Func.ObjName == "=" {
			if col := extractColRefFromExpr(e.F.Args[0]); col != nil {
				return []int32{col.Col.ColPos}
			}
		} else if e.F.Func.ObjName == "and" {
			left := extractColIndicesFromExpr(e.F.Args[0])
			right := extractColIndicesFromExpr(e.F.Args[1])
			return append(left, right...)
		}
	}
	return nil
}

// extractColRefFromExpr recursively unwraps cast/type expressions to find the underlying column reference.
func extractColRefFromExpr(expr *plan.Expr) *plan.Expr_Col {
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return e
	case *plan.Expr_F:
		// Handle cast-like functions: try first argument
		if len(e.F.Args) > 0 {
			return extractColRefFromExpr(e.F.Args[0])
		}
	}
	return nil
}

// serializeUniqueKey serializes unique key column values into a string for hash map lookup.
// Returns empty string if any column is NULL (NULL never conflicts per SQL semantics).
// The caller-provided buf is reset and reused to avoid per-call allocations.
// Float types are canonicalized to match SQL '=' semantics (scale-based rounding, -0/+0 normalization).
func serializeUniqueKey(buf *bytes.Buffer, vecs []*vector.Vector, colIndices []int32, row int) string {
	buf.Reset()
	for _, colIdx := range colIndices {
		v := vecs[colIdx]
		if v.GetNulls().Contains(uint64(row)) {
			return ""
		}
		typ := v.GetType()
		switch typ.Oid {
		case types.T_float32:
			val := vector.MustFixedColWithTypeCheck[float32](v)[row]
			if typ.Scale > 0 {
				pow := math.Pow10(int(typ.Scale))
				val = float32(math.Round(float64(val)*pow) / pow)
			}
			if val == 0 {
				val = 0
			}
			bits := math.Float32bits(val)
			buf.Write([]byte{0, 0, 0, 4, byte(bits >> 24), byte(bits >> 16), byte(bits >> 8), byte(bits)})
		case types.T_float64:
			val := vector.MustFixedColWithTypeCheck[float64](v)[row]
			if val == 0 {
				val = 0
			}
			bits := math.Float64bits(val)
			buf.Write([]byte{0, 0, 0, 8,
				byte(bits >> 56), byte(bits >> 48), byte(bits >> 40), byte(bits >> 32),
				byte(bits >> 24), byte(bits >> 16), byte(bits >> 8), byte(bits)})
		default:
			b := v.GetRawBytesAt(row)
			l := len(b)
			buf.WriteByte(byte(l >> 24))
			buf.WriteByte(byte(l >> 16))
			buf.WriteByte(byte(l >> 8))
			buf.WriteByte(byte(l))
			buf.Write(b)
		}
	}
	return buf.String()
}

// findConflictByHashMap checks if a row conflicts with any existing row using hash maps.
// Returns the conflicting row index and message, or (-1, "") if no conflict.
func findConflictByHashMap(
	buf *bytes.Buffer,
	vecs []*vector.Vector,
	uniqueKeyColIndices [][]int32,
	conflictMaps []map[string]int,
	uniqueCols []string,
	row int,
) (int, string) {
	for i, colIndices := range uniqueKeyColIndices {
		key := serializeUniqueKey(buf, vecs, colIndices, row)
		if key == "" {
			continue
		}
		if idx, exists := conflictMaps[i][key]; exists {
			return idx, fmt.Sprintf("Duplicate entry for key '%s'", uniqueCols[i])
		}
	}
	return -1, ""
}

// addToConflictMaps adds a row's unique key values to all conflict hash maps.
func addToConflictMaps(
	buf *bytes.Buffer,
	vecs []*vector.Vector,
	uniqueKeyColIndices [][]int32,
	conflictMaps []map[string]int,
	rowIdx int,
) {
	for i, colIndices := range uniqueKeyColIndices {
		key := serializeUniqueKey(buf, vecs, colIndices, rowIdx)
		if key != "" {
			conflictMaps[i][key] = rowIdx
		}
	}
}

func fetchOneRowAsBatch(idx int, originBatch *batch.Batch, proc *process.Process, attrs []string) (*batch.Batch, error) {
	newBatch := batch.NewWithSize(len(attrs))
	newBatch.Attrs = attrs
	var uErr error
	for i, v := range originBatch.Vecs {
		newVec := vector.NewVec(*v.GetType())
		uErr = newVec.UnionOne(v, int64(idx), proc.Mp())
		if uErr != nil {
			newBatch.Clean(proc.Mp())
			return nil, uErr
		}
		newBatch.SetVector(int32(i), newVec)
	}
	newBatch.SetRowCount(1)
	return newBatch, nil
}

func updateOldBatch(evalBatch *batch.Batch, updateExpr map[string]*plan.Expr, proc *process.Process, columnCount int, attrs []string) (*batch.Batch, error) {
	var originVec *vector.Vector
	newBatch := batch.NewWithSize(len(attrs))
	newBatch.Attrs = attrs
	for i, attr := range newBatch.Attrs {
		if i < columnCount {
			// update insert cols
			if expr, exists := updateExpr[attr]; exists {
				runExpr := plan2.DeepCopyExpr(expr)
				resetColPos(runExpr, columnCount)
				newVec, err := colexec.GetWritableResultFromExpression(proc, runExpr, []*batch.Batch{evalBatch})
				if err != nil {
					newBatch.Clean(proc.Mp())
					return nil, err
				}
				newBatch.SetVector(int32(i), newVec)
			} else {
				originVec = evalBatch.Vecs[i+columnCount]
				newVec := vector.NewVec(*originVec.GetType())
				err := newVec.UnionOne(originVec, int64(0), proc.Mp())
				if err != nil {
					newBatch.Clean(proc.Mp())
					return nil, err
				}
				newBatch.SetVector(int32(i), newVec)
			}
		} else {
			// keep old cols
			originVec = evalBatch.Vecs[i]
			newVec := vector.NewVec(*originVec.GetType())
			err := newVec.UnionOne(originVec, int64(0), proc.Mp())
			if err != nil {
				newBatch.Clean(proc.Mp())
				return nil, err
			}
			newBatch.SetVector(int32(i), newVec)
		}
	}

	newBatch.SetRowCount(1)
	return newBatch, nil
}

// checkConflict uses expression evaluation to detect conflicts in checkConflictBatch.
// This is the legacy O(N) per-call approach, kept for testing purposes.
// The hot path now uses findConflictByHashMap for O(1) lookups instead.
func checkConflict(proc *process.Process, newBatch *batch.Batch, checkConflictBatch *batch.Batch,
	checkExpressionExecutor []colexec.ExpressionExecutor, uniqueCols []string, colCount int) (int, string, error) {
	if checkConflictBatch.RowCount() == 0 {
		return -1, "", nil
	}
	for j := 0; j < colCount; j++ {
		fromVec := newBatch.Vecs[j]
		toVec := checkConflictBatch.Vecs[j+colCount]
		for i := 0; i < checkConflictBatch.RowCount(); i++ {
			err := toVec.Copy(fromVec, int64(i), 0, proc.Mp())
			if err != nil {
				return 0, "", err
			}
		}
	}

	// build the check expr
	for i, executor := range checkExpressionExecutor {
		result, err := executor.Eval(proc, []*batch.Batch{checkConflictBatch}, nil)
		if err != nil {
			return 0, "", err
		}

		// Return the conflicting row index in checkConflictBatch, not the unique key ordinal.
		isConflict := vector.MustFixedColWithTypeCheck[bool](result)
		for rowIdx, flag := range isConflict {
			if flag {
				conflictMsg := fmt.Sprintf("Duplicate entry for key '%s'", uniqueCols[i])
				return rowIdx, conflictMsg, nil
			}
		}
	}

	return -1, "", nil
}
