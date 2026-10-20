// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// FormatDedupKey formats a duplicate key from the expression vector used by a
// DEDUP join. FLOAT/DOUBLE primary-key identity expressions are serial(...)
// encodings, so decode those bytes with the original column types instead of
// leaking the binary identity key into the user-facing duplicate-entry error.
func FormatDedupKey(vec *vector.Vector, row int, colTypes []plan.Type) (key string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			key = ""
			err = moerr.NewInternalErrorNoCtxf("cannot format duplicate key: %v", recovered)
		}
	}()
	if vec == nil || row < 0 || row >= vec.Length() {
		return "", moerr.NewInternalErrorNoCtxf("cannot format duplicate key at row %d", row)
	}
	if len(colTypes) == 1 {
		originalType := types.T(colTypes[0].Id)
		if (originalType == types.T_float32 || originalType == types.T_float64) &&
			(vec.GetType().Oid == types.T_varchar || vec.GetType().Oid == types.T_varbinary) {
			items, err := types.StringifyTuple(vec.GetBytesAt(row), colTypes)
			if err != nil {
				return "", err
			}
			return items[0], nil
		}
		return vec.RowToString(row), nil
	}

	items, err := types.StringifyTuple(vec.GetBytesAt(row), colTypes)
	if err != nil {
		return "", err
	}
	return "(" + strings.Join(items, ",") + ")", nil
}

// FormatDedupEntry formats the user-visible part of a duplicate-entry
// diagnostic.  Index-table keys are normally serialized into a varchar; when
// that serialization contains a tuple, decode it back to the logical key so
// INSERT/UPDATE IGNORE warnings match the existing duplicate error path.
func FormatDedupEntry(vec *vector.Vector, row int, colName string, colTypes []plan.Type) (entry string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			entry = ""
			err = moerr.NewInternalErrorNoCtxf("cannot format duplicate key: %v", recovered)
		}
	}()
	if vec == nil || row < 0 || row >= vec.Length() {
		return "", moerr.NewInternalErrorNoCtxf("cannot format duplicate key at row %d", row)
	}
	if len(colTypes) == 1 && colName == catalog.IndexTableIndexColName &&
		vec.GetType().Oid == types.T_varchar {
		t, _, schema, err := types.DecodeTuple(vec.GetBytesAt(row))
		if err == nil && len(schema) > 1 {
			return t.ErrString(make([]int32, len(schema))), nil
		}
	}
	return FormatDedupKey(vec, row, colTypes)
}

// FormatDedupKeyFromRow renders lossless logical index parts. Only the rejected
// row is evaluated, so successful inserts do not allocate diagnostic vectors.
// The expressions reference a single incoming child; supplying the same window
// for both relation positions also covers a physically swapped DEDUP join.
func FormatDedupKeyFromRow(proc *process.Process, exprs []*plan.Expr, input *batch.Batch, row int) (entry string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			entry = ""
			err = moerr.NewInternalErrorNoCtxf("cannot format logical duplicate key: %v", recovered)
		}
	}()
	if len(exprs) == 0 {
		return "", moerr.NewInternalErrorNoCtx("missing logical duplicate key")
	}
	one, err := input.Window(row, row+1)
	if err != nil {
		return "", err
	}
	defer one.Clean(nil)
	items := make([]string, len(exprs))
	for i, expr := range exprs {
		value, err := formatDedupPart(proc, expr, one)
		if err != nil {
			return "", err
		}
		items[i] = value
	}
	if len(items) == 1 {
		return items[0], nil
	}
	return "(" + strings.Join(items, ",") + ")", nil
}

func formatDedupPart(proc *process.Process, expr *plan.Expr, one *batch.Batch) (string, error) {
	vec, free, err := GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{one, one})
	if err != nil {
		return "", err
	}
	defer free()
	// Keep ordinary UTF-8 text readable while escaping NUL, control characters
	// and invalid byte sequences in the user's actual logical value.
	quoted := strconv.Quote(vec.RowToString(0))
	return quoted[1 : len(quoted)-1], nil
}
