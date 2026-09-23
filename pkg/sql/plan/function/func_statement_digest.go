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

package function

import (
	"context"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// StatementDigestText returns MySQL-compatible normalized statement text.
func StatementDigestText(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	proc *process.Process,
	length int,
	selectList *FunctionSelectList,
) error {
	if len(parameters) == 0 || parameters[0] == nil {
		return moerr.NewInvalidInputNoCtx("STATEMENT_DIGEST_TEXT requires one argument")
	}
	if length == 0 {
		return nil
	}
	if !statementDigestHasActiveInput(parameters[0], length, selectList) {
		vector.MustFunctionResult[types.Varlena](result).SetNullResult(uint64(length))
		return nil
	}
	ctx := context.Background()
	if proc != nil && proc.Ctx != nil {
		ctx = proc.Ctx
	}
	sqlMode := statementDigestSQLMode(proc)
	maxDigestLength, err := statementDigestMaxLength(proc)
	if err != nil {
		return err
	}
	// MySQL only exposes parser diagnostics for a source SQL literal.  A
	// constant vector is not sufficient: it can be the result of a folded
	// expression, a cast, a subquery, or a prepared parameter.  String-source
	// provenance is retained by the binder/executor for exactly this boundary.
	binaryInput := statementDigestTextHasBinaryInput(parameters[0], length, selectList)
	discloseParseError := statementDigestTextAllLiteralInputs(parameters[0], length, selectList) && !binaryInput
	if statementDigestTextHasGeometryInput(parameters[0], length, selectList) {
		return moerr.NewUndisclosedParseErrorInDigestFunction(ctx)
	}

	return opUnaryBytesToBytesWithErrorCheck(
		parameters, result, proc, length,
		func(sql []byte) ([]byte, error) {
			// Encoding failures never expose parser diagnostics, even when the
			// value originated from a direct SQL literal. Invalid bytes are not
			// valid SQL text and can contain application-owned binary data.
			if !utf8.Valid(sql) {
				return nil, moerr.NewUndisclosedParseErrorInDigestFunction(ctx)
			}
			normalized, err := mysql.NormalizeStatementDigest(ctx, string(sql), sqlMode, maxDigestLength)
			if err != nil {
				if discloseParseError {
					return nil, moerr.NewParseErrorInDigestFunction(ctx, err.Error())
				}
				return nil, moerr.NewUndisclosedParseErrorInDigestFunction(ctx)
			}
			return []byte(normalized), nil
		},
		selectList,
	)
}

// Settings, provenance and type checks are evaluation, too. Do not perform
// them for NULL-only input or unselected branches, including compact masks.
func statementDigestHasActiveInput(parameter *vector.Vector, length int, selectList *FunctionSelectList) bool {
	if selectList.IgnoreAllRow() {
		return false
	}
	for row := 0; row < length; row++ {
		if selectList.Contains(uint64(row)) {
			continue
		}
		physicalRow := row
		if parameter.IsConst() {
			physicalRow = 0
		}
		if !parameter.IsNull(uint64(physicalRow)) {
			return true
		}
	}
	return false
}

// statementDigestTextAllLiteralInputs is deliberately conservative for a
// mixed vector: one expression-owned row is enough to suppress parser details
// for the whole batch, since the operation returns one error for the batch.
func statementDigestTextAllLiteralInputs(
	parameter *vector.Vector,
	length int,
	selectList *FunctionSelectList,
) bool {
	if parameter == nil || length <= 0 {
		return false
	}
	seen := false
	for row := 0; row < length; row++ {
		if selectList != nil && selectList.Contains(uint64(row)) {
			continue
		}
		physicalRow := row
		if parameter.IsConst() {
			physicalRow = 0
		}
		if parameter.IsNull(uint64(physicalRow)) {
			continue
		}
		seen = true
		if parameter.GetStringSourceAt(physicalRow) != types.StringSourceLiteral {
			return false
		}
	}
	return seen
}

// statementDigestTextHasBinaryInput reports binary provenance so malformed
// bytes cannot disclose parser details. Binary values are still passed to the
// normal parser: MySQL accepts binary-typed input when its bytes form valid SQL.
func statementDigestTextHasBinaryInput(
	parameter *vector.Vector,
	length int,
	selectList *FunctionSelectList,
) bool {
	if parameter == nil || length <= 0 {
		return false
	}
	for row := 0; row < length; row++ {
		if selectList != nil && selectList.Contains(uint64(row)) {
			continue
		}
		physicalRow := row
		if parameter.IsConst() {
			physicalRow = 0
		}
		if parameter.IsNull(uint64(physicalRow)) {
			continue
		}
		if parameter.GetIsBinaryStringAt(physicalRow) {
			return true
		}
	}
	return false
}

func statementDigestTextHasGeometryInput(
	parameter *vector.Vector,
	length int,
	selectList *FunctionSelectList,
) bool {
	if parameter == nil || length <= 0 {
		return false
	}
	for row := 0; row < length; row++ {
		if selectList != nil && selectList.Contains(uint64(row)) {
			continue
		}
		physicalRow := row
		if parameter.IsConst() {
			physicalRow = 0
		}
		if parameter.IsNull(uint64(physicalRow)) {
			continue
		}
		switch parameter.GetType().Oid {
		case types.T_geometry, types.T_geometry32:
			return true
		}
	}
	return false
}

func statementDigestMaxLength(proc *process.Process) (int, error) {
	return process.ResolveMaxDigestLengthWithError(proc)
}

func statementDigestSQLMode(proc *process.Process) string {
	mode := process.ResolveSqlMode(proc)
	if mode == process.EmptySqlModeSentinel {
		return ""
	}
	return mode
}
