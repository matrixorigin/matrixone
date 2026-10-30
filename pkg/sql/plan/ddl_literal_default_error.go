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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// mapLiteralDefaultCastError reports a literal DEFAULT that cannot be
// converted to its column type as ER_INVALID_DEFAULT (1067), as MySQL does in
// every sql_mode (INT DEFAULT 'abc', TINYINT DEFAULT 300, DATE DEFAULT 'abc',
// DECIMAL DEFAULT 'abc'). The cast used to leak its own error (20203 invalid
// argument, 1690 out of range, 20301 invalid datetime/decimal value) from
// CREATE TABLE and every ALTER TABLE form. Only the conversion of a literal
// is mapped: the binder and the checks before the cast keep their errors. Expression
// defaults (parenthesized) are evaluated like any expression and keep their
// error.
func mapLiteralDefaultCastError(ctx context.Context, literal tree.Expr, colName string, err error) error {
	if err == nil || !isLiteralDefaultAst(literal) {
		return err
	}
	for _, code := range []uint16{
		moerr.ErrInvalidArg,
		moerr.ErrOutOfRange,
		moerr.ErrDataTruncated,
		moerr.ErrTruncatedWrongValue,
		moerr.ErrTruncatedWrongValueForField,
		moerr.ErrInvalidInput,
	} {
		if moerr.IsMoErrCode(err, code) {
			return moerr.NewErrInvalidDefault(ctx, colName)
		}
	}
	return err
}

func isLiteralDefaultAst(expr tree.Expr) bool {
	switch e := expr.(type) {
	case *tree.NumVal:
		return e.ValType != tree.P_null
	case *tree.StrVal:
		return true
	case *tree.UnaryExpr:
		if e.Op == tree.UNARY_MINUS || e.Op == tree.UNARY_PLUS {
			return isLiteralDefaultAst(e.Expr)
		}
	}
	return false
}
