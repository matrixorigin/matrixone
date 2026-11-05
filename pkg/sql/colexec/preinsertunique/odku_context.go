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

package preinsertunique

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func odkuMaterializedColumn(e *plan.Expr) bool {
	c := e.GetCol()
	return c != nil && c.RelPos == 0 && c.ColPos >= 0
}

// ValidateODKUContext checks layout without opening an account, compiling an
// executor or reading data. Compiler preflight and operator Prepare share this
// authority. Actual child vector types and lengths are checked when they arrive.
func ValidateODKUContext(ctx *plan.PreInsertUkCtx) error {
	if ctx == nil {
		return nil
	}
	if ctx.OrderedOdku != (ctx.OdkuActions != nil) {
		return moerr.NewNotSupportedNoCtx("ODKU requires the ordered action operator")
	}
	if !ctx.OrderedOdku {
		return nil
	}
	c := ctx.OdkuActions
	if len(c.SourceColumns) == 0 || len(c.OldColumns) == 0 || len(c.InputColumns) != len(c.OldColumns) || len(c.Constraints) == 0 ||
		len(c.UpdateColumns) == 0 || len(c.UpdateColumns) != len(c.UpdateExprs) || c.PkColumn < 0 || int(c.PkColumn) >= len(c.OldColumns) ||
		c.RowIdColumn < 0 || int(c.RowIdColumn) >= len(c.OldColumns) ||
		!odkuMaterializedColumn(c.SourceOrdinal) || c.SourceOrdinal.Typ.Id != int32(types.T_int64) ||
		uint64(c.InsertCheckConstraintCount) > uint64(len(c.InsertChecks)) {
		return moerr.NewInternalErrorNoCtx("invalid ODKU action layout")
	}

	for _, e := range c.SourceColumns {
		if !odkuMaterializedColumn(e) {
			return moerr.NewInternalErrorNoCtx("unmaterialized ODKU source column")
		}
	}
	for i, e := range c.OldColumns {
		p := c.InputColumns[i]
		if !odkuMaterializedColumn(e) || p < -1 || int(p) >= len(c.SourceColumns) {
			return moerr.NewInternalErrorNoCtx("invalid ODKU image column")
		}
		if p >= 0 && !odkuKeyTypeEqual(odkuType(e.Typ), odkuType(c.SourceColumns[p].Typ)) {
			return moerr.NewInternalErrorNoCtx("ODKU source image type mismatch")
		}
	}
	if prior := c.PriorData; prior != nil {
		if prior.Ref == nil || prior.Ref.Obj == 0 || prior.TableDef == nil || len(prior.TableDef.Cols) != len(c.OldColumns) || c.InputColumns[c.PkColumn] < 0 {
			return moerr.NewInternalErrorNoCtx("invalid ODKU prior-data layout")
		}
		for i, col := range prior.TableDef.Cols {
			if col == nil || !odkuKeyTypeEqual(odkuType(col.Typ), odkuType(c.OldColumns[i].Typ)) {
				return moerr.NewInternalErrorNoCtx("ODKU prior-data image type mismatch")
			}
		}
		for _, key := range c.Constraints {
			if key == nil || key.LookupRef == nil || key.LookupRef.Obj == 0 {
				return moerr.NewInternalErrorNoCtx("missing ODKU prior constraint relation")
			}
		}
	}
	if c.OldColumns[c.RowIdColumn].Typ.Id != int32(types.T_Rowid) {
		return moerr.NewInternalErrorNoCtx("invalid ODKU original row identity type")
	}
	for i, p := range c.UpdateColumns {
		if p < 0 || int(p) >= len(c.OldColumns) {
			return moerr.NewInternalErrorNoCtx("invalid ODKU assignment column")
		}
		if err := validateODKUActionExpr(c.UpdateExprs[i], c); err != nil {
			return err
		}
	}
	if c.ActionFinalColumn < -1 || int(c.ActionFinalColumn) >= len(c.SourceColumns) ||
		(c.ActionFinalColumn >= 0 && (c.SourceColumns[c.ActionFinalColumn].Typ.Id != int32(types.T_bool) || len(c.ForeignKeyChecks) == 0)) {
		return moerr.NewInternalErrorNoCtx("invalid ODKU action-final column")
	}
	for _, fk := range c.ForeignKeyChecks {
		if fk == nil || fk.Eligibility.GetCol() == nil || len(fk.Columns) == 0 {
			return moerr.NewInternalErrorNoCtx("invalid ODKU foreign-key metadata")
		}
		p := fk.Eligibility.GetCol()
		if p.RelPos != 1 || p.ColPos < 0 || int(p.ColPos) >= len(c.SourceColumns) || c.SourceColumns[p.ColPos].Typ.Id != int32(types.T_bool) {
			return moerr.NewInternalErrorNoCtx("invalid ODKU eligibility column")
		}
		for _, col := range fk.Columns {
			if col.RelPos != 0 || col.ColPos < 0 || int(col.ColPos) >= len(c.OldColumns) {
				return moerr.NewInternalErrorNoCtx("invalid ODKU foreign-key image column")
			}
		}
	}
	for _, key := range c.Constraints {
		if key == nil || !odkuMaterializedColumn(key.InputKey) || !odkuMaterializedColumn(key.PriorTarget) || key.CurrentKey == nil || len(key.Types) == 0 ||
			!odkuKeyTypeEqual(odkuType(key.InputKey.Typ), odkuType(key.CurrentKey.Typ)) || !odkuKeyTypeEqual(odkuType(key.PriorTarget.Typ), odkuType(c.OldColumns[c.PkColumn].Typ)) {
			return moerr.NewInternalErrorNoCtx("invalid ODKU constraint layout")
		}
		if err := validateODKUActionExpr(key.CurrentKey, c); err != nil {
			return err
		}
	}
	for _, list := range [][]*plan.Expr{c.InsertChecks, c.UpdateChecks} {
		for _, expr := range list {
			if err := validateODKUActionExpr(expr, c); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateODKUActionExpr(expr *plan.Expr, c *plan.OdkuActionCtx) error {
	if expr == nil || expr.Expr == nil {
		return moerr.NewInternalErrorNoCtx("missing ODKU action expression")
	}
	if col := expr.GetCol(); col != nil {
		columns := c.OldColumns
		if col.RelPos == 1 {
			columns = c.SourceColumns
		} else if col.RelPos != 0 {
			return moerr.NewInternalErrorNoCtx("invalid ODKU action relation")
		}
		if col.ColPos < 0 || int(col.ColPos) >= len(columns) || !odkuKeyTypeEqual(odkuType(expr.Typ), odkuType(columns[col.ColPos].Typ)) {
			return moerr.NewInternalErrorNoCtx("invalid ODKU action column")
		}
	}
	if f := expr.GetF(); f != nil {
		if f.Func == nil {
			return moerr.NewInternalErrorNoCtx("invalid ODKU action function")
		}
		for _, arg := range f.Args {
			if err := validateODKUActionExpr(arg, c); err != nil {
				return err
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if err := validateODKUActionExpr(arg, c); err != nil {
				return err
			}
		}
	}
	switch expr.Expr.(type) {
	case *plan.Expr_Sub, *plan.Expr_Corr, *plan.Expr_W:
		return moerr.NewInternalErrorNoCtx("unmaterialized ODKU action expression")
	}
	return nil
}
