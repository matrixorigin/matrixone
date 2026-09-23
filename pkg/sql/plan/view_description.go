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
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const MaxViewMetadataColumns = 4096

// viewDescriptionRelation returns the column row source used by SHOW's existing
// formatting and filtering expressions. No rows are read from the persisted
// View column snapshot, and no generated definition is written back.
func viewDescriptionRelation(
	ctx CompilerContext, def *TableDef, accountID uint32, databaseName, viewName string,
) (string, error) {
	cols, err := DescribeViewColumns(ctx, def.ViewSql.View)
	if err != nil {
		return "", err
	}
	if len(cols) > MaxViewMetadataColumns {
		return "", moerr.NewInternalError(ctx.GetContext(), "View metadata exceeds its column budget")
	}
	if len(cols) == 0 {
		return "", moerr.NewInternalError(ctx.GetContext(), "View has no output columns")
	}
	rows := make([]string, 0, len(cols))
	for i, col := range cols {
		if err := ctx.GetContext().Err(); err != nil {
			return "", err
		}
		typ := MakeTypeByPlan2Type(col.Typ)
		typeBytes, err := types.Encode(&typ)
		if err != nil {
			return "", err
		}
		defaultDef := col.Default
		if defaultDef == nil {
			defaultDef = &planpb.Default{NullAbility: !col.Typ.NotNullable}
		}
		defaultBytes, err := types.Encode(defaultDef)
		if err != nil {
			return "", err
		}
		notNull := 0
		if col.Typ.NotNullable {
			notNull = 1
		}
		rows = append(rows, fmt.Sprintf(
			"row(%d,%d,%s,%s,%s,%d,unhex('%s'),%s,%d,unhex('%s'),0,0,0,cast(null as varchar),'')",
			accountID, def.TblId, formatStrLit(databaseName), formatStrLit(viewName),
			formatStrLit(col.GetOriginCaseName()), i+1, hex.EncodeToString(typeBytes), formatStrLit(col.Typ.Enumvalues), notNull,
			hex.EncodeToString(defaultBytes)))
	}
	return "(select * from (values " + strings.Join(rows, ",") + ") as view_columns(" +
		"account_id,att_relname_id,att_database,att_relname,attname,attnum,atttyp,attr_enum,attnotnull," +
		"att_default,att_is_hidden,att_is_auto_increment,attr_has_generated,attr_generated,att_comment))", nil
}
