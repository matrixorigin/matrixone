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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// validateEnumSetMemberDuplicates rejects an ENUM/SET column definition that
// lists one member twice (MySQL ER_DUPLICATED_VALUE_IN_TYPE, 1291). Members
// are compared the way MO stores and assigns them: a case-insensitive column
// collation folds case, while a binary or legacy bytewise collation keeps
// case-distinct members apart. SET members are stored without trailing
// spaces, so those are ignored; ENUM keeps raw padded labels (ENUM('a ','a')
// assigns 'a ' and 'a' to different members), so only identical labels
// collide there.
func validateEnumSetMemberDuplicates(ctx context.Context, column *tree.ColumnTableDef, tableCharset uint32) error {
	astType, ok := column.Type.(*tree.T)
	if !ok {
		return nil
	}
	var family string
	switch defines.MysqlType(astType.InternalType.Oid) {
	case defines.MYSQL_TYPE_ENUM:
		family = "ENUM"
	case defines.MYSQL_TYPE_SET:
		family = "SET"
	default:
		return nil
	}
	// ENUM/SET plan types do not carry a collation; resolve the column's
	// effective text collation through the same attribute rules as VARCHAR.
	probe := plan.Type{Id: int32(types.T_varchar), Charset: uint32(types.CharsetUTF8)}
	if err := applyDefaultAndColumnAttributesToType(ctx, &probe, tableCharset, column.Attributes); err != nil {
		// Attribute errors are reported by the regular type resolution.
		return nil
	}
	foldCase := types.IsCaseInsensitiveCollation(uint8(probe.Charset))
	seen := make(map[string]string, len(astType.InternalType.EnumValues))
	for _, value := range astType.InternalType.EnumValues {
		member := value
		if family == "SET" {
			member = strings.TrimRight(value, " ")
		}
		key := member
		if foldCase {
			key = strings.ToLower(member)
		}
		if first, ok := seen[key]; ok {
			return moerr.NewErrDuplicatedValueInType(ctx, column.Name.ColNameOrigin(), first, family)
		}
		seen[key] = member
	}
	return nil
}

// validateEnumSetConstantDefault rejects a constant DEFAULT that is not a
// member of its ENUM/SET column (MySQL ER_INVALID_DEFAULT, 1067, in every
// sql_mode). The enum/set assignment functions stay unevaluated at DDL time
// because their truncation policy depends on the inserting session's
// sql_mode, so without this check every INSERT relying on the default failed
// with 1265 (or stored ” in non-strict mode). The check uses the same parse
// as the runtime assignment, so exactly the defaults a strict INSERT would
// accept remain valid. Expression defaults and non-constant sources are left
// to the insert.
func validateEnumSetConstantDefault(ctx context.Context, proc *process.Process, typ plan.Type, defaultExpr *plan.Expr, colName string) error {
	if typ.Id != int32(types.T_enum) && !isSetPlanType(&typ) {
		return nil
	}
	fn := defaultExpr.GetF()
	if fn == nil || len(fn.Args) != 2 {
		return nil
	}
	members := fn.Args[0].GetLit()
	if members == nil || members.Isnull {
		return nil
	}
	src := fn.Args[1]
	if !function.IsConstant(src, false) {
		return nil
	}
	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(src), proc, false, true)
	if err != nil {
		return err
	}
	lit := folded.GetLit()
	if lit == nil || lit.Isnull {
		return nil
	}
	definition := members.GetSval()
	var truncated bool
	switch fn.Func.ObjName {
	case "cast_value_to_index":
		_, truncated, err = types.ParseEnumAssignment(definition, lit.GetSval())
	case "cast_set_value_to_index":
		_, truncated, err = types.ParseSetAssignment(definition, lit.GetSval())
	case "cast_index_value_to_index", "cast_set_index_value_to_index":
		number, ok := enumSetIntegerDefault(lit, src.Typ)
		if !ok {
			return nil
		}
		if fn.Func.ObjName == "cast_index_value_to_index" {
			_, truncated, err = types.ParseEnumNumberAssignment(definition, number)
		} else {
			var bitmap uint64
			bitmap, err = types.SetValidBitmap(definition)
			truncated = number&bitmap != number
		}
	default:
		return nil
	}
	if err != nil || truncated {
		return moerr.NewErrInvalidDefault(ctx, colName)
	}
	return nil
}

// enumSetIntegerDefault reads an integer default the way the runtime
// enum/set integer assignment reinterprets it (uint64 of the source value).
func enumSetIntegerDefault(lit *plan.Literal, typ plan.Type) (uint64, bool) {
	switch types.T(typ.Id) {
	case types.T_int64:
		return uint64(lit.GetI64Val()), true
	case types.T_uint64:
		return lit.GetU64Val(), true
	case types.T_uint16:
		return uint64(lit.GetU16Val()), true
	}
	return 0, false
}
