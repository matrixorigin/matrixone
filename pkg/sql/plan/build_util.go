// Copyright 2021 - 2022 Matrix Origin
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

package plan

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
)

func appendQueryNode(query *Query, node *Node) int32 {
	nodeID := int32(len(query.Nodes))
	node.NodeId = nodeID
	query.Nodes = append(query.Nodes, node)

	return nodeID
}

func getTypeFromAst(typ tree.ResolvableTypeReference) (*plan.Type, error) {
	if n, ok := typ.(*tree.T); ok {
		switch uint8(n.InternalType.Oid) {
		case defines.MYSQL_TYPE_TINY:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint8), Width: n.InternalType.Width, Size: 1}, nil
			}
			return &plan.Type{Id: int32(types.T_int8), Width: n.InternalType.Width, Size: 1}, nil
		case defines.MYSQL_TYPE_SHORT:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint16), Width: n.InternalType.Width, Size: 2}, nil
			}
			return &plan.Type{Id: int32(types.T_int16), Width: n.InternalType.Width, Size: 2}, nil
		case defines.MYSQL_TYPE_LONG:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint32), Width: n.InternalType.Width, Size: 4}, nil
			}
			return &plan.Type{Id: int32(types.T_int32), Width: n.InternalType.Width, Size: 4}, nil
		case defines.MYSQL_TYPE_LONGLONG:
			if n.InternalType.Unsigned {
				return &plan.Type{Id: int32(types.T_uint64), Width: n.InternalType.Width, Size: 8}, nil
			}
			return &plan.Type{Id: int32(types.T_int64), Width: n.InternalType.Width, Size: 8}, nil
		case defines.MYSQL_TYPE_FLOAT:
			return &plan.Type{Id: int32(types.T_float32), Width: n.InternalType.Width, Size: 4, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_DOUBLE:
			return &plan.Type{Id: int32(types.T_float64), Width: n.InternalType.Width, Size: 8, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_STRING:
			width := n.InternalType.DisplayWith
			if width == -1 {
				// create table t1(a char) -> DisplayWith = -1；but get width=1 in MySQL and PgSQL
				width = 1
			}
			if n.InternalType.FamilyString == "char" { // type char
				return &plan.Type{Id: int32(types.T_char), Size: 24, Width: width}, nil
			}
			return &plan.Type{Id: int32(types.T_varchar), Size: 24, Width: width}, nil
		case defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VARCHAR:
			width := n.InternalType.DisplayWith
			if width == -1 {
				// create table t1(a char) -> DisplayWith = -1；but get width=1 in MySQL and PgSQL
				width = 1
			}
			if n.InternalType.FamilyString == "char" { // type char
				return &plan.Type{Id: int32(types.T_char), Size: 24, Width: width}, nil
			}
			return &plan.Type{Id: int32(types.T_varchar), Size: 24, Width: width}, nil
		case defines.MYSQL_TYPE_DATE:
			return &plan.Type{Id: int32(types.T_date), Size: 4}, nil
		case defines.MYSQL_TYPE_DATETIME:
			// currently the ast's width for datetime's is 26, this is not accurate and may need revise, not important though, as we don't need it anywhere else except to differentiate empty vector.Typ.
			return &plan.Type{Id: int32(types.T_datetime), Size: 8, Width: n.InternalType.Width, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_TIMESTAMP:
			return &plan.Type{Id: int32(types.T_timestamp), Size: 8, Width: n.InternalType.Width, Precision: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_DECIMAL:
			if n.InternalType.DisplayWith > 16 {
				return &plan.Type{Id: int32(types.T_decimal128), Size: 16, Width: n.InternalType.DisplayWith, Scale: n.InternalType.Precision}, nil
			}
			return &plan.Type{Id: int32(types.T_decimal64), Size: 8, Width: n.InternalType.DisplayWith, Scale: n.InternalType.Precision}, nil
		case defines.MYSQL_TYPE_BOOL:
			return &plan.Type{Id: int32(types.T_bool), Size: 1}, nil
		case defines.MYSQL_TYPE_BLOB:
			return &plan.Type{Id: int32(types.T_blob), Size: 24}, nil
		case defines.MYSQL_TYPE_JSON:
			return &plan.Type{Id: int32(types.T_json)}, nil
		default:
			return nil, errors.New("", fmt.Sprintf("Data type: '%s', will be supported in future version.", tree.String(&n.InternalType, dialect.MYSQL)))
		}
	}
	return nil, errors.New(errno.IndeterminateDatatype, "Unknown data type.")
}

func buildDefaultExpr(col *tree.ColumnTableDef, typ *plan.Type) (*plan.Default, error) {
	nullAbility := true
	var expr tree.Expr = nil

	for _, attr := range col.Attributes {
		if s, ok := attr.(*tree.AttributeNull); ok {
			nullAbility = s.Is
			break
		}
	}

	for _, attr := range col.Attributes {
		if s, ok := attr.(*tree.AttributeDefault); ok {
			expr = s.Expr
			break
		}
	}

	if !nullAbility && isNullExpr(expr) {
		return nil, errors.New(errno.InvalidColumnDefinition, fmt.Sprintf("Invalid default value for '%s'", col.Name.Parts[0]))
	}

	if expr == nil {
		return &plan.Default{
			NullAbility:  nullAbility,
			Expr:         nil,
			OriginString: "",
		}, nil
	}

	binder := NewDefaultBinder(nil, nil, typ)
	planExpr, err := binder.BindExpr(expr, 0, false)
	if err != nil {
		return nil, err
	}

	defaultExpr, err := makePlan2CastExpr(planExpr, typ)
	if err != nil {
		return nil, err
	}

	// try to calculate default value, return err if fails
	r := rule.NewConstantFlod()
	if _, err := colexec.EvalExpr(r.GetBatch(), nil, defaultExpr); err != nil {
		return nil, err
	}

	return &plan.Default{
		NullAbility:  nullAbility,
		Expr:         defaultExpr,
		OriginString: tree.String(expr, dialect.MYSQL),
	}, nil
}

func isNullExpr(expr tree.Expr) bool {
	if expr == nil {
		return false
	}
	v, ok := expr.(*tree.NumVal)
	return ok && v.ValType == tree.P_null
}

func convertValueIntoBool(name string, args []*Expr, isLogic bool) error {
	if !isLogic && (len(args) != 2 || (args[0].Typ.Id != int32(types.T_bool) && args[1].Typ.Id != int32(types.T_bool))) {
		return nil
	}
	for _, arg := range args {
		if arg.Typ.Id == int32(types.T_bool) {
			continue
		}
		switch ex := arg.Expr.(type) {
		case *plan.Expr_C:
			switch value := ex.C.Value.(type) {
			case *plan.Const_Ival:
				if value.Ival == 0 {
					ex.C.Value = &plan.Const_Bval{Bval: false}
				} else if value.Ival == 1 {
					ex.C.Value = &plan.Const_Bval{Bval: true}
				} else {
					return errors.New("", fmt.Sprintf("Can't cast '%v' as boolean type.", value.Ival))
				}
				arg.Typ.Id = int32(types.T_bool)
			}
		}
	}
	return nil
}

func getFunctionObjRef(funcID int64, name string) *ObjectRef {
	return &ObjectRef{
		Obj:     funcID,
		ObjName: name,
	}
}

func getDefaultExpr(d *plan.Default, typ *plan.Type) (*Expr, error) {
	if !d.NullAbility && d.Expr == nil {
		return nil, errors.New("", "invalid default value")
	}
	if d.Expr == nil {
		return &Expr{
			Expr: &plan.Expr_C{
				C: &Const{
					Isnull: true,
				},
			},
			Typ: &plan.Type{
				Id:       typ.Id,
				Nullable: true,
			},
		}, nil
	}
	return d.Expr, nil
}
