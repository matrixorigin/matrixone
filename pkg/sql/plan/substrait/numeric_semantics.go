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

package substrait

import (
	"context"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
)

func decimalSignature(args []*planpb.Expr, result *planpb.Type) bool {
	if exactDecimalBits(result) != 0 {
		return true
	}
	for _, arg := range args {
		if arg != nil && exactDecimalBits(&arg.Typ) != 0 {
			return true
		}
	}
	return false
}

func exactScalarName(id int32) string {
	switch id {
	case function.PLUS:
		return "add"
	case function.MINUS:
		return "subtract"
	case function.MULTI:
		return "multiply"
	case function.DIV:
		return "divide"
	case function.INTEGER_DIV:
		return "integer_divide"
	case function.MOD:
		return "modulo"
	case function.UNARY_MINUS:
		return "negate"
	case function.CAST:
		return "cast"
	case function.EQUAL:
		return "equal"
	case function.NOT_EQUAL:
		return "not_equal"
	case function.LESS_THAN:
		return "less"
	case function.LESS_EQUAL:
		return "less_equal"
	case function.GREAT_THAN:
		return "greater"
	case function.GREAT_EQUAL:
		return "greater_equal"
	case function.CASE:
		return "if_then"
	case function.COALESCE:
		return "coalesce"
	case function.ISNULL:
		return "is_null"
	case function.ISNOTNULL:
		return "is_not_null"
	default:
		return ""
	}
}

func (e *exporter) hasSemanticCapability(kind semanticCapabilityKind, name string, ref *planpb.ObjectRef, args []*planpb.Expr, out *planpb.Type) (bool, error) {
	if e.profile.exactDecimalV1 && (decimalSignature(args, out) || name == "coalesce") {
		return e.hasExactSemanticCapability(kind, name, ref, args, out)
	}
	return hasSemanticCapability(kind, name, ref, args, out)
}

func sameDecimalDescriptor(a, b *planpb.Type) bool {
	return exactDecimalBits(a) != 0 && exactDecimalBits(a) == exactDecimalBits(b) && a.Width == b.Width && a.Scale == b.Scale
}

func (e *exporter) hasExactSemanticCapability(kind semanticCapabilityKind, name string, ref *planpb.ObjectRef, args []*planpb.Expr, out *planpb.Type) (bool, error) {
	if ref == nil || out == nil || uint64(ref.Obj)&function.Distinct != 0 {
		return false, nil
	}
	id, overload := function.DecodeOverloadID(ref.Obj)
	if _, exists := function.GetFunctionByIdWithoutError(ref.Obj); !exists {
		return false, nil
	}
	if _, err := e.profile.substraitType(out); err != nil {
		return false, err
	}
	inputTypes := make([]types.Type, len(args))
	for i, arg := range args {
		if arg == nil {
			return false, nil
		}
		if _, err := e.profile.substraitType(&arg.Typ); err != nil {
			return false, err
		}
		inputTypes[i] = types.New(types.T(arg.Typ.Id), arg.Typ.Width, arg.Typ.Scale)
	}
	if kind == semanticAggregate {
		if len(args) != 1 {
			return false, nil
		}
		switch id {
		case function.SUM, function.AVG:
			if (id == function.SUM && name != "sum") || (id == function.AVG && name != "avg") ||
				exactDecimalBits(out) < 128 || exactDecimalBits(out) < exactDecimalBits(&args[0].Typ) || out.Width > 65 ||
				(exactDecimalBits(&args[0].Typ) == 0 && types.T(args[0].Typ.Id) != types.T_int64) ||
				(id == function.SUM && out.Scale != args[0].Typ.Scale) ||
				(id == function.AVG && out.Scale < args[0].Typ.Scale) {
				return false, nil
			}
		case function.MIN, function.MAX:
			if (id == function.MIN && name != "min") || (id == function.MAX && name != "max") || !sameDecimalDescriptor(&args[0].Typ, out) || out.Width > 65 {
				return false, nil
			}
		case function.COUNT:
			if name != "count" || types.T(out.Id) != types.T_int64 {
				return false, nil
			}
		default:
			return false, nil
		}
	} else {
		if name == "" || exactScalarName(id) != name {
			return false, nil
		}
		switch id {
		case function.CAST:
			if overload != 0 || len(args) != 2 || args[1].GetT() == nil || exactDecimalBits(out) == 0 || !sameDecimalDescriptor(&args[1].Typ, out) {
				return false, nil
			}
			if exactDecimalBits(&args[0].Typ) == 0 {
				digits := int32(0)
				switch types.T(args[0].Typ.Id) {
				case types.T_int8:
					digits = 3
				case types.T_int16:
					digits = 5
				case types.T_int32:
					digits = 10
				case types.T_int64:
					digits = 19
				}
				// A valid decimal domain with this many integer digits contains
				// both signed endpoints at every scale, not just sampled values.
				if digits == 0 || out.Width-out.Scale < digits {
					return false, nil
				}
			}
		case function.CASE:
			if len(args) < 3 || len(args)%2 != 1 || !tpchCaseArgs(args, out) {
				return false, nil
			}
		case function.COALESCE:
			if len(args) == 0 {
				return false, nil
			}
			if exactDecimalBits(out) == 0 {
				switch types.T(out.Id) {
				case types.T_bool, types.T_int8, types.T_int16, types.T_int32, types.T_int64:
				default:
					return false, nil
				}
			}
			for _, arg := range args {
				if arg.Typ.Id != out.Id || arg.Typ.Width != out.Width || arg.Typ.Scale != out.Scale {
					return false, nil
				}
			}
		case function.ISNULL, function.ISNOTNULL:
			if len(args) != 1 || types.T(out.Id) != types.T_bool {
				return false, nil
			}
		case function.UNARY_MINUS:
			if len(args) != 1 || exactDecimalBits(&args[0].Typ) == 0 || exactDecimalBits(out) < exactDecimalBits(&args[0].Typ) || out.Width < args[0].Typ.Width || out.Scale != args[0].Typ.Scale {
				return false, nil
			}
		default:
			if len(args) != 2 || exactDecimalBits(&args[0].Typ) == 0 || exactDecimalBits(&args[1].Typ) == 0 {
				return false, nil
			}
			if id == function.INTEGER_DIV {
				if types.T(out.Id) != types.T_int64 {
					return false, nil
				}
			} else if id == function.EQUAL || id == function.NOT_EQUAL || id == function.LESS_THAN || id == function.LESS_EQUAL || id == function.GREAT_THAN || id == function.GREAT_EQUAL {
				if types.T(out.Id) != types.T_bool {
					return false, nil
				}
			} else if exactDecimalBits(out) == 0 || out.Width > 65 {
				return false, nil
			}
			if exactDecimalBits(out) != 0 {
				if exactDecimalBits(out) < max(exactDecimalBits(&args[0].Typ), exactDecimalBits(&args[1].Typ)) {
					return false, nil
				}
				switch id {
				case function.PLUS, function.MINUS, function.MOD:
					if out.Scale != max(args[0].Typ.Scale, args[1].Typ.Scale) {
						return false, nil
					}
				case function.MULTI:
					if exactDecimalBits(out) < 128 || out.Scale != min(args[0].Typ.Scale+args[1].Typ.Scale, max(int32(12), args[0].Typ.Scale, args[1].Typ.Scale)) {
						return false, nil
					}
				case function.DIV:
					if exactDecimalBits(out) < 128 || out.Scale > 30 {
						return false, nil
					}
				}
			}
		}
	}
	resolved, err := function.GetFunctionByName(context.Background(), ref.ObjName, inputTypes)
	if err != nil || resolved.GetEncodedOverloadID() != ref.Obj {
		return false, nil
	}
	// Arithmetic results were bound before MO inserted physical working casts.
	// Re-deriving precision from those casts would change valid MO results, and
	// re-deriving division would lose the bound session increment. Validate the
	// v1 domain above and retain that declared descriptor verbatim instead.
	if kind == semanticAggregate || exactDecimalBits(out) == 0 || id == function.CAST || id == function.CASE || id == function.COALESCE {
		result := resolved.GetReturnType()
		if int32(result.Oid) != out.Id || result.Width != out.Width || result.Scale != out.Scale {
			return false, nil
		}
	}
	if kind == semanticAggregate && aggregateCanReturnNullOnEmpty(id) && !out.NotNullable {
		return true, nil
	}
	return function.DeduceNotNullable(ref.Obj, args) == out.NotNullable || semanticNotNullable(ref.Obj, args) == out.NotNullable, nil
}

func (e *exporter) exactScalarExpr(result *planpb.Expr, call *planpb.Function, inputs []int) (*spb.Expression, error) {
	id, _ := function.DecodeOverloadID(call.Func.Obj)
	name := exactScalarName(id)
	if id == function.CAST {
		if literal, handled, err := e.exactNumericLiteralCast(result, call); handled {
			return literal, err
		}
	}
	supported, err := e.hasExactSemanticCapability(semanticScalar, name, call.Func, call.Args, &result.Typ)
	if err != nil {
		return nil, err
	}
	if name == "" || !supported {
		return nil, notEligiblef(EligibilityExpression, "exact %s overload %d has no declared Sirius semantic equivalence (result %d,%d,%d,required=%t)", name, call.Func.Obj, result.Typ.Id, result.Typ.Width, result.Typ.Scale, result.Typ.NotNullable)
	}
	if id == function.CASE {
		return e.caseExpr(result, call, inputs)
	}
	arguments := call.Args
	if id == function.CAST {
		arguments = arguments[:1]
	}
	args := make([]*spb.Expression, len(arguments))
	for i, argument := range arguments {
		args[i], err = e.expr(argument, inputs)
		if err != nil {
			return nil, err
		}
	}
	if id != function.COALESCE && id != function.ISNULL && id != function.ISNOTNULL {
		name = "mo_decimal_" + name
	}
	return e.scalar(name, &result.Typ, args...), nil
}

func (e *exporter) exactNumericLiteralCast(result *planpb.Expr, call *planpb.Function) (*spb.Expression, bool, error) {
	id, overload := function.DecodeOverloadID(call.Func.Obj)
	if id != function.CAST || overload != 0 || len(call.Args) != 2 || call.Args[0] == nil || call.Args[1] == nil || call.Args[1].GetT() == nil || !sameDecimalDescriptor(&call.Args[1].Typ, &result.Typ) {
		return nil, false, nil
	}
	source := call.Args[0].GetLit()
	if source == nil || source.IsBin || source.IsSerialized {
		return nil, false, nil
	}
	declared, err := e.substraitType(&result.Typ)
	if err != nil {
		return nil, true, err
	}
	if source.Isnull {
		literal, err := e.literal(source, &result.Typ)
		return literal, true, err
	}
	var coefficient types.Decimal256
	if value, ok := source.Value.(*planpb.Literal_Sval); ok {
		if !source.DecimalLiteralRequiresV82 || types.T(call.Args[0].Typ.Id) != types.T_varchar || len(value.Sval) > MaxPlanBytes {
			return nil, false, nil
		}
		coefficient, err = types.ParseDecimal256(value.Sval, result.Typ.Width, result.Typ.Scale)
		if err != nil {
			return nil, true, moerr.NewInvalidInputNoCtx("invalid MO exact numeric literal")
		}
	} else {
		var value int64
		var bits int
		switch literal := source.Value.(type) {
		case *planpb.Literal_I8Val:
			if types.T(call.Args[0].Typ.Id) == types.T_int8 {
				value, bits = int64(literal.I8Val), 8
			}
		case *planpb.Literal_I16Val:
			if types.T(call.Args[0].Typ.Id) == types.T_int16 {
				value, bits = int64(literal.I16Val), 16
			}
		case *planpb.Literal_I32Val:
			if types.T(call.Args[0].Typ.Id) == types.T_int32 {
				value, bits = int64(literal.I32Val), 32
			}
		case *planpb.Literal_I64Val:
			if types.T(call.Args[0].Typ.Id) == types.T_int64 {
				value, bits = literal.I64Val, 64
			}
		}
		if bits == 0 {
			return nil, false, nil
		}
		if bits < 64 && (value < -(int64(1)<<(bits-1)) || value >= int64(1)<<(bits-1)) {
			return nil, true, moerr.NewInternalErrorNoCtx("substrait: integer literal carrier mismatch")
		}
		coefficient.B0_63 = uint64(value)
		if value < 0 {
			coefficient.B64_127, coefficient.B128_191, coefficient.B192_255 = ^uint64(0), ^uint64(0), ^uint64(0)
		}
		coefficient, err = coefficient.Scale(result.Typ.Scale)
		if err != nil {
			return nil, false, nil
		}
		limit, err := (types.Decimal256{B0_63: 1}).Scale(result.Typ.Width)
		if err != nil || (!coefficient.Sign() && !coefficient.Less(limit)) || (coefficient.Sign() && !limit.Minus().Less(coefficient)) {
			return nil, false, nil
		}
		// A proven constant becomes literal data, not a runtime cast whose
		// target would have to contain the entire signed source domain.
	}
	data := make([]byte, exactDecimalBits(&result.Typ)/8)
	for i, word := range []uint64{coefficient.B0_63, coefficient.B64_127, coefficient.B128_191, coefficient.B192_255} {
		if i*8 >= len(data) {
			break
		}
		binary.LittleEndian.PutUint64(data[i*8:], word)
	}
	return exactCoefficientLiteral(data, declared, !result.Typ.NotNullable), true, nil
}
