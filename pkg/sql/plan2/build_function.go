package plan2

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func checkFloatType(typ plan.Type_TypeId, alias string) error {
	switch typ {
	case plan.Type_FLOAT32:
		fallthrough
	case plan.Type_FLOAT64:
		return nil
	default:
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not float", alias))
	}
}

func checkIntType(typ plan.Type_TypeId, alias string) error {
	switch typ {
	case plan.Type_INT8:
		fallthrough
	case plan.Type_INT16:
		fallthrough
	case plan.Type_INT32:
		fallthrough
	case plan.Type_INT64:
		fallthrough
	case plan.Type_INT128:
		fallthrough
	case plan.Type_UINT8:
		fallthrough
	case plan.Type_UINT16:
		fallthrough
	case plan.Type_UINT32:
		fallthrough
	case plan.Type_UINT64:
		fallthrough
	case plan.Type_UINT128:
		return nil
	default:
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not int", alias))
	}
}

func checkDecimalType(typ plan.Type_TypeId, alias string) error {
	switch typ {
	case plan.Type_DECIMAL:
		fallthrough
	case plan.Type_DECIMAL64:
		fallthrough
	case plan.Type_DECIMAL128:
		return nil
	default:
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not decimal", alias))
	}
}

func checkNumberType(typ plan.Type_TypeId, alias string) error {
	err := checkIntType(typ, alias)
	if err == nil {
		return nil
	}
	err = checkFloatType(typ, alias)
	if err == nil {
		return nil
	}
	err = checkDecimalType(typ, alias)
	if err != nil {
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not number", alias))
	}
	return nil
}

func checkTimeType(typ plan.Type_TypeId, alias string) error {
	switch typ {
	case plan.Type_DATE:
		fallthrough
	case plan.Type_TIME:
		fallthrough
	case plan.Type_DATETIME:
		fallthrough
	case plan.Type_TIMESTAMP:
		fallthrough
	case plan.Type_INTERVAL:
		return nil
	default:
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not time", alias))
	}
}

func checkFunctionArgs(fun *FunctionSig, args []*plan.Expr) error {
	for idx, typ := range fun.ArgType {
		argType := fun.ArgTypeClass[typ]
		switch argType {
		case plan.Type_ANY:
			//do nothing
		case plan.Type_ANYFLOAT:
			return checkFloatType(args[idx].Typ.Id, args[idx].Alias)
		case plan.Type_ANYINT:
			return checkIntType(args[idx].Typ.Id, args[idx].Alias)
		case plan.Type_ANYNUMBER:
			return checkNumberType(args[idx].Typ.Id, args[idx].Alias)
		case plan.Type_ANYTIME:
			return checkTimeType(args[idx].Typ.Id, args[idx].Alias)
		default:
			if argType != args[idx].Typ.Id {
				return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("type error: arg '%v' is not %v", args[idx].Alias, plan.Type_TypeId_name[int32(argType)]))
			}
		}
	}
	return nil
}

func coverArgToHigh(args []*plan.Expr) error {
	leftTypeId := args[0].Typ.Id
	rightTypeId := args[1].Typ.Id

	// _, leftIsConstant := args[0].Expr.(*plan.Expr_C)
	// _, rightIsConstant := args[1].Expr.(*plan.Expr_C)

	leftIsInt := checkIntType(leftTypeId, "") == nil
	rightIsInt := checkFloatType(rightTypeId, "") == nil
	leftIsFloat := checkFloatType(leftTypeId, "") == nil
	rightIsFloat := checkFloatType(rightTypeId, "") == nil
	leftIsNumber := checkNumberType(leftTypeId, "") == nil
	rightIsNumber := checkNumberType(rightTypeId, "") == nil

	switch {
	case leftIsInt && rightIsInt:
		//fixme need compare, return f64 now, next pr to fixed
		args[0].Typ.Id = plan.Type_INT64
		args[1].Typ.Id = plan.Type_INT64
	case leftIsFloat && rightIsFloat:
		//fixme need compare, return f64 now, next pr to fixed
		args[0].Typ.Id = plan.Type_FLOAT64
		args[1].Typ.Id = plan.Type_FLOAT64
	case leftIsInt && rightIsFloat:
		//fixme cover left to float in next pr
		args[0].Typ.Id = plan.Type_FLOAT64
		args[1].Typ.Id = plan.Type_FLOAT64
	case leftIsFloat && leftIsInt:
		//fixme cover right to float in next pr
		args[0].Typ.Id = plan.Type_FLOAT64
		args[1].Typ.Id = plan.Type_FLOAT64
	case leftIsNumber && !rightIsNumber:
		//fixme rewrite right to null? in next pr
		//selct (int_col1 / string_col1) a from tbl   MySQL will return null
	case !leftIsNumber && rightIsNumber:
		//fixme rewrite left to null? in next pr
		//selct (int_col1 / string_col1) a from tbl   MySQL will return null
	}

	return nil
}

func covertFunctionArgsType(fun *FunctionSig, args []*plan.Expr) error {
	switch fun.Name {
	case "+":
		fallthrough
	case "-":
		fallthrough
	case "*":
		fallthrough
	case "/":
		fallthrough
	case "%":
		return coverArgToHigh(args)
	default:
		return nil
	}
}

func getFunctionReturnType(fun *FunctionSig, args []*plan.Expr) *plan.Type {
	returnType := fun.ArgTypeClass[0]

	switch returnType {
	case plan.Type_ANYINT:
		fallthrough
	case plan.Type_ANYFLOAT:
		fallthrough
	case plan.Type_ANYNUMBER:
		return args[0].Typ
	default:
		//todo confirm nullable/with/precision
		return &plan.Type{
			Id: returnType,
		}
	}
}

func getFunctionExprByNameAndExprs(name string, exprs []tree.Expr, ctx CompilerContext, query *Query, SelectCtx *SelectContext) (*plan.Expr, error) {
	//Get function
	functionSig, ok := BuiltinFunctionsMap[name]
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("function name '%v' is not exist", name))
	}

	//Check parameters length
	if len(functionSig.ArgType) != len(exprs) {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("number of parameters does not match for function '%v'", functionSig.Name))
	}

	//Get original input expr
	var args []*plan.Expr
	for _, astExpr := range exprs {
		expr, err := buildExpr(astExpr, ctx, query, SelectCtx)
		if err != nil {
			return nil, err
		}
		args = append(args, expr)
	}

	//Check input type
	err := checkFunctionArgs(functionSig, args)
	if err != nil {
		return nil, err
	}

	//Convert input parameter types if necessary
	//todo somethings we will get constant return here
	err = covertFunctionArgsType(functionSig, args)
	if err != nil {
		return nil, err
	}

	//Determine the return value type
	returnType := getFunctionReturnType(functionSig, args)

	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(name),
				Args: args,
			},
		},
		Typ: returnType,
	}, nil
}

func getFunctionObjRef(name string) *plan.ObjectRef {
	return &plan.ObjectRef{
		ObjName: name,
	}
}
