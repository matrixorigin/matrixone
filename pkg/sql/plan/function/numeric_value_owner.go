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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// NumericValueProgramCastOwner resolves a CAST9 whose value and BOOL inputs
// are the two outputs of the same lexical scalar-value program. It returns
// the program's original value and optional source-kind expressions so plan
// analysis and runtime metadata evaluation can reason about the same owner.
// Physical value/kind columns and standalone program refs are not lexical
// owners and are rejected here.
func NumericValueProgramCastOwner(expr *planpb.Expr) (source, sourceKind *planpb.Expr, ok bool) {
	fn, ok := numericValueCastFunction(expr)
	if !ok {
		return nil, nil, false
	}
	return NumericValueProgramPairSourceOwner(fn.Args[0], fn.Args[2])
}

// NumericValueProgramPairSourceOwner validates sibling slot-zero and slot-one
// references to one lexical program and returns its raw source and optional
// source-kind expression. It supports metadata analysis of non-CAST9 owners;
// callers must keep the executable value and BOOL refs intact.
func NumericValueProgramPairSourceOwner(value, kind *planpb.Expr) (source, sourceKind *planpb.Expr, ok bool) {
	if value == nil || kind == nil {
		return nil, nil, false
	}
	valueFn := value.GetF()
	kindFn := kind.GetF()
	if valueFn == nil || valueFn.Func == nil || kindFn == nil || kindFn.Func == nil {
		return nil, nil, false
	}
	valueID, valueOverload := DecodeOverloadID(valueFn.Func.GetObj())
	kindID, kindOverload := DecodeOverloadID(kindFn.Func.GetObj())
	valueArgs, kindArgs := valueFn.Args, kindFn.Args
	if valueID != SCALAR_PROGRAM_REF || valueOverload != 0 || kindID != SCALAR_PROGRAM_REF || kindOverload != 0 ||
		len(valueArgs) != 3 || len(kindArgs) != 2 || valueArgs[0] == nil ||
		valueArgs[1] == nil || valueArgs[2] == nil || kindArgs[0] == nil || kindArgs[1] == nil {
		return nil, nil, false
	}
	valueScope, valueScopeOK := scalarNumericProgramInt64(valueArgs[1])
	valueSlot, valueSlotOK := scalarNumericProgramInt64(valueArgs[2])
	kindScope, kindScopeOK := scalarNumericProgramInt64(kindArgs[0])
	kindSlot, kindSlotOK := scalarNumericProgramInt64(kindArgs[1])
	if !valueScopeOK || !valueSlotOK || !kindScopeOK || !kindSlotOK ||
		valueScope <= 0 || valueScope != kindScope || valueSlot != 0 || kindSlot != 1 {
		return nil, nil, false
	}

	definition := valueArgs[0].GetF()
	if definition == nil || definition.Func == nil || len(definition.Args) < 2 || len(definition.Args) > 3 ||
		definition.Args[0] == nil || definition.Args[1] == nil {
		return nil, nil, false
	}
	definitionID, definitionOverload := DecodeOverloadID(definition.Func.GetObj())
	definitionScope, definitionScopeOK := scalarNumericProgramInt64(definition.Args[0])
	if definitionID != SCALAR_PROGRAM || definitionOverload != 0 || !definitionScopeOK || definitionScope != valueScope ||
		(types.T(value.Typ.Id) != types.T(definition.Args[1].Typ.Id) ||
			value.Typ.Width != definition.Args[1].Typ.Width ||
			value.Typ.Scale != definition.Args[1].Typ.Scale) {
		return nil, nil, false
	}
	if len(definition.Args) == 3 {
		if definition.Args[2] == nil || types.T(definition.Args[2].Typ.Id) != types.T_bool {
			return nil, nil, false
		}
		sourceKind = definition.Args[2]
	}
	return definition.Args[1], sourceKind, true
}

// NumericValueCastPhysicalColumns recognizes the relational form emitted when
// a producer has materialized the value and its BOOL kind as ordinary columns.
// It deliberately does not treat standalone program refs or arbitrary
// expressions as relational owners.
func NumericValueCastPhysicalColumns(expr *planpb.Expr) (value, kind *planpb.Expr, ok bool) {
	fn, ok := numericValueCastFunction(expr)
	if !ok {
		return nil, nil, false
	}
	value, kind = fn.Args[0], fn.Args[2]
	if value.GetCol() == nil || kind.GetCol() == nil {
		return nil, nil, false
	}
	return value, kind, true
}

func numericValueCastFunction(expr *planpb.Expr) (*planpb.Function, bool) {
	if expr == nil || expr.GetF() == nil || expr.GetF().GetFunc() == nil {
		return nil, false
	}
	fn := expr.GetF()
	fid, overload := DecodeOverloadID(fn.Func.GetObj())
	if fid != CAST || overload != NumericValueCastOverload || len(fn.Args) != 4 ||
		fn.Args[0] == nil || fn.Args[1] == nil || fn.Args[2] == nil || fn.Args[3] == nil ||
		fn.Args[1].GetT() == nil || types.T(fn.Args[2].Typ.Id) != types.T_bool {
		return nil, false
	}
	policy := fn.Args[3].GetLit()
	if policy == nil || policy.Isnull {
		return nil, false
	}
	policyValue, policyOK := policy.Value.(*planpb.Literal_I32Val)
	if !policyOK || policyValue.I32Val < NumericValueCastPolicyOrdinary ||
		policyValue.I32Val > NumericValueCastPolicySetOperation {
		return nil, false
	}
	return fn, true
}

func scalarNumericProgramInt64(expr *planpb.Expr) (int64, bool) {
	if expr == nil || expr.GetLit() == nil || expr.GetLit().Isnull {
		return 0, false
	}
	value, ok := expr.GetLit().Value.(*planpb.Literal_I64Val)
	if !ok || value == nil {
		return 0, false
	}
	return value.I64Val, true
}
