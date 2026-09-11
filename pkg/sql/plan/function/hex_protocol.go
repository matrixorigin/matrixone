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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const (
	// HexMySQLNumericOverloadStart separates the pre-MORPCVersion64 HEX
	// identities from corrected numeric identities. Keep IDs 0..7 and their
	// executors stable for plans compiled by older versions.
	HexMySQLNumericOverloadStart = 8
	HexFloat32Overload           = 11
	HexFloat64Overload           = 12
	HexExplicitFloat32Overload   = 13
	HexExplicitFloat64Overload   = 14
)

// MigrateLegacyHexOverload upgrades one persisted HEX expression after every CN
// supports MORPCVersion64. It mutates only the execution-plan copy supplied by
// the compiler; catalog protobufs remain unchanged.
func MigrateLegacyHexOverload(expr *plan.Expr) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 1 {
		return
	}
	functionID, overloadID := DecodeOverloadID(fn.Func.Obj)
	if functionID != HEX || (overloadID != 4 && overloadID != 5) {
		return
	}

	arg := fn.Args[0]
	if arg == nil {
		return
	}
	if cast := arg.GetF(); cast != nil && cast.Func != nil && cast.Func.ObjName == "cast" && len(cast.Args) == 1 {
		_, castOverload := DecodeOverloadID(cast.Func.Obj)
		if castOverload != 0 || cast.SyntaxExplicitCast {
			if overloadID == 4 {
				fn.Func.Obj = EncodeOverloadID(HEX, HexExplicitFloat32Overload)
			} else {
				fn.Func.Obj = EncodeOverloadID(HEX, HexExplicitFloat64Overload)
			}
			return
		}
		if decimalOverload, ok := hexDecimalOverload(types.T(cast.Args[0].Typ.Id)); ok {
			fn.Args[0] = cast.Args[0]
			fn.Func.Obj = EncodeOverloadID(HEX, decimalOverload)
			return
		}
	}
	if decimalOverload, ok := hexDecimalOverload(types.T(arg.Typ.Id)); ok {
		fn.Func.Obj = EncodeOverloadID(HEX, decimalOverload)
		return
	}
	if overloadID == 4 {
		fn.Func.Obj = EncodeOverloadID(HEX, HexFloat32Overload)
	} else {
		fn.Func.Obj = EncodeOverloadID(HEX, HexFloat64Overload)
	}
}

func hexDecimalOverload(oid types.T) (int32, bool) {
	switch oid {
	case types.T_decimal64:
		return HexMySQLNumericOverloadStart, true
	case types.T_decimal128:
		return HexMySQLNumericOverloadStart + 1, true
	case types.T_decimal256:
		return HexMySQLNumericOverloadStart + 2, true
	default:
		return 0, false
	}
}
