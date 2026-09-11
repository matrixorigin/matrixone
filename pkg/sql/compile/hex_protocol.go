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

package compile

import (
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// validateHexMySQLNumericProtocol fences corrected numeric identities at plan
// admission (including persisted DDL expressions), cached execution, and remote
// pipeline boundaries. Once every CN supports version 64, it also migrates old
// execution-plan copies while leaving their catalog protobufs unchanged.
func validateHexMySQLNumericProtocol(proc *process.Process, value any) error {
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
			v, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			version, valid := v.(int64)
			if ok && valid && version >= defines.MORPCVersion64 {
				if executionPlan, isPlan := value.(*plan.Plan); isPlan {
					migrateLegacyHexInValue(reflect.ValueOf(executionPlan), nil)
				}
				return nil
			}
		}
	}
	if !containsFunctionInValue(reflect.ValueOf(value), nil, func(id, overload int32) bool {
		return id == function.HEX && overload >= function.HexMySQLNumericOverloadStart
	}) {
		return nil
	}
	return moerr.NewNotSupportedNoCtxf(
		"MySQL numeric HEX semantics require all CNs to support MORPC protocol version %d",
		defines.MORPCVersion64)
}

func migrateLegacyHexInValue(v reflect.Value, seen map[uintptr]struct{}) {
	if !v.IsValid() {
		return
	}
	if v.Kind() == reflect.Interface {
		if !v.IsNil() {
			migrateLegacyHexInValue(v.Elem(), seen)
		}
		return
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		if seen == nil {
			seen = make(map[uintptr]struct{})
		}
		ptr := v.Pointer()
		if _, ok := seen[ptr]; ok {
			return
		}
		seen[ptr] = struct{}{}
		if v.Type() == planExprPtrType {
			function.MigrateLegacyHexOverload(v.Interface().(*plan.Expr))
		}
		migrateLegacyHexInValue(v.Elem(), seen)
		return
	}

	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			migrateLegacyHexInValue(v.Index(i), seen)
		}
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			migrateLegacyHexInValue(iter.Key(), seen)
			migrateLegacyHexInValue(iter.Value(), seen)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				migrateLegacyHexInValue(v.Field(i), seen)
			}
		}
	}
}
