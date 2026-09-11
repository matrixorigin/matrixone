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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// validateHexMySQLNumericProtocol fences the DECIMAL and explicit-REAL-CAST
// identities at plan admission (including persisted DDL expressions), cached
// execution, and remote pipeline boundaries. Legacy HEX identities remain
// executable during a rolling upgrade.
func validateHexMySQLNumericProtocol(proc *process.Process, value any) error {
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
			v, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			version, valid := v.(int64)
			if ok && valid && version >= defines.MORPCVersion64 {
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
