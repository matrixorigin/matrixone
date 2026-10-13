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

// validateOctStringProtocol fences new OCT identities at both plan admission
// (including expressions persisted by DDL) and remote pipeline boundaries.
// Old DECIMAL128 identities remain executable during a rolling upgrade.
func validateOctStringProtocol(proc *process.Process, value any) error {
	if proc != nil {
		if rt := moruntime.ServiceRuntime(proc.GetService()); rt != nil {
			v, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			version, valid := v.(int64)
			if ok && valid && version >= defines.MORPCVersion62 {
				return nil
			}
		}
	}
	// Reuse the protobuf walker so nested CHECK/default/generated expressions
	// and child pipelines cannot bypass the same admission rule.
	if !containsFunctionInValue(reflect.ValueOf(value), nil, func(id, overload int32) bool {
		return id == function.OCT && overload >= function.OctStringOverloadStart
	}) {
		return nil
	}
	return moerr.NewNotSupportedNoCtxf("VARCHAR OCT requires all CNs to support MORPC protocol version %d", defines.MORPCVersion62)
}
