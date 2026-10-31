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

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// validateJSONStringConsumerProtocol applies the shared expression source fence
// to local and cached plans containing JSON-aware ELT, subtype-preserving
// binary CAST, corrected opaque text rendering, typed JSON MIN/MAX, or
// ODKU/session result contracts. CONCAT/WS keep overload 0 while their JSON
// text rendering requires the independently detected floor.
func validateJSONStringConsumerProtocol(proc *process.Process, owner any) error {
	needed, err := plan.MayRequireLocalSourceFence(owner)
	if err != nil || !needed {
		return err
	}
	features, err := plan.RequiredRemoteExpressionFeatures(owner)
	if err != nil || !features.RequiresLocalSourceFence() {
		return err
	}
	return validateRemoteExpressionFeatures(proc, features)
}
