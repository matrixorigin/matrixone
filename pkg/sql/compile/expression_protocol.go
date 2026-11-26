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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// constrainRemoteExpressionWorkers rejects expression contracts that need
// rebinding, here and for local execution alike, and keeps a query whose
// mixed JSON/numeric ASOF join cannot be split on one CN.
func (c *Compile) constrainRemoteExpressionWorkers(qry *plan.Query) error {
	features, err := plan.RequiredRemoteExpressionFeatures(qry)
	if err != nil {
		return err
	}
	if features.InvalidDecimalStatisticalResultContract {
		return moerr.NewNotSupportedNoCtx("decimal statistical result contract requires rebinding")
	}
	if features.InvalidTemporalResultContract {
		return moerr.NewNotSupportedNoCtx("temporal result vector contract mismatch requires rebinding")
	}
	if features.LegacyIntervalUnits {
		return moerr.NewNotSupportedNoCtx("legacy interval unit contract requires rebinding")
	}
	if queryHasMixedJSONNumericAsof(qry) {
		// This pass runs after the first scheduling pass and before scopes
		// are compiled. Replace both execution type and the worker list.
		c.execType = plan2.ExecTypeAP_ONECN
		c.cnList, err = c.scheduleQueryWorkers()
		return err
	}
	return nil
}

// validateTemporalResultContracts rejects a plan, possibly cached, whose CTAS
// population requirement is invalid and needs rebinding, before Prepare or
// statement effects.
func validateTemporalResultContracts(owner any) error {
	if queryPlan, ok := owner.(*plan.Plan); ok {
		if create := createTableForNameAdmission(queryPlan); create != nil &&
			create.CtasExecutionProtocolVersion < 0 {
			return moerr.NewNotSupportedNoCtx("invalid CTAS population protocol requirement requires rebinding")
		}
	}
	return nil
}
