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

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type planReuseLayer uint8

const (
	ordinaryPlanGeneration planReuseLayer = iota
	preparedLogicalPlan
	preparedPhysicalCompile
)

type planReuseDecision struct {
	valid, reusable bool
}

// planReuse is the cache owner's policy for its three distinct reuse layers.
// It derives only the requested layer: physical-compile checks do not pay for
// logical metadata walks, and ordinary generation updates do not repeat the
// COM_QUERY admission checks. A fresh scalar value is not a fresh access choice.
func planReuse(schemaChanged bool, p *plan.Plan, layer planReuseLayer) planReuseDecision {
	allow := planReuseDecision{valid: true, reusable: true}
	deny := planReuseDecision{valid: true}
	switch layer {
	case ordinaryPlanGeneration:
		if p == nil {
			return planReuseDecision{}
		}
		if query, ok := p.Plan.(*plan.Plan_Query); ok {
			if query == nil || query.Query == nil {
				return planReuseDecision{}
			}
			return planReuseDecision{valid: true, reusable: !query.Query.ExecutionDependentAccess}
		}
		return allow
	case preparedLogicalPlan:
		if schemaChanged {
			return deny
		}
		if p == nil {
			return allow
		}
		if ddl := p.GetDdl(); ddl != nil {
			if create := ddl.GetCreateTable(); create != nil && (create.DatabaseDefaults != nil || create.GetTemporary()) {
				return deny
			}
			if alter := ddl.GetAlterTable(); alter != nil && alter.GetTableDef().GetIsTemporary() {
				return deny
			}
			if ddl.GetAlterDatabase() != nil || ddl.GetCreateDatabase().GetDefaults() != nil {
				return deny
			}
		}
		query := p.GetQuery()
		if query != nil && (query.GetHasForeignKeyAction() ||
			query.GetViewMetadataDependsOnUdf() || plan2.PreparedPlanDependsOnSubscriptionMetadata(p) ||
			plan2.PreparedPlanDependsOnIndexCoverage(p)) {
			return deny
		}
		return allow
	case preparedPhysicalCompile:
		if p == nil {
			return allow
		}
		// These values become immutable aggregate/window configuration during
		// physical construction; a reusable logical template is insufficient.
		if plan2.PreparedPlanHasPercentileParams(p) || plan2.PreparedPlanHasEwmParams(p) {
			return deny
		}
		query := p.GetQuery()
		if query == nil {
			return allow
		}
		for _, node := range query.GetNodes() {
			// The external snapshot is fixed during compilation and is not
			// covered by the MatrixOne schema-change timestamp.
			if node != nil && node.GetExternScan() != nil && node.GetExternScan().GetIcebergScan() != nil {
				return deny
			}
		}
		return planReuseDecision{valid: true, reusable: !query.GetHasForeignKeyAction()}
	default:
		return planReuseDecision{}
	}
}
