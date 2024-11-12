// Copyright 2024 Matrix Origin
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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
)

func NewMarshalPlanHandlerWithoutPlanV2(ctx context.Context, plan *plan.Plan, opts ...marshalPlanOptions) *marshalPlanHandler {
	// TODO: need mem improvement
	//uuid := uuid.UUID(stmt.StatementID)
	//stmt.MarkResponseAt()

	if plan == nil || plan.GetQuery() == nil {
		return &marshalPlanHandler{
			query:       nil,
			marshalPlan: nil,
			//stmt:        stmt,
			//uuid:        uuid,
			buffer:            nil,
			isInternalSubStmt: true,
		}
	}
	query := plan.GetQuery()
	h := &marshalPlanHandler{
		query: query,
		//stmt:   stmt,
		//uuid:   uuid,
		buffer:            nil,
		isInternalSubStmt: true,
	}

	// SET options
	for _, opt := range opts {
		opt(&h.marshalPlanConfig)
	}
	return h
}

// marshalExecPlan handles the logic of serializing the plan to JSON
// and setting related properties like plan stats and phyPlan.
func (h *marshalPlanHandler) marshalExecPlan(ctx context.Context, phyPlan *models.PhyPlan) {
	if h.needMarshalPlan() {
		h.marshalPlan = explain.BuildJsonPlan(ctx, h.uuid, &explain.MarshalPlanOptions, h.query)
		h.marshalPlan.NewPlanStats.SetWaitActiveCost(h.waitActiveCost)
		if phyPlan != nil {
			h.marshalPlan.PhyPlan = *phyPlan
		}
	}
}

//----------------------------------------------------------------------------------------------------------------------
