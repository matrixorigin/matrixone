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

package function

import (
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func buildInM0ExplainPhy(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	return buildInM0ExplainPhyWithCfg(parameters, result, proc, length, nil)
}

func buildInM0ExplainPhyWithCfg(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, cfg *config.OBCUConfig) error {
	var phyplan string
	var planData models.ExplainData
	rs := vector.MustFunctionResult[types.Varlena](result)

	p1 := vector.GenerateFunctionStrParameter(parameters[0])
	p2 := vector.GenerateFunctionStrParameter(parameters[1])

	for i := uint64(0); i < uint64(length); i++ {
		planJson, null1 := p1.GetStrValue(i) /* stats json array */
		target, null2 := p2.GetStrValue(i)   /* duration_ns */

		if null1 || null2 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		if len(planJson) == 0 {
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}

		if err := json.Unmarshal(planJson, &planData); err != nil {
			if err = rs.AppendBytes(nil, true); err != nil {
				return err
			}
			//return moerr.NewInternalError(proc.Ctx, "failed to parse json arr: %v", err)

			if len(planData.PhyPlan.LocalScope) == 0 {
				if err := rs.AppendBytes(nil, true); err != nil {
					return err
				}
				continue
			}
		}
		format := util.UnsafeBytesToString(target)
		switch strings.ToLower(format) {
		case "normal":
			phyplan = models.ExplainPhyPlan(&planData.PhyPlan)
		case "info":
			phyplan = models.ExplainPhyPlan(&planData.PhyPlan)
		case "analyze":
			phyplan = models.ExplainPhyPlan(&planData.PhyPlan)
		default:
			if err := rs.AppendBytes(nil, true); err != nil {
				return err
			}
			continue
		}
		if err := rs.AppendBytes(functionUtil.QuickStrToBytes(phyplan), false); err != nil {
			return err
		}
	}
	return nil
}
