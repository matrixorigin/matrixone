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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"slices"
)

func DeepCopyCorrelation(spec *plan.CorrelationSpec) *plan.CorrelationSpec {
	if spec == nil {
		return nil
	}
	result := *spec
	result.StepOwners = slices.Clone(spec.StepOwners)
	result.Programs = make([]*plan.CorrelationProgram, len(spec.Programs))
	for i, p := range spec.Programs {
		if p == nil {
			continue
		}
		copy := *p
		copy.InputTypes = copyCorrelationTypes(p.InputTypes)
		copy.OutputTypes = copyCorrelationTypes(p.OutputTypes)
		result.Programs[i] = &copy
	}
	result.Stages = make([]*plan.CorrelationStage, len(spec.Stages))
	for i, s := range spec.Stages {
		if s != nil {
			copy := *s
			result.Stages[i] = &copy
		}
	}
	return &result
}

func copyCorrelationTypes(types []plan.Type) []plan.Type {
	if types == nil {
		return nil
	}
	result := make([]plan.Type, len(types))
	for i := range types {
		result[i] = *DeepCopyType(&types[i])
	}
	return result
}
