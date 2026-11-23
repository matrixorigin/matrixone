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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// isPhysicalIndex is used only for listing catalog rows. Invalid nonempty
// metadata is ambiguous, so it is hidden rather than aborting the listing.
func isPhysicalIndex(name, kind string, extraInfo []byte) bool {
	if catalog.RequiresPrivilegedRelationAccess(name, kind, 0) {
		return true
	}
	if len(extraInfo) == 0 {
		return false
	}
	var extra api.SchemaExtra
	if err := extra.Unmarshal(extraInfo); err != nil {
		return true
	}
	return catalog.RequiresPrivilegedRelationAccess(name, kind, extra.FeatureFlag)
}

func builtInIsPhysicalIndex(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	names := vector.GenerateFunctionStrParameter(parameters[0])
	kinds := vector.GenerateFunctionStrParameter(parameters[1])
	extras := vector.GenerateFunctionStrParameter(parameters[2])
	results := vector.MustFunctionResult[bool](result)
	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selectList, row) {
			if err := results.Append(false, true); err != nil {
				return err
			}
			continue
		}
		name, nullName := names.GetStrValue(row)
		kind, nullKind := kinds.GetStrValue(row)
		extra, nullExtra := extras.GetStrValue(row)
		if nullName || nullKind || nullExtra {
			if err := results.Append(false, true); err != nil {
				return err
			}
			continue
		}
		if err := results.Append(isPhysicalIndex(functionUtil.QuickBytesToStr(name), functionUtil.QuickBytesToStr(kind), extra), false); err != nil {
			return err
		}
	}
	return nil
}
