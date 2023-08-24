// Copyright 2021 - 2022 Matrix Origin
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

package functionAgg

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
)

const defaultSeparator = ","

func NewAggGroupConcat(overloadID int64, dist bool, inputTypes []types.Type, outputType types.Type, config any) (agg.Agg[any], error) {
	var aggPriv *agg.GroupConcat

	bytes, ok := config.([]byte)
	if ok && len(bytes) > 0 {
		aggPriv = agg.NewGroupConcat(string(bytes))
	} else {
		aggPriv = agg.NewGroupConcat(defaultSeparator)
	}

	switch inputTypes[0].Oid {
	case types.T_varchar:
		if dist {
			return agg.NewUnaryDistAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill), nil
		}
		return agg.NewUnaryAgg(overloadID, aggPriv, false, inputTypes[0], outputType, aggPriv.Grows, aggPriv.Eval, aggPriv.Merge, aggPriv.Fill, nil), nil
	}

	return nil, moerr.NewInternalErrorNoCtx("unsupported type '%s' for group_concat", inputTypes[0])
}
