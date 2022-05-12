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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func initAggregateFunction() {
	var err error

	for name, fs := range aggregates {
		for _, f := range fs {
			err = appendFunction(name, f)
			if err != nil {
				panic(err)
			}
		}
	}
}

var aggregates = map[string][]Function{
	"max": {
		{
			Index:         0,
			Flag:          plan.Function_AGG,
			Kind:          STANDARD_FUNCTION,
			Args:          []types.T{types.T_int64},
			TypeCheckFn:   strictTypeCheck,
			ReturnTyp:     types.T_int64,
			AggregateInfo: nil, // ring.intRing and ring.type is int64, retTyp is int64
		},
	},
}
