// Copyright 2021 Matrix Origin
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

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// NewAgg generate the aggregation related struct from the function overload id.
var NewAgg func(overloadID int64, isDistinct bool, inputTypes []types.Type) (Agg[any], error)

// NewAggWithConfig generate the aggregation related struct from the function overload id and deliver a config information.
var NewAggWithConfig func(overloadID int64, isDistinct bool, inputTypes []types.Type, config any) (Agg[any], error)

// IsWinOrderFun check if the function is a window function.
var IsWinOrderFun func(overloadID int64) bool

func InitAggFramework(
	newAgg func(overloadID int64, isDistinct bool, inputTypes []types.Type) (Agg[any], error),
	newAggWithConfig func(overloadID int64, isDistinct bool, inputTypes []types.Type, config any) (Agg[any], error),
	isWinOrderFun func(overloadID int64) bool) {

	NewAgg = newAgg
	NewAggWithConfig = newAggWithConfig
	IsWinOrderFun = isWinOrderFun
}
