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

package aggregate

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

const (
	Sum = iota
	Avg
	Max
	Min
	Count
	StarCount
	ApproxCountDistinct
	Variance
	BitAnd
	BitXor
	BitOr
	StdDevPop
	AnyValue
)

var Names = [...]string{
	Sum:                 "sum",
	Avg:                 "avg",
	Max:                 "max",
	Min:                 "min",
	Count:               "count",
	StarCount:           "starcount",
	ApproxCountDistinct: "approx_count_distinct",
	Variance:            "var",
	BitAnd:              "bit_and",
	BitXor:              "bit_xor",
	BitOr:               "bit_or",
	StdDevPop:           "stddev_pop",
	AnyValue:            "any",
}

type Aggregate struct {
	Op   int
	Dist bool
	E    *plan.Expr
}
