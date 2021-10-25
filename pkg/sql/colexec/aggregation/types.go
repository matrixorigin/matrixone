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

package aggregation

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	// user function
	Avg = iota
	Max
	Min
	Sum
	Count
	StarCount
	// system function
	SumCount
)

var AggName = [...]string{
	Avg:       "avg",
	Max:       "max",
	Min:       "min",
	Sum:       "sum",
	Count:     "count",
	StarCount: "starCount",
	SumCount:  "sumCount",
}

type Extend struct {
	Op    int
	Name  string
	Alias string
	Agg   Aggregation
}

type Aggregation interface {
	Reset()
	Type() types.Type
	Dup() Aggregation
	Eval() interface{}
	Fill([]int64, *vector.Vector) error
	EvalCopy(*process.Process) (*vector.Vector, error)
}
