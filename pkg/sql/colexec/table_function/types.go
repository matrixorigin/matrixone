// Copyright 2022 Matrix Origin
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

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Argument struct {
	ctr *container

	Rets      []*plan.ColDef
	Args      []*plan.Expr
	Attrs     []string
	Params    []byte
	Name      string
	retSchema []types.Type
}

type container struct {
	executorsForArgs []colexec.ExpressionExecutor
}

func (arg *Argument) Free(proc *process.Process, pipelineFailed bool) {
	if arg.ctr != nil {
		arg.ctr.cleanExecutors()
	}
}

func (ctr *container) cleanExecutors() {
	for i := range ctr.executorsForArgs {
		ctr.executorsForArgs[i].Free()
	}
}

type unnestParam struct {
	FilterMap map[string]struct{} `json:"filterMap"`
	ColName   string              `json:"colName"`
}

var (
	unnestDeniedFilters = []string{"col", "seq"}
	defaultFilterMap    = map[string]struct{}{
		"key":   {},
		"path":  {},
		"index": {},
		"value": {},
		"this":  {},
	}
)

const (
	unnestMode      = "both"
	unnestRecursive = false
)

type generateSeriesNumber interface {
	int32 | int64 | types.Datetime
}
