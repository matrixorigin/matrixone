// Copyright 2022 Matrix Origin
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

package unnest

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type Param struct {
	Attrs   []string
	Cols    []*plan.ColDef
	Extern  *tree.UnnestParam
	filters []string
	colName string
	seq     int32
	isCol   bool // use to mark the unnest args is from column in table
}

type Argument struct {
	Es *Param
}

var (
	deniedFilters = []string{"col", "seq"}
)

const (
	mode      = "both"
	recursive = false
)
