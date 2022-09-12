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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type Param struct {
	Attrs  []string
	Cols   []*plan.ColDef
	Extern *tree.UnnestParam
}

type Argument struct {
	Es *Param
}

var (
	end           = false
	seq     int32 = 0
	colName       = "UNNET_DEFAULT"
	//rows     = 0
	cols       = []string{"col", "seq", "key", "path", "index", "value", "this"}
	colDefs    []*plan.ColDef
	name2Types = map[string]types.Type{
		"col":   colTypes[0],
		"seq":   colTypes[1],
		"key":   colTypes[2],
		"path":  colTypes[3],
		"index": colTypes[4],
		"value": colTypes[5],
		"this":  colTypes[6],
	}
	colTypes = []types.Type{
		{
			Oid:   types.T_varchar,
			Width: 4,
		},
		{
			Oid: types.T_int32,
		},
		{
			Oid:   types.T_varchar,
			Width: 24,
		},
		{
			Oid:   types.T_varchar,
			Width: 24,
		},
		{
			Oid:   types.T_varchar,
			Width: 4,
		},
		{
			Oid:   types.T_varchar,
			Width: 256,
		},
		{
			Oid:   types.T_varchar,
			Width: 256,
		},
	}
)

const (
	mode      = "both"
	recursive = false
)
