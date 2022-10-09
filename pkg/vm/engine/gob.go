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

package engine

import (
	"encoding/gob"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func init() {

	// register TableDef types
	gob.Register(new(ViewDef))
	gob.Register(new(CommentDef))
	gob.Register(new(PartitionDef))
	gob.Register(new(AttributeDef))
	gob.Register(new(IndexTableDef))
	gob.Register(new(PropertiesDef))
	gob.Register(new(PrimaryIndexDef))

	// register vector column types
	gob.Register([]bool{})
	gob.Register([]int8{})
	gob.Register([]int16{})
	gob.Register([]int32{})
	gob.Register([]int64{})
	gob.Register([]uint8{})
	gob.Register([]uint16{})
	gob.Register([]uint32{})
	gob.Register([]uint64{})
	gob.Register([]float32{})
	gob.Register([]float64{})
	gob.Register([]string{})
	gob.Register([][]any{})
	gob.Register([]types.Type{})
	gob.Register([]types.Date{})
	gob.Register([]types.Datetime{})
	gob.Register([]types.Timestamp{})
	gob.Register([]types.Decimal64{})
	gob.Register([]types.Decimal128{})

	// plan types
	gob.Register(&plan.Expr_C{})
	gob.Register(&plan.Expr_P{})
	gob.Register(&plan.Expr_V{})
	gob.Register(&plan.Expr_Col{})
	gob.Register(&plan.Expr_F{})
	gob.Register(&plan.Expr_Sub{})
	gob.Register(&plan.Expr_Corr{})
	gob.Register(&plan.Expr_T{})
	gob.Register(&plan.Expr_List{})
	gob.Register(&plan.Const_Ival{})
	gob.Register(&plan.Const_Dval{})
	gob.Register(&plan.Const_Sval{})
	gob.Register(&plan.Const_Bval{})
	gob.Register(&plan.Const_Uval{})
	gob.Register(&plan.Const_Fval{})
	gob.Register(&plan.Const_Dateval{})
	gob.Register(&plan.Const_Datetimeval{})
	gob.Register(&plan.Const_Decimal64Val{})
	gob.Register(&plan.Const_Decimal128Val{})
	gob.Register(&plan.Const_Timestampval{})
	gob.Register(&plan.Const_Jsonval{})
	gob.Register(&plan.Const_Defaultval{})
	gob.Register(&plan.Default{})
}
