// Copyright 2023 Matrix Origin
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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type Udf struct {
	Body     string `json:"body"`
	Language string `json:"language"`
	RetType  string `json:"rettype"`
	Args     []*Arg `json:"args"`

	ArgsType []types.Type `json:"-"`
}

// Arg of Udf
type Arg struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func (u *Udf) GetPlanExpr() *plan.Expr {
	bytes, _ := json.Marshal(u)
	return &plan.Expr{
		Typ: type2PlanType(types.T_text.ToType()),
		Expr: &plan.Expr_C{
			C: &plan.Const{
				Isnull: false,
				Value: &plan.Const_Sval{
					Sval: string(bytes),
				},
			},
		},
	}
}

func (u *Udf) GetArgsPlanType() []*plan.Type {
	typ := u.GetArgsType()
	ptyp := make([]*plan.Type, len(typ))
	for i, t := range typ {
		ptyp[i] = type2PlanType(t)
	}
	return ptyp
}

func (u *Udf) GetRetPlanType() *plan.Type {
	typ := u.GetRetType()
	return type2PlanType(typ)
}

func (u *Udf) GetArgsType() []types.Type {
	return u.ArgsType
}

func (u *Udf) GetRetType() types.Type {
	return types.Types[u.RetType].ToType()
}

func type2PlanType(typ types.Type) *plan.Type {
	return &plan.Type{
		Id:    int32(typ.Oid),
		Width: typ.Width,
		Scale: typ.Scale,
	}
}

func UdfArgTypeMatch(from []types.Type, to []types.T) (bool, int) {
	sta, cost := tryToMatch(from, to)
	return sta != matchFailed, cost
}

func UdfArgTypeCast(from []types.Type, to []types.T) []types.Type {
	castType := make([]types.Type, len(from))
	for i := range castType {
		if to[i] == from[i].Oid {
			castType[i] = from[i]
		} else {
			castType[i] = to[i].ToType()
			setTargetScaleFromSource(&from[i], &castType[i])
		}
	}
	return castType
}
