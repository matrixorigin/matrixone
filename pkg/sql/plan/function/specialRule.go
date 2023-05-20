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
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// I want to use a structure to deal the function like `select current_timestamp(4)`
// in fact, 4 is not a parameter of function. but 4 will make the return type to be timestamp(4)
// if we only receive []types.Type,  cannot determine the return type exactly.
//
//
// rule := newSpecialRule(name, expr)
// f, err := GetFunctionByName(name, types)
// rule.Action(f)
//
// TODO: not implement now.

type FunctionSpecialRule interface {
	RetTypeAdjust(p *types.Type)
}

func NewSpecialRule(name string, expressions []*plan.Expr) (FunctionSpecialRule, error) {
	if name == "current_timestamp" && len(expressions) == 1 {
		// XXX too hard.
		return nil, moerr.NewNYI(context.TODO(), "special rule not impl.")
	}
	return &noSpecialRule{}, nil
}

type noSpecialRule struct{}

func (rule *noSpecialRule) RetTypeAdjust(t *types.Type) {}
