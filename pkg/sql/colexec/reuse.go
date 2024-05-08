// Copyright 2024 Matrix Origin
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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func init() {

	reuse.CreatePool[FixedVectorExpressionExecutor](
		func() *FixedVectorExpressionExecutor {
			return &FixedVectorExpressionExecutor{}
		},
		func(s *FixedVectorExpressionExecutor) { *s = FixedVectorExpressionExecutor{} },
		reuse.DefaultOptions[FixedVectorExpressionExecutor]().
			WithEnableChecker(),
	)

	reuse.CreatePool[ColumnExpressionExecutor](
		func() *ColumnExpressionExecutor {
			return &ColumnExpressionExecutor{}
		},
		func(s *ColumnExpressionExecutor) { *s = ColumnExpressionExecutor{} },
		reuse.DefaultOptions[ColumnExpressionExecutor]().
			WithEnableChecker(),
	)

	reuse.CreatePool[ParamExpressionExecutor](
		func() *ParamExpressionExecutor {
			return &ParamExpressionExecutor{}
		},
		func(s *ParamExpressionExecutor) { *s = ParamExpressionExecutor{} },
		reuse.DefaultOptions[ParamExpressionExecutor]().
			WithEnableChecker(),
	)

	reuse.CreatePool[VarExpressionExecutor](
		func() *VarExpressionExecutor {
			return &VarExpressionExecutor{}
		},
		func(s *VarExpressionExecutor) { *s = VarExpressionExecutor{} },
		reuse.DefaultOptions[VarExpressionExecutor]().
			WithEnableChecker(),
	)

	reuse.CreatePool[FunctionExpressionExecutor](
		func() *FunctionExpressionExecutor {
			return &FunctionExpressionExecutor{}
		},
		func(s *FunctionExpressionExecutor) { *s = FunctionExpressionExecutor{} },
		reuse.DefaultOptions[FunctionExpressionExecutor]().
			WithEnableChecker(),
	)

}

func (expr FixedVectorExpressionExecutor) TypeName() string {
	return "FixedVectorExpressionExecutor"
}

func NewFixedVectorExpressionExecutor(m *mpool.MPool, fixed bool, resultVector *vector.Vector) *FixedVectorExpressionExecutor {
	fe := reuse.Alloc[FixedVectorExpressionExecutor](nil)
	*fe = FixedVectorExpressionExecutor{
		m:            m,
		fixed:        fixed,
		resultVector: resultVector,
	}
	return fe
}

func (expr ColumnExpressionExecutor) TypeName() string {
	return "ColumnExpressionExecutor"
}

func NewColumnExpressionExecutor() *ColumnExpressionExecutor {
	ce := reuse.Alloc[ColumnExpressionExecutor](nil)
	return ce
}

func (expr ParamExpressionExecutor) TypeName() string {
	return "ParamExpressionExecutor"
}

func NewParamExpressionExecutor(mp *mpool.MPool, pos int, typ types.Type) *ParamExpressionExecutor {
	pe := reuse.Alloc[ParamExpressionExecutor](nil)
	*pe = ParamExpressionExecutor{
		mp:  mp,
		pos: pos,
		typ: typ,
	}
	return pe
}

func (expr VarExpressionExecutor) TypeName() string {
	return "VarExpressionExecutor"
}

func NewVarExpressionExecutor() *VarExpressionExecutor {
	ve := reuse.Alloc[VarExpressionExecutor](nil)
	return ve
}

func (expr FunctionExpressionExecutor) TypeName() string {
	return "FunctionExpressionExecutor"
}

func NewFunctionExpressionExecutor() *FunctionExpressionExecutor {
	fe := reuse.Alloc[FunctionExpressionExecutor](nil)
	return fe
}
