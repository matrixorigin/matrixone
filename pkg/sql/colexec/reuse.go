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

func (e FixedVectorExpressionExecutor) TypeName() string {
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

func (e ColumnExpressionExecutor) TypeName() string {
	return "ColumnExpressionExecutor"
}

func NewColumnExpressionExecutor() *ColumnExpressionExecutor {
	ce := reuse.Alloc[ColumnExpressionExecutor](nil)
	return ce
}

func (e ParamExpressionExecutor) TypeName() string {
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

func (e VarExpressionExecutor) TypeName() string {
	return "VarExpressionExecutor"
}

func NewVarExpressionExecutor() *VarExpressionExecutor {
	ve := reuse.Alloc[VarExpressionExecutor](nil)
	return ve
}

func (e FunctionExpressionExecutor) TypeName() string {
	return "FunctionExpressionExecutor"
}

func NewFunctionExpressionExecutor() *FunctionExpressionExecutor {
	fe := reuse.Alloc[FunctionExpressionExecutor](nil)
	return fe
}
