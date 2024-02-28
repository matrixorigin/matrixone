package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
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
}

func (e FixedVectorExpressionExecutor) TypeName() string {
	return "FixedVectorExpressionExecutor"
}

func NewFixedVectorExpressionExecutor(m *mpool.MPool, resultVector *vector.Vector) *FixedVectorExpressionExecutor {
	fe := reuse.Alloc[FixedVectorExpressionExecutor](nil)
	fe.m = m
	fe.resultVector = resultVector
	return fe
}
