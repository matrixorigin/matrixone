package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan2/function/builtin"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAcosUint64(t *testing.T) {
	testCases := []builtin.TestCase{
		{
			AcosUint64,
			[]*vector.Vector{builtin.MakeUInt64Vector([]uint64{5, 1, 0})},
			[]*vector.Vector{builtin.MakeFloat64Vector([]float64{0, 0, 1.5707963267948966})},
			false,
		},
	}
	err := builtin.RunTestCaseFloat64(testCases)
	require.NoError(t, err)
}
