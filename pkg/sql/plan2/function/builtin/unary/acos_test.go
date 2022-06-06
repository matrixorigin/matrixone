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
