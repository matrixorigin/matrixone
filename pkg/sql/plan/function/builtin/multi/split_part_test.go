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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSplitPart(t *testing.T) {
	var (
		proc         = testutil.NewProc()
		mp           = testutil.TestUtilMp
		v1Scalar     = testutil.NewStringVector(1, types.T_varchar.ToType(), mp, false, []string{"a,b,c"})
		v1           = testutil.NewStringVector(10, types.T_varchar.ToType(), mp, false, []string{"a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c", "a,b,c"})
		v2Scalar     = testutil.NewStringVector(1, types.T_varchar.ToType(), mp, false, []string{","})
		v2           = testutil.NewStringVector(10, types.T_varchar.ToType(), mp, false, []string{",", ",", ",", ",", ",", ",", ",", ",", ",", ","})
		v3Scalar     = testutil.NewUInt32Vector(1, types.T_uint32.ToType(), mp, false, []uint32{1})
		v3           = testutil.NewUInt32Vector(10, types.T_uint32.ToType(), mp, false, []uint32{1, 1, 1, 1, 1, 1, 1, 1, 1, 1})
		v1ScalarNull = testutil.NewStringVector(1, types.T_varchar.ToType(), mp, true, []string{"a,b,c"})
		v2ScalarNull = testutil.NewStringVector(1, types.T_varchar.ToType(), mp, true, []string{","})
		v3ScalarNull = testutil.NewUInt32Vector(1, types.T_uint32.ToType(), mp, true, []uint32{1})
	)
	v1Scalar.MakeScalar(1)
	v2Scalar.MakeScalar(1)
	v3Scalar.MakeScalar(1)
	v1ScalarNull.MakeScalar(1)
	v1ScalarNull.Nsp.Set(0)
	v2ScalarNull.MakeScalar(1)
	v2ScalarNull.Nsp.Set(0)
	v3ScalarNull.MakeScalar(1)
	v3ScalarNull.Nsp.Set(0)
	_, err := testSplitPart([]*vector.Vector{v1, v2, v3}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1, v2, v3Scalar}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1, v2Scalar, v3}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1, v2Scalar, v3Scalar}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1Scalar, v2, v3}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1Scalar, v2, v3Scalar}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1Scalar, v2Scalar, v3}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1Scalar, v2Scalar, v3Scalar}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1ScalarNull, v2, v3}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1ScalarNull, v2, v3Scalar}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1ScalarNull, v2Scalar, v3}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1ScalarNull, v2Scalar, v3Scalar}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1, v2ScalarNull, v3}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1, v2ScalarNull, v3Scalar}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1, v2, v3ScalarNull}, proc)
	require.NoError(t, err)
	_, err = testSplitPart([]*vector.Vector{v1ScalarNull, v2ScalarNull, v3ScalarNull}, proc)

	// error case
	vector.AppendString(v1, []string{"a,b,c"}, mp)
	vector.AppendString(v2, []string{","}, mp)
	vector.AppendFixed[uint32](v3, []uint32{0}, mp)
	_, err = testSplitPart([]*vector.Vector{v1, v2, v3}, proc)
	require.Error(t, err)
}

func testSplitPart(vec []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return SplitPart(vec, proc)
}
