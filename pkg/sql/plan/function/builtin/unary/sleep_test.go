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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSleep(t *testing.T) {
	proc := testutil.NewProc()
	vs := []*vector.Vector{testutil.NewUInt64Vector(1, types.T_uint64.ToType(), proc.Mp(), false, []uint64{1})}
	_, err := Sleep[uint64](vs, proc)
	require.NoError(t, err)
	vs[0].MakeScalar(1)
	_, err = Sleep[uint64](vs, proc)
	require.NoError(t, err)
	vs[0].MakeScalar(2)
	_, err = Sleep[uint64](vs, proc)
	require.NoError(t, err)
	vs = []*vector.Vector{testutil.NewFloat64Vector(1, types.T_float64.ToType(), proc.Mp(), false, []float64{-1})}
	_, err = Sleep[float64](vs, proc)
	require.Error(t, err)
	vs[0].MakeScalar(1)
	_, err = Sleep[float64](vs, proc)
	require.Error(t, err)
}
