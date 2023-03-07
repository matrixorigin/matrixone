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

package ctl

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/ctl"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHandlerWithServiceTypeNotSupported(t *testing.T) {
	vec1 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec1, []byte("not support service"), false, mpool.MustNewZero()))

	vec2 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec2, []byte("ping"), false, mpool.MustNewZero()))

	vec3 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec3, []byte(""), false, mpool.MustNewZero()))

	proc := testutil.NewProcess()
	_, err := Handler([]*vector.Vector{vec1, vec2, vec3}, proc)
	require.Error(t, err)
}

func TestHandlerWithCommandNotSupported(t *testing.T) {
	vec1 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec1, []byte("dn"), false, mpool.MustNewZero()))

	vec2 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec2, []byte("not supported command"), false, mpool.MustNewZero()))

	vec3 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec3, []byte(""), false, mpool.MustNewZero()))

	proc := testutil.NewProcess()
	_, err := Handler([]*vector.Vector{vec1, vec2, vec3}, proc)
	require.Error(t, err)
}

func TestHandler(t *testing.T) {
	vec1 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec1, []byte("dn"), false, mpool.MustNewZero()))

	vec2 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec2, []byte("test_cmd"), false, mpool.MustNewZero()))

	vec3 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec3, []byte(""), false, mpool.MustNewZero()))
	proc := testutil.NewProcess()
	supportedCmds[strings.ToUpper("test_cmd")] = func(proc *process.Process,
		service serviceType,
		parameter string,
		sender requestSender) (pb.CtlResult, error) {
		return pb.CtlResult{}, nil
	}

	vec, err := Handler([]*vector.Vector{vec1, vec2, vec3}, proc)
	require.NoError(t, err)
	require.NotNil(t, vec)
}
