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

package debug

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/debug"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHandlerWithServiceTypeNotSupported(t *testing.T) {
	vec1 := vector.New(types.New(types.T_varchar, 0, 0, 0))
	require.NoError(t, vec1.Append([]byte("not support service"), false, mpool.MustNewZero()))

	vec2 := vector.New(types.New(types.T_varchar, 0, 0, 0))
	require.NoError(t, vec2.Append([]byte("ping"), false, mpool.MustNewZero()))

	vec3 := vector.New(types.New(types.T_varchar, 0, 0, 0))
	require.NoError(t, vec3.Append([]byte(""), false, mpool.MustNewZero()))

	_, err := Handler([]*vector.Vector{vec1, vec2, vec3}, nil)
	require.Error(t, err)
}

func TestHandlerWithCommandNotSupported(t *testing.T) {
	vec1 := vector.New(types.New(types.T_varchar, 0, 0, 0))
	require.NoError(t, vec1.Append([]byte("dn"), false, mpool.MustNewZero()))

	vec2 := vector.New(types.New(types.T_varchar, 0, 0, 0))
	require.NoError(t, vec2.Append([]byte("not supported command"), false, mpool.MustNewZero()))

	vec3 := vector.New(types.New(types.T_varchar, 0, 0, 0))
	require.NoError(t, vec3.Append([]byte(""), false, mpool.MustNewZero()))

	_, err := Handler([]*vector.Vector{vec1, vec2, vec3}, nil)
	require.Error(t, err)
}

func TestHandler(t *testing.T) {
	vec1 := vector.New(types.New(types.T_varchar, 0, 0, 0))
	require.NoError(t, vec1.Append([]byte("dn"), false, mpool.MustNewZero()))

	vec2 := vector.New(types.New(types.T_varchar, 0, 0, 0))
	require.NoError(t, vec2.Append([]byte("test_cmd"), false, mpool.MustNewZero()))

	vec3 := vector.New(types.New(types.T_varchar, 0, 0, 0))
	require.NoError(t, vec3.Append([]byte(""), false, mpool.MustNewZero()))

	supportedCmds[strings.ToUpper("test_cmd")] = func(ctx context.Context,
		service serviceType,
		parameter string,
		sender requestSender,
		clusterDetailsGetter engine.GetClusterDetailsFunc) (pb.DebugResult, error) {
		return pb.DebugResult{}, nil
	}

	vec, err := Handler([]*vector.Vector{vec1, vec2, vec3},
		process.New(context.Background(), mpool.MustNewZero(), nil, nil, nil, nil))
	require.NoError(t, err)
	require.NotNil(t, vec)
}
