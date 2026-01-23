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

package ctl

import (
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHandleWorkspaceThreshold(t *testing.T) {
	id := uuid.New().String()
	addr := "127.0.0.1:7755"
	initRuntime([]string{id}, []string{addr})

	runtime.SetupServiceBasedRuntime(id, runtime.ServiceRuntime(""))

	cli, err := client.NewQueryClient(id, morpc.Config{})
	require.Nil(t, err)

	proc := new(process.Process)
	proc.Base = &process.BaseProcess{}
	proc.Base.QueryClient = cli

	result, err := handleWorkspaceThreshold(proc, tn, "xxx", nil)
	require.Equal(t, Result{}, result)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongService))

	runtime.ServiceRuntime("").SetGlobalVariables(runtime.ClusterService, new(mockCluster))

	result, err = handleWorkspaceThreshold(proc, cn, "2:3", nil)
	require.NoError(t, err)
	require.Equal(t, result, Result{
		Method: WorkspaceThreshold,
		Data: []Res{
			{
				PodID:    "not exist",
				ErrorStr: "internal error: invalid CN query address ",
			},
		},
	})
}
