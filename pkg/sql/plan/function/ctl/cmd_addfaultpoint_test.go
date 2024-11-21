// Copyright 2023 Matrix Origin
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
	"fmt"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	qclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_CanHandleCnFaultInjection(t *testing.T) {
	var a1, a2, a3, a4 struct {
		proc      *process.Process
		service   serviceType
		parameter string
		sender    requestSender
	}
	id := uuid.New().String()
	addr := "127.0.0.1:7777"
	initRuntime([]string{id}, []string{addr})

	runtime.SetupServiceBasedRuntime(id, runtime.ServiceRuntime(""))

	cli, err := qclient.NewQueryClient(id, morpc.Config{})
	require.Nil(t, err)

	proc := new(process.Process)
	proc.Base = &process.BaseProcess{}
	proc.Base.QueryClient = cli

	// testing query with wrong serviceType
	a1.proc = proc
	a1.service = "log"
	ret, err := handleAddFaultPoint(a1.proc, a1.service, a1.parameter, a1.sender)
	require.Equal(t, ret, Result{})
	require.Equal(t, err, moerr.NewWrongServiceNoCtx("CN or DN", string(a1.service)))

	// testing query with tn cmd
	a2.proc = proc
	a2.service = tn
	a2.parameter = "test.:::.echo.0."
	_, err = handleAddFaultPoint(a2.proc, a2.service, a2.parameter, a2.sender)
	require.NoError(t, err)

	// testing query with cn cmd
	a3.proc = proc
	a3.service = cn
	a3.parameter = "all.enable_fault_injection"
	ret, err = handleAddFaultPoint(a3.proc, a3.service, a3.parameter, a3.sender)
	require.NoError(t, err)
	require.Equal(t, ret, Result{
		Method: AddFaultPointMethod,
		Data:   fmt.Sprintf("%v:OK; ", id),
	})

	a4.proc = proc
	a4.service = cn
	a4.parameter = "all.test.:::.echo.0."
	ret, err = handleAddFaultPoint(a4.proc, a4.service, a4.parameter, a4.sender)
	require.NoError(t, err)
	require.Equal(t, ret, Result{
		Method: AddFaultPointMethod,
		Data:   fmt.Sprintf("%v:OK; ", id),
	})

}
