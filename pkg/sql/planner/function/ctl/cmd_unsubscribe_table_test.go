// Copyright 2021 - 2024 Matrix Origin
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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func Test_doUnsubscribeTable(t *testing.T) {

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("", rt)
	//ctx, cancel := context.WithCancel(context.TODO())
	mc := clusterservice.NewMOCluster(
		"",
		nil,
		3*time.Second,
		clusterservice.WithDisableRefresh(),
		clusterservice.WithServices(
			[]metadata.CNService{
				{
					ServiceID: "mock",
					//LockServiceAddress: testSockets,
				},
			}, nil,
		),
	)
	defer mc.Close()
	rt.SetGlobalVariables(runtime.ClusterService, mc)

	proc := testutil.NewProc(t)
	proc.Base.QueryClient = &testQClient{}
	err := doUnsubscribeTable(
		proc,
		"mock",
		3, 10)
	assert.NoError(t, err)
}
