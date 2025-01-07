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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestHandleMoTableStatsCtl(t *testing.T) {
	qc, err := client.NewQueryClient("", morpc.Config{})
	require.NoError(t, err)

	proc := testutil.NewProc(testutil.WithQueryClient(qc))

	result, err := handleMoTableStats(proc, tn, "xxx", nil)
	require.Equal(t, Result{}, result)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrWrongService))

	runtime.ServiceRuntime("").SetGlobalVariables(runtime.ClusterService, new(mockCluster))

	result, err = handleMoTableStats(proc, cn, "A:B", nil)
	require.NoError(t, err)
	require.Equal(t, result, Result{
		Method: MoTableStats,
		Data: map[string]string{
			"not exist": "internal error: invalid CN query address ",
		},
	})
}

var _ clusterservice.MOCluster = new(mockCluster)

type mockCluster struct {
}

func (m mockCluster) GetCNService(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
	apply(metadata.CNService{
		ServiceID: "not exist",
	})
}

func (m mockCluster) GetTNService(selector clusterservice.Selector, apply func(metadata.TNService) bool) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) GetAllTNServices() []metadata.TNService {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) GetCNServiceWithoutWorkingState(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) ForceRefresh(sync bool) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Close() {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) DebugUpdateCNLabel(uuid string, kvs map[string][]string) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) DebugUpdateCNWorkState(uuid string, state int) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) RemoveCN(id string) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) AddCN(service metadata.CNService) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) UpdateCN(service metadata.CNService) {
	//TODO implement me
	panic("implement me")
}
