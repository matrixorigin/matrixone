// Copyright 2025 Matrix Origin
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

package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// minimal mock cluster for tn handler wiring
type mockCluster struct{}

func (m mockCluster) GetCNService(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
	apply(metadata.CNService{ServiceID: "not-exist"})
}
func (m mockCluster) GetTNService(selector clusterservice.Selector, apply func(metadata.TNService) bool) {
}
func (m mockCluster) GetAllTNServices() []metadata.TNService { return nil }
func (m mockCluster) GetCNServiceWithoutWorkingState(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
}
func (m mockCluster) ForceRefresh(sync bool)                               {}
func (m mockCluster) Close()                                               {}
func (m mockCluster) DebugUpdateCNLabel(string, map[string][]string) error { return nil }
func (m mockCluster) DebugUpdateCNWorkState(string, int) error             { return nil }
func (m mockCluster) RemoveCN(string)                                      {}
func (m mockCluster) AddCN(metadata.CNService)                             {}
func (m mockCluster) UpdateCN(metadata.CNService)                          {}

func Test_requestSnapshotRead_Smoke(t *testing.T) {
	ctx := context.Background()

	// register a mock cluster service to avoid panic
	runtime.ServiceRuntime("").SetGlobalVariables(runtime.ClusterService, mockCluster{})

	// build a minimal process for the table
	proc := testutil.NewProc(t)
	// set a benign account name for session info
	proc.Base.SessionInfo.Account = "sys"

	tbl := &txnTable{}
	tbl.proc.Store(proc)

	// a zero ts is acceptable for smoke test
	ts := types.BuildTS(0, 0)
	// ensure compatibility with requestSnapshotRead API which uses timestamp.Timestamp
	_ = timestamp.Timestamp{}

	_, _ = requestSnapshotRead(ctx, tbl, &ts)
}
