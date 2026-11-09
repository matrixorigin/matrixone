// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

type nativeVisibilityCluster struct{ clusterservice.MOCluster }

func (nativeVisibilityCluster) GetAllTNServices() []metadata.TNService { return nil }

func TestReadOnlyNativeCompilePreservesCatalogReadDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	op, closeClient := client.NewTestTxnOperator(t.Context())
	defer closeClient()
	rt := moruntime.ServiceRuntime("")
	old, had := rt.GetGlobalVariables(moruntime.ClusterService)
	cluster := &nativeVisibilityCluster{}
	rt.SetGlobalVariables(moruntime.ClusterService, cluster)
	defer func() {
		if had {
			rt.SetGlobalVariables(moruntime.ClusterService, old)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.ClusterService, cluster)
		}
	}()
	workspace := disttae.NewTxnWorkSpace(&disttae.Engine{}, proc)
	require.True(t, workspace.Readonly())
	op.AddWorkspace(workspace)
	proc.Base.TxnOperator = op
	reader := op.(interface {
		TryCatalogReadStamp() (client.CatalogReadStamp, bool)
	})
	stamp, ok := reader.TryCatalogReadStamp()
	require.True(t, ok)
	revision, stable := workspace.CatalogVisibility()
	require.True(t, stable)
	c := NewCompile("", "", "select 1", "", "", nil, proc, nil, false, nil, time.Now())
	defer c.Release()
	after, stable := workspace.CatalogVisibility()
	require.True(t, stable)
	require.Equal(t, revision, after, "Compile's unchanged empty boundary must preserve the native source read domain")
	current, ok := reader.TryCatalogReadStamp()
	require.True(t, ok)
	require.Equal(t, stamp, current)
	require.Zero(t, c.TxnOffset)
}
