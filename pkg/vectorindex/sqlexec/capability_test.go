// Copyright 2026 Matrix Origin
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

package sqlexec

import (
	"context"
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// A SqlProcess carries EITHER a Proc or a SqlCtx. The CDC path -- the ONLY writer of tail frame
// rows -- carries the SqlCtx one, because RunTxnWithSqlContext builds it with Proc nil. A gate
// that reads only Proc therefore answers "not activated" for every CDC flush and silently
// withholds every row it is supposed to allow, leaving tail sizing on the chunk-count bound
// forever. Nothing fails loudly when that happens, which is why it is pinned here.
func TestClusterCapabilityReadsBothSqlProcessShapes(t *testing.T) {
	rt := moruntime.ServiceRuntime("")
	require.NotNil(t, rt)
	prev, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, prev) })

	// The SqlCtx shape, exactly as the CDC consumer builds it: no Proc at all.
	sqlCtxShape := NewSqlProcessWithContext(NewSqlContext(context.Background(), "", nil, 0, nil))
	require.Nil(t, sqlCtxShape.Proc, "this is the shape the CDC path uses")

	procShape := NewSqlProcess(testutil.NewProcess(t))
	require.NotNil(t, procShape.Proc)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion58)
	require.True(t, ClusterHasIndexProvenance(sqlCtxShape),
		"the CDC writer must see an activated deployment, or no tail frame row is ever written")
	require.True(t, ClusterHasIndexProvenance(procShape))

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion58-1)
	require.False(t, ClusterHasIndexProvenance(sqlCtxShape), "and must still respect the gate")
	require.False(t, ClusterHasIndexProvenance(procShape))

	require.False(t, ClusterHasIndexProvenance(nil))
	require.False(t, ClusterHasIndexProvenance(&SqlProcess{}), "neither shape present")
}
