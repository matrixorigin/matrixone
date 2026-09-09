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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// gateCtx plans on a service whose rollout gate the test controls.
type gateCtx struct{ proc *process.Process }

func (gateCtx) GetContext() context.Context { return context.Background() }
func (c gateCtx) ResolveVariable(string, bool, bool) (interface{}, error) {
	return nil, nil
}
func (c gateCtx) GetProcess() *process.Process { return c.proc }

func planAt(t *testing.T, version int64) []*planpb.TableDef {
	t.Helper()
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	require.NotNil(t, rt)
	prev, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.MOProtocolVersion, prev) })
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)

	_, tableDefs, err := Hooks{}.BuildSecondaryIndexDefs(
		gateCtx{proc: proc}, indexOn("vec"), vecColMap("id", "vec"), nil, "id")
	require.NoError(t, err)
	require.NotEmpty(t, tableDefs)
	return tableDefs
}

func metadataColNames(t *testing.T, defs []*planpb.TableDef) []string {
	t.Helper()
	for _, d := range defs {
		if d != nil && d.TableType == catalog.Hnsw_TblType_Metadata {
			names := make([]string, 0, len(d.Cols))
			for _, c := range d.Cols {
				require.NotNil(t, c, "a nil column means the table was sized for columns it did not fill")
				names = append(names, c.Name)
			}
			return names
		}
	}
	t.Fatal("no metadata table def")
	return nil
}

// CREATE INDEX is the second producer of a wide metadata table, and the v4_0_7 migration's gate
// does not cover it. Before activation the table must be born in the LEGACY shape: an old CN's
// writer INSERTs four values positionally, so a six-column table fails it on arity -- and a
// column DEFAULT cannot repair a value count, because the arity is rejected before any default
// is consulted.
func TestCreateIndexWaitsForTheDeploymentBeforeWideningMetadata(t *testing.T) {
	before := metadataColNames(t, planAt(t, defines.MORPCVersion58-1))
	require.Equal(t, []string{
		catalog.Hnsw_TblCol_Metadata_Index_Id,
		catalog.Hnsw_TblCol_Metadata_Checksum,
		catalog.Hnsw_TblCol_Metadata_Timestamp,
		catalog.Hnsw_TblCol_Metadata_Filesize,
	}, before, "an old CN is still out there; write the shape its positional INSERT fits")

	after := metadataColNames(t, planAt(t, defines.MORPCVersion58))
	require.Equal(t, []string{
		catalog.Hnsw_TblCol_Metadata_Index_Id,
		catalog.Hnsw_TblCol_Metadata_Checksum,
		catalog.Hnsw_TblCol_Metadata_Timestamp,
		catalog.Hnsw_TblCol_Metadata_Filesize,
		catalog.Hnsw_TblCol_Metadata_Nrow,
		catalog.Hnsw_TblCol_Metadata_Build_Ts,
	}, after, "once every service understands them, new indexes are born wide")
}
