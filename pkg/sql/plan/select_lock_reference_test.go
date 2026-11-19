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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCollectSelectLockReferences(t *testing.T) {
	snapshot := &pb.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}, Tenant: &pb.SnapshotTenant{TenantID: 7}}
	path := []*pb.ViewStep{{DatabaseName: "app", ViewName: "v", Snapshot: snapshot}}
	for _, mode := range []lockpb.LockMode{lockpb.LockMode_Exclusive, lockpb.LockMode_Shared} {
		t.Run(mode.String(), func(t *testing.T) {
			scan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, ObjRef: &pb.ObjectRef{Obj: 1, SchemaName: "app", ObjName: "t"}, ViewPath: path, ScanSnapshot: snapshot, BindingTags: []int32{1}, TableDef: &pb.TableDef{TblId: 1, Pkey: &pb.PrimaryKeyDef{PkeyColName: "id"}, Cols: []*pb.ColDef{{Name: "id", Typ: pb.Type{Id: int32(types.T_int32)}}}}}
			b := &QueryBuilder{qry: &pb.Query{Nodes: []*pb.Node{scan}}, compCtx: NewMockCompilerContext(true, newPlanTestProcess(t)), ctxByNode: []*BindContext{nil}}
			targets, err := b.collectLockTargets(0, nil, mode)
			require.NoError(t, err)
			require.Len(t, targets, 1)
			if mode == lockpb.LockMode_Shared {
				require.Empty(t, b.qry.SelectLockReferences)
				return
			}
			require.Len(t, b.qry.SelectLockReferences, 1)
			ref := b.qry.SelectLockReferences[0]
			require.Equal(t, snapshot, ref.ObjectRef.Snapshot)
			require.Equal(t, path, ref.ViewPath)
			ref.ObjectRef.Snapshot.TS.PhysicalTime = 99
			ref.ViewPath[0].ViewName = "other"
			require.EqualValues(t, 42, snapshot.TS.PhysicalTime)
			require.Equal(t, "v", path[0].ViewName)
			scan.ObjRef = nil
			_, err = b.collectLockTargets(0, nil, mode)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
		})
	}
}
