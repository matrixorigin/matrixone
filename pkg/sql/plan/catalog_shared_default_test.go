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

package plan

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestAlterCopyPrimaryKeySharedDefaults(t *testing.T) {
	ctx := NewMockCompilerContext2(gomock.NewController(t))
	ctx.EXPECT().GetContext().Return(context.Background()).AnyTimes()
	shared := &pb.Default{NullAbility: true}
	original := &pb.TableDef{Name: "t", Cols: []*pb.ColDef{
		{Name: "a", Typ: pb.Type{Id: int32(types.T_int64)}, Default: shared},
		{Name: "b", Typ: pb.Type{Id: int32(types.T_int64)}, Default: shared},
	}}
	copyDef, err := buildCopyTableDef(context.Background(), original)
	require.NoError(t, err)
	require.NotSame(t, copyDef.Cols[0].Default, copyDef.Cols[1].Default)
	spec := &tree.PrimaryKeyIndex{KeyParts: []*tree.KeyPart{{ColName: tree.NewUnresolvedName(tree.NewCStr("a", 0))}}}
	require.NoError(t, AddPrimaryKey(ctx, &pb.AlterTable{CopyTableDef: copyDef}, spec, nil))
	require.False(t, copyDef.Cols[0].Default.NullAbility)
	require.True(t, copyDef.Cols[1].Default.NullAbility)
	require.True(t, original.Cols[0].Default.NullAbility)
	require.True(t, original.Cols[1].Default.NullAbility)
}
