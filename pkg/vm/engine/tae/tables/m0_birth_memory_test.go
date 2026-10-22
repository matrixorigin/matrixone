//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tables

import (
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestM0BirthMemoryNodePreservesFillAndAppends(t *testing.T) {
	write := catalog.NewEmptySchema("customer")
	require.NoError(t, write.AppendCol("a", types.T_int32.ToType()))
	require.NoError(t, write.AppendCol("b", types.T_int32.ToType()))
	require.NoError(t, write.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
	write.ColDefs[2].Hidden = true
	require.NoError(t, write.Finalize(false))
	read := write.Clone()
	rowid := read.ColDefs[3]
	// Test the data alignment path directly; SQL admission remains closed.
	flag := &catalog.ColDef{Name: "flag", Idx: 3, SeqNum: 3, Type: types.T_bool.ToType(), NullAbility: true}
	read.ColDefs = append(read.ColDefs[:3], flag, rowid)
	rowid.Idx = 4
	read.Extra.NextColSeqnum = 4
	read.Version = 1
	read.Extra.AppendMissingState = &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"), BaseWidth: 3, PkSeqnum: 2,
		Columns: []*plan.ColumnBirthFill{{Seqnum: 3, BirthAppendOrdinal: 1, Type: plan.Type{Id: int32(types.T_bool)}, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1}}}
	require.NoError(t, read.Finalize(true))
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	input := containers.BuildBatch(write.AllNames(), write.AllTypes(), containers.Options{Allocator: mp})
	defer input.Close()
	for _, v := range input.Vecs {
		v.Append(nil, true)
		v.Append(nil, true)
	}
	rt := dbutils.NewRuntime()
	defer rt.VectorPool.Transient.Destory()
	defer rt.VectorPool.Small.Destory()
	node := &memoryNode{writeSchema: write, data: input, object: &baseObject{rt: rt}}
	var out *containers.Batch
	require.NoError(t, node.getDataWindowLocked(&out, read, []int{3}, 0, 2, mp))
	require.Equal(t, false, out.Vecs[0].Get(0))
	require.False(t, out.Vecs[0].IsNull(1))
	require.NoError(t, node.getDataWindowLocked(&out, read, []int{3}, 0, 2, mp))
	require.Equal(t, 4, out.Vecs[0].Length())
	out.Close()
	out = nil
	write.Version = 1
	require.Error(t, node.getDataWindowLocked(&out, read, []int{3}, 0, 2, mp))
	require.Nil(t, out, "invalid new batch is rejected before output allocation")
	require.Equal(t, uint16(objectio.SEQNUM_ROWID), write.PhyAddrKey.SeqNum)
}
