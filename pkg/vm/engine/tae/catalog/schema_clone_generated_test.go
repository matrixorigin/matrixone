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

package catalog

import (
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func TestTableMVCCClonePreservesGeneratedColumn(t *testing.T) {
	schema := MockSchemaAll(3, 1)
	encoded, err := types.Encode(&plan.GeneratedCol{OriginString: "a + 1", IsStored: true})
	require.NoError(t, err)
	schema.ColDefs[0].GeneratedCol = encoded
	want := append([]byte(nil), encoded...)

	cloned := (&TableMVCCNode{Schema: schema}).CloneData().Schema
	require.Equal(t, want, cloned.ColDefs[0].GeneratedCol)
	require.Nil(t, cloned.ColDefs[2].GeneratedCol)
	cloned.ColDefs[0].GeneratedCol[0] ^= 0xff
	require.Equal(t, want, schema.ColDefs[0].GeneratedCol)
}

func TestCheckpointColumnBatchRestoresGeneratedColumn(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	packer := types.NewPacker()
	defer packer.Close()
	typ := types.T_int32.ToType()
	typBytes, err := types.Encode(&typ)
	require.NoError(t, err)
	generated, err := types.Encode(&plan.GeneratedCol{OriginString: "a + 1", IsStored: true})
	require.NoError(t, err)
	cols := []pkgcatalog.Column{
		{TableId: 7, TableName: "t", Name: "a", Num: 1, Typ: typBytes},
		{TableId: 7, TableName: "t", Name: "g", Num: 2, Seqnum: 1, Typ: typBytes, HasGenerated: 1, GeneratedExpr: generated},
		{TableId: 7, TableName: "t", Name: "ordinary", Num: 3, Seqnum: 2, Typ: typBytes, GeneratedExpr: generated},
	}
	cnBatch, err := pkgcatalog.GenCreateColumnTuples(cols, mp, packer)
	require.NoError(t, err)
	tnBatch := containers.ToTNBatch(cnBatch, mp)
	defer tnBatch.Close()
	vec := func(name string) *vector.Vector { return tnBatch.GetVectorByName(name).GetDownstreamVector() }
	schema := NewEmptySchema("t")
	next := schema.ReadFromBatch(tnBatch,
		vector.MustFixedColNoTypeCheck[uint64](vec(pkgcatalog.SystemColAttr_RelID)),
		vector.MustFixedColNoTypeCheck[int8](vec(pkgcatalog.SystemColAttr_NullAbility)),
		vector.MustFixedColNoTypeCheck[int8](vec(pkgcatalog.SystemColAttr_IsHidden)),
		vector.MustFixedColNoTypeCheck[int8](vec(pkgcatalog.SystemColAttr_IsClusterBy)),
		vector.MustFixedColNoTypeCheck[int8](vec(pkgcatalog.SystemColAttr_IsAutoIncrement)),
		vector.MustFixedColNoTypeCheck[int32](vec(pkgcatalog.SystemColAttr_Num)),
		vector.MustFixedColNoTypeCheck[uint16](vec(pkgcatalog.SystemColAttr_Seqnum)), 0,
		func(name string, id uint64) bool { return name == "t" && id == 7 })
	require.Equal(t, len(cols), next)
	require.Len(t, schema.ColDefs, len(cols))
	require.Empty(t, schema.ColDefs[0].GeneratedCol)
	require.Equal(t, generated, schema.ColDefs[1].GeneratedCol)
	require.Empty(t, schema.ColDefs[2].GeneratedCol)
	vec(pkgcatalog.SystemColAttr_Generated).GetBytesAt(1)[0] ^= 0xff
	require.Equal(t, generated, schema.ColDefs[1].GeneratedCol)
}
