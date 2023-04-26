// Copyright 2022 Matrix Origin
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

package memoryengine

import (
	"encoding"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestOperationsMarshalAndUnmarshal(t *testing.T) {
	// prepare data structure
	mp := mpool.MustNewZero()
	colA := testutil.NewVector(
		5,
		types.T_int64.ToType(),
		mp,
		false,
		[]int64{
			1, 2, 3, 4, 5,
		},
	)
	colB := testutil.NewVector(
		5,
		types.T_int64.ToType(),
		mp,
		false,
		[]int64{
			6, 7, 8, 9, 10,
		},
	)
	bat := batch.New(false, []string{"a", "b"})
	bat.Vecs[0] = colA
	bat.Vecs[1] = colB
	bat.InitZsOne(5)
	bat.Cnt = 1

	tableDefA := &engine.AttributeDef{
		Attr: engine.Attribute{
			Name:    "a",
			Type:    types.T_int64.ToType(),
			Primary: true,
		},
	}
	tableDefB := &engine.AttributeDef{
		Attr: engine.Attribute{
			Name:    "b",
			Type:    types.T_int64.ToType(),
			Primary: false,
		},
	}
	tableDefC := &engine.ConstraintDef{
		Cts: []engine.Constraint{
			&engine.PrimaryKeyDef{
				Pkey: &plan.PrimaryKeyDef{
					PkeyColName: "a",
					Names:       []string{"a"},
				},
			},
		},
	}

	// prepare test cases
	cases := []struct {
		in           encoding.BinaryMarshaler
		out          encoding.BinaryUnmarshaler
		validateFunc validateFunc
	}{
		{
			in: &OpenDatabaseResp{
				ID:   ID(1),
				Name: "testing",
			},
			out:          &OpenDatabaseResp{},
			validateFunc: require.Equal,
		},
		{
			in: &OpenDatabaseReq{
				AccessInfo: AccessInfo{
					AccountID: 1,
					UserID:    2,
					RoleID:    3,
				},
				Name: "testing",
			},
			out:          &OpenDatabaseReq{},
			validateFunc: require.Equal,
		},
		{
			in: &CreateDatabaseResp{
				ID: ID(1),
			},
			out:          &CreateDatabaseResp{},
			validateFunc: require.Equal,
		},
		{
			in: &CreateDatabaseReq{
				ID: ID(1),
				AccessInfo: AccessInfo{
					AccountID: 1,
					UserID:    2,
					RoleID:    3,
				},
				Name: "testing",
			},
			out:          &CreateDatabaseReq{},
			validateFunc: require.Equal,
		},
		{
			in: &GetDatabasesResp{
				Names: []string{
					"test1",
					"test2",
					"test3",
				},
			},
			out:          &GetDatabasesResp{},
			validateFunc: require.Equal,
		},
		{
			in: &GetDatabasesReq{
				AccessInfo: AccessInfo{
					AccountID: 1,
					UserID:    2,
					RoleID:    3,
				},
			},
			out:          &GetDatabasesReq{},
			validateFunc: require.Equal,
		},
		{
			in: &DeleteDatabaseResp{
				ID: ID(1),
			},
			out:          &DeleteDatabaseResp{},
			validateFunc: require.Equal,
		},
		{
			in: &DeleteDatabaseReq{
				AccessInfo: AccessInfo{
					AccountID: 1,
					UserID:    2,
					RoleID:    3,
				},
				Name: "testing",
			},
			out:          &DeleteDatabaseReq{},
			validateFunc: require.Equal,
		},
		{
			in: &OpenRelationResp{
				ID:           ID(1),
				Type:         RelationTable,
				DatabaseName: "database1",
				RelationName: "relation1",
			},
			out:          &OpenRelationResp{},
			validateFunc: require.Equal,
		},
		{
			in: &OpenRelationReq{
				DatabaseID:   ID(1),
				DatabaseName: "database1",
				Name:         "test1",
			},
			out:          &OpenRelationReq{},
			validateFunc: require.Equal,
		},
		{
			in: &CreateRelationResp{
				ID: ID(1),
			},
			out:          &CreateRelationResp{},
			validateFunc: require.Equal,
		},
		{
			in: &CreateRelationReq{
				ID:           ID(1),
				DatabaseID:   ID(2),
				DatabaseName: "database1",
				Name:         "test1",
				Type:         RelationTable,
				Defs: []engine.TableDefPB{
					tableDefA.ToPBVersion(),
					tableDefB.ToPBVersion(),
					tableDefC.ToPBVersion(),
				},
			},
			out:          &CreateRelationReq{},
			validateFunc: require.Equal,
		},
		{
			in: &GetRelationsResp{
				Names: []string{
					"test1",
					"test2",
					"test3",
				},
			},
			out:          &GetRelationsResp{},
			validateFunc: require.Equal,
		},
		{
			in: &GetRelationsReq{
				DatabaseID: ID(1),
			},
			out:          &GetRelationsReq{},
			validateFunc: require.Equal,
		},
		{
			in: &DeleteRelationResp{
				ID: ID(1),
			},
			out:          &DeleteRelationResp{},
			validateFunc: require.Equal,
		},
		{
			in: &DeleteRelationReq{
				DatabaseID:   ID(1),
				DatabaseName: "database1",
				Name:         "test1",
			},
			out:          &DeleteRelationReq{},
			validateFunc: require.Equal,
		},
		{
			in: &GetTableDefsResp{
				Defs: []engine.TableDefPB{
					tableDefA.ToPBVersion(),
					tableDefB.ToPBVersion(),
					tableDefC.ToPBVersion(),
				},
			},
			out:          &GetTableDefsResp{},
			validateFunc: require.Equal,
		},
		{
			in: &GetTableDefsReq{
				TableID: ID(1),
			},
			out:          &GetTableDefsReq{},
			validateFunc: require.Equal,
		},
		{
			in:           &WriteResp{},
			out:          &WriteResp{},
			validateFunc: require.Equal,
		},
		{
			in: &WriteReq{
				TableID:      ID(1),
				DatabaseName: "database1",
				TableName:    "table1",
				Batch:        bat,
			},
			out:          &WriteReq{},
			validateFunc: validateWriteReq,
		},
		{
			in: &NewTableIterResp{
				IterID: ID(1),
			},
			out:          &NewTableIterResp{},
			validateFunc: require.Equal,
		},
		{
			in: &NewTableIterReq{
				TableID: ID(1),
			},
			out:          &NewTableIterReq{},
			validateFunc: require.Equal,
		},
		{
			in: &ReadResp{
				Batch: bat,
			},
			out:          &ReadResp{},
			validateFunc: validateReadResp,
		},
		{
			in: &ReadReq{
				IterID: ID(1),
				ColNames: []string{
					"test1",
					"test2",
					"test3",
				},
			},
			out:          &ReadReq{},
			validateFunc: require.Equal,
		},
		{
			in:           &DeleteResp{},
			out:          &DeleteResp{},
			validateFunc: require.Equal,
		},
		{
			in: &DeleteReq{
				TableID:      ID(10),
				DatabaseName: "database1",
				TableName:    "table1",
				ColumnName:   "colume1",
				Vector:       colB,
			},
			out:          &DeleteReq{},
			validateFunc: validateDeleteReq,
		},
		{
			in: &GetPrimaryKeysReq{
				TableID: ID(11),
			},
			out:          &GetPrimaryKeysReq{},
			validateFunc: require.Equal,
		},
		{
			in: &GetPrimaryKeysResp{
				Attrs: []engine.Attribute{
					{
						Name:    "a",
						Type:    types.T_int64.ToType(),
						Primary: true,
					},
					{
						Name:    "b",
						Type:    types.T_int64.ToType(),
						Primary: false,
					},
				},
			},
			out:          &GetPrimaryKeysResp{},
			validateFunc: require.Equal,
		},
		{
			in: &CloseTableIterReq{
				IterID: ID(12),
			},
			out:          &CloseTableIterReq{},
			validateFunc: require.Equal,
		},
		{
			in:           &CloseTableIterResp{},
			out:          &CloseTableIterResp{},
			validateFunc: require.Equal,
		},
		{
			in: &GetHiddenKeysReq{
				TableID: ID(14),
			},
			out:          &GetHiddenKeysReq{},
			validateFunc: require.Equal,
		},
		{
			in: &GetHiddenKeysResp{
				Attrs: []engine.Attribute{
					{
						Name:    "a",
						Type:    types.T_int64.ToType(),
						Primary: true,
					},
					{
						Name:    "b",
						Type:    types.T_int64.ToType(),
						Primary: false,
					},
				},
			},
			out:          &GetHiddenKeysResp{},
			validateFunc: require.Equal,
		},
		{
			in: &TableStatsReq{
				TableID: ID(15),
			},
			out:          &TableStatsReq{},
			validateFunc: require.Equal,
		},
		{
			in: &TableStatsResp{
				Rows: 16,
			},
			out:          &TableStatsResp{},
			validateFunc: require.Equal,
		},
		{
			in: &GetTableColumnsReq{
				TableID: ID(16),
			},
			out:          &GetTableColumnsReq{},
			validateFunc: require.Equal,
		},
		{
			in: &GetTableColumnsResp{
				Attrs: []engine.Attribute{
					{
						Name:    "a",
						Type:    types.T_int64.ToType(),
						Primary: true,
					},
					{
						Name:    "b",
						Type:    types.T_int64.ToType(),
						Primary: false,
					},
				},
			},
			out:          &GetTableColumnsResp{},
			validateFunc: require.Equal,
		},
		{
			in: &TruncateRelationReq{
				NewTableID:   ID(10),
				OldTableID:   ID(11),
				DatabaseID:   ID(12),
				DatabaseName: "database1",
				Name:         "table1",
			},
			out:          &TruncateRelationReq{},
			validateFunc: require.Equal,
		},
		{
			in: &TruncateRelationResp{
				ID: ID(13),
			},
			out:          &TruncateRelationResp{},
			validateFunc: require.Equal,
		},
		{
			in: &AddTableDefReq{
				TableID:      ID(11),
				DatabaseName: "database1",
				TableName:    "table1",
				Def:          tableDefA.ToPBVersion(),
			},
			out:          &AddTableDefReq{},
			validateFunc: require.Equal,
		},
		{
			in:           &AddTableDefResp{},
			out:          &AddTableDefResp{},
			validateFunc: require.Equal,
		},
		{
			in: &DelTableDefReq{
				TableID:      ID(11),
				DatabaseName: "database1",
				TableName:    "table1",
				Def:          tableDefA.ToPBVersion(),
			},
			out:          &DelTableDefReq{},
			validateFunc: require.Equal,
		},
		{
			in:           &DelTableDefResp{},
			out:          &DelTableDefResp{},
			validateFunc: require.Equal,
		},
		{
			in: &UpdateReq{
				TableID:      ID(1),
				DatabaseName: "database1",
				TableName:    "table1",
				Batch:        bat,
			},
			out:          &UpdateReq{},
			validateFunc: validateUpdateReq,
		},
	}

	// test all cases
	for i := 0; i < len(cases); i++ {
		testCase := cases[i]
		marshalThenUnmarshal(t, testCase.in, testCase.out)
		testCase.validateFunc(t, testCase.in, testCase.out)
	}
}

func marshalThenUnmarshal[
	I encoding.BinaryMarshaler,
	O encoding.BinaryUnmarshaler,
](t *testing.T, in I, out O) {
	data, err := in.MarshalBinary()
	require.NoError(t, err)

	err = out.UnmarshalBinary(data)
	require.NoError(t, err)
}

type validateFunc func(
	t require.TestingT,
	expected interface{},
	actual interface{},
	msgAndArgs ...interface{},
)

func validateWriteReq(
	t require.TestingT,
	expected interface{},
	actual interface{},
	msgAndArgs ...interface{},
) {
	in, ok := expected.(*WriteReq)
	require.True(t, ok)
	out, ok := actual.(*WriteReq)
	require.True(t, ok)

	// it is irreversible between vector.Vector.MarshalBinary and
	// vector.Vector.UnmarshalBinary.
	in.Batch.Vecs = nil
	out.Batch.Vecs = nil
	require.Equal(t, in, out)
}

func validateUpdateReq(
	t require.TestingT,
	expected interface{},
	actual interface{},
	msgAndArgs ...interface{},
) {
	in, ok := expected.(*UpdateReq)
	require.True(t, ok)
	out, ok := actual.(*UpdateReq)
	require.True(t, ok)

	// it is irreversible between vector.Vector.MarshalBinary and
	// vector.Vector.UnmarshalBinary.
	in.Batch.Vecs = nil
	out.Batch.Vecs = nil
	require.Equal(t, in, out)
}

func validateReadResp(
	t require.TestingT,
	expected interface{},
	actual interface{},
	msgAndArgs ...interface{},
) {
	in, ok := expected.(*ReadResp)
	require.True(t, ok)
	out, ok := actual.(*ReadResp)
	require.True(t, ok)

	// it is irreversible between vector.Vector.MarshalBinary and
	// vector.Vector.UnmarshalBinary.
	in.Batch.Vecs = nil
	out.Batch.Vecs = nil
	require.Equal(t, in, out)
}

func validateDeleteReq(
	t require.TestingT,
	expected interface{},
	actual interface{},
	msgAndArgs ...interface{},
) {
	in, ok := expected.(*DeleteReq)
	require.True(t, ok)
	out, ok := actual.(*DeleteReq)
	require.True(t, ok)

	// it is irreversible between vector.Vector.MarshalBinary and
	// vector.Vector.UnmarshalBinary.
	in.Vector = nil
	out.Vector = nil
	require.Equal(t, in, out)
}
