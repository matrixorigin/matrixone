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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIndexDefIncludedColumnsRoundTripAndDeepCopy(t *testing.T) {
	indexDef := &planpb.IndexDef{
		IndexName:       "idx_embedding",
		Parts:           []string{"embedding"},
		IndexAlgo:       "ivfflat",
		IndexAlgoParams: `{"lists":"2","op_type":"vector_l2_ops"}`,
		IncludedColumns: []string{"title", "category"},
		Option: &planpb.IndexOption{
			Visibility: planpb.IndexOption_VISIBILITY_INVISIBLE,
		},
	}

	data, err := indexDef.Marshal()
	require.NoError(t, err)

	var decoded planpb.IndexDef
	require.NoError(t, decoded.Unmarshal(data))
	require.Equal(t, indexDef.IncludedColumns, decoded.IncludedColumns)

	copied := DeepCopyIndexDef(indexDef)
	require.Equal(t, indexDef.IncludedColumns, copied.IncludedColumns)
	require.Equal(t, planpb.IndexOption_VISIBILITY_INVISIBLE, copied.Option.Visibility)

	indexDef.IncludedColumns[0] = "headline"
	require.Equal(t, []string{"title", "category"}, copied.IncludedColumns)
}

func TestIndexDefPreservesRemovedVisibilityMarkerAsUnknownWireData(t *testing.T) {
	data, err := (&planpb.IndexDef{IndexName: "idx_a"}).Marshal()
	require.NoError(t, err)
	data = append(data, 0x70, 0x01) // Former field 14: visibility_set=true.

	var decoded planpb.IndexDef
	require.NoError(t, decoded.Unmarshal(data))
	require.False(t, decoded.Visible)

	roundTrip, err := decoded.Marshal()
	require.NoError(t, err)
	require.True(t, bytes.Contains(roundTrip, []byte{0x70, 0x01}))
}

func TestSparseIndexMetadataMutationPathsFailClosed(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		dml      bool
		nilFirst bool
	}{
		{name: "insert", sql: "insert into single_idx_t values (1, 100)", dml: true, nilFirst: true},
		{name: "load", sql: "load data inline format='csv', data='1,100' into table single_idx_t fields terminated by ','", dml: true},
		{name: "replace", sql: "replace into single_idx_t values (1, 100)", dml: true},
		{name: "update", sql: "update single_idx_t set val = 100 where id = 1", dml: true, nilFirst: true},
		{name: "delete", sql: "delete from single_idx_t where id = 1", dml: true},
		{name: "select for update", sql: "select * from single_idx_t where val = 100 for update", dml: true, nilFirst: true},
		{name: "alter table", sql: "alter table single_idx_t rename column val to val2", nilFirst: true},
		{name: "rename table", sql: "rename table single_idx_t to single_idx_copy"},
		{name: "truncate table", sql: "truncate table single_idx_t"},
		{name: "drop table", sql: "drop table single_idx_t", nilFirst: true},
		{name: "create index", sql: "create index idx_id on single_idx_t(id)"},
		{name: "drop index", sql: "drop index idx_val on single_idx_t", nilFirst: true},
		{name: "create table like", sql: "create table single_idx_copy like single_idx_t"},
		{name: "clone table", sql: "create table single_idx_copy clone single_idx_t", nilFirst: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(test.dml)
			tableDef := mock.ctxt.tables["single_idx_t"]
			require.NotNil(t, tableDef)
			require.NotEmpty(t, tableDef.Indexes)
			if test.nilFirst {
				tableDef.Indexes = append([]*planpb.IndexDef{nil}, tableDef.Indexes...)
			} else {
				tableDef.Indexes = append(tableDef.Indexes, nil)
			}

			_, err := runOneStmt(mock, t, test.sql)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), err)
			require.ErrorContains(t, err, "nil index metadata")
		})
	}
}

func TestSparseIndexMetadataRelatedMutationTablesFailClosed(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*MockOptimizer)
		sql   string
	}{
		{
			name: "cascade target",
			setup: func(mock *MockOptimizer) {
				mock.ctxt.tables["replace_fk_sc"].Indexes = []*planpb.IndexDef{nil}
			},
			sql: "delete from replace_fk_sp where id = 1",
		},
		{
			name: "foreign key unique parent",
			setup: func(mock *MockOptimizer) {
				parent := mock.ctxt.tables["replace_fk_p"]
				child := mock.ctxt.tables["replace_fk_c"]
				child.Cols[1].Typ = parent.Cols[1].Typ
				child.Fkeys[0].ForeignCols = []uint64{parent.Cols[1].ColId}
				parent.Indexes = []*planpb.IndexDef{nil, {
					IndexName: "uk_v", IndexTableName: "__mo_index_fk_parent_v",
					Parts: []string{"v"}, Unique: true, TableExist: true,
				}}
			},
			sql: "insert into replace_fk_c values (10, 'x')",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			test.setup(mock)

			_, err := runOneStmt(mock, t, test.sql)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), err)
			require.ErrorContains(t, err, "nil index metadata")
		})
	}
}

func TestSparseIndexMetadataLegacyDMLTargetsFailClosed(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		nilFirst bool
	}{
		{
			name:     "joined update nil first",
			sql:      "update emp left join dept on emp.deptno = dept.deptno set emp.sal = 5000",
			nilFirst: true,
		},
		{
			name: "joined update nil after valid",
			sql:  "update emp left join dept on emp.deptno = dept.deptno set emp.sal = 5000",
		},
		{
			name:     "multi-table delete nil first",
			sql:      "delete emp, dept from emp, dept where emp.deptno = dept.deptno",
			nilFirst: true,
		},
		{
			name: "multi-table delete nil after valid",
			sql:  "delete emp, dept from emp, dept where emp.deptno = dept.deptno",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			tableDef := mock.ctxt.tables["emp"]
			require.NotNil(t, tableDef)
			require.NotEmpty(t, tableDef.Indexes)
			if test.nilFirst {
				tableDef.Indexes = append([]*planpb.IndexDef{nil}, tableDef.Indexes...)
			} else {
				tableDef.Indexes = append(tableDef.Indexes, nil)
			}

			_, err := runOneStmt(mock, t, test.sql)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), err)
			require.ErrorContains(t, err, "nil index metadata")
		})
	}
}

func TestValidateTableIndexDefinitions(t *testing.T) {
	require.NoError(t, validateTableIndexDefinitions(&planpb.TableDef{
		Name:    "dense",
		Indexes: []*planpb.IndexDef{{IndexName: "idx_a"}},
	}))

	for _, indexes := range [][]*planpb.IndexDef{
		{nil, {IndexName: "idx_a"}},
		{{IndexName: "idx_a"}, nil},
	} {
		err := validateTableIndexDefinitions(&planpb.TableDef{Name: "sparse", Indexes: indexes})
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), err)
		require.ErrorContains(t, err, "nil index metadata")
	}
}
