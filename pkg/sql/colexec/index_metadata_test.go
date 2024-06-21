// Copyright 2021 Matrix Origin
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

package colexec

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestInsertIndexMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	proc := testutil.NewProc()
	proc.TxnOperator = txnOperator

	mockEngine := mock_frontend.NewMockEngine(ctrl)
	mockEngine.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockEngine.EXPECT().AllocateIDByKey(gomock.Any(), gomock.Any()).Return(uint64(272510), nil).AnyTimes()
	//-------------------------------------------------mo_catalog + mo_indexes-----------------------------------------------------------
	catalog_database := mock_frontend.NewMockDatabase(ctrl)
	mockEngine.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, txnOperator).Return(catalog_database, nil).AnyTimes()

	indexes_relation := mock_frontend.NewMockRelation(ctrl)
	indexes_relation.EXPECT().Ranges(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	indexes_relation.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	indexes_relation.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	reader := mock_frontend.NewMockReader(ctrl)
	reader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, attrs []string, b, c interface{}) (*batch.Batch, error) {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())

		err := vector.AppendFixed(bat.GetVector(0), types.Rowid([types.RowidSize]byte{}), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}

		err = vector.AppendFixed(bat.GetVector(1), uint64(272464), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}

		err = vector.AppendBytes(bat.GetVector(2), []byte("empno"), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}
		bat.SetRowCount(bat.GetVector(1).Length())
		return bat, nil
	}).AnyTimes()
	reader.EXPECT().Close().Return(nil).AnyTimes()

	indexes_relation.EXPECT().NewReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]engine.Reader{reader}, nil).AnyTimes()
	catalog_database.EXPECT().Relation(gomock.Any(), catalog.MO_INDEXES, gomock.Any()).Return(indexes_relation, nil).AnyTimes()
	//---------------------------------------------------------------------------------------------------------------------------
	mock_emp_Relation := mock_frontend.NewMockRelation(ctrl)
	mock_emp_Relation.EXPECT().TableDefs(gomock.Any()).Return(buildMockTableDefs(mock_emp_table), nil).AnyTimes()
	mock_emp_Relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(272464)).AnyTimes()

	mock_db1_database := mock_frontend.NewMockDatabase(ctrl)
	mock_db1_database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(mock_emp_Relation, nil).AnyTimes()
	mock_db1_database.EXPECT().GetDatabaseId(gomock.Any()).Return("123456").AnyTimes()

	type args struct {
		eg      engine.Engine
		ctx     context.Context
		db      engine.Database
		proc    *process.Process
		tblName string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test03",
			args: args{
				eg:      mockEngine,
				ctx:     proc.Ctx,
				db:      mock_db1_database,
				tblName: "emp",
				proc:    proc,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := InsertIndexMetadata(tt.args.eg, tt.args.ctx, tt.args.db, tt.args.proc, tt.args.tblName); (err != nil) != tt.wantErr {
				t.Errorf("InsertIndexMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInsertOneIndexMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)

	proc := testutil.NewProc()
	proc.TxnOperator = txnOperator

	mockEngine := mock_frontend.NewMockEngine(ctrl)
	mockEngine.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockEngine.EXPECT().AllocateIDByKey(gomock.Any(), gomock.Any()).Return(uint64(272510), nil).AnyTimes()
	//-------------------------------------------------mo_catalog + mo_indexes-----------------------------------------------------------
	catalog_database := mock_frontend.NewMockDatabase(ctrl)

	mockEngine.EXPECT().Database(gomock.Any(), catalog.MO_CATALOG, txnOperator).Return(catalog_database, nil).AnyTimes()

	indexes_relation := mock_frontend.NewMockRelation(ctrl)
	indexes_relation.EXPECT().Ranges(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	indexes_relation.EXPECT().Delete(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	indexes_relation.EXPECT().Write(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	reader := mock_frontend.NewMockReader(ctrl)
	reader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, attrs []string, b, c interface{}) (*batch.Batch, error) {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
		bat.Vecs[2] = vector.NewVec(types.T_varchar.ToType())

		err := vector.AppendFixed(bat.GetVector(0), types.Rowid([types.RowidSize]byte{}), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}

		err = vector.AppendFixed(bat.GetVector(1), uint64(272464), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}

		err = vector.AppendBytes(bat.GetVector(2), []byte("empno"), false, testutil.TestUtilMp)
		if err != nil {
			require.Nil(t, err)
		}
		bat.SetRowCount(bat.GetVector(1).Length())
		return bat, nil
	}).AnyTimes()
	reader.EXPECT().Close().Return(nil).AnyTimes()

	indexes_relation.EXPECT().NewReader(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]engine.Reader{reader}, nil).AnyTimes()
	catalog_database.EXPECT().Relation(gomock.Any(), catalog.MO_INDEXES, gomock.Any()).Return(indexes_relation, nil).AnyTimes()
	//---------------------------------------------------------------------------------------------------------------------------

	mock_emp_Relation := mock_frontend.NewMockRelation(ctrl)
	mock_emp_Relation.EXPECT().TableDefs(gomock.Any()).Return(buildMockTableDefs(mock_emp_table), nil).AnyTimes()
	mock_emp_Relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(272464)).AnyTimes()

	mock_db1_database := mock_frontend.NewMockDatabase(ctrl)
	mock_db1_database.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(mock_emp_Relation, nil).AnyTimes()
	mock_db1_database.EXPECT().GetDatabaseId(gomock.Any()).Return("123456").AnyTimes()

	type args struct {
		eg      engine.Engine
		ctx     context.Context
		db      engine.Database
		proc    *process.Process
		tblName string
		idxdef  *plan.IndexDef
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test04",
			args: args{
				eg:      mockEngine,
				ctx:     proc.Ctx,
				db:      mock_db1_database,
				tblName: "emp",
				proc:    proc,
				idxdef: &plan.IndexDef{
					IdxId:          "",
					IndexName:      "idx11",
					Parts:          []string{"ename", "sal", "depto"},
					Unique:         false,
					IndexTableName: "",
					TableExist:     false,
					Comment:        "this is a index on emp",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := InsertOneIndexMetadata(tt.args.eg, tt.args.ctx, tt.args.db, tt.args.proc, tt.args.tblName, tt.args.idxdef); (err != nil) != tt.wantErr {
				t.Errorf("InsertOneIndexMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// Define an anonymous function to construct the table structure
func buildMockTableDefs(table_cols []string) []engine.TableDef {
	exeCols := make([]engine.TableDef, len(table_cols))
	for i := 0; i < len(table_cols); i++ {
		if i == 0 {
			exeCols[i] = &engine.ConstraintDef{
				Cts: []engine.Constraint{
					&engine.IndexDef{
						Indexes: []*plan.IndexDef{
							{
								IdxId:          "",
								IndexName:      "empno",
								Parts:          []string{"empno,ename"},
								Unique:         true,
								IndexTableName: "__mo_index_unique_c1d278ec-bfd6-11ed-9e9d-000c29203f30",
								TableExist:     true,
								Comment:        "",
							},
						},
					},
					&engine.PrimaryKeyDef{
						Pkey: &plan.PrimaryKeyDef{
							PkeyColId:   0,
							PkeyColName: "empno",
							Names:       []string{"empno"},
						},
					},
				},
			}
		} else {
			exeCols[i] = &engine.AttributeDef{
				Attr: engine.Attribute{
					Name:          table_cols[i],
					Alg:           compress.None,
					Type:          mock_emp_map[table_cols[i]],
					Default:       nil,
					OnUpdate:      nil,
					Primary:       i == 0,
					Comment:       "comment message",
					ClusterBy:     false,
					AutoIncrement: i == 1,
				},
			}
		}
	}
	return exeCols
}

var mock_emp_table = []string{
	"constraint",
	mock_emp_empno,
	mock_emp_ename,
	mock_emp_job,
	mock_emp_mgr,
	mock_emp_hiredate,
	mock_emp_sal,
	mock_emp_comm,
	mock_emp_deptno,
}

const (
	mock_emp_empno    = "empno"
	mock_emp_ename    = "ename"
	mock_emp_job      = "job"
	mock_emp_mgr      = "mgr"
	mock_emp_hiredate = "hiredate"
	mock_emp_sal      = "sal"
	mock_emp_comm     = "comm"
	mock_emp_deptno   = "deptno"
)

var mock_emp_map = map[string]types.Type{
	mock_emp_empno: {
		Oid:   types.T_uint32,
		Size:  4,
		Width: 32,
		Scale: -1,
	},
	mock_emp_ename: {
		Oid:   types.T_varchar,
		Size:  24,
		Width: 15,
		Scale: 0,
	},
	mock_emp_job: {
		Oid:   types.T_varchar,
		Size:  24,
		Width: 10,
		Scale: 0,
	},
	mock_emp_mgr: {
		Oid:   types.T_uint32,
		Size:  4,
		Width: 32,
		Scale: -1,
	},
	mock_emp_hiredate: {
		Oid:   types.T_date,
		Size:  4,
		Width: 0,
		Scale: 0,
	},
	mock_emp_sal: {
		Oid:   types.T_decimal64,
		Size:  8,
		Width: 7,
		Scale: 2,
	},
	mock_emp_comm: {
		Oid:   types.T_decimal64,
		Size:  8,
		Width: 7,
		Scale: 2,
	},
	mock_emp_deptno: {
		Oid:   types.T_uint32,
		Size:  4,
		Width: 32,
		Scale: -1,
	},
}
