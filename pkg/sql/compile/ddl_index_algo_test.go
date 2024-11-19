package compile

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func TestScope_indexTableBuild(t *testing.T) {
	indexTableDef := &plan.TableDef{
		TblId:  0,
		Name:   "__mo_index_secondary_01932ed7-a7e1-7c40-b914-25053a71a825",
		Hidden: false,
		Cols: []*plan.ColDef{
			{
				Name:   "doc_id",
				Hidden: false,
				Alg:    plan.CompressType_Lz4,
				Typ: plan.Type{
					Id:          22,
					NotNullable: false,
					AutoIncr:    false,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan.Default{},
				NotNull: false,
				Primary: false,
			},
			{
				Name:   "pos",
				Hidden: false,
				Alg:    plan.CompressType_Lz4,
				Typ: plan.Type{
					Id:          22,
					NotNullable: false,
					AutoIncr:    false,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan.Default{},
				NotNull: false,
				Primary: false,
			},
			{
				Name:   "word",
				Hidden: false,
				Alg:    plan.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       65535,
					Scale:       0,
				},
				Default: &plan.Default{},
				NotNull: false,
				Primary: false,
			},
			{
				Name:   "__mo_fake_pk_col",
				Hidden: true,
				Alg:    plan.CompressType_Lz4,
				Typ: plan.Type{
					Id:          28,
					NotNullable: false,
					AutoIncr:    true,
					Width:       0,
					Scale:       0,
				},
				Default: &plan.Default{},
				NotNull: true,
				Primary: true,
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColId:   0,
			PkeyColName: "__mo_fake_pk_col",
			Names:       []string{"__mo_fake_pk_col"},
		},
		Defs: []*plan.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "i",
							},
						},
					},
				},
			},
		},
	}

	convey.Convey("indexTableBuild FaultTolerance1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		proc.ReplaceTopCtx(ctx)

		eng := mock_frontend.NewMockEngine(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil)

		c := NewCompile("test", "test", "create fulltext index ftidx on t1 (b)", "", "", eng, proc, nil, false, nil, time.Now())

		err := indexTableBuild(c, indexTableDef, mockDbMeta)
		assert.Error(t, err)
	})

	convey.Convey("indexTableBuild FaultTolerance2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		proc.ReplaceTopCtx(ctx)

		eng := mock_frontend.NewMockEngine(ctrl)
		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, moerr.NewInternalErrorNoCtx("test"))
		mockDbMeta.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(moerr.NewInternalErrorNoCtx("test"))

		c := NewCompile("test", "test", "create fulltext index ftidx on t1 (b)", "", "", eng, proc, nil, false, nil, time.Now())

		err := indexTableBuild(c, indexTableDef, mockDbMeta)
		assert.Error(t, err)
	})

	convey.Convey("indexTableBuild FaultTolerance3", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		txnCli, txnOp := newTestTxnClientAndOp(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp

		ctx := context.Background()
		proc.Ctx = context.Background()
		proc.ReplaceTopCtx(ctx)

		eng := mock_frontend.NewMockEngine(ctrl)
		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, moerr.NewInternalErrorNoCtx("test")).AnyTimes()
		mockDbMeta.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		c := NewCompile("test", "test", "create fulltext index ftidx on t1 (b)", "", "", eng, proc, nil, false, nil, time.Now())

		err := indexTableBuild(c, indexTableDef, mockDbMeta)
		assert.Error(t, err)
	})
}

func TestScope_handleMasterIndexTable(t *testing.T) {

	indexTableDef := &plan.TableDef{
		TblId:  0,
		Name:   "",
		Hidden: false,
		Cols: []*plan.ColDef{
			{
				Name:   "__mo_index_idx_col",
				Hidden: false,
				Alg:    plan.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       65535,
					Scale:       0,
				},
				Default: &plan.Default{},
				NotNull: false,
				Primary: false,
			},
			{
				Name:   "__mo_index_pri_col",
				Hidden: false,
				Alg:    plan.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       30,
					Scale:       0,
				},
				Default: &plan.Default{},
				NotNull: false,
				Primary: false,
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			PkeyColId:   0,
			PkeyColName: "__mo_index_idx_col",
			Names:       []string{"__mo_index_idx_col"},
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idx1",
				Parts:          []string{"a"},
				Unique:         false,
				IndexTableName: "__mo_index_secondary_01932ed7-a7e1-7c40-b914-25053a71a825",
				TableExist:     true,
				IndexAlgo:      "master",
			},
		},
		Defs: []*plan.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "i",
							},
						},
					},
				},
			},
		},
	}

	qry := &plan.CreateIndex{
		Database: "db1",
		Table:    "t1",
		TableDef: &plan.TableDef{
			TblId:  293166,
			Name:   "t1",
			Hidden: false,
			Cols: []*plan.ColDef{
				{
					ColId:  1,
					Name:   "a",
					Hidden: false,
					Alg:    plan.CompressType_Lz4,
					Typ: plan.Type{
						Id:          61,
						NotNullable: false,
						AutoIncr:    false,
						Width:       30,
						Scale:       0,
						Table:       "t1",
					},
					Default: &plan.Default{
						NullAbility: true,
					},
					NotNull:    false,
					Primary:    false,
					OriginName: "a",
					Seqnum:     0,
				},
				{
					ColId:  1,
					Name:   "b",
					Hidden: false,
					Alg:    plan.CompressType_Lz4,
					Typ: plan.Type{
						Id:          61,
						NotNullable: true,
						AutoIncr:    false,
						Width:       30,
						Scale:       0,
						Table:       "t1",
					},
					Default:    &plan.Default{},
					NotNull:    false,
					Primary:    true,
					OriginName: "b",
					Seqnum:     1,
				},
				{
					ColId:  2,
					Name:   "__mo_rowid",
					Hidden: true,
					Alg:    plan.CompressType_Lz4,
					Typ: plan.Type{
						Id:          101,
						NotNullable: true,
						AutoIncr:    false,
						Width:       0,
						Scale:       0,
						Table:       "t1",
					},
					Default:    &plan.Default{},
					NotNull:    false,
					Primary:    true,
					OriginName: "__mo_rowid",
					Seqnum:     2,
				},
			},
			TableType: "r",
			Createsql: "create table t1(a varchar(30), b varchar(30) primary key)",
			Pkey: &plan.PrimaryKeyDef{
				PkeyColId:   0,
				PkeyColName: "b",
				Names:       []string{"b"},
			},
			Defs: []*plan.TableDef_DefType{
				{
					Def: &plan.TableDef_DefType_Properties{
						Properties: &plan.PropertiesDef{
							Properties: []*plan.Property{
								{
									Key:   "relkind",
									Value: "r",
								},
								{
									Key:   "rel_createsql",
									Value: "create table t1 (a varchar(30), b varchar(30) primary key)",
								},
							},
						},
					},
				},
			},
		},
		Index: &plan.CreateTable{
			IfNotExists: false,
			TableDef: &plan.TableDef{
				Indexes: []*plan.IndexDef{
					{
						IndexName:      "idx1",
						Parts:          []string{"a"},
						Unique:         false,
						IndexTableName: "__mo_index_secondary_01932ed7-a7e1-7c40-b914-25053a71a825",
						TableExist:     true,
						IndexAlgo:      "master",
					},
				},
			},
			IndexTables: []*plan.TableDef{indexTableDef},
		},
	}

	createIndexPn := &plan.Plan{
		Plan: &plan.Plan_Ddl{
			Ddl: &plan.DataDefinition{
				DdlType: plan.DataDefinition_CREATE_INDEX,
				Definition: &plan.DataDefinition_CreateIndex{
					CreateIndex: &plan.CreateIndex{
						Database: "db1",
						Table:    "t1",
						TableDef: &plan.TableDef{
							TblId:  293166,
							Name:   "t1",
							Hidden: false,
							Cols: []*plan.ColDef{
								{
									ColId:  1,
									Name:   "a",
									Hidden: false,
									Alg:    plan.CompressType_Lz4,
									Typ: plan.Type{
										Id:          61,
										NotNullable: false,
										AutoIncr:    false,
										Width:       30,
										Scale:       0,
										Table:       "t1",
									},
									Default: &plan.Default{
										NullAbility: true,
									},
									NotNull:    false,
									Primary:    false,
									OriginName: "a",
									Seqnum:     0,
								},
								{
									ColId:  1,
									Name:   "b",
									Hidden: false,
									Alg:    plan.CompressType_Lz4,
									Typ: plan.Type{
										Id:          61,
										NotNullable: true,
										AutoIncr:    false,
										Width:       30,
										Scale:       0,
										Table:       "t1",
									},
									Default:    &plan.Default{},
									NotNull:    false,
									Primary:    true,
									OriginName: "b",
									Seqnum:     1,
								},
								{
									ColId:  2,
									Name:   "__mo_rowid",
									Hidden: true,
									Alg:    plan.CompressType_Lz4,
									Typ: plan.Type{
										Id:          101,
										NotNullable: true,
										AutoIncr:    false,
										Width:       0,
										Scale:       0,
										Table:       "t1",
									},
									Default:    &plan.Default{},
									NotNull:    false,
									Primary:    true,
									OriginName: "__mo_rowid",
									Seqnum:     2,
								},
							},
							TableType: "r",
							Createsql: "create table t1(a varchar(30), b varchar(30) primary key)",
							Pkey: &plan.PrimaryKeyDef{
								PkeyColId:   0,
								PkeyColName: "b",
								Names:       []string{"b"},
							},
							Defs: []*plan.TableDef_DefType{
								{
									Def: &plan.TableDef_DefType_Properties{
										Properties: &plan.PropertiesDef{
											Properties: []*plan.Property{
												{
													Key:   "relkind",
													Value: "r",
												},
												{
													Key:   "rel_createsql",
													Value: "create table t1 (a varchar(30), b varchar(30) primary key)",
												},
											},
										},
									},
								},
							},
						},
						Index: &plan.CreateTable{
							IfNotExists: false,
							TableDef:    indexTableDef,
						},
					},
				},
			},
		},
	}

	convey.Convey("indexTableBuild FaultTolerance1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess()
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		proc.ReplaceTopCtx(ctx)

		eng := mock_frontend.NewMockEngine(ctrl)
		relation := mock_frontend.NewMockRelation(ctrl)
		mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
		mockDbMeta.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
		//mockDbMeta.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil)
		cnt := 0
		mockDbMeta.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, name string, arg any) (engine.Relation, error) {
			if cnt == 0 {
				cnt++
				return nil, moerr.NewInternalErrorNoCtx("no exists")
			} else {
				return relation, nil
			}
		}).AnyTimes()

		c := NewCompile("test", "test", "create fulltext index ftidx on t1 (b)", "", "", eng, proc, nil, false, nil, time.Now())

		scope := newScope(CreateIndex).withPlan(createIndexPn)

		indexInfo := qry.GetIndex()
		err := scope.handleMasterIndexTable(c, mockDbMeta, indexInfo.GetTableDef().Indexes[0], qry.Database, qry.GetTableDef(), indexInfo)
		assert.Error(t, err)
	})

	//convey.Convey("indexTableBuild FaultTolerance2", t, func() {
	//	ctrl := gomock.NewController(t)
	//	defer ctrl.Finish()
	//
	//	proc := testutil.NewProcess()
	//	proc.Base.SessionInfo.Buf = buffer.New()
	//
	//	ctx := context.Background()
	//	proc.Ctx = context.Background()
	//	proc.ReplaceTopCtx(ctx)
	//
	//	eng := mock_frontend.NewMockEngine(ctrl)
	//	mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
	//	mockDbMeta.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, moerr.NewInternalErrorNoCtx("test"))
	//	mockDbMeta.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(moerr.NewInternalErrorNoCtx("test"))
	//
	//	c := NewCompile("test", "test", "create fulltext index ftidx on t1 (b)", "", "", eng, proc, nil, false, nil, time.Now())
	//
	//	err := indexTableBuild(c, indexTableDef, mockDbMeta)
	//	assert.Error(t, err)
	//})
	//
	//convey.Convey("indexTableBuild FaultTolerance3", t, func() {
	//	ctrl := gomock.NewController(t)
	//	defer ctrl.Finish()
	//
	//	proc := testutil.NewProcess()
	//	proc.Base.SessionInfo.Buf = buffer.New()
	//
	//	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	//	proc.Base.TxnClient = txnCli
	//	proc.Base.TxnOperator = txnOp
	//
	//	ctx := context.Background()
	//	proc.Ctx = context.Background()
	//	proc.ReplaceTopCtx(ctx)
	//
	//	eng := mock_frontend.NewMockEngine(ctrl)
	//	mockDbMeta := mock_frontend.NewMockDatabase(ctrl)
	//	mockDbMeta.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, moerr.NewInternalErrorNoCtx("test")).AnyTimes()
	//	mockDbMeta.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	//
	//	c := NewCompile("test", "test", "create fulltext index ftidx on t1 (b)", "", "", eng, proc, nil, false, nil, time.Now())
	//
	//	err := indexTableBuild(c, indexTableDef, mockDbMeta)
	//	assert.Error(t, err)
	//})
}
