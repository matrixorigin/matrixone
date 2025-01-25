// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

//func Test_handleCreateDynamicTable(t *testing.T) {
//	genSession := func(ctrl *gomock.Controller, pu *config.ParameterUnit) *Session {
//		sv, err := getSystemVariables("test/system_vars_config.toml")
//		if err != nil {
//			t.Error(err)
//		}
//
//		ioses, err := NewIOSession(&testConn{}, pu, "")
//		if err != nil {
//			t.Error(err)
//		}
//		proto := NewMysqlClientProtocol("", 0, ioses, 1024, sv)
//		ctx := defines.AttachAccountId(context.Background(), sysAccountID)
//		session := NewSession(ctx, "", proto, nil)
//		return session
//	}
//
//	//handleCreateDynamicTable(tt.args.ctx, tt.args.ses, tt.args.st)
//	convey.Convey("test", t, func() {
//		ctrl := gomock.NewController(t)
//		defer ctrl.Finish()
//		testutil.SetupAutoIncrService("")
//		ctx := context.TODO()
//		txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
//		txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
//		txnOperator.EXPECT().Commit(ctx).Return(nil).AnyTimes()
//		txnOperator.EXPECT().Rollback(ctx).Return(nil).AnyTimes()
//		txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
//		txnOperator.EXPECT().EnterRunSql().Return().AnyTimes()
//		txnOperator.EXPECT().ExitRunSql().Return().AnyTimes()
//		txnOperator.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).Return().AnyTimes()
//		txnClient := mock_frontend.NewMockTxnClient(ctrl)
//		txnClient.EXPECT().New(gomock.Any(), gomock.Any(), gomock.Any()).Return(txnOperator, nil).AnyTimes()
//		eng := mock_frontend.NewMockEngine(ctrl)
//		eng.EXPECT().New(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
//		eng.EXPECT().Hints().Return(engine.Hints{
//			CommitOrRollbackTimeout: time.Second,
//		}).AnyTimes()
//
//		db := mock_frontend.NewMockDatabase(ctrl)
//		db.EXPECT().Relations(gomock.Any()).Return(nil, nil).AnyTimes()
//
//		table := mock_frontend.NewMockRelation(ctrl)
//		table.EXPECT().Ranges(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
//		table.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
//		table.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()
//		table.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{}).AnyTimes()
//		table.EXPECT().GetPrimaryKeys(gomock.Any()).Return(nil, nil).AnyTimes()
//		table.EXPECT().GetHideKeys(gomock.Any()).Return(nil, nil).AnyTimes()
//		table.EXPECT().Stats(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
//		table.EXPECT().TableColumns(gomock.Any()).Return(nil, nil).AnyTimes()
//		table.EXPECT().GetTableID(gomock.Any()).Return(uint64(10)).AnyTimes()
//		table.EXPECT().GetEngineType().Return(engine.Disttae).AnyTimes()
//		table.EXPECT().ApproxObjectsNum(gomock.Any()).Return(0).AnyTimes()
//		db.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(table, nil).AnyTimes()
//		db.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
//		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(db, nil).AnyTimes()
//
//		pu := config.NewParameterUnit(&config.FrontendParameters{}, eng, txnClient, nil)
//		pu.TaskService = &testTaskService{}
//		setPu("", pu)
//		setSessionAlloc("", NewLeakCheckAllocator())
//		ses := genSession(ctrl, pu)
//		ses.GetTxnHandler().txnOp = txnOperator
//
//		tcc := ses.GetTxnCompileCtx()
//		tcc.execCtx = &ExecCtx{reqCtx: ctx, ses: ses}
//
//		ast, _ := mysql.ParseOne(ctx, "CREATE TABLE abc.t1 (id INT NOT NULL,name varchar(50) NOT NULL,PRIMARY KEY (id));", 0)
//
//		gostub.Stub(&buildPlanWithAuthorization, func(reqCtx context.Context, ses FeSession, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
//			return nil, moerr.NewInternalErrorNoCtx("")
//		})
//
//		err := handleCreateDynamicTable(ctx, ses, ast.(*tree.CreateTable))
//		convey.So(err, convey.ShouldNotBeNil)
//	})
//}
