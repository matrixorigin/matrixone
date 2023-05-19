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

package frontend

import (
	"context"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ ComputationWrapper = &TxnComputationWrapper{}
var _ ComputationWrapper = &NullComputationWrapper{}

type NullComputationWrapper struct {
	*TxnComputationWrapper
}

func InitNullComputationWrapper(ses *Session, stmt tree.Statement, proc *process.Process) *NullComputationWrapper {
	return &NullComputationWrapper{
		TxnComputationWrapper: InitTxnComputationWrapper(ses, stmt, proc),
	}
}

func (ncw *NullComputationWrapper) GetAst() tree.Statement {
	return ncw.stmt
}

func (ncw *NullComputationWrapper) SetDatabaseName(db string) error {
	return nil
}

func (ncw *NullComputationWrapper) GetColumns() ([]interface{}, error) {
	return []interface{}{}, nil
}

func (ncw *NullComputationWrapper) GetAffectedRows() uint64 {
	return 0
}

func (ncw *NullComputationWrapper) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	return nil, nil
}

func (ncw *NullComputationWrapper) RecordExecPlan(ctx context.Context) error {
	return nil
}

func (ncw *NullComputationWrapper) GetUUID() []byte {
	return ncw.uuid[:]
}

func (ncw *NullComputationWrapper) Run(ts uint64) error {
	return nil
}

func (ncw *NullComputationWrapper) GetLoadTag() bool {
	return false
}

type TxnComputationWrapper struct {
	stmt    tree.Statement
	plan    *plan2.Plan
	proc    *process.Process
	ses     *Session
	compile *compile.Compile

	uuid uuid.UUID
}

func InitTxnComputationWrapper(ses *Session, stmt tree.Statement, proc *process.Process) *TxnComputationWrapper {
	uuid, _ := uuid.NewUUID()
	return &TxnComputationWrapper{
		stmt: stmt,
		proc: proc,
		ses:  ses,
		uuid: uuid,
	}
}

func (cwft *TxnComputationWrapper) GetAst() tree.Statement {
	return cwft.stmt
}

func (cwft *TxnComputationWrapper) GetProcess() *process.Process {
	return cwft.proc
}

func (cwft *TxnComputationWrapper) SetDatabaseName(db string) error {
	return nil
}

func (cwft *TxnComputationWrapper) GetColumns() ([]interface{}, error) {
	var err error
	cols := plan2.GetResultColumnsFromPlan(cwft.plan)
	switch cwft.GetAst().(type) {
	case *tree.ShowColumns:
		if len(cols) == 7 {
			cols = []*plan2.ColDef{
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Field"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Type"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Null"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Key"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Default"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Extra"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Comment"},
			}
		} else {
			cols = []*plan2.ColDef{
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Field"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Type"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Collation"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Null"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Key"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Default"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Extra"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Privileges"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Comment"},
			}
		}
	}
	columns := make([]interface{}, len(cols))
	for i, col := range cols {
		c := new(MysqlColumn)
		c.SetName(col.Name)
		c.SetOrgName(col.Name)
		c.SetTable(col.Typ.Table)
		c.SetOrgTable(col.Typ.Table)
		c.SetAutoIncr(col.Typ.AutoIncr)
		c.SetSchema(cwft.ses.GetTxnCompileCtx().DefaultDatabase())
		err = convertEngineTypeToMysqlType(cwft.ses.requestCtx, types.T(col.Typ.Id), c)
		if err != nil {
			return nil, err
		}
		setColFlag(c)
		setColLength(c, col.Typ.Width)
		setCharacter(c)

		// For binary/varbinary with mysql_type_varchar.Change the charset.
		if types.T(col.Typ.Id) == types.T_binary || types.T(col.Typ.Id) == types.T_varbinary {
			c.SetCharset(0x3f)
		}

		c.SetDecimal(col.Typ.Scale)
		convertMysqlTextTypeToBlobType(c)
		columns[i] = c
	}
	return columns, err
}

func (cwft *TxnComputationWrapper) GetClock() clock.Clock {
	rt := runtime.ProcessLevelRuntime()
	return rt.Clock()
}

func (cwft *TxnComputationWrapper) GetAffectedRows() uint64 {
	return cwft.compile.GetAffectedRows()
}

func (cwft *TxnComputationWrapper) Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	var err error
	defer RecordStatementTxnID(requestCtx, cwft.ses)
	if cwft.ses.IfInitedTempEngine() {
		requestCtx = context.WithValue(requestCtx, defines.TemporaryDN{}, cwft.ses.GetTempTableStorage())
		cwft.ses.SetRequestContext(requestCtx)
		cwft.proc.Ctx = context.WithValue(cwft.proc.Ctx, defines.TemporaryDN{}, cwft.ses.GetTempTableStorage())
		cwft.ses.GetTxnHandler().AttachTempStorageToTxnCtx()
	}
	cacheHit := cwft.plan != nil
	if !cacheHit {
		cwft.plan, err = buildPlan(requestCtx, cwft.ses, cwft.ses.GetTxnCompileCtx(), cwft.stmt)
	} else if cwft.ses != nil && cwft.ses.GetTenantInfo() != nil {
		cwft.ses.accountId = getAccountId(requestCtx)
		err = authenticateCanExecuteStatementAndPlan(requestCtx, cwft.ses, cwft.stmt, cwft.plan)
	}
	if err != nil {
		return nil, err
	}
	cwft.ses.p = cwft.plan
	if ids := isResultQuery(cwft.plan); ids != nil {
		if err = checkPrivilege(ids, requestCtx, cwft.ses); err != nil {
			return nil, err
		}
	}
	if _, ok := cwft.stmt.(*tree.Execute); ok {
		executePlan := cwft.plan.GetDcl().GetExecute()
		stmtName := executePlan.GetName()
		prepareStmt, err := cwft.ses.GetPrepareStmt(stmtName)
		if err != nil {
			return nil, err
		}

		// TODO check if schema change, obj.Obj is zero all the time in 0.6
		// for _, obj := range preparePlan.GetSchemas() {
		// 	newObj, _ := cwft.ses.txnCompileCtx.Resolve(obj.SchemaName, obj.ObjName)
		// 	if newObj == nil || newObj.Obj != obj.Obj {
		// 		return nil, moerr.NewInternalError("", fmt.Sprintf(ctx, "table '%s' has been changed, please reset Prepare statement '%s'", obj.ObjName, stmtName))
		// 	}
		// }

		preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare()
		if len(executePlan.Args) != len(preparePlan.ParamTypes) {
			return nil, moerr.NewInvalidInput(requestCtx, "Incorrect arguments to EXECUTE")
		}
		if prepareStmt.IsInsertValues {
			for _, node := range preparePlan.Plan.GetQuery().Nodes {
				if node.RowsetData != nil {
					tableDef := node.TableDef
					colCount := len(tableDef.Cols)
					colsData := node.RowsetData.Cols
					rowCount := len(colsData[0].Data)

					bat := prepareStmt.InsertBat
					bat.CleanOnlyData()
					for i := 0; i < colCount; i++ {
						if err = rowsetDataToVector(cwft.proc.Ctx, cwft.proc, cwft.ses.txnCompileCtx,
							colsData[i].Data, bat.Vecs[i], prepareStmt.emptyBatch, executePlan.Args, prepareStmt.ufs[i]); err != nil {
							return nil, err
						}
					}
					bat.AddCnt(1)
					for i := 0; i < rowCount; i++ {
						bat.Zs = append(bat.Zs, 1)
					}
					cwft.proc.SetPrepareBatch(bat)
					break
				}
			}
			cwft.plan = preparePlan.Plan
		} else {
			newPlan := plan2.DeepCopyPlan(preparePlan.Plan)

			// replace ? and @var with their values
			resetParamRule := plan2.NewResetParamRefRule(requestCtx, executePlan.Args)
			resetVarRule := plan2.NewResetVarRefRule(cwft.ses.GetTxnCompileCtx(), cwft.ses.GetTxnCompileCtx().GetProcess())
			constantFoldRule := plan2.NewConstantFoldRule(cwft.ses.GetTxnCompileCtx())
			vp := plan2.NewVisitPlan(newPlan, []plan2.VisitPlanRule{resetParamRule, resetVarRule, constantFoldRule})
			err = vp.Visit(requestCtx)
			if err != nil {
				return nil, err
			}
			cwft.plan = newPlan
		}

		// reset plan & stmt
		cwft.stmt = prepareStmt.PrepareStmt
		// reset some special stmt for execute statement
		switch cwft.stmt.(type) {
		case *tree.ShowTableStatus:
			cwft.ses.showStmtType = ShowTableStatus
			cwft.ses.SetData(nil)
		case *tree.SetVar, *tree.ShowVariables, *tree.ShowErrors, *tree.ShowWarnings:
			return nil, nil
		}

		//check privilege
		/* prepare not need check privilege
		   err = authenticateUserCanExecutePrepareOrExecute(requestCtx, cwft.ses, prepareStmt.PrepareStmt, newPlan)
		   if err != nil {
		   	return nil, err
		   }
		*/
	} else {
		var vp *plan2.VisitPlan
		if cacheHit {
			vp = plan2.NewVisitPlan(cwft.plan, []plan2.VisitPlanRule{plan2.NewResetVarRefRule(cwft.ses.GetTxnCompileCtx(), cwft.ses.GetTxnCompileCtx().GetProcess()), plan2.NewRecomputeRealTimeRelatedFuncRule(cwft.ses.GetTxnCompileCtx().GetProcess())})
		} else {
			vp = plan2.NewVisitPlan(cwft.plan, []plan2.VisitPlanRule{plan2.NewResetVarRefRule(cwft.ses.GetTxnCompileCtx(), cwft.ses.GetTxnCompileCtx().GetProcess())})
		}
		err = vp.Visit(requestCtx)
		if err != nil {
			return nil, err
		}
	}

	txnHandler := cwft.ses.GetTxnHandler()
	var txnCtx context.Context
	txnCtx, cwft.proc.TxnOperator, err = txnHandler.GetTxn()
	if err != nil {
		return nil, err
	}
	addr := ""
	if len(cwft.ses.GetParameterUnit().ClusterNodes) > 0 {
		addr = cwft.ses.GetParameterUnit().ClusterNodes[0].Addr
	}
	cwft.proc.Ctx = txnCtx
	cwft.proc.FileService = cwft.ses.GetParameterUnit().FileService

	var tenant string
	tInfo := cwft.ses.GetTenantInfo()
	if tInfo != nil {
		tenant = tInfo.GetTenant()
	}
	cwft.compile = compile.New(
		addr,
		cwft.ses.GetDatabaseName(),
		cwft.ses.GetSql(),
		tenant,
		cwft.ses.GetUserName(),
		txnCtx,
		cwft.ses.GetStorage(),
		cwft.proc,
		cwft.stmt,
		cwft.ses.isInternal,
		cwft.ses.getCNLabels(),
	)

	if _, ok := cwft.stmt.(*tree.ExplainAnalyze); ok {
		fill = func(obj interface{}, bat *batch.Batch) error { return nil }
	}
	err = cwft.compile.Compile(txnCtx, cwft.plan, cwft.ses, fill)
	if err != nil {
		return nil, err
	}
	// check if it is necessary to initialize the temporary engine
	if cwft.compile.NeedInitTempEngine(cwft.ses.IfInitedTempEngine()) {
		// 0. init memory-non-dist storage
		dnStore, err := cwft.ses.SetTempTableStorage(cwft.GetClock())
		if err != nil {
			return nil, err
		}

		// temporary storage is passed through Ctx
		requestCtx = context.WithValue(requestCtx, defines.TemporaryDN{}, cwft.ses.GetTempTableStorage())

		// 1. init memory-non-dist engine
		tempEngine := memoryengine.New(
			requestCtx,
			memoryengine.NewDefaultShardPolicy(
				mpool.MustNewZeroNoFixed(),
			),
			memoryengine.RandomIDGenerator,
			clusterservice.NewMOCluster(
				nil,
				0,
				clusterservice.WithDisableRefresh(),
				clusterservice.WithServices(nil, []metadata.DNService{
					*dnStore,
				})),
		)

		// 2. bind the temporary engine to the session and txnHandler
		_ = cwft.ses.SetTempEngine(requestCtx, tempEngine)
		cwft.compile.SetTempEngine(requestCtx, tempEngine)
		txnHandler.SetTempEngine(tempEngine)
		cwft.ses.GetTxnHandler().AttachTempStorageToTxnCtx()

		// 3. init temp-db to store temporary relations
		err = tempEngine.Create(requestCtx, defines.TEMPORARY_DBNAME, cwft.ses.txnHandler.txnOperator)
		if err != nil {
			return nil, err
		}

		// 4. add auto_IncrementTable fortemp-db
		err = colexec.CreateAutoIncrTable(cwft.ses.GetStorage(), requestCtx, cwft.proc, defines.TEMPORARY_DBNAME)
		if err != nil {
			return nil, err
		}

		cwft.ses.EnableInitTempEngine()
	}
	return cwft.compile, err
}

func (cwft *TxnComputationWrapper) RecordExecPlan(ctx context.Context) error {
	if stm := motrace.StatementFromContext(ctx); stm != nil {
		stm.SetExecPlan(cwft.plan, SerializeExecPlan)
	}
	return nil
}

func (cwft *TxnComputationWrapper) GetUUID() []byte {
	return cwft.uuid[:]
}

func (cwft *TxnComputationWrapper) Run(ts uint64) error {
	logDebugf(cwft.ses.GetDebugString(), "compile.Run begin")
	defer func() {
		logDebugf(cwft.ses.GetDebugString(), "compile.Run end")
	}()
	err := cwft.compile.Run(ts)
	return err
}

func (cwft *TxnComputationWrapper) GetLoadTag() bool {
	return cwft.plan.GetQuery().GetLoadTag()
}
