// Copyright 2024 Matrix Origin
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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/sync/errgroup"
)

func handleCreateFunction(ctx context.Context, ses FeSession, cf *tree.CreateFunction) error {
	tenant := ses.GetTenantInfo()
	return InitFunction(ctx, ses.(*Session), tenant, cf)
}

func handleDropFunction(ctx context.Context, ses FeSession, df *tree.DropFunction, proc *process.Process) error {
	return doDropFunction(ctx, ses.(*Session), df, func(path string) error {
		return proc.FileService.Delete(ctx, path)
	})
}

func handleCreateProcedure(ctx context.Context, ses FeSession, cp *tree.CreateProcedure) error {
	tenant := ses.GetTenantInfo()

	return InitProcedure(ctx, ses.(*Session), tenant, cp)
}

func handleDropProcedure(ctx context.Context, ses FeSession, dp *tree.DropProcedure) error {
	return doDropProcedure(ctx, ses.(*Session), dp)
}

func handleCallProcedure(ctx context.Context, ses FeSession, call *tree.CallStmt, proc *process.Process) error {
	proto := ses.GetMysqlProtocol()
	results, err := doInterpretCall(ctx, ses.(*Session), call)
	if err != nil {
		return err
	}

	ses.SetMysqlResultSet(nil)

	resp := NewGeneralOkResponse(COM_QUERY, ses.GetTxnHandler().GetServerStatus())

	if len(results) == 0 {
		if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
			return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
		}
	} else {
		for i, result := range results {
			mer := NewMysqlExecutionResult(0, 0, 0, 0, result.(*MysqlResultSet))
			resp = ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, i == len(results)-1)
			if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
				return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
			}
		}
	}
	return nil
}

func Upload(ctx context.Context, ses FeSession, localPath string, storageDir string) (string, error) {
	loadLocalReader, loadLocalWriter := io.Pipe()

	// watch and cancel
	// TODO use context.AfterFunc in go1.21
	funcCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer loadLocalReader.Close()

		<-funcCtx.Done()
	}()

	// write to pipe
	loadLocalErrGroup := new(errgroup.Group)
	loadLocalErrGroup.Go(func() error {
		param := &tree.ExternParam{
			ExParamConst: tree.ExParamConst{
				Filepath: localPath,
			},
		}
		return processLoadLocal(ctx, ses, param, loadLocalWriter)
	})

	// read from pipe and upload
	ioVector := fileservice.IOVector{
		FilePath: fileservice.JoinPath(defines.SharedFileServiceName, path.Join("udf", storageDir, localPath[strings.LastIndex(localPath, "/")+1:])),
		Entries: []fileservice.IOEntry{
			{
				Size:           -1,
				ReaderForWrite: loadLocalReader,
			},
		},
	}

	fileService := gPu.FileService
	_ = fileService.Delete(ctx, ioVector.FilePath)
	err := fileService.Write(ctx, ioVector)
	err = errors.Join(err, loadLocalErrGroup.Wait())
	if err != nil {
		return "", err
	}

	return ioVector.FilePath, nil
}

func InitFunction(ctx context.Context, ses *Session, tenant *TenantInfo, cf *tree.CreateFunction) (err error) {
	var initMoUdf string
	var retTypeStr string
	var dbName string
	var dbExists bool
	var checkExistence string
	var argsJson []byte
	var argsCondition string
	var fmtctx *tree.FmtCtx
	var argList []*function.Arg
	var typeList []string
	var erArray []ExecResult

	// a database must be selected or specified as qualifier when create a function
	if cf.Name.HasNoNameQualifier() {
		if ses.DatabaseNameIsEmpty() {
			return moerr.NewNoDBNoCtx()
		}
		dbName = ses.GetDatabaseName()
	} else {
		dbName = string(cf.Name.Name.SchemaName)
	}

	// authticate db exists
	dbExists, err = checkDatabaseExistsOrNot(ctx, ses.GetBackgroundExec(ctx), dbName)
	if err != nil {
		return err
	}
	if !dbExists {
		return moerr.NewBadDB(ctx, dbName)
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// format return type
	fmtctx = tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
	retTypeStr, err = plan2.GetFunctionTypeStrFromAst(cf.ReturnType.Type)
	if err != nil {
		return err
	}

	// build argmap and marshal as json
	argList = make([]*function.Arg, len(cf.Args))
	typeList = make([]string, len(cf.Args))
	for i := 0; i < len(cf.Args); i++ {
		argList[i] = &function.Arg{}
		argList[i].Name = cf.Args[i].GetName(fmtctx)
		fmtctx.Reset()
		typ, err := plan2.GetFunctionArgTypeStrFromAst(cf.Args[i])
		if err != nil {
			return err
		}
		argList[i].Type = typ
		typeList[i] = typ
	}
	argsJson, err = json.Marshal(argList)
	if err != nil {
		return err
	}

	if len(typeList) == 0 {
		argsCondition = "is null"
	} else if len(typeList) == 1 {
		argsCondition = fmt.Sprintf(`= '"%v"'`, typeList[0])
	} else {
		typesJson, _ := json.Marshal(typeList)
		argsCondition = fmt.Sprintf(`= '%v'`, string(typesJson))
	}

	// validate duplicate function declaration
	bh.ClearExecResultSet()
	checkExistence = fmt.Sprintf(checkUdfExistence, string(cf.Name.Name.ObjectName), dbName, argsCondition)
	err = bh.Exec(ctx, checkExistence)
	if err != nil {
		return err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	if execResultArrayHasData(erArray) && !cf.Replace {
		return moerr.NewUDFAlreadyExistsNoCtx(string(cf.Name.Name.ObjectName))
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	var body string
	if cf.Language == string(tree.SQL) {
		body = cf.Body
	} else {
		if cf.Import {
			// check
			if cf.Language == string(tree.PYTHON) {
				if !strings.HasSuffix(cf.Body, ".py") &&
					!strings.HasSuffix(cf.Body, ".whl") {
					return moerr.NewInvalidInput(ctx, "file '"+cf.Body+"', only support '*.py', '*.whl'")
				}
				if strings.HasSuffix(cf.Body, ".whl") {
					dotIdx := strings.LastIndex(cf.Handler, ".")
					if dotIdx < 1 {
						return moerr.NewInvalidInput(ctx, "handler '"+cf.Handler+"', when you import a *.whl, the handler should be in the format of '<file or module name>.<function name>'")
					}
				}
			}
			// upload
			storageDir := string(cf.Name.Name.ObjectName) + "_" + strings.Join(typeList, "-") + "_"
			cf.Body, err = Upload(ctx, ses, cf.Body, storageDir)
			if err != nil {
				return err
			}
		}

		nb := function.NonSqlUdfBody{
			Handler: cf.Handler,
			Import:  cf.Import,
			Body:    cf.Body,
		}
		var byt []byte
		byt, err = json.Marshal(nb)
		if err != nil {
			return err
		}
		body = strconv.Quote(string(byt))
		body = body[1 : len(body)-1]
	}

	if execResultArrayHasData(erArray) { // replace
		var id int64
		id, err = erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return err
		}
		initMoUdf = fmt.Sprintf(updateMoUserDefinedFunctionFormat,
			ses.GetTenantInfo().GetDefaultRoleID(),
			string(argsJson),
			retTypeStr, body, cf.Language,
			tenant.User, types.CurrentTimestamp().String2(time.UTC, 0), "FUNCTION", "DEFINER", "", "utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci",
			int32(id))
	} else { // create
		initMoUdf = fmt.Sprintf(initMoUserDefinedFunctionFormat,
			string(cf.Name.Name.ObjectName),
			ses.GetTenantInfo().GetDefaultRoleID(),
			string(argsJson),
			retTypeStr, body, cf.Language, dbName,
			tenant.User, types.CurrentTimestamp().String2(time.UTC, 0), types.CurrentTimestamp().String2(time.UTC, 0), "FUNCTION", "DEFINER", "", "utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci")
	}

	err = bh.Exec(ctx, initMoUdf)
	if err != nil {
		return err
	}

	return err
}

func InitProcedure(ctx context.Context, ses *Session, tenant *TenantInfo, cp *tree.CreateProcedure) (err error) {
	var initMoProcedure string
	var dbName string
	var checkExistence string
	var argsJson []byte
	// var fmtctx *tree.FmtCtx
	var erArray []ExecResult

	// a database must be selected or specified as qualifier when create a function
	if cp.Name.HasNoNameQualifier() {
		if ses.DatabaseNameIsEmpty() {
			return moerr.NewNoDBNoCtx()
		}
		dbName = ses.GetDatabaseName()
	} else {
		dbName = string(cp.Name.Name.SchemaName)
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// build argmap and marshal as json
	fmtctx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))

	// build argmap and marshal as json
	argList := make(map[string]tree.ProcedureArgForMarshal)
	for i := 0; i < len(cp.Args); i++ {
		curName := cp.Args[i].GetName(fmtctx)
		fmtctx.Reset()
		argList[curName] = tree.ProcedureArgForMarshal{
			Name:      cp.Args[i].(*tree.ProcedureArgDecl).Name,
			Type:      cp.Args[i].(*tree.ProcedureArgDecl).Type,
			InOutType: cp.Args[i].(*tree.ProcedureArgDecl).InOutType,
		}
	}
	argsJson, err = json.Marshal(argList)
	if err != nil {
		return err
	}

	// validate duplicate procedure declaration
	bh.ClearExecResultSet()
	checkExistence = getSqlForCheckProcedureExistence(string(cp.Name.Name.ObjectName), dbName)
	err = bh.Exec(ctx, checkExistence)
	if err != nil {
		return err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	if execResultArrayHasData(erArray) {
		return moerr.NewProcedureAlreadyExistsNoCtx(string(cp.Name.Name.ObjectName))
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	initMoProcedure = fmt.Sprintf(initMoStoredProcedureFormat,
		string(cp.Name.Name.ObjectName),
		string(argsJson),
		cp.Body, dbName,
		tenant.User, types.CurrentTimestamp().String2(time.UTC, 0), types.CurrentTimestamp().String2(time.UTC, 0), "PROCEDURE", "DEFINER", "", "utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci")
	err = bh.Exec(ctx, initMoProcedure)
	if err != nil {
		return err
	}
	return err
}

func doInterpretCall(ctx context.Context, ses *Session, call *tree.CallStmt) ([]ExecResult, error) {
	// fetch related
	var spBody string
	var dbName string
	var sql string
	var argstr string
	var err error
	var erArray []ExecResult
	var argList map[string]tree.ProcedureArgForMarshal
	// execute related
	var interpreter Interpreter
	var varScope [](map[string]interface{})
	var argsMap map[string]tree.Expr
	var argsAttr map[string]tree.InOutArgType

	// a database must be selected or specified as qualifier when create a function
	if call.Name.HasNoNameQualifier() {
		if ses.DatabaseNameIsEmpty() {
			return nil, moerr.NewNoDBNoCtx()
		}
		dbName = ses.GetDatabaseName()
	} else {
		dbName = string(call.Name.Name.SchemaName)
	}

	sql, err = getSqlForSpBody(ctx, string(call.Name.Name.ObjectName), dbName)
	if err != nil {
		return nil, err
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	bh.ClearExecResultSet()

	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if execResultArrayHasData(erArray) {
		// function with provided name and db exists, for now we don't support overloading for stored procedure, so go to handle deletion.
		spBody, err = erArray[0].GetString(ctx, 0, 0)
		if err != nil {
			return nil, err
		}
		argstr, err = erArray[0].GetString(ctx, 0, 1)
		if err != nil {
			return nil, err
		}

		// perform argument length validation
		// postpone argument type check until actual execution of its procedure body. This will be handled by the binder.
		err = json.Unmarshal([]byte(argstr), &argList)
		if err != nil {
			return nil, err
		}
		if len(argList) != len(call.Args) {
			return nil, moerr.NewInvalidArg(ctx, string(call.Name.Name.ObjectName)+" procedure have invalid input args length", len(call.Args))
		}
	} else {
		return nil, moerr.NewNoUDFNoCtx(string(call.Name.Name.ObjectName))
	}

	stmt, err := parsers.Parse(ctx, dialect.MYSQL, spBody, 1, 0)
	if err != nil {
		return nil, err
	}

	fmtctx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))

	argsAttr = make(map[string]tree.InOutArgType)
	argsMap = make(map[string]tree.Expr) // map arg to param

	// build argsAttr and argsMap
	logutil.Info("Interpret procedure call length:" + strconv.Itoa(len(argList)))
	i := 0
	for curName, v := range argList {
		argsAttr[curName] = v.InOutType
		argsMap[curName] = call.Args[i]
		i++
	}

	interpreter.ctx = ctx
	interpreter.fmtctx = fmtctx
	interpreter.ses = ses
	interpreter.varScope = &varScope
	interpreter.bh = bh
	interpreter.result = nil
	interpreter.argsMap = argsMap
	interpreter.argsAttr = argsAttr
	interpreter.outParamMap = make(map[string]interface{})

	err = interpreter.ExecuteSp(stmt[0], dbName)
	if err != nil {
		return nil, err
	}
	return interpreter.GetResult(), nil
}

type rmPkg func(path string) error

func doDropFunction(ctx context.Context, ses *Session, df *tree.DropFunction, rm rmPkg) (err error) {
	var sql string
	var argstr string
	var bodyStr string
	var checkDatabase string
	var dbName string
	var funcId int64
	var erArray []ExecResult

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// a database must be selected or specified as qualifier when create a function
	if df.Name.HasNoNameQualifier() {
		if ses.DatabaseNameIsEmpty() {
			return moerr.NewNoDBNoCtx()
		}
		dbName = ses.GetDatabaseName()
	} else {
		dbName = string(df.Name.Name.SchemaName)
	}

	// validate database name and signature (name + args)
	bh.ClearExecResultSet()
	checkDatabase = fmt.Sprintf(checkUdfArgs, string(df.Name.Name.ObjectName), dbName)
	err = bh.Exec(ctx, checkDatabase)
	if err != nil {
		return err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	if execResultArrayHasData(erArray) {
		receivedArgsType := make([]string, len(df.Args))
		for i, arg := range df.Args {
			typ, err := plan2.GetFunctionArgTypeStrFromAst(arg)
			if err != nil {
				return err
			}
			receivedArgsType[i] = typ
		}

		// function with provided name and db exists, now check arguments
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			argstr, err = erArray[0].GetString(ctx, i, 0)
			if err != nil {
				return err
			}
			funcId, err = erArray[0].GetInt64(ctx, i, 1)
			if err != nil {
				return err
			}
			bodyStr, err = erArray[0].GetString(ctx, i, 2)
			if err != nil {
				return err
			}
			argList := make([]*function.Arg, 0)
			json.Unmarshal([]byte(argstr), &argList)
			if len(argList) == len(df.Args) {
				match := true
				for j, arg := range argList {
					typ := receivedArgsType[j]
					if arg.Type != typ {
						match = false
						break
					}
				}
				if !match {
					continue
				}
				handleArgMatch := func() (rtnErr error) {
					//put it into the single transaction
					rtnErr = bh.Exec(ctx, "begin;")
					defer func() {
						rtnErr = finishTxn(ctx, bh, rtnErr)
						if rtnErr == nil {
							u := &function.NonSqlUdfBody{}
							if json.Unmarshal([]byte(bodyStr), u) == nil && u.Import {
								rm(u.Body)
							}
						}
					}()
					if rtnErr != nil {
						return rtnErr
					}

					sql = fmt.Sprintf(deleteUserDefinedFunctionFormat, funcId)

					rtnErr = bh.Exec(ctx, sql)
					if rtnErr != nil {
						return rtnErr
					}
					return rtnErr
				}
				return handleArgMatch()
			}
		}
	}
	// no such function
	return moerr.NewNoUDFNoCtx(string(df.Name.Name.ObjectName))
}

func doDropProcedure(ctx context.Context, ses *Session, dp *tree.DropProcedure) (err error) {
	var sql string
	var checkDatabase string
	var dbName string
	var procId int64
	var erArray []ExecResult

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	if dp.Name.HasNoNameQualifier() {
		if ses.DatabaseNameIsEmpty() {
			return moerr.NewNoDBNoCtx()
		}
		dbName = ses.GetDatabaseName()
	} else {
		dbName = string(dp.Name.Name.SchemaName)
	}

	// validate database name and signature (name + args)
	bh.ClearExecResultSet()
	checkDatabase = fmt.Sprintf(checkStoredProcedureArgs, string(dp.Name.Name.ObjectName), dbName)
	err = bh.Exec(ctx, checkDatabase)
	if err != nil {
		return err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	if execResultArrayHasData(erArray) {
		// function with provided name and db exists, for now we don't support overloading for stored procedure, so go to handle deletion.
		procId, err = erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return err
		}
		handleArgMatch := func() (rtnErr error) {
			//put it into the single transaction
			rtnErr = bh.Exec(ctx, "begin;")
			defer func() {
				rtnErr = finishTxn(ctx, bh, rtnErr)
			}()
			if rtnErr != nil {
				return rtnErr
			}

			sql = fmt.Sprintf(deleteStoredProcedureFormat, procId)

			rtnErr = bh.Exec(ctx, sql)
			if rtnErr != nil {
				return rtnErr
			}
			return rtnErr
		}
		return handleArgMatch()
	} else {
		// no such procedure
		if dp.IfExists {
			return nil
		}
		return moerr.NewNoUDFNoCtx(string(dp.Name.Name.ObjectName))
	}
}

func doDropFunctionWithDB(ctx context.Context, ses *Session, stmt tree.Statement, rm rmPkg) (err error) {
	var sql string
	var bodyStr string
	var funcId int64
	var erArray []ExecResult
	var dbName string

	switch st := stmt.(type) {
	case *tree.DropDatabase:
		dbName = string(st.Name)
	default:
	}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	// validate database name and signature (name + args)
	bh.ClearExecResultSet()
	sql = getSqlForCheckUdfWithDb(dbName)
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}

	if execResultArrayHasData(erArray) {
		for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
			funcId, err = erArray[0].GetInt64(ctx, i, 0)
			if err != nil {
				return err
			}
			bodyStr, err = erArray[0].GetString(ctx, i, 1)
			if err != nil {
				return err
			}

			handleArgMatch := func() (rtnErr error) {
				//put it into the single transaction
				rtnErr = bh.Exec(ctx, "begin;")
				defer func() {
					rtnErr = finishTxn(ctx, bh, rtnErr)
					if rtnErr == nil {
						u := &function.NonSqlUdfBody{}
						if json.Unmarshal([]byte(bodyStr), u) == nil && u.Import {
							rm(u.Body)
						}
					}
				}()
				if rtnErr != nil {
					return rtnErr
				}

				sql = fmt.Sprintf(deleteUserDefinedFunctionFormat, funcId)

				rtnErr = bh.Exec(ctx, sql)
				if rtnErr != nil {
					return rtnErr
				}
				return rtnErr
			}

			err = handleArgMatch()
			if err != nil {
				return err
			}
		}
	}

	return err
}
