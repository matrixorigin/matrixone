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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/objectkey"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	commonutil "github.com/matrixorigin/matrixone/pkg/common/util"
	mo_config "github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	planrule "github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/util/debug/goroutine"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type CloseFlag struct {
	//closed flag
	closed uint32
}

// 1 for closed
// 0 for others
func (cf *CloseFlag) setClosed(value uint32) {
	atomic.StoreUint32(&cf.closed, value)
}

func (cf *CloseFlag) Open() {
	cf.setClosed(0)
}

func (cf *CloseFlag) Close() {
	cf.setClosed(1)
}

func (cf *CloseFlag) IsClosed() bool {
	return atomic.LoadUint32(&cf.closed) != 0
}

func (cf *CloseFlag) IsOpened() bool {
	return atomic.LoadUint32(&cf.closed) == 0
}

func Max(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}

// GetRoutineId gets the routine id
func GetRoutineId() uint64 {
	return goroutine.GetRoutineId()
}

/*
path exists in the system
return:
true/false - exists or not.
true/false - file or directory
error
*/
var PathExists = func(path string) (bool, bool, error) {
	fi, err := os.Stat(path)
	if err == nil {
		return true, !fi.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, false, err
	}

	return false, false, err
}

func getSystemVariables(configFile string) (*mo_config.FrontendParameters, error) {
	sv := &mo_config.FrontendParameters{
		MongoDB: *mo_config.NewMongoDBParameters(),
	}
	var err error
	_, err = toml.DecodeFile(configFile, sv)
	if err != nil {
		return nil, err
	}
	return sv, err
}

func getParameterUnit(configFile string, eng engine.Engine, txnClient TxnClient) (*mo_config.ParameterUnit, error) {
	sv, err := getSystemVariables(configFile)
	if err != nil {
		return nil, err
	}
	sv.SetDefaultValues()
	pu := mo_config.NewParameterUnit(sv, eng, txnClient, engine.Nodes{})

	return pu, nil
}

// WildcardMatch implements wildcard pattern match algorithm.
// pattern and target are ascii characters
// TODO: add \_ and \%
func WildcardMatch(pattern, target string) bool {
	var p = 0
	var t = 0
	var positionOfPercentPlusOne int = -1
	var positionOfTargetEncounterPercent int = -1
	plen := len(pattern)
	tlen := len(target)
	for t < tlen {
		//%
		if p < plen && pattern[p] == '%' {
			p++
			positionOfPercentPlusOne = p
			if p >= plen {
				//pattern end with %
				return true
			}
			//means % matches empty
			positionOfTargetEncounterPercent = t
		} else if p < plen && (pattern[p] == '_' || pattern[p] == target[t]) { //match or _
			p++
			t++
		} else {
			if positionOfPercentPlusOne == -1 {
				//have not matched a %
				return false
			}
			if positionOfTargetEncounterPercent == -1 {
				return false
			}
			//backtrace to last % position + 1
			p = positionOfPercentPlusOne
			//means % matches multiple characters
			positionOfTargetEncounterPercent++
			t = positionOfTargetEncounterPercent
		}
	}
	//skip %
	for p < plen && pattern[p] == '%' {
		p++
	}
	return p >= plen
}

// getExprValue executes the expression and returns the value.
func getExprValue(e tree.Expr, ses *Session, execCtx *ExecCtx, isBin ...*bool) (interface{}, error) {
	return getExprValueWithPrepareMode(e, ses, execCtx, false, isBin...)
}

func getExprValueWithPrepareMode(
	e tree.Expr,
	ses *Session,
	execCtx *ExecCtx,
	preparedExpression bool,
	isBin ...*bool,
) (interface{}, error) {
	value, _, err := getExprValueWithPrepareMeta(e, ses, execCtx, preparedExpression, nil, nil, isBin...)
	return value, err
}

func getExprValueWithPrepareMeta(
	e tree.Expr,
	ses *Session,
	execCtx *ExecCtx,
	preparedExpression bool,
	materializedResult **plan.Expr,
	prepareParamKind *vector.PrepareParamKind,
	isBin ...*bool,
) (interface{}, plan.Type, error) {
	/*
		CORNER CASE:
			SET character_set_results = utf8; // e = tree.UnresolvedName{'utf8'}.

			tree.UnresolvedName{'utf8'} can not be resolved as the column of some table.
	*/
	switch v := e.(type) {
	case *tree.UnresolvedName:
		// set @a = on, type of a is bool.
		if len(isBin) > 0 {
			*isBin[0] = false
		}
		if prepareParamKind != nil {
			*prepareParamKind = vector.PrepareParamNone
		}
		return v.ColName(), plan.Type{Id: int32(types.T_text)}, nil
	}

	var err error

	table := &tree.TableName{}
	table.ObjectName = "dual"

	//1.composite the 'select (expr) from dual'
	compositedSelect := &tree.Select{
		Select: &tree.SelectClause{
			Exprs: tree.SelectExprs{
				tree.SelectExpr{
					Expr: e,
				},
			},
			From: &tree.From{
				Tables: tree.TableExprs{
					&tree.JoinTableExpr{
						JoinType: tree.JOIN_TYPE_CROSS,
						Left: &tree.AliasedTableExpr{
							Expr: table,
						},
					},
				},
			},
		},
	}

	//2.run the select

	//run the statement in the same session
	ses.ClearResultBatches()
	//!!!different ExecCtx
	tempExecCtx := ExecCtx{
		reqCtx: execCtx.reqCtx,
		ses:    ses,
	}
	defer func() {
		// The synthetic SELECT is executed through doComQuery, which points the
		// session compiler context at tempExecCtx.  Restore the caller's context
		// before the next statement in a multi-statement packet is planned;
		// tempExecCtx.Close clears its request context and would otherwise leave
		// a nil context/process behind.
		tempExecCtx.Close()
		if tcc := ses.GetTxnCompileCtx(); tcc != nil {
			tcc.SetExecCtx(execCtx)
		}
	}()
	var preparedParamVals []any
	var preparedBinaryExecute bool
	if preparedExpression && execCtx.cw != nil {
		preparedParamVals = execCtx.cw.ParamVals()
		preparedBinaryExecute = execCtx.input != nil && execCtx.input.isBinaryProtExecute
	}
	err = executeStmtInSameSession(
		tempExecCtx.reqCtx, ses, &tempExecCtx, compositedSelect,
		preparedExpression, preparedParamVals, preparedBinaryExecute)
	if err != nil {
		return nil, plan.Type{}, err
	}

	batches := ses.GetResultBatches()
	if len(batches) == 0 {
		return nil, plan.Type{}, moerr.NewInternalErrorf(execCtx.reqCtx, "the expr %s does not generate a value", e.String())
	}

	if batches[0].VectorCount() > 1 {
		return nil, plan.Type{}, moerr.NewInternalErrorf(execCtx.reqCtx, "the expr %s generates multi columns value", e.String())
	}

	//evaluate the count of rows, the count of columns
	count := 0
	var resultVec *vector.Vector
	for _, b := range batches {
		if b.RowCount() == 0 {
			continue
		}
		count += b.RowCount()
		if count > 1 {
			return nil, plan.Type{}, moerr.NewInternalErrorf(execCtx.reqCtx, "the expr %s generates multi rows value", e.String())
		}
		if resultVec == nil && b.GetVector(0).Length() != 0 {
			resultVec = b.GetVector(0)
		}
	}

	if resultVec == nil {
		return nil, plan.Type{}, moerr.NewInternalErrorf(execCtx.reqCtx, "the expr %s does not generate a value", e.String())
	}

	// for the decimal type, we need the type of expr
	//!!!NOTE: the type here may be different from the one in the result vector.
	var planExpr *plan.Expr
	oid := resultVec.GetType().Oid
	if oid == types.T_decimal64 || oid == types.T_decimal128 || oid == types.T_decimal256 {
		planExpr, err = bindSetVariableResultExpr(
			e, ses.GetTxnCompileCtx(), preparedExpression)
		if err != nil {
			return nil, plan.Type{}, err
		}
	}

	if len(isBin) > 0 {
		*isBin[0] = resultVec.GetIsBin()
	}
	if prepareParamKind != nil {
		*prepareParamKind = resultVec.GetPrepareParamKind()
		if *prepareParamKind == vector.PrepareParamNone {
			*prepareParamKind, err = transparentPrepareParamKind(e, ses)
			if err != nil {
				return nil, plan.Type{}, err
			}
		}
		if *prepareParamKind == vector.PrepareParamNone {
			*prepareParamKind = prepareParamKindFromType(resultVec.GetType().Oid)
		}
	}
	resultType := plan2.MakePlan2Type(resultVec.GetType())
	value, err := getValueFromVector(execCtx.reqCtx, resultVec, ses, planExpr)
	if err != nil {
		return nil, plan.Type{}, err
	}
	if materializedResult != nil {
		literal := planrule.GetConstantValue(resultVec, false, 0)
		if literal == nil && resultVec.GetType().Oid == types.T_enum {
			literal = planrule.GetConstantValue(resultVec, true, 0)
		}
		if literal != nil {
			*materializedResult = &plan.Expr{Typ: resultType, Expr: &plan.Expr_Lit{Lit: literal}}
		} else {
			source := plan2.MakePlan2StringConstExprWithType(fmt.Sprintf("%v", value))
			target := &plan.Expr{Typ: resultType, Expr: &plan.Expr_T{T: &plan.TargetType{}}}
			*materializedResult, err = plan2.BindFuncExprImplByPlanExpr(
				execCtx.reqCtx, "cast", []*plan.Expr{source, target})
			if err != nil {
				return nil, plan.Type{}, err
			}
		}
	}
	return value, resultType, nil
}

func collectScalarSubqueries(expr tree.Expr, subqueries *[]*tree.Subquery) {
	if expr == nil {
		return
	}
	switch current := expr.(type) {
	case *tree.Subquery:
		*subqueries = append(*subqueries, current)
	case *tree.ComparisonExpr:
		collectScalarSubqueries(current.Left, subqueries)
		collectScalarSubqueries(current.Right, subqueries)
	case *tree.AndExpr:
		collectScalarSubqueries(current.Left, subqueries)
		collectScalarSubqueries(current.Right, subqueries)
	case *tree.OrExpr:
		collectScalarSubqueries(current.Left, subqueries)
		collectScalarSubqueries(current.Right, subqueries)
	case *tree.XorExpr:
		collectScalarSubqueries(current.Left, subqueries)
		collectScalarSubqueries(current.Right, subqueries)
	case *tree.BinaryExpr:
		collectScalarSubqueries(current.Left, subqueries)
		collectScalarSubqueries(current.Right, subqueries)
	case *tree.UnaryExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.NotExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.ParenExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.IsNullExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.IsNotNullExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.IsUnknownExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.IsNotUnknownExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.IsTrueExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.IsNotTrueExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.IsFalseExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.IsNotFalseExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.CastExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.BitCastExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.IntervalExpr:
		collectScalarSubqueries(current.Expr, subqueries)
	case *tree.SerialExtractExpr:
		collectScalarSubqueries(current.SerialExpr, subqueries)
		collectScalarSubqueries(current.IndexExpr, subqueries)
	case *tree.FuncExpr:
		for _, arg := range current.Exprs {
			collectScalarSubqueries(arg, subqueries)
		}
		for _, order := range current.OrderBy {
			if order != nil {
				collectScalarSubqueries(order.Expr, subqueries)
			}
		}
		if current.WindowSpec != nil {
			for _, partition := range current.WindowSpec.PartitionBy {
				collectScalarSubqueries(partition, subqueries)
			}
			for _, order := range current.WindowSpec.OrderBy {
				if order != nil {
					collectScalarSubqueries(order.Expr, subqueries)
				}
			}
			if frame := current.WindowSpec.Frame; frame != nil {
				if frame.Start != nil {
					collectScalarSubqueries(frame.Start.Expr, subqueries)
				}
				if frame.End != nil {
					collectScalarSubqueries(frame.End.Expr, subqueries)
				}
			}
		}
	case *tree.Tuple:
		for _, item := range current.Exprs {
			collectScalarSubqueries(item, subqueries)
		}
	case *tree.RangeCond:
		collectScalarSubqueries(current.Left, subqueries)
		collectScalarSubqueries(current.From, subqueries)
		collectScalarSubqueries(current.To, subqueries)
	case *tree.CaseExpr:
		collectScalarSubqueries(current.Expr, subqueries)
		for _, when := range current.Whens {
			collectScalarSubqueries(when.Cond, subqueries)
			collectScalarSubqueries(when.Val, subqueries)
		}
		collectScalarSubqueries(current.Else, subqueries)
	case *tree.ExprList:
		for _, item := range current.Exprs {
			collectScalarSubqueries(item, subqueries)
		}
	}
}

func replacePreparedPlanSubqueries(expr *plan.Expr, replacements []*plan.Expr, position *int) (*plan.Expr, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.GetSub() != nil {
		if *position >= len(replacements) {
			return nil, moerr.NewInternalErrorNoCtx("prepared SET expression subquery count mismatch")
		}
		replacement := replacements[*position]
		*position = *position + 1
		if replacement.GetLit().GetIsnull() {
			replacement.Typ = expr.Typ
		}
		return replacement, nil
	}
	if fn := expr.GetF(); fn != nil {
		for i, arg := range fn.Args {
			var err error
			fn.Args[i], err = replacePreparedPlanSubqueries(arg, replacements, position)
			if err != nil {
				return nil, err
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for i, item := range list.List {
			var err error
			list.List[i], err = replacePreparedPlanSubqueries(item, replacements, position)
			if err != nil {
				return nil, err
			}
		}
	}
	if lit := expr.GetLit(); lit != nil && lit.Src != nil {
		var err error
		lit.Src, err = replacePreparedPlanSubqueries(lit.Src, replacements, position)
		if err != nil {
			return nil, err
		}
	}
	if window := expr.GetW(); window != nil {
		var err error
		window.WindowFunc, err = replacePreparedPlanSubqueries(window.WindowFunc, replacements, position)
		if err != nil {
			return nil, err
		}
		for i, partition := range window.PartitionBy {
			window.PartitionBy[i], err = replacePreparedPlanSubqueries(partition, replacements, position)
			if err != nil {
				return nil, err
			}
		}
		for _, order := range window.OrderBy {
			if order != nil {
				order.Expr, err = replacePreparedPlanSubqueries(order.Expr, replacements, position)
				if err != nil {
					return nil, err
				}
			}
		}
		if frame := window.Frame; frame != nil {
			if frame.Start != nil {
				frame.Start.Val, err = replacePreparedPlanSubqueries(frame.Start.Val, replacements, position)
				if err != nil {
					return nil, err
				}
			}
			if frame.End != nil {
				frame.End.Val, err = replacePreparedPlanSubqueries(frame.End.Val, replacements, position)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return expr, nil
}

func getPreparedPlanExprValueWithSubqueries(
	astExpr tree.Expr,
	specializedExpr *plan.Expr,
	ses *Session,
	execCtx *ExecCtx,
	prepareParamKind *vector.PrepareParamKind,
	isBin *bool,
) (interface{}, plan.Type, error) {
	var subqueries []*tree.Subquery
	collectScalarSubqueries(astExpr, &subqueries)
	replacements := make([]*plan.Expr, len(subqueries))
	for i, subquery := range subqueries {
		var subqueryKind vector.PrepareParamKind
		var subqueryIsBin bool
		_, _, err := getExprValueWithPrepareMeta(
			subquery, ses, execCtx, true, &replacements[i], &subqueryKind, &subqueryIsBin)
		if err != nil {
			return nil, plan.Type{}, err
		}
	}
	runtimeExpr := plan2.DeepCopyExpr(specializedExpr)
	position := 0
	var err error
	runtimeExpr, err = replacePreparedPlanSubqueries(runtimeExpr, replacements, &position)
	if err != nil {
		return nil, plan.Type{}, err
	}
	if position != len(replacements) {
		return nil, plan.Type{}, moerr.NewInternalErrorNoCtx("prepared SET expression subquery count mismatch")
	}
	return getPreparedPlanExprValueWithMeta(runtimeExpr, ses, execCtx, prepareParamKind, isBin)
}

func preparedPlanExprContainsSubquery(expr *plan.Expr) bool {
	contains := false
	_ = plan.VisitExprTree(expr, func(candidate *plan.Expr) error {
		contains = contains || candidate.GetSub() != nil
		return nil
	})
	return contains
}

func getPreparedPlanExprValueWithMeta(
	expr *plan.Expr,
	ses *Session,
	execCtx *ExecCtx,
	prepareParamKind *vector.PrepareParamKind,
	isBin *bool,
) (interface{}, plan.Type, error) {
	executor, err := colexec.NewExpressionExecutor(execCtx.proc, expr)
	if err != nil {
		return nil, plan.Type{}, err
	}
	defer executor.Free()
	input := batch.NewWithSize(0)
	input.SetRowCount(1)
	defer input.Clean(execCtx.proc.Mp())
	result, err := executor.Eval(execCtx.proc, []*batch.Batch{input}, nil)
	if err != nil {
		return nil, plan.Type{}, err
	}
	if isBin != nil {
		*isBin = result.GetIsBin()
	}
	if prepareParamKind != nil {
		*prepareParamKind = result.GetPrepareParamKind()
		if *prepareParamKind == vector.PrepareParamNone {
			*prepareParamKind = prepareParamKindFromType(result.GetType().Oid)
		}
	}
	value, err := getValueFromVector(execCtx.reqCtx, result, ses, expr)
	return value, plan2.MakePlan2Type(result.GetType()), err
}

// transparentPrepareParamKind closes the metadata boundary introduced by SET's
// synthetic SELECT evaluation. A direct parameter or variable retains its
// source conversion category even if projection materialization drops vector-
// local metadata. Parentheses are transparent; casts and other expressions are
// intentionally not, because their result type defines the conversion category.
func transparentPrepareParamKind(e tree.Expr, ses *Session) (vector.PrepareParamKind, error) {
	for {
		switch expr := e.(type) {
		case *tree.ParenExpr:
			e = expr.Expr
		case *tree.ParamExpr:
			proc := ses.GetProc()
			// Parser ordinals are one-based; the normalized plan/process positions
			// are zero-based (see decrementParamOrdinalRule).
			if proc == nil || expr.Offset <= 0 {
				return vector.PrepareParamNone, nil
			}
			return proc.GetPrepareParamKind(expr.Offset - 1), nil
		case *tree.VarExpr:
			return ses.GetTxnCompileCtx().ResolveVariablePrepareParamKind(
				expr.Name, expr.System, expr.Global)
		default:
			return vector.PrepareParamNone, nil
		}
	}
}

func bindSetVariableResultExpr(
	e tree.Expr,
	compilerContext plan2.CompilerContext,
	preparedExpression bool,
) (*plan.Expr, error) {
	builder := plan2.NewQueryBuilder(
		plan.Query_SELECT, compilerContext, preparedExpression, false)
	bindContext := plan2.NewBindContext(builder, nil)
	binder := plan2.NewSetVarBinder(builder, bindContext)
	return binder.BindExpr(e, 0, false)
}

// only support single value and unary minus
func GetSimpleExprValue(ctx context.Context, e tree.Expr, feSes FeSession) (interface{}, error) {
	return getSimpleExprValue(ctx, e, feSes, nil)
}

// GetSimpleExprValueWithType evaluates an expression after coercing it to the
// supplied assignment target. This preserves declared stored-procedure types
// even when their runtime representation is a Go string (for example DECIMAL).
func GetSimpleExprValueWithType(ctx context.Context, e tree.Expr, feSes FeSession, targetType plan.Type) (interface{}, error) {
	return getSimpleExprValue(ctx, e, feSes, &targetType)
}

func getSimpleExprValue(ctx context.Context, e tree.Expr, feSes FeSession, targetType *plan.Type) (interface{}, error) {
	var planExpr *plan.Expr
	if v, ok := e.(*tree.UnresolvedName); ok && !storedProcedureVariableExists(ctx, v.ColName()) {
		// Preserve SET @a = on behavior. A stored-procedure variable with the
		// same syntax is instead bound through its declared type below.
		if targetType == nil {
			return v.ColName(), nil
		}
		planExpr = plan2.MakePlan2StringConstExprWithType(v.ColName())
	} else {
		builder := plan2.NewQueryBuilder(plan.Query_SELECT, feSes.GetTxnCompileCtx(), false, false)
		bindContext := plan2.NewBindContext(builder, nil)
		binder := plan2.NewSetVarBinder(builder, bindContext)
		var err error
		planExpr, err = binder.BindExpr(e, 0, false)
		if err != nil {
			return nil, err
		}
	}

	if targetType != nil {
		var err error
		planExpr, err = plan2.MakePlan2AssignmentCastExpr(ctx, planExpr, *targetType)
		if err != nil {
			return nil, err
		}
	}

	txnCompileCtx := feSes.GetTxnCompileCtx()
	// set @a = 'on', type of a is bool. And mo cast rule does not fit set variable rule so delay to convert type.
	// Here the evalExpr may execute some function that needs engine.Engine.
	txnCompileCtx.GetProcess().ReplaceTopCtx(
		attachValue(txnCompileCtx.GetProcess().GetTopContext(),
			defines.EngineKey{},
			feSes.GetTxnHandler().GetStorage()))

	vec, free, err := colexec.GetReadonlyResultFromNoColumnExpression(txnCompileCtx.GetProcess(), planExpr)
	if err != nil {
		return nil, err
	}

	value, err := getValueFromVector(ctx, vec, feSes, planExpr)
	free()
	return value, err
}

func storedProcedureVariableExists(ctx context.Context, name string) bool {
	inSp, _ := ctx.Value(defines.InSp{}).(bool)
	if !inSp {
		return false
	}
	scopes, ok := ctx.Value(defines.VarScopeKey{}).(*[]map[string]interface{})
	if !ok || scopes == nil {
		return false
	}
	name = strings.ToLower(name)
	for i := len(*scopes) - 1; i >= 0; i-- {
		if _, ok := (*scopes)[i][name]; ok {
			return true
		}
	}
	return false
}

func getValueFromVector(ctx context.Context, vec *vector.Vector, feSes FeSession, expr *plan2.Expr) (interface{}, error) {
	if vec.IsConstNull() || vec.GetNulls().Contains(0) {
		return nil, nil
	}
	switch vec.GetType().Oid {
	case types.T_bool:
		return vector.MustFixedColNoTypeCheck[bool](vec)[0], nil
	case types.T_bit:
		return vector.MustFixedColNoTypeCheck[uint64](vec)[0], nil
	case types.T_int8:
		return vector.MustFixedColNoTypeCheck[int8](vec)[0], nil
	case types.T_int16:
		return vector.MustFixedColNoTypeCheck[int16](vec)[0], nil
	case types.T_int32:
		return vector.MustFixedColNoTypeCheck[int32](vec)[0], nil
	case types.T_int64:
		return vector.MustFixedColNoTypeCheck[int64](vec)[0], nil
	case types.T_uint8:
		return vector.MustFixedColNoTypeCheck[uint8](vec)[0], nil
	case types.T_uint16:
		return vector.MustFixedColNoTypeCheck[uint16](vec)[0], nil
	case types.T_uint32:
		return vector.MustFixedColNoTypeCheck[uint32](vec)[0], nil
	case types.T_uint64:
		return vector.MustFixedColNoTypeCheck[uint64](vec)[0], nil
	case types.T_float32:
		return vector.MustFixedColNoTypeCheck[float32](vec)[0], nil
	case types.T_float64:
		return vector.MustFixedColNoTypeCheck[float64](vec)[0], nil
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_text, types.T_blob, types.T_datalink:
		return vec.GetStringAt(0), nil
	case types.T_array_float32:
		return vector.GetArrayAt[float32](vec, 0), nil
	case types.T_array_float64:
		return vector.GetArrayAt[float64](vec, 0), nil
	case types.T_array_bf16:
		return vector.GetArrayAt[types.BF16](vec, 0), nil
	case types.T_array_float16:
		return vector.GetArrayAt[types.Float16](vec, 0), nil
	case types.T_array_int8:
		return vector.GetArrayAt[int8](vec, 0), nil
	case types.T_array_uint8:
		return vector.GetArrayAt[uint8](vec, 0), nil
	case types.T_decimal64:
		val := vector.GetFixedAtNoTypeCheck[types.Decimal64](vec, 0)
		return val.Format(expr.Typ.Scale), nil
	case types.T_decimal128:
		val := vector.GetFixedAtNoTypeCheck[types.Decimal128](vec, 0)
		return val.Format(expr.Typ.Scale), nil
	case types.T_decimal256:
		val := vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, 0)
		return val.Format(expr.Typ.Scale), nil
	case types.T_json:
		val := vec.GetBytesAt(0)
		byteJson := types.DecodeJson(val)
		return byteJson.String(), nil
	case types.T_uuid:
		val := vector.MustFixedColNoTypeCheck[types.Uuid](vec)[0]
		return val.String(), nil
	case types.T_date:
		val := vector.MustFixedColNoTypeCheck[types.Date](vec)[0]
		return val.String(), nil
	case types.T_time:
		val := vector.MustFixedColNoTypeCheck[types.Time](vec)[0]
		return val.String(), nil
	case types.T_datetime:
		val := vector.MustFixedColNoTypeCheck[types.Datetime](vec)[0]
		return val.String(), nil
	case types.T_timestamp:
		val := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)[0]
		return val.String2(feSes.GetTimeZone(), vec.GetType().Scale), nil
	case types.T_year:
		val := vector.MustFixedColNoTypeCheck[types.MoYear](vec)[0]
		return val.String(), nil
	case types.T_enum:
		return vector.MustFixedColNoTypeCheck[types.Enum](vec)[0], nil
	default:
		return nil, moerr.NewInvalidArg(ctx, "variable type", vec.GetType().Oid.String())
	}
}

type statementStatus int

const (
	success statementStatus = iota
	fail
)

func (s statementStatus) String() string {
	switch s {
	case success:
		return "success"
	case fail:
		return "fail"
	}
	return "running"
}

// logStatementStatus prints the status of the statement into the log.
func logStatementStatus(
	ctx context.Context,
	ses FeSession,
	_ tree.Statement,
	status statementStatus,
	err error,
) {
	logStatementStringStatus(ctx, ses, "", status, err)
}

// logStatementStringStatus
// if stmtStr == "", get the query statement from FeSession or motrace.StatementInfo
// (which migrate from logStatementStatus).
// This op is aim to avoid string copy in 'status == success' case.
func logStatementStringStatus(
	ctx context.Context,
	ses FeSession,
	stmtStr string,
	status statementStatus,
	err error,
) {
	var getFormatedSqlStr = func() string {
		var str = stmtStr
		if len(stmtStr) == 0 {
			if stm := ses.GetStmtInfo(); stm == nil {
				str = ses.GetSqlOfStmt()
			} else {
				// case `execute __prepared_stmt_id__;`: this value holds the raw prepare statement and raw args.
				str = stm.CopyStatementInfo()
			}
		}
		str = commonutil.Abbreviate(str, int(getPu(ses.GetService()).SV.LengthOfQueryPrinted))
		return str
	}
	if status == success {
		if ses.LogDebug() {
			str := getFormatedSqlStr()
			ses.Debug(ctx, "query trace status", logutil.StatementField(str), logutil.StatusField(status.String()))
		}
		err = nil // make sure: it is nil for EndStatement
	} else {
		str := getFormatedSqlStr()
		ses.Error(
			ctx,
			"query trace status",
			logutil.StatementField(str),
			logutil.StatusField(status.String()),
			logutil.ErrorField(err),
			logutil.TxnInfoField(ses.GetStaticTxnInfo()),
		)
	}
	if status == fail {
		if concrete, ok := ses.(*Session); ok && concrete.deferStatementCompletion(err) {
			return
		}
	}
	finishStatementAccounting(ctx, ses, err)
}

func finishStatementAccounting(ctx context.Context, ses FeSession, err error) {
	// A same-session derived statement without its own StatementInfo belongs to
	// the enclosing client statement. The outer request owns both its terminal
	// accounting and protocol counters.
	if ses.IsDerivedStmt() && ses.GetStmtInfo() == nil && resource.RootFromContext(ctx) != nil {
		return
	}
	if concrete, ok := ses.(*Session); ok {
		concrete.rotateResponseOutputWait(ctx)
	}
	var outBytes, outPacket int64
	switch resper := ses.GetResponser().(type) {
	case *MysqlResp:
		outBytes, outPacket = resper.mysqlRrWr.CalculateOutTrafficBytes(true)
	}
	// pls make sure: NO ONE use the ses.tStmt after EndStatement
	if !ses.IsBackgroundSession() {
		if stmt := ses.GetStmtInfo(); stmt != nil {
			stmt.EndStatement(ctx, err, ses.SendRows(), outBytes, outPacket)
		}
	}
	// need just below EndStatement
	ses.SetTStmt(nil)
}

func (ses *Session) beginResponseAccounting() {
	// Requests are serialized per session, so reset at the request boundary.
	// This prevents handshake and statement-less responses from leaking into the
	// next SQL statement's protocol counters.
	if resper, ok := ses.GetResponser().(*MysqlResp); ok {
		resper.mysqlRrWr.CalculateOutTrafficBytes(true)
	}
	ses.responseAccounting = true
	ses.pendingStatementFailed = false
	ses.pendingStatementError = nil
	ses.installResponseOutputWaitTracker(new(responseOutputWaitTracker))
}

type responseOutputWaitTrackerInstaller interface {
	setResponseOutputWaitTracker(*responseOutputWaitTracker)
}

func (ses *Session) installResponseOutputWaitTracker(tracker *responseOutputWaitTracker) {
	ses.responseOutputWait = tracker
	if resper, ok := ses.GetResponser().(*MysqlResp); ok {
		if installer, ok := resper.mysqlRrWr.(responseOutputWaitTrackerInstaller); ok {
			installer.setResponseOutputWaitTracker(tracker)
		}
	}
}

func (ses *Session) rotateResponseOutputWait(ctx context.Context) {
	tracker := ses.responseOutputWait
	var next *responseOutputWaitTracker
	if ses.responseAccounting {
		next = new(responseOutputWaitTracker)
	}
	ses.installResponseOutputWaitTracker(next)
	if tracker == nil {
		return
	}
	totalNS := tracker.totalNS.Load()
	operatorNS := tracker.operatorNS.Load()
	root := resource.RootFromContext(ctx)
	if totalNS < 0 || operatorNS < 0 || operatorNS > totalNS {
		if root != nil {
			root.AddLocal(resource.Delta{Quality: resource.QualityInvariantFailure})
		}
		return
	}
	// Immediate writes inside Output.Call are already classified by its
	// analyzer and subtracted from active time. Add only writes that happened
	// later (buffer flush, EOF/OK, or an error response) at the statement root.
	unclassifiedNS := totalNS - operatorNS
	if unclassifiedNS > 0 && root != nil {
		var usage resource.Usage
		usage.WaitNS[resource.WaitOutput] = uint64(unclassifiedNS)
		root.MergeExecution(resource.ExecutionSummary{Usage: usage})
	}
}

func (ses *Session) deferStatementCompletion(err error) bool {
	if !ses.responseAccounting {
		return false
	}
	ses.pendingStatementFailed = true
	if ses.pendingStatementError == nil {
		ses.pendingStatementError = err
	}
	return true
}

func (ses *Session) finishResponseAccounting(ctx context.Context, responseErr error, responseFailed bool) {
	if !ses.responseAccounting {
		return
	}
	ses.responseAccounting = false
	err := ses.pendingStatementError
	failed := ses.pendingStatementFailed
	ses.pendingStatementFailed = false
	ses.pendingStatementError = nil
	if err == nil && (failed || responseFailed) {
		err = responseErr
	}
	if err == nil && failed {
		err = moerr.NewInternalError(ctx, "statement failed")
	}
	// Always consume the request counters, including requests that did not
	// create a StatementInfo (PING, rewrite sidecars, and similar commands).
	finishStatementAccounting(ctx, ses, err)
}

func getLogger(sid string) *log.MOLogger {
	return moruntime.GetLogger(sid)
}

// appendSessionField append session id, transaction id and statement id to the fields
// history:
// #15877, discard ses.GetTxnInfo(), it need ses.Lock(). may cause deadlock: locked by itself.
// #16028, depend on ses.GetStmtProfile() itself do the log. get rid of StatementInfo.
func appendSessionField(fields []zap.Field, ses FeSession) []zap.Field {
	if ses != nil {
		fields = append(fields, logutil.SessionIdField(uuid.UUID(ses.GetUUID()).String()))
		p := ses.GetStmtProfile()
		if p.GetStmtId() != dumpUUID {
			fields = append(fields, logutil.StatementIdField(uuid.UUID(p.GetStmtId()).String()))
		}
		if txnId := p.GetTxnId(); txnId != dumpUUID {
			fields = append(fields, logutil.TxnIdField(hex.EncodeToString(txnId[:])))
		}
	}
	return fields
}

// isCmdFieldListSql checks the sql is the cmdFieldListSql or not.
func isCmdFieldListSql(sql string) bool {
	if len(sql) < cmdFieldListSqlLen {
		return false
	}
	prefix := sql[:cmdFieldListSqlLen]
	return strings.Compare(strings.ToLower(prefix), cmdFieldListSql) == 0
}

// makeCmdFieldListSql makes the internal CMD_FIELD_LIST sql
func makeCmdFieldListSql(query string) string {
	nullIdx := strings.IndexRune(query, rune(0))
	if nullIdx != -1 {
		query = query[:nullIdx]
	}
	return cmdFieldListSql + " " + query
}

// parseCmdFieldList parses the internal cmd field list
func parseCmdFieldList(ctx context.Context, sql string) (*InternalCmdFieldList, error) {
	if !isCmdFieldListSql(sql) {
		return nil, moerr.NewInternalError(ctx, "it is not the CMD_FIELD_LIST")
	}
	tableName := strings.TrimSpace(sql[len(cmdFieldListSql):])
	return &InternalCmdFieldList{tableName: tableName}, nil
}

// isCmdGetSnapshotTsSql checks the sql is the cmdGetSnapshotTsSql or not.
func isCmdGetSnapshotTsSql(sql string) bool {
	if len(sql) < cmdGetSnapshotTsSqlLen {
		return false
	}
	prefix := sql[:cmdGetSnapshotTsSqlLen]
	return strings.Compare(strings.ToLower(prefix), cmdGetSnapshotTsSql) == 0
}

// makeGetSnapshotTsSql makes the internal getsnapshotts sql
func makeGetSnapshotTsSql(snapshotName, accountName, publicationName string) string {
	return fmt.Sprintf("%s %s %s %s", cmdGetSnapshotTsSql, snapshotName, accountName, publicationName)
}

// parseCmdGetSnapshotTs parses the internal cmd getsnapshotts
// format: getsnapshotts <snapshotName> <accountName> <publicationName>
func parseCmdGetSnapshotTs(ctx context.Context, sql string) (*InternalCmdGetSnapshotTs, error) {
	if !isCmdGetSnapshotTsSql(sql) {
		return nil, moerr.NewInternalError(ctx, "it is not the GET_SNAPSHOT_TS command")
	}
	params := strings.TrimSpace(sql[cmdGetSnapshotTsSqlLen:])
	parts := strings.Fields(params)
	if len(parts) != 3 {
		return nil, moerr.NewInternalError(ctx, "invalid getsnapshotts command format, expected: getsnapshotts <snapshotName> <accountName> <publicationName>")
	}
	return &InternalCmdGetSnapshotTs{
		snapshotName:    parts[0],
		accountName:     parts[1],
		publicationName: parts[2],
	}, nil
}

// isCmdGetDatabasesSql checks the sql is the cmdGetDatabasesSql or not.
func isCmdGetDatabasesSql(sql string) bool {
	if len(sql) < cmdGetDatabasesSqlLen {
		return false
	}
	prefix := sql[:cmdGetDatabasesSqlLen]
	return strings.Compare(strings.ToLower(prefix), cmdGetDatabasesSql) == 0
}

// makeGetDatabasesSql makes the internal getdatabases sql
func makeGetDatabasesSql(snapshotName, accountName, publicationName, level, dbName, tableName string) string {
	return fmt.Sprintf("%s %s %s %s %s %s %s", cmdGetDatabasesSql, snapshotName, accountName, publicationName, level, dbName, tableName)
}

// parseCmdGetDatabases parses the internal cmd getdatabases
// format: getdatabases <snapshotName> <accountName> <publicationName> <level> <dbName> <tableName>
func parseCmdGetDatabases(ctx context.Context, sql string) (*InternalCmdGetDatabases, error) {
	if !isCmdGetDatabasesSql(sql) {
		return nil, moerr.NewInternalError(ctx, "it is not the GET_DATABASES command")
	}
	params := strings.TrimSpace(sql[cmdGetDatabasesSqlLen:])
	parts := strings.Fields(params)
	if len(parts) != 6 {
		return nil, moerr.NewInternalError(ctx, "invalid getdatabases command format, expected: getdatabases <snapshotName> <accountName> <publicationName> <level> <dbName> <tableName>")
	}
	return &InternalCmdGetDatabases{
		snapshotName:    parts[0],
		accountName:     parts[1],
		publicationName: parts[2],
		level:           parts[3],
		dbName:          parts[4],
		tableName:       parts[5],
	}, nil
}

// isCmdGetMoIndexesSql checks the sql is the cmdGetMoIndexesSql or not.
func isCmdGetMoIndexesSql(sql string) bool {
	if len(sql) < cmdGetMoIndexesSqlLen {
		return false
	}
	prefix := sql[:cmdGetMoIndexesSqlLen]
	return strings.Compare(strings.ToLower(prefix), cmdGetMoIndexesSql) == 0
}

// makeGetMoIndexesSql makes the internal getmoindexes sql
func makeGetMoIndexesSql(tableId uint64, subscriptionAccountName, publicationName, snapshotName string) string {
	return fmt.Sprintf("%s %d %s %s %s", cmdGetMoIndexesSql, tableId, subscriptionAccountName, publicationName, snapshotName)
}

// parseCmdGetMoIndexes parses the internal cmd getmoindexes
// format: getmoindexes <tableId> <subscriptionAccountName> <publicationName> <snapshotName>
func parseCmdGetMoIndexes(ctx context.Context, sql string) (*InternalCmdGetMoIndexes, error) {
	if !isCmdGetMoIndexesSql(sql) {
		return nil, moerr.NewInternalError(ctx, "it is not the GET_MO_INDEXES command")
	}
	params := strings.TrimSpace(sql[cmdGetMoIndexesSqlLen:])
	parts := strings.Fields(params)
	if len(parts) != 4 {
		return nil, moerr.NewInternalError(ctx, "invalid getmoindexes command format, expected: getmoindexes <tableId> <subscriptionAccountName> <publicationName> <snapshotName>")
	}
	tableId, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "invalid tableId: %s", parts[0])
	}
	return &InternalCmdGetMoIndexes{
		tableId:                 tableId,
		subscriptionAccountName: parts[1],
		publicationName:         parts[2],
		snapshotName:            parts[3],
	}, nil
}

// isCmdGetDdlSql checks the sql is the cmdGetDdlSql or not.
func isCmdGetDdlSql(sql string) bool {
	if len(sql) < cmdGetDdlSqlLen {
		return false
	}
	prefix := sql[:cmdGetDdlSqlLen]
	return strings.Compare(strings.ToLower(prefix), cmdGetDdlSql) == 0
}

// makeGetDdlSql makes the internal getddl sql
func makeGetDdlSql(snapshotName, subscriptionAccountName, publicationName, level, dbName, tableName string) string {
	return fmt.Sprintf("%s %s %s %s %s %s %s", cmdGetDdlSql, snapshotName, subscriptionAccountName, publicationName, level, dbName, tableName)
}

// parseCmdGetDdl parses the internal cmd getddl
// format: getddl <snapshotName> <subscriptionAccountName> <publicationName> <level> <dbName> <tableName>
func parseCmdGetDdl(ctx context.Context, sql string) (*InternalCmdGetDdl, error) {
	if !isCmdGetDdlSql(sql) {
		return nil, moerr.NewInternalError(ctx, "it is not the GET_DDL command")
	}
	params := strings.TrimSpace(sql[cmdGetDdlSqlLen:])
	parts := strings.Fields(params)
	if len(parts) != 6 {
		return nil, moerr.NewInternalError(ctx, "invalid getddl command format, expected: getddl <snapshotName> <subscriptionAccountName> <publicationName> <level> <dbName> <tableName>")
	}
	return &InternalCmdGetDdl{
		snapshotName:            parts[0],
		subscriptionAccountName: parts[1],
		publicationName:         parts[2],
		level:                   parts[3],
		dbName:                  parts[4],
		tableName:               parts[5],
	}, nil
}

// isCmdGetObjectSql checks the sql is the cmdGetObjectSql or not.
func isCmdGetObjectSql(sql string) bool {
	if len(sql) < cmdGetObjectSqlLen {
		return false
	}
	prefix := sql[:cmdGetObjectSqlLen]
	return strings.Compare(strings.ToLower(prefix), cmdGetObjectSql) == 0
}

// makeGetObjectSql makes the internal getobject sql
func makeGetObjectSql(subscriptionAccountName, publicationName, objectName string, chunkIndex int64) string {
	return fmt.Sprintf("%s %s %s %s %d", cmdGetObjectSql, subscriptionAccountName, publicationName, objectName, chunkIndex)
}

// parseCmdGetObject parses the internal cmd getobject
// format: getobject <subscriptionAccountName> <publicationName> <objectName> <chunkIndex>
func parseCmdGetObject(ctx context.Context, sql string) (*InternalCmdGetObject, error) {
	if !isCmdGetObjectSql(sql) {
		return nil, moerr.NewInternalError(ctx, "it is not the GET_OBJECT command")
	}
	params := strings.TrimSpace(sql[cmdGetObjectSqlLen:])
	parts := strings.Fields(params)
	if len(parts) != 4 {
		return nil, moerr.NewInternalError(ctx, "invalid getobject command format, expected: getobject <subscriptionAccountName> <publicationName> <objectName> <chunkIndex>")
	}
	chunkIndex, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "invalid chunkIndex: %s", parts[3])
	}
	return &InternalCmdGetObject{
		subscriptionAccountName: parts[0],
		publicationName:         parts[1],
		objectName:              parts[2],
		chunkIndex:              chunkIndex,
	}, nil
}

// isCmdObjectListSql checks the sql is the cmdObjectListSql or not.
func isCmdObjectListSql(sql string) bool {
	if len(sql) < cmdObjectListSqlLen {
		return false
	}
	prefix := sql[:cmdObjectListSqlLen]
	return strings.Compare(strings.ToLower(prefix), cmdObjectListSql) == 0
}

// makeObjectListSql makes the internal objectlist sql
func makeObjectListSql(snapshotName, againstSnapshotName, subscriptionAccountName, publicationName string) string {
	return fmt.Sprintf("%s %s %s %s %s", cmdObjectListSql, snapshotName, againstSnapshotName, subscriptionAccountName, publicationName)
}

// parseCmdObjectList parses the internal cmd objectlist
// format: objectlist <snapshotName> <againstSnapshotName> <subscriptionAccountName> <publicationName>
// Note: againstSnapshotName can be "-" to indicate empty
func parseCmdObjectList(ctx context.Context, sql string) (*InternalCmdObjectList, error) {
	if !isCmdObjectListSql(sql) {
		return nil, moerr.NewInternalError(ctx, "it is not the OBJECT_LIST command")
	}
	params := strings.TrimSpace(sql[cmdObjectListSqlLen:])
	parts := strings.Fields(params)
	if len(parts) != 4 {
		return nil, moerr.NewInternalError(ctx, "invalid objectlist command format, expected: objectlist <snapshotName> <againstSnapshotName> <subscriptionAccountName> <publicationName>")
	}
	againstSnapshotName := parts[1]
	if againstSnapshotName == "-" {
		againstSnapshotName = ""
	}
	return &InternalCmdObjectList{
		snapshotName:            parts[0],
		againstSnapshotName:     againstSnapshotName,
		subscriptionAccountName: parts[2],
		publicationName:         parts[3],
	}, nil
}

// isCmdCheckSnapshotFlushedSql checks the sql is the cmdCheckSnapshotFlushedSql or not.
func isCmdCheckSnapshotFlushedSql(sql string) bool {
	if len(sql) < cmdCheckSnapshotFlushedSqlLen {
		return false
	}
	prefix := sql[:cmdCheckSnapshotFlushedSqlLen]
	return strings.Compare(strings.ToLower(prefix), cmdCheckSnapshotFlushedSql) == 0
}

// makeCheckSnapshotFlushedSql makes the internal checksnapshotflushed sql
func makeCheckSnapshotFlushedSql(snapshotName, subscriptionAccountName, publicationName string) string {
	return fmt.Sprintf("%s %s %s %s", cmdCheckSnapshotFlushedSql, snapshotName, subscriptionAccountName, publicationName)
}

// parseCmdCheckSnapshotFlushed parses the internal cmd checksnapshotflushed
// format: checksnapshotflushed <snapshotName> <subscriptionAccountName> <publicationName>
func parseCmdCheckSnapshotFlushed(ctx context.Context, sql string) (*InternalCmdCheckSnapshotFlushed, error) {
	if !isCmdCheckSnapshotFlushedSql(sql) {
		return nil, moerr.NewInternalError(ctx, "it is not the CHECK_SNAPSHOT_FLUSHED command")
	}
	params := strings.TrimSpace(sql[cmdCheckSnapshotFlushedSqlLen:])
	parts := strings.Fields(params)
	if len(parts) != 3 {
		return nil, moerr.NewInternalError(ctx, "invalid checksnapshotflushed command format, expected: checksnapshotflushed <snapshotName> <subscriptionAccountName> <publicationName>")
	}
	return &InternalCmdCheckSnapshotFlushed{
		snapshotName:            parts[0],
		subscriptionAccountName: parts[1],
		publicationName:         parts[2],
	}, nil
}

func getVariableValue(varDefault interface{}) string {
	switch val := varDefault.(type) {
	case int64:
		return fmt.Sprintf("%d", val)
	case uint64:
		return fmt.Sprintf("%d", val)
	case int8:
		return fmt.Sprintf("%d", val)
	case float64:
		// 0.1 => 0.100000
		// 0.0000001 -> 1.000000e-7
		if val >= 1e-6 {
			return fmt.Sprintf("%.6f", val)
		} else {
			return fmt.Sprintf("%.6e", val)
		}
	case string:
		return val
	default:
		return ""
	}
}

func makeServerVersion(pu *mo_config.ParameterUnit, version string) string {
	return pu.SV.ServerVersionPrefix + version
}

// getUserProfile returns the account, user, role of the account
func getUserProfile(account *TenantInfo) (string, string, string) {
	var (
		accountName string
		userName    string
		roleName    string
	)

	if account != nil {
		accountName = account.GetTenant()
		userName = account.GetUser()
		roleName = account.GetDefaultRole()
	} else {
		accountName = sysAccountName
		userName = rootName
		roleName = moAdminRoleName
	}
	return accountName, userName, roleName
}

// RewriteError rewrites the error info
func RewriteError(err error, username string) (uint16, string, string) {
	if err == nil {
		return moerr.ER_INTERNAL_ERROR, "", ""
	}
	var errorCode uint16
	var sqlState string
	var msg string

	errMsg := strings.ToLower(err.Error())
	if isAuthenticationRejected(err) || needConvertedToAccessDeniedError(errMsg) {
		failed := moerr.MysqlErrorMsgRefer[moerr.ER_ACCESS_DENIED_ERROR]
		if len(username) > 0 {
			tipsFormat := "Access denied for user %s. %s"
			msg = fmt.Sprintf(tipsFormat, getUserPart(username), err.Error())
		} else {
			msg = err.Error()
		}
		errorCode = failed.ErrorCode
		sqlState = failed.SqlStates[0]
	} else {
		//Reference To : https://github.com/matrixorigin/matrixone/pull/12396/files#r1374443578
		switch errImpl := err.(type) {
		case *moerr.Error:
			if errImpl.MySQLCode() != moerr.ER_UNKNOWN_ERROR {
				errorCode = errImpl.MySQLCode()
			} else {
				errorCode = errImpl.ErrorCode()
			}
			msg = err.Error()
			sqlState = errImpl.SqlState()
		default:
			failed := moerr.MysqlErrorMsgRefer[moerr.ER_INTERNAL_ERROR]
			msg = err.Error()
			errorCode = failed.ErrorCode
			sqlState = failed.SqlStates[0]
		}

	}
	return errorCode, sqlState, msg
}

func needConvertedToAccessDeniedError(errMsg string) bool {
	if strings.Contains(errMsg, "check password failed") ||
		/*
			following two cases are suggested by the peers from the mo cloud team.
			we keep the consensus with them.
		*/
		strings.Contains(errMsg, "suspended") ||
		strings.Contains(errMsg, "source address") &&
			strings.Contains(errMsg, "is not authorized") {
		return true
	}
	return false
}

const (
	quitStr = "MysqlClientQuit"
)

// makeExecuteSql appends the PREPARE sql and its values of parameters for the EXECUTE statement.
// Format 1: execute ... using ...
// execute.... // prepare stmt1 from .... ; set var1 = val1 ; set var2 = val2 ;
// Format 2: COM_STMT_EXECUTE
// execute.... // prepare stmt1 from .... ; param0 ; param1 ...
func makeExecuteSql(ctx context.Context, ses *Session, stmt tree.Statement, binExec bool, prepareName string) string {
	if ses == nil || stmt == nil {
		return ""
	}
	isExec := false
	name := ""
	var Variables []*tree.VarExpr
	if binExec {
		isExec = true
		name = prepareName
	} else if t, ok := stmt.(*tree.Execute); ok {
		isExec = true
		name = string(t.Name)
		Variables = t.Variables
	}
	preSql := ""
	bb := &strings.Builder{}
	//fill prepare parameters
	if isExec {
		prepareStmt, err := ses.GetPrepareStmt(ctx, name)
		if err != nil || prepareStmt == nil {
			return ""
		}
		preSql = strings.TrimSpace(prepareStmt.Sql)
		bb.WriteString(preSql)
		bb.WriteString(" ; ")
		if len(Variables) != 0 {
			//for EXECUTE ... USING statement. append variables if there is.
			//get SET VAR sql
			setVarSqls := make([]string, len(Variables))
			for i, v := range Variables {
				userVal, err := ses.GetUserDefinedVar(v.Name)
				if err == nil && userVal != nil && len(userVal.Sql) != 0 {
					setVarSqls[i] = userVal.Sql
				}
			}
			bb.WriteString(strings.Join(setVarSqls, " ; "))
		} else if prepareStmt.params != nil {
			//for COM_STMT_EXECUTE
			//get value of parameters
			paramCnt := prepareStmt.params.Length()
			paramValues := make([]string, paramCnt)
			vs := vector.MustFixedColNoTypeCheck[types.Varlena](prepareStmt.params)
			for i := 0; i < paramCnt; i++ {
				isNull := prepareStmt.params.GetNulls().Contains(uint64(i))
				if isNull {
					paramValues[i] = "NULL"
				} else {
					paramValues[i] = vs[i].UnsafeGetString(prepareStmt.params.GetArea())
				}
			}
			bb.WriteString(strings.Join(paramValues, " ; "))
		}
	}
	return bb.String()
}

func convertRowsIntoBatch(pool *mpool.MPool, cols []Column, rows [][]any) (*batch.Batch, *plan.ResultColDef, error) {
	planColDefs, colTyps, colNames, err := mysqlColDef2PlanResultColDef(cols)
	if err != nil {
		return nil, nil, err
	}
	//1. make vector type
	bat := batch.New(colNames)
	//2. make batch
	cnt := len(rows)
	bat.SetRowCount(cnt)
	for colIndex, typ := range colTyps {
		bat.Vecs[colIndex] = vector.NewVec(typ)
		nsp := nulls.NewWithSize(cnt)

		switch typ.Oid {
		case types.T_varchar:
			for rowIdx, row := range rows {
				var val string
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					strVal, ok := row[colIndex].(string)
					if ok {
						val = strVal
					} else {
						val = fmt.Sprintf("%v", row[colIndex])
					}
				}
				err := vector.AppendBytes(bat.Vecs[colIndex], []byte(val), false, pool)
				if err != nil {
					return nil, nil, err
				}
			}

		case types.T_text:
			for rowIdx, row := range rows {
				var val string
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					strVal, ok := row[colIndex].(string)
					if ok {
						val = strVal
					} else {
						val = fmt.Sprintf("%v", row[colIndex])
					}
				}

				err := vector.AppendBytes(bat.Vecs[colIndex], []byte(val), false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_int8:
			for rowIdx, row := range rows {
				var val int8
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(int8)
				}
				err := vector.AppendFixed[int8](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_int16:
			for rowIdx, row := range rows {
				var val int16
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(int16)
				}

				err := vector.AppendFixed[int16](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_year:
			for rowIdx, row := range rows {
				var val types.MoYear
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(types.MoYear)
				}

				err := vector.AppendFixed[types.MoYear](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_int32:
			for rowIdx, row := range rows {
				var val int32
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(int32)
				}

				err := vector.AppendFixed[int32](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_int64:
			for rowIdx, row := range rows {
				var val int64
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(int64)
				}

				err := vector.AppendFixed[int64](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_float64:
			for rowIdx, row := range rows {
				var val float64
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(float64)
				}

				err := vector.AppendFixed[float64](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_float32:
			for rowIdx, row := range rows {
				var val float32
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(float32)
				}

				err := vector.AppendFixed[float32](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_date:
			for rowIdx, row := range rows {
				var val types.Date
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(types.Date)
				}

				err := vector.AppendFixed[types.Date](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_time:
			for rowIdx, row := range rows {
				var val types.Time
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(types.Time)
				}

				err := vector.AppendFixed[types.Time](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_datetime:
			for rowIdx, row := range rows {
				var val types.Datetime
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(types.Datetime)
				}

				err := vector.AppendFixed[types.Datetime](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_timestamp:
			for rowIdx, row := range rows {
				var val types.Timestamp
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					timeStampRowVal := row[colIndex]
					switch v := timeStampRowVal.(type) {
					case types.Timestamp:
						val = v
					case string:
						val, err = types.ParseTimestamp(time.Local, v, typ.Scale)
						if err != nil {
							return nil, nil, err
						}
					default:
						return nil, nil, moerr.NewInternalErrorNoCtxf("%v can't convert to timestamp type", v)
					}
				}
				err := vector.AppendFixed[types.Timestamp](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_decimal64:
			for rowIdx, row := range rows {
				var val types.Decimal64
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else if val, err = getDecimal64FromRowValue(row[colIndex], typ); err != nil {
					return nil, nil, err
				}

				err := vector.AppendFixed[types.Decimal64](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_decimal128:
			for rowIdx, row := range rows {
				var val types.Decimal128
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else if val, err = getDecimal128FromRowValue(row[colIndex], typ); err != nil {
					return nil, nil, err
				}

				err := vector.AppendFixed[types.Decimal128](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_decimal256:
			for rowIdx, row := range rows {
				var val types.Decimal256
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else if val, err = getDecimal256FromRowValue(row[colIndex], typ); err != nil {
					return nil, nil, err
				}

				err := vector.AppendFixed[types.Decimal256](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		case types.T_enum:
			for rowIdx, row := range rows {
				var val types.Enum
				if row[colIndex] == nil {
					nsp.Add(uint64(rowIdx))
				} else {
					val = row[colIndex].(types.Enum)
				}

				err := vector.AppendFixed[types.Enum](bat.Vecs[colIndex], val, false, pool)
				if err != nil {
					return nil, nil, err
				}
			}
		default:
			return nil, nil, moerr.NewInternalErrorNoCtxf("unsupported type %d", typ.Oid)
		}

		bat.Vecs[colIndex].SetNulls(nsp)
	}
	return bat, planColDefs, nil
}

func getDecimal64FromRowValue(v any, typ types.Type) (types.Decimal64, error) {
	switch val := v.(type) {
	case types.Decimal64:
		return val, nil
	case string:
		return types.ParseDecimal64(val, typ.Width, typ.Scale)
	case []byte:
		return types.ParseDecimal64(string(val), typ.Width, typ.Scale)
	default:
		return 0, moerr.NewInternalErrorNoCtxf("%v can't convert to decimal64 type", v)
	}
}

func getDecimal128FromRowValue(v any, typ types.Type) (types.Decimal128, error) {
	switch val := v.(type) {
	case types.Decimal128:
		return val, nil
	case string:
		return types.ParseDecimal128(val, typ.Width, typ.Scale)
	case []byte:
		return types.ParseDecimal128(string(val), typ.Width, typ.Scale)
	default:
		return types.Decimal128{}, moerr.NewInternalErrorNoCtxf("%v can't convert to decimal128 type", v)
	}
}

func getDecimal256FromRowValue(v any, typ types.Type) (types.Decimal256, error) {
	switch val := v.(type) {
	case types.Decimal256:
		return val, nil
	case string:
		return types.ParseDecimal256(val, typ.Width, typ.Scale)
	case []byte:
		return types.ParseDecimal256(string(val), typ.Width, typ.Scale)
	default:
		return types.Decimal256{}, moerr.NewInternalErrorNoCtxf("%v can't convert to decimal256 type", v)
	}
}

func cleanBatch(pool *mpool.MPool, data ...*batch.Batch) {
	for _, item := range data {
		if item != nil {
			item.Clean(pool)
		}
	}
}

func mysqlColDef2PlanResultColDef(cols []Column) (*plan.ResultColDef, []types.Type, []string, error) {
	if len(cols) == 0 {
		return nil, nil, nil, nil
	}

	resultCols := make([]*plan.ColDef, len(cols))
	resultColTypes := make([]types.Type, len(cols))
	resultColNames := make([]string, len(cols))
	for i, col := range cols {
		resultColNames[i] = col.Name()
		resultCols[i] = &plan.ColDef{
			Name: col.Name(),
		}
		var pType plan.Type
		var tType types.Type
		switch col.ColumnType() {
		case defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_VARCHAR:
			pType = plan.Type{
				Id: int32(types.T_varchar),
			}
			tType = types.New(types.T_varchar, types.MaxVarcharLen, 0)
		case defines.MYSQL_TYPE_TEXT:
			pType = plan.Type{
				Id: int32(types.T_text),
			}
			tType = types.New(types.T_text, types.MaxVarcharLen, 0)
		case defines.MYSQL_TYPE_TINY:
			pType = plan.Type{
				Id: int32(types.T_int8),
			}
			tType = types.New(types.T_int8, 0, 0)
		case defines.MYSQL_TYPE_SHORT:
			pType = plan.Type{
				Id: int32(types.T_int16),
			}
			tType = types.New(types.T_int16, 0, 0)
		case defines.MYSQL_TYPE_YEAR:
			pType = plan.Type{
				Id:    int32(types.T_year),
				Width: 4,
			}
			tType = types.T_year.ToType()
		case defines.MYSQL_TYPE_LONG:
			pType = plan.Type{
				Id: int32(types.T_int32),
			}
			tType = types.New(types.T_int32, 0, 0)
		case defines.MYSQL_TYPE_LONGLONG:
			pType = plan.Type{
				Id: int32(types.T_int64),
			}
			tType = types.New(types.T_int64, 0, 0)
		case defines.MYSQL_TYPE_DOUBLE:
			pType = plan.Type{
				Id: int32(types.T_float64),
			}
			tType = types.New(types.T_float64, 0, 0)
		case defines.MYSQL_TYPE_FLOAT:
			pType = plan.Type{
				Id: int32(types.T_float32),
			}
			tType = types.New(types.T_float32, 0, 0)
		case defines.MYSQL_TYPE_DATE:
			pType = plan.Type{
				Id: int32(types.T_date),
			}
			tType = types.New(types.T_date, 0, 0)
		case defines.MYSQL_TYPE_TIME:
			pType = plan.Type{
				Id: int32(types.T_time),
			}
			tType = types.New(types.T_time, 0, 0)
		case defines.MYSQL_TYPE_DATETIME:
			pType = plan.Type{
				Id: int32(types.T_datetime),
			}
			tType = types.New(types.T_datetime, 0, 0)
		case defines.MYSQL_TYPE_TIMESTAMP:
			pType = plan.Type{
				Id: int32(types.T_timestamp),
			}
			tType = types.New(types.T_timestamp, 0, 0)
		case defines.MYSQL_TYPE_ENUM:
			pType = plan.Type{
				Id: int32(types.T_enum),
			}
			tType = types.New(types.T_enum, 0, 0)
		case defines.MYSQL_TYPE_DECIMAL, defines.MYSQL_TYPE_NEWDECIMAL:
			var err error
			tType, err = mysqlDecimalColType(col)
			if err != nil {
				return nil, nil, nil, err
			}
			pType = plan.Type{
				Id:    int32(tType.Oid),
				Width: tType.Width,
				Scale: tType.Scale,
			}
		default:
			return nil, nil, nil, moerr.NewInternalErrorNoCtxf("unsupported mysql type %d", col.ColumnType())
		}
		resultCols[i].Typ = pType
		resultColTypes[i] = tType
	}
	return &plan.ResultColDef{
		ResultCols: resultCols,
	}, resultColTypes, resultColNames, nil
}

const mysqlDecimalExtraLength uint32 = 1

type mysqlDecimalColumn interface {
	Column
	Decimal() uint8
}

func mysqlDecimalColType(col Column) (types.Type, error) {
	decimalCol, ok := col.(mysqlDecimalColumn)
	if !ok {
		return types.Type{}, moerr.NewInternalErrorNoCtxf("missing decimal scale for mysql type %d", col.ColumnType())
	}

	scale := int32(decimalCol.Decimal())
	precision, err := mysqlDecimalPrecisionFromColumn(col, scale)
	if err != nil {
		return types.Type{}, err
	}

	return mysqlDecimalType(precision, scale), nil
}

func mysqlDecimalPrecisionFromColumn(col Column, scale int32) (int32, error) {
	length := col.Length()
	if length == 0 {
		return 0, moerr.NewInternalErrorNoCtxf("missing decimal precision for mysql type %d", col.ColumnType())
	}

	// MySQL DECIMAL display length includes the sign and decimal point when present.
	metadataExtraLength := uint32(0)
	if scale > 0 {
		metadataExtraLength += mysqlDecimalExtraLength
	}
	if col.IsSigned() {
		metadataExtraLength += mysqlDecimalExtraLength
	}

	if length <= metadataExtraLength {
		return 0, moerr.NewInternalErrorNoCtxf("invalid decimal metadata length %d scale %d for mysql type %d", length, scale, col.ColumnType())
	}

	precision := length - metadataExtraLength
	if precision < uint32(scale) {
		return 0, moerr.NewInternalErrorNoCtxf("invalid decimal precision %d scale %d for mysql type %d", precision, scale, col.ColumnType())
	}
	if precision > uint32(types.T_decimal256.ToType().Width) {
		return 0, moerr.NewInternalErrorNoCtxf("invalid decimal precision %d for mysql type %d", precision, col.ColumnType())
	}

	return int32(precision), nil
}

func mysqlDecimalDisplayLength(precision, scale int32, signed bool) uint32 {
	length := uint32(precision)
	if scale > 0 {
		length += mysqlDecimalExtraLength
	}
	if signed && precision > 0 {
		length += mysqlDecimalExtraLength
	}
	if length == 0 {
		return 1
	}
	return length
}

func mysqlDecimalType(precision, scale int32) types.Type {
	switch {
	case precision > types.T_decimal128.ToType().Width:
		return types.New(types.T_decimal256, precision, scale)
	case precision > types.T_decimal64.ToType().Width:
		return types.New(types.T_decimal128, precision, scale)
	default:
		return types.New(types.T_decimal64, precision, scale)
	}
}

func setMysqlColumnTypeInfo(ctx context.Context, typ types.Type, col *MysqlColumn) error {
	if err := convertEngineTypeToMysqlType(ctx, typ.Oid, col); err != nil {
		return err
	}
	if typ.Oid == types.T_blob {
		length := uint32(math.MaxUint32)
		if typ.Width > 0 {
			length = uint32(typ.Width)
		}
		setMysqlBinaryBlobColumnMetadata(col, length)
		return nil
	}
	setMysqlColumnTypeMetadata(col, typ)
	setCharacter(col)
	switch typ.Charset {
	case types.CharsetUTF8:
		// CharsetUTF8 is MatrixOne's explicit utf8mb4_general_ci identity.
		// setCharacter uses the older utf8_general_ci protocol default, so
		// override it with the exact utf8mb4 collation ID.
		col.SetCharset(uint16(Utf8mb4CollationID))
	case types.CharsetUTF8MB4Bin:
		// A _bin collation still describes nonbinary UTF-8 text. Protocol
		// collation 63 is reserved for the binary character set.
		col.SetCharset(uint16(utf8mb4BinCollationID))
	case types.CharsetBinary:
		// Some internal functions intentionally return packed bytes in a VARCHAR
		// container. Keep those values binary even though their physical OID is a
		// text OID; clients must not attempt UTF-8 conversion on the payload.
		col.SetCharset(charsetBinary)
	}
	if typ.Oid == types.T_binary || typ.Oid == types.T_varbinary {
		col.SetFlag(col.Flag() | uint16(defines.BINARY_FLAG))
	}
	return nil
}

func setMysqlBinaryBlobColumnMetadata(col *MysqlColumn, length uint32) {
	col.SetColumnType(defines.MYSQL_TYPE_BLOB)
	col.SetCharset(charsetBinary)
	col.SetLength(length)
	col.SetFlag(col.Flag() | uint16(defines.BLOB_FLAG|defines.BINARY_FLAG))
}

const mysqlDecimalNotSpecified = 0x1f

func setMysqlColumnTypeMetadata(col *MysqlColumn, typ types.Type) {
	if typ.IsDecimal() {
		// DECIMAL display length depends on scale and signedness, not just precision.
		col.SetLength(mysqlDecimalDisplayLength(typ.Width, typ.Scale, col.IsSigned()))
	} else if typ.Oid == types.T_year {
		// Keep YEAR metadata consistent with regular query result columns.
		col.SetLength(4)
	} else if typ.Oid == types.T_date {
		col.SetLength(10)
	} else if typ.Oid == types.T_time {
		col.SetLength(mysqlTemporalDisplayLength(10, typ.Scale))
	} else if typ.Oid == types.T_datetime || typ.Oid == types.T_timestamp {
		col.SetLength(mysqlTemporalDisplayLength(19, typ.Scale))
	} else if typ.Oid == types.T_text {
		// TEXT-family widths are already declared in bytes. A width of zero is
		// the ordinary TEXT declaration (65535 bytes), not an empty result.
		length := uint32(types.MaxStringSize)
		if typ.Width > 0 {
			length = uint32(typ.Width)
		}
		col.SetLength(length)
	} else if typ.Oid == types.T_char || typ.Oid == types.T_varchar {
		// Protocol::ColumnDefinition41 expresses column_length in bytes. Character
		// string widths are declared in characters, so the byte multiplier must
		// match the collation emitted by setMysqlColumnTypeInfo.
		if typ.Oid == types.T_varchar && typ.Width == 0 {
			// Synthesized VARCHAR result columns historically use zero as an
			// unspecified width and must keep their unbounded metadata.
			col.SetLength(math.MaxUint32)
		} else {
			col.SetLength(mysqlStringColumnLength(typ.Width, mysqlTextMaxBytesPerCharacter(typ.Charset)))
		}
	} else if typ.Oid == types.T_binary || typ.Oid == types.T_varbinary {
		// Binary string widths are already declared in bytes.
		col.SetLength(mysqlStringColumnLength(typ.Width, 1))
	} else {
		setColLength(col, typ.Width)
	}
	// MySQL uses 0x1f (DECIMAL_NOT_SPECIFIED) for FLOAT and DOUBLE
	// without an explicit display scale. Clients use this metadata when
	// converting binary floating-point results to text.
	if (typ.Oid == types.T_float32 || typ.Oid == types.T_float64) &&
		(typ.Scale < 0 || typ.Width == 0 && typ.Scale == 0) {
		col.SetDecimal(mysqlDecimalNotSpecified)
		return
	}
	col.SetDecimal(typ.Scale)
}

func mysqlTemporalDisplayLength(base int, scale int32) uint32 {
	if scale > 0 {
		return uint32(base + 1 + int(scale))
	}
	return uint32(base)
}

func mysqlTextMaxBytesPerCharacter(charset uint8) uint32 {
	switch charset {
	case types.CharsetUTF8, types.CharsetUTF8MB4Bin:
		return utf8mb4MaxBytesPerCharacter
	case types.CharsetBinary:
		return 1
	default:
		// Legacy and unknown text metadata is emitted as utf8_general_ci.
		return utf8MaxBytesPerCharacter
	}
}

func mysqlStringColumnLength(width int32, maxBytesPerCharacter uint32) uint32 {
	if width < 0 {
		return math.MaxUint32
	}
	length := uint64(width) * uint64(maxBytesPerCharacter)
	if length > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(length)
}

// errCodeRollbackWholeTxn denotes that the error code
// that should rollback the whole txn
var errCodeRollbackWholeTxn = map[uint16]bool{
	moerr.ErrRetryForCNRollingRestart: false,
	moerr.ErrDeadLockDetected:         false,
	moerr.ErrLockTableBindChanged:     false,
	moerr.ErrLockTableNotFound:        false,
	moerr.ErrDeadlockCheckBusy:        false,
	moerr.ErrLockConflict:             false,
	moerr.ErrRemoteLockWaitTimeout:    false,
	moerr.ErrLockWaitTimeout:          false,
	moerr.ErrTxnUnknown:               false,
	moerr.ErrBackendClosed:            false,
	moerr.ErrNoAvailableBackend:       false,
	moerr.ErrBackendCannotConnect:     false,
}

// sessionRollsBackTxnOnError reports whether the session has opted into
// treating this error as fatal to the whole transaction rather than to the
// statement alone.
//
// The static errCodeRollbackWholeTxn set above is infrastructure -- deadlock,
// lock timeout, a backend that went away -- failures after which the
// transaction genuinely cannot continue, and it is only twelve of the ~240
// error codes MO defines. Every other error, from a syntax error to a
// constraint violation, rolls back the statement alone and leaves the
// transaction open, which is MySQL's behaviour and MO's default. An
// application that treats any failed statement as fatal to its unit of work
// can ask for the stricter behaviour per session.
//
// Only real errors qualify. moerr also carries Ok signals, Info codes and
// Warning codes; a warning such as a truncated value travels as the same type
// but must never discard a transaction, so IsRealError gates this.
//
// A background session never opts in: backSession.GetSessionSysVar answers nil
// for anything outside its small allowlist, so internal work -- catalog
// maintenance, restores, the statement of another user's session -- keeps
// MySQL semantics even when the variable is set globally.
func sessionRollsBackTxnOnError(ses FeSession, inputErr error) bool {
	if ses == nil || inputErr == nil {
		return false
	}
	// Only moerr distinguishes an error from a warning, and only a warning is
	// exempt. Anything that is NOT a moerr has no warning form to be -- it is
	// a failure -- so it must roll back like any other error, or the setting
	// would silently mean "any error MO happens to have wrapped".
	var me *moerr.Error
	if errors.As(inputErr, &me) && !me.IsRealError() {
		return false
	}
	val, err := ses.GetSessionSysVar("mo_rollback_txn_on_error")
	if err != nil {
		return false
	}
	v, _ := val.(int8)
	return v > 0
}

func isErrorRollbackWholeTxn(inputErr error) bool {
	if inputErr == nil {
		return false
	}
	me, ok := inputErr.(*moerr.Error)
	if !ok {
		// This is not a moerr
		return false
	}
	if _, has := errCodeRollbackWholeTxn[me.ErrorCode()]; has {
		return true
	}
	return false
}

func getRandomErrorRollbackWholeTxn() error {
	x := rand.Intn(len(errCodeRollbackWholeTxn))
	arr := make([]uint16, 0, len(errCodeRollbackWholeTxn))
	for k := range errCodeRollbackWholeTxn {
		arr = append(arr, k)
	}
	return newErrorRollbackWholeTxn(arr[x])
}

// newErrorRollbackWholeTxn keeps the test error factory in sync with
// errCodeRollbackWholeTxn. Its deterministic input lets tests cover every map
// entry instead of relying on getRandomErrorRollbackWholeTxn to select it.
func newErrorRollbackWholeTxn(code uint16) error {
	switch code {
	case moerr.ErrRetryForCNRollingRestart:
		return moerr.NewRetryForCNRollingRestart()
	case moerr.ErrDeadLockDetected:
		return moerr.NewDeadLockDetectedNoCtx()
	case moerr.ErrLockTableBindChanged:
		return moerr.NewLockTableBindChangedNoCtx()
	case moerr.ErrLockTableNotFound:
		return moerr.NewLockTableNotFoundNoCtx()
	case moerr.ErrDeadlockCheckBusy:
		return moerr.NewDeadlockCheckBusyNoCtx()
	case moerr.ErrLockConflict:
		return moerr.NewLockConflictNoCtx()
	case moerr.ErrRemoteLockWaitTimeout:
		return moerr.NewRemoteLockWaitTimeoutNoCtx()
	case moerr.ErrLockWaitTimeout:
		return moerr.NewLockWaitTimeoutNoCtx()
	case moerr.ErrTxnUnknown:
		return moerr.NewTxnUnknown(context.Background(), "test")
	case moerr.ErrBackendClosed:
		return moerr.NewBackendClosedNoCtx()
	case moerr.ErrNoAvailableBackend:
		return moerr.NewNoAvailableBackendNoCtx()
	case moerr.ErrBackendCannotConnect:
		return moerr.NewBackendCannotConnectNoCtx("test")
	default:
		panic(fmt.Sprintf("unsupported error code %d", code))
	}
}

func skipClientQuit(info string) bool {
	return strings.Contains(info, quitStr)
}

// UserInput
// normally, just use the sql.
// for some special statement, like 'set_var', we need to use the stmt.
// if the stmt is not nil, we neglect the sql.
type UserInput struct {
	sql              string
	hashedSql        string
	stmtName         string
	stmt             tree.Statement
	parserSQLMode    string
	useParserSQLMode bool
	rewritePolicy    *rewritePolicySnapshot
	// rewritePolicyMaterialized means sql already carries the frozen policy as
	// a leading hint. Nested ANALYZE queries use it to enable hint decoding
	// without injecting the same rules a second time.
	rewritePolicyMaterialized bool
	preparePlan               *plan.Plan // binary protocol execute
	sqlSourceType             []string
	isRestore                 bool
	isBinaryProtExecute       bool
	// preparedDefaultDatabase is captured from COM_STMT_EXECUTE before txn
	// admission; binary execution passes the inner AST rather than tree.Execute.
	preparedDefaultDatabase string
	// isCursorExecute marks a COM_STMT_EXECUTE using MySQL's
	// CURSOR_TYPE_READ_ONLY flag. Its rows are retained for COM_STMT_FETCH.
	isCursorExecute bool
	// isSetExpression marks an AST-only SELECT synthesized to evaluate a SET
	// assignment. Such statements have no stable SQL cache key.
	isSetExpression bool
	// isPreparedExpression marks a nested SET-derived expression that is being
	// evaluated as part of prepared-statement execution.
	isPreparedExpression  bool
	preparedParamVals     []any
	preparedBinaryExecute bool
	// isInternalInput mark this UserInput is come from mo internal.
	// replace old logic: (stmt != nil)
	// cc isInternal()
	isInternalInput bool
	// operator account, the account executes restoration
	// e.g. sys takes a snapshot sn1 for acc1, then restores acc1 from snapshot sn1. In this scenario, sys is the operator account
	isRestoreByTs bool
	opAccount     uint32
	toAccount     uint32
	// remapDb carries the policy captured when a prepared statement was built.
	// EXECUTE text has no original rewrite hint, so the policy must be restored
	// explicitly before authorization and planning.
	remapDb map[string]string
}

func (ui *UserInput) getSql() string {
	return ui.sql
}

func (ui *UserInput) genHash() {
	ui.hashedSql = hashString(ui.sql)
}

func (ui *UserInput) getHash() string {
	return ui.hashedSql
}

func (ui *UserInput) getPreparePlan() *plan.Plan {
	return ui.preparePlan
}

// getStmt if the stmt is not nil, we neglect the sql.
func (ui *UserInput) getStmt() tree.Statement {
	return ui.stmt
}

func (ui *UserInput) getSqlSourceTypes() []string {
	return ui.sqlSourceType
}

// isInternal return true if the stmt is not nil.
// it means the statement is not from any client.
// currently, we use it to handle the 'set_var' statement.
func (ui *UserInput) isInternal() bool {
	return ui.isInternalInput
}

func (ui *UserInput) isPreparedExpr() bool {
	return ui != nil && ui.isPreparedExpression
}

func (ui *UserInput) canUsePlanCache() bool {
	return ui != nil && !ui.isSetExpression
}

func (ui *UserInput) genSqlSourceType(ses FeSession) {
	sql := ui.getSql()
	ui.sqlSourceType = nil
	if ui.isInternal() {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	tenant := ses.GetTenantInfo()
	if tenant == nil || strings.HasPrefix(sql, cmdFieldListSql) {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	flag, _, _ := isSpecialUser(tenant.GetUser())
	if flag {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	if tenant.GetTenant() == sysAccountName && tenant.GetUser() == "internal" {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	for len(sql) > 0 {
		p1 := strings.Index(sql, "/*")
		p2 := strings.Index(sql, "*/")
		if p1 < 0 || p2 < 0 || p2 <= p1+1 {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.ExternSql)
			return
		}
		source := strings.TrimSpace(sql[p1+2 : p2])
		if source == cloudUserTag {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.CloudUserSql)
		} else if source == cloudNoUserTag {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.CloudNoUserSql)
		} else if source == saveResultTag {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.CloudUserSql)
		} else {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.ExternSql)
		}
		sql = sql[p2+2:]
	}
}

func (ui *UserInput) getSqlSourceType(i int) string {
	sqlType := constant.ExternSql
	if i < len(ui.sqlSourceType) {
		sqlType = ui.sqlSourceType[i]
	}
	return sqlType
}

const (
	issue3482SqlPrefix    = "load data local infile"
	issue3482SqlPrefixLen = len(issue3482SqlPrefix)
)

// !!!NOTE!!! For debug
// https://github.com/matrixorigin/MO-Cloud/issues/3482
// TODO: remove it in the future
func (ui *UserInput) isIssue3482Sql() bool {
	if ui == nil {
		return false
	}
	sql := ui.getSql()
	sqlLen := len(sql)
	if sqlLen <= issue3482SqlPrefixLen {
		return false
	}
	return strings.HasPrefix(strings.ToLower(sql), issue3482SqlPrefix)
}

func unboxExprStr(ctx context.Context, expr tree.Expr) (string, error) {
	if e, ok := expr.(*tree.NumVal); ok && e.ValType == tree.P_char {
		return e.String(), nil
	}
	return "", moerr.NewInternalError(ctx, "invalid expr type")
}

type strParamBinder struct {
	ctx    context.Context
	params *vector.Vector
	err    error
}

func (b *strParamBinder) bind(e tree.Expr) string {
	if b.err != nil {
		return ""
	}

	switch val := e.(type) {
	case *tree.NumVal:
		return val.String()
	case *tree.ParamExpr:
		return b.params.GetStringAt(val.Offset - 1)
	default:
		b.err = moerr.NewInternalErrorf(b.ctx, "invalid params type %T", e)
		return ""
	}
}

func (b *strParamBinder) bindIdentStr(ident *tree.AccountIdentified) string {
	if b.err != nil {
		return ""
	}

	switch ident.Typ {
	case tree.AccountIdentifiedByPassword,
		tree.AccountIdentifiedWithSSL:
		return b.bind(ident.Str)
	default:
		return ""
	}
}

func resetBits(t *uint32, val uint32) {
	if t == nil {
		return
	}
	*t = val
}

func setBits(t *uint32, bit uint32) {
	if t == nil {
		return
	}
	*t |= bit
}

func clearBits(t *uint32, bit uint32) {
	if t == nil {
		return
	}
	*t &= ^bit
}

func bitsIsSet(t uint32, bit uint32) bool {
	return t&bit != 0
}

func attachValue(ctx context.Context, key, val any) context.Context {
	if ctx == nil {
		panic("context is nil")
	}

	return context.WithValue(ctx, key, val)
}

const KeySep = objectkey.Separator

func genKey(dbName, tblName string) string {
	return objectkey.Encode(dbName, tblName)
}

func normalizeViewDependencyKey(key string) (string, error) {
	databaseName, viewName, _, err := plan2.ParseViewDependencyKey(key)
	if err != nil {
		return "", err
	}
	return genKey(databaseName, viewName), nil
}

func splitKey(key string) (string, string) {
	return objectkey.Decode(key)
}

type toposort struct {
	next map[string][]string
}

func (g *toposort) addVertex(v string) {
	if _, ok := g.next[v]; ok {
		return
	}
	g.next[v] = make([]string, 0)
}

func (g *toposort) addEdge(from, to string) {
	if _, ok := g.next[from]; !ok {
		g.next[from] = make([]string, 0)
	}
	g.next[from] = append(g.next[from], to)
}

func (g *toposort) sort() (ans []string, err error) {
	inDegree := make(map[string]uint)
	for u := range g.next {
		inDegree[u] = 0
	}
	for _, nextVertices := range g.next {
		for _, v := range nextVertices {
			inDegree[v] += 1
		}
	}

	var noPreVertices []string
	for v, deg := range inDegree {
		if deg == 0 {
			noPreVertices = append(noPreVertices, v)
		}
	}

	for len(noPreVertices) > 0 {
		// find vertex whose inDegree = 0
		v := noPreVertices[0]
		noPreVertices = noPreVertices[1:]
		ans = append(ans, v)

		// update the next vertices from v
		for _, to := range g.next[v] {
			inDegree[to] -= 1
			if inDegree[to] == 0 {
				noPreVertices = append(noPreVertices, to)
			}
		}
	}

	if len(ans) != len(inDegree) {
		err = moerr.NewInternalErrorNoCtx("There is a cycle in dependency graph")
	}
	return
}

func ToRequest(payload []byte) *Request {
	req := &Request{
		cmd:  CommandType(payload[0]),
		data: payload[1:],
	}

	return req
}

// CancelCheck checks if the given context has been canceled.
// If the context is canceled, it returns the context's error.
func CancelCheck(Ctx context.Context) error {
	select {
	case <-Ctx.Done():
		return Ctx.Err()
	default:
		return nil
	}
}

func checkMoreResultSet(status uint16, isLastStmt bool) uint16 {
	if !isLastStmt {
		status |= SERVER_MORE_RESULTS_EXISTS
	}
	return status
}

func Copy[T any](src []T) []T {
	if src == nil {
		return nil
	}
	if len(src) == 0 {
		return []T{}
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}

func hashString(s string) string {
	hash := sha256.New()
	hash.Write(commonutil.UnsafeStringToBytes(s))
	hashBytes := hash.Sum(nil)
	return hex.EncodeToString(hashBytes)
}

func colDef2MysqlColumn(ctx context.Context, col *plan.ColDef) (*MysqlColumn, error) {
	var err error
	c := new(MysqlColumn)
	c.SetName(col.Name)
	c.SetOrgName(col.GetOriginCaseName())
	c.SetTable(col.TblName)
	orgTable := col.OriginTblName
	if orgTable == "" {
		orgTable = col.TblName
	}
	c.SetOrgTable(orgTable)
	c.SetAutoIncr(col.Typ.AutoIncr)
	c.SetSchema(col.DbName)
	typ := types.NewWithCharset(
		types.T(col.Typ.Id), col.Typ.Width, col.Typ.Scale, uint8(col.Typ.Charset),
	)
	if err = setMysqlColumnTypeInfo(ctx, typ, c); err != nil {
		return nil, err
	}
	if typ.Oid == types.T_blob && col.OriginTblName != "" {
		// A directly selected table BLOB has MySQL's regular BLOB capacity.
		// Width-less computed BLOB expressions keep the conservative upper bound
		// installed by setMysqlColumnTypeInfo instead.
		c.SetLength(math.MaxUint16)
	}
	setColFlag(c, col)

	// For TIMESTAMPADD function compatibility with MySQL:
	// GetResultColumnsFromPlan sets the return type based on input type and unit:
	// - DATE input + date unit → DATE type (MYSQL_TYPE_DATE)
	// - DATE input + time unit → DATETIME type (MYSQL_TYPE_DATETIME)
	// - DATETIME input → DATETIME type (MYSQL_TYPE_DATETIME)

	convertMysqlTextTypeToBlobType(c)
	return c, nil
}

// isLegal checks if the sqls are legal parsed by the mo parser.
// if there is at least one sql can be parsed, it returns true
func isLegal(name string, sqls []string) bool {
	name = strings.TrimSpace(name)
	if len(name) == 0 || len(sqls) == 0 {
		return false
	}
	for _, sql := range sqls {
		if len(sql) == 0 {
			return false
		}
	}
	yes := false
	for _, sql := range sqls {
		_, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		if err != nil {
			continue
		}
		yes = true
		break
	}
	return yes
}

// hasSpecialChars checks the string have special characters (','  '.'  ':' '`')
func hasSpecialChars(s string) bool {
	return strings.ContainsAny(s, ",.:`")
}

/*
accountNameIsLegal checks the account name legal or not.
rule:

	if create account name or create account `name` can succeed,
	it is legal.

	it means all most all string can be legal.
*/
func accountNameIsLegal(name string) bool {
	if hasSpecialChars(name) {
		return false
	}
	name = strings.TrimSpace(name)
	createAccountSqls := []string{
		"create account " + name + " ADMIN_NAME 'admin' IDENTIFIED BY '111'",
		"create account `" + name + "` ADMIN_NAME 'admin' IDENTIFIED BY '111'",
	}
	return isLegal(name, createAccountSqls)
}

/*
dbNameIsLegal checks the database name legal or not.
rule:

	if create database name or create database `name` can succeed,
	it is legal.

	it means all most all string can be legal.
*/
func dbNameIsLegal(name string) bool {
	name = strings.TrimSpace(name)
	if hasSpecialChars(name) {
		return false
	}
	if name == cdc.CDCPitrGranularity_All {
		return true
	}

	createDBSqls := []string{
		"create database " + name,
		"create database `" + name + "`",
	}
	return isLegal(name, createDBSqls)
}

/*
tableNameIsLegal checks the table name legal or not.
rule:

	if create table name or create table `name` can succeed,
	it is legal.

	it means all most all string can be legal.
*/
func tableNameIsLegal(name string) bool {
	name = strings.TrimSpace(name)
	if hasSpecialChars(name) {
		return false
	}
	if name == cdc.CDCPitrGranularity_All {
		return true
	}

	createTableSqls := []string{
		"create table " + name + "(a int)",
		"create table `" + name + "`(a int)",
	}
	return isLegal(name, createTableSqls)
}

//func tableNameIsRegexpr(s string) bool {
//	if len(s) < 2 {
//		return false
//	}
//	if strings.HasPrefix(s, "/") && strings.HasSuffix(s, "/") {
//		_, err := regexp.Compile(s)
//		if err != nil {
//			return false
//		}
//		return true
//	}
//	return false
//}

// replaceStr replaces s[start:end] by s2
func replaceStr(s string, start, end int, s2 string) string {
	if start >= end || start < 0 || end < 0 {
		return s
	}
	if end <= len(s) {
		return s[:start] + s2 + s[end:]
	}
	return s
}

func buildTableDefFromMoColumns(ctx context.Context, accountId uint64, dbName, table string, ses FeSession) (*plan.TableDef, error) {
	bh := NewShareTxnBackgroundExec(ctx, ses, false)
	defer bh.Close()
	var (
		sql     string
		erArray []ExecResult
		err     error
	)

	sql, err = getTableColumnDefSql(accountId, dbName, table)
	if err != nil {
		return nil, err
	}

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}
	if !execResultArrayHasData(erArray) {
		return nil, moerr.NewNoSuchTable(ctx, dbName, table)
	}

	cols, err := extractTableDefColumns(erArray, ctx, dbName, table)
	if err != nil {
		return nil, err
	}

	return &plan.TableDef{
		Name:   table,
		DbName: dbName,
		Cols:   cols,
	}, nil
}

func extractTableDefColumns(erArray []ExecResult, ctx context.Context, dbName, table string) ([]*plan.ColDef, error) {
	cols := make([]*plan.ColDef, 0)
	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			colName, err := result.GetString(ctx, i, 0)
			if err != nil {
				return nil, err
			}

			colType, err := result.GetString(ctx, i, 1)
			if err != nil {
				return nil, err
			}

			typ := new(types.Type)
			err = typ.Unmarshal([]byte(colType))
			if err != nil {
				return nil, err
			}

			colNum, err := result.GetUint64(ctx, i, 2)
			if err != nil {
				return nil, err
			}

			attDefault, err := result.GetString(ctx, i, 4)
			if err != nil {
				return nil, err
			}
			def := new(plan.Default)
			err = types.Decode([]byte(attDefault), def)
			if err != nil {
				return nil, err
			}

			isHidden, err := result.GetInt64(ctx, i, 6)
			if err != nil {
				return nil, err
			}

			cols = append(cols, &plan.ColDef{
				TblName:    table,
				DbName:     dbName,
				ColId:      colNum,
				Name:       strings.ToLower(colName),
				OriginName: colName,
				Hidden:     isHidden == 1,
				Typ: plan.Type{
					Id:          int32(typ.Oid),
					Width:       typ.Width,
					Scale:       typ.Scale,
					Charset:     uint32(typ.Charset),
					Table:       table,
					NotNullable: !def.NullAbility,
				},
				Default: def,
			})
		}
	}
	return cols, nil
}

var _ Allocator = new(LeakCheckAllocator)

const (
	leakCheckAllocatorModeNormal = iota
	leakCheckAllocatorModeAllocReturnErr
	leakCheckAllocatorModeAllocPanic
)

type LeakCheckAllocator struct {
	sync.Mutex
	allocated uint64
	freed     uint64
	records   map[unsafe.Pointer]int
	mod       int
}

func NewLeakCheckAllocator() *LeakCheckAllocator {
	return &LeakCheckAllocator{
		records: make(map[unsafe.Pointer]int),
	}
}

func (lca *LeakCheckAllocator) Alloc(capacity int) ([]byte, error) {
	lca.Lock()
	defer lca.Unlock()
	if lca.mod == leakCheckAllocatorModeAllocReturnErr {
		return nil, moerr.NewInternalErrorNoCtx("leak check allocator returns eror")
	} else if lca.mod == leakCheckAllocatorModeAllocPanic {
		panic("leak check allocator panic")
	}
	buf := make([]byte, capacity)
	lca.allocated += uint64(len(buf))
	lca.records[unsafe.Pointer(&buf[0])] = capacity
	return buf, nil
}

func (lca *LeakCheckAllocator) Free(bytes []byte) {
	if len(bytes) == 0 {
		return
	}
	lca.Lock()
	defer lca.Unlock()
	if _, ok := lca.records[unsafe.Pointer(&bytes[0])]; ok {
		delete(lca.records, unsafe.Pointer(&bytes[0]))
	} else {
		panic(fmt.Sprintf("no such ptr %v", unsafe.Pointer(&bytes[0])))
	}
	lca.freed += uint64(len(bytes))
}

func (lca *LeakCheckAllocator) CheckBalance() bool {
	lca.Lock()
	defer lca.Unlock()
	return lca.allocated == lca.freed && len(lca.records) == 0
}
