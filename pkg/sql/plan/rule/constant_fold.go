// Copyright 2021 - 2022 Matrix Origin
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

package rule

import (
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type ConstantFold struct {
	bat        *batch.Batch
	isPrepared bool
}

func NewConstantFold(isPrepared bool) *ConstantFold {
	bat := batch.EmptyForConstFoldBatch
	return &ConstantFold{
		bat:        bat,
		isPrepared: isPrepared,
	}
}

func (r *ConstantFold) GetBatch() *batch.Batch {
	return r.bat
}

// Match always true
func (r *ConstantFold) Match(node *plan.Node) bool {
	return true
}

func (r *ConstantFold) Apply(node *plan.Node, _ *plan.Query, proc *process.Process) {
	if node.Limit != nil {
		node.Limit = r.constantFold(node.Limit, proc)
	}
	if node.Offset != nil {
		node.Offset = r.constantFold(node.Offset, proc)
	}

	for i := range node.OnList {
		node.OnList[i] = r.constantFold(node.OnList[i], proc)
	}

	for i := range node.FilterList {
		node.FilterList[i] = r.constantFold(node.FilterList[i], proc)
	}

	for i := range node.BlockFilterList {
		node.BlockFilterList[i] = r.constantFold(node.BlockFilterList[i], proc)
	}

	for i := range node.ProjectList {
		node.ProjectList[i] = r.constantFold(node.ProjectList[i], proc)
	}

	for i := range node.GroupBy {
		node.GroupBy[i] = r.constantFold(node.GroupBy[i], proc)
	}

	// for i := range n.GroupingSet {
	// 	n.GroupingSet[i] = r.constantFold(n.GroupingSet[i], proc)
	// }

	for i := range node.AggList {
		node.AggList[i] = r.constantFold(node.AggList[i], proc)
	}

	for i := range node.WinSpecList {
		node.WinSpecList[i] = r.constantFold(node.WinSpecList[i], proc)
	}

	for _, orderBy := range node.OrderBy {
		orderBy.Expr = r.constantFold(orderBy.Expr, proc)
	}

	if node.IndexReaderParam != nil {
		for _, orderBy := range node.IndexReaderParam.OrderBy {
			orderBy.Expr = r.constantFold(orderBy.Expr, proc)
		}

		node.IndexReaderParam.Limit = r.constantFold(node.IndexReaderParam.Limit, proc)

		if distRange := node.IndexReaderParam.DistRange; distRange != nil {
			distRange.LowerBound = r.constantFold(distRange.LowerBound, proc)
			distRange.UpperBound = r.constantFold(distRange.UpperBound, proc)
		}
	}

	// for i := range n.TblFuncExprList {
	// 	n.TblFuncExprList[i] = r.constantFold(n.TblFuncExprList[i], proc)
	// }

	// for i := range n.FillVal {
	// 	n.FillVal[i] = r.constantFold(n.FillVal[i], proc)
	// }

	for i := range node.OnUpdateExprs {
		node.OnUpdateExprs[i] = r.constantFold(node.OnUpdateExprs[i], proc)
	}
}

// IsNullIntegerArgumentCast protects the source domain until EXECUTE.
// Folding this to an INT64 NULL would make a selecting expression choose a
// signed domain even when its actual non-NULL source is UINT64.
func IsNullIntegerArgumentCast(expr *plan.Expr) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 2 {
		return false
	}
	id, overload := function.DecodeOverloadID(fn.Func.Obj)
	return id == function.CAST &&
		function.IsIntegerArgumentCastOverload(overload) &&
		fn.Args[0].GetLit().GetIsnull()
}

func (r *ConstantFold) constantFold(expr *plan.Expr, proc *process.Process) *plan.Expr {
	return r.foldExpr(expr, proc, false, false)
}

func (r *ConstantFold) foldExpr(expr *plan.Expr, proc *process.Process, spatialChecked, jsonFeatureChecked bool) *plan.Expr {
	if expr == nil {
		return expr
	}

	if expr.Typ.Id == int32(types.T_interval) {
		// INTERVAL is meaningful as an argument to temporal functions, but the
		// expression executor cannot materialize it as a standalone scalar
		// literal. Retaining the expression is the constant-fold rule's normal
		// fail-closed result; a public scalar boundary rejects it if no enclosing
		// temporal operation consumes it.
		return expr
	}

	fn := expr.GetF()
	if fn == nil {
		if elist := expr.GetList(); elist != nil {
			exprList := elist.List
			cannotFold := false
			for i := range exprList {
				exprList[i] = r.foldExpr(exprList[i], proc, spatialChecked, jsonFeatureChecked)
				if exprList[i].GetLit() == nil {
					cannotFold = true
				}
			}

			if cannotFold {
				return expr
			}
			requiresStringProvenance, err := plan.RequiresMORPCVersion23StringProvenance(exprList)
			if err != nil || requiresStringProvenance {
				// LiteralVec uses the stable Vector wire format, which cannot carry
				// per-item runtime string domains. Keep the literal list executable
				// and visible to the remote protocol capability analysis.
				return expr
			}
			requiresDecimalProvenance, err := plan.RequiresMORPCVersion89DecimalLiteralSemantics(exprList)
			if err != nil {
				return expr
			}
			isSerialized := ContainsSerializedLiteral(exprList)

			vec, err := colexec.GenerateConstListExpressionExecutor(proc, exprList)
			if err != nil {
				return expr
			}
			defer vec.Free(proc.Mp())
			if vec.GetStringSources() != nil {
				// LiteralVec has only a uniform source field. Keep mixed-owner
				// lists structured so each scalar literal retains its identity.
				return expr
			}

			// Nullable IN-lists must keep their null bitmap aligned with values.
			if !vec.IsConstNull() && !vec.GetNulls().Any() {
				if err := vec.InplaceSortAndCompact(proc.Mp()); err != nil {
					return expr
				}
			}

			data, err := vec.MarshalBinary()
			if err != nil {
				return expr
			}

			return &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Vec{
					Vec: &plan.LiteralVec{
						Len:                       int32(vec.Length()),
						Data:                      data,
						IsSerialized:              isSerialized,
						StringSource:              uint32(vec.GetStringSource()),
						DecimalLiteralRequiresV82: requiresDecimalProvenance,
					},
				},
			}
		}

		return expr
	}
	// The active planning generation's decision barrier pins the operand
	// structure of recorded provenance-bearing coercive decisions: folding
	// would erase the distinguishing pre-coercion syntax those decisions'
	// ledger entries stand on (issue #30938, design decision log 55). A
	// barrier-matched node skips the argument recursion and the destructive
	// rewrite alike, so the recorded witness structure survives every
	// post-recording fold; provenance-free statements answer from the empty
	// set and fold base-identically.
	if DecimalBoundaryBarrierHit(proc, expr) {
		return expr
	}

	overloadID := fn.Func.GetObj()
	if r.isPrepared && IsNullIntegerArgumentCast(expr) {
		return expr
	}
	f, exists := function.GetFunctionByIdWithoutError(overloadID)

	if !exists {
		return expr
	}
	// Share the feature walk between spatial and JSON admission. Keep positive
	// identities executable and propagate the negative proof to descendants;
	// JSON ELT's selected subtree still belongs to its lazy executor.
	if !spatialChecked || !jsonFeatureChecked {
		features, err := plan.RequiredRemoteExpressionFeatures(expr)
		if err != nil || (!spatialChecked && features.SpatialDistanceSemantics) ||
			(!jsonFeatureChecked && (features.JSONStringConsumerOverload || features.JSONOpaqueTextContracts)) {
			return expr
		}
		spatialChecked, jsonFeatureChecked = true, true
	}
	// ELT's selected-subtree evaluation belongs to its lazy executor; eagerly
	// folding every argument would evaluate unselected invalid expressions.
	functionID, overload := function.DecodeOverloadID(overloadID)
	if functionID == function.ELT && overload == 1 {
		return expr
	}
	// Row-scoped diagnostics are admitted speculatively below; only a
	// warning-free evaluation may replace them with a literal.
	if f.IsVolatile() {
		return expr
	}
	if IsLegacyTimeAssignmentOutsideInternalRange(fn) {
		return expr
	}
	if f.IsRealTimeRelated() && r.isPrepared {
		return expr
	}
	if r.isPrepared && IsImplicitFloatCastOfExplicitDecimalConstant(expr) {
		// A parameterized parent can be rebound from FLOAT to DECIMAL at execute
		// time. Keep the exact explicit DECIMAL source available underneath the
		// provisional binder cast; folding it to FLOAT here irreversibly loses
		// digits before execute-time common-type specialization can run.
		return expr
	}
	isVec := false
	for i := range fn.Args {
		fn.Args[i] = r.foldExpr(fn.Args[i], proc, spatialChecked, jsonFeatureChecked)
		isVec = isVec || fn.Args[i].GetVec() != nil && !IsScalarConstantVector(fn.Args[i])
	}
	if r.isPrepared && ContainsSqlModeDependentTemporalCall(expr) {
		return expr
	}
	if f.IsAgg() || f.IsWin() {
		return expr
	}
	if !function.IsFoldCandidate(expr, false) {
		return expr
	}
	if isVec && !CanMaterializeConstantVector(expr) {
		return expr
	}

	if ContainsTruncatedFloatTextConversion(expr) {
		return expr
	}

	// Preserve diagnostic-bearing division subtrees through their parents;
	// a later execution may use a different sql_mode or statement policy.
	if ContainsSqlModeDependentDivision(expr) {
		return expr
	}

	// A constant tree carrying widened decimal multiplication provenance
	// keeps its executable expression shape across every destructive fold
	// shape — valued scalar-vector materialization and the Isnull literal
	// return alike — because the fold erases exactly the multiplication
	// provenance the CTAS storage boundary's envelope recognition reads
	// (issue #30938, design decision log 54). Constant trees without
	// provenance fold unchanged, and the unaffected argument recursion above
	// already gave them the base engine's results.
	if CarriesWideDecimalMultiplicationProvenance(expr) {
		return expr
	}

	vec, free, warned, err := EvaluateConstantExpression(proc, expr, r.bat)
	if err != nil {
		return expr
	}
	defer free()
	if warned {
		return expr
	}

	if isVec {
		requiresDecimalProvenance, err := plan.RequiresMORPCVersion89DecimalLiteralSemantics(fn.Args)
		if err != nil {
			return expr
		}
		if vec.GetStringSources() != nil {
			return expr
		}
		data, err := vec.MarshalBinary()
		if err != nil {
			return expr
		}

		return &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Vec{
				Vec: &plan.LiteralVec{
					Len:                       int32(vec.Length()),
					Data:                      data,
					StringSource:              uint32(vec.GetStringSource()),
					DecimalLiteralRequiresV82: requiresDecimalProvenance,
				},
			},
		}
	}

	c := GetConstantValue(vec, false, 0)
	if c == nil {
		constant, err := MaterializeDecimal256Constant(proc, expr, vec)
		if err == nil && constant != nil {
			expr.Expr = &plan.Expr_Vec{Vec: constant}
		}
		return expr
	}
	PreserveFoldedLiteralStringDomain(expr, c)
	PreserveFoldedDecimalLiteralSemantics(expr, c)

	MarkFoldedLiteralSerialized(overloadID, fn.Args, c)

	if f.IsRealTimeRelated() {
		c.Src = &plan.Expr{
			Typ: plan.Type{
				Id:          expr.Typ.Id,
				NotNullable: expr.Typ.NotNullable,
				Width:       expr.Typ.Width,
				Scale:       expr.Typ.Scale,
				AutoIncr:    expr.Typ.AutoIncr,
				Table:       expr.Typ.Table,
			},
			Expr: &plan.Expr_F{
				F: &plan.Function{
					Func: &plan.ObjectRef{
						Server:     fn.Func.GetServer(),
						Db:         fn.Func.GetDb(),
						Schema:     fn.Func.GetSchema(),
						Obj:        fn.Func.GetObj(),
						ServerName: fn.Func.GetServerName(),
						DbName:     fn.Func.GetDbName(),
						SchemaName: fn.Func.GetSchemaName(),
						ObjName:    fn.Func.GetObjName(),
					},
					Args: make([]*plan.Expr, 0),
				},
			},
		}
	} else {
		existRealTimeFunc := false
		for i, expr := range fn.Args {
			if ac, cok := expr.Expr.(*plan.Expr_Lit); cok && ac.Lit.Src != nil {
				if _, pok := ac.Lit.Src.Expr.(*plan.Expr_V); !pok {
					fn.Args[i] = ac.Lit.Src
					existRealTimeFunc = true
				}
			}
		}
		if existRealTimeFunc {
			c.Src = &plan.Expr{
				Typ: plan.Type{
					Id:          expr.Typ.Id,
					NotNullable: expr.Typ.NotNullable,
					Width:       expr.Typ.Width,
					Scale:       expr.Typ.Scale,
					AutoIncr:    expr.Typ.AutoIncr,
					Table:       expr.Typ.Table,
				},
				Expr: &plan.Expr_F{
					F: fn,
				},
			}
		}
	}
	if r.isPrepared {
		if source, ok := ProvisionalPreparedExactPeerSource(expr); ok {
			c.Src = source
		}
	}

	ec := &plan.Expr_Lit{
		Lit: c,
	}
	// Preserve the original expr.Typ (from retType) instead of using vec.GetType()
	// This ensures that expr.Typ matches the retType, preventing type mismatches
	// when FunctionExpressionExecutor creates result wrapper using planExpr.Typ
	// For example, TIMESTAMPADD with DATE input returns DATETIME type (from retType),
	// but the actual vector type might be DATETIME (after TempSetType) or DATE (before TempSetType)
	// We should preserve the retType (DATETIME) to ensure consistency
	expr.Typ.Scale = vec.GetType().Scale
	expr.Typ.Width = vec.GetType().Width
	expr.Expr = ec

	return expr
}

// IsImplicitFloatCastOfExplicitDecimalConstant reports the narrow binder cast
// whose exact source must survive until a parameterized parent is rebound.
func IsImplicitFloatCastOfExplicitDecimalConstant(expr *plan.Expr) bool {
	if expr == nil || !types.T(expr.Typ.Id).IsFloat() {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
		return false
	}
	_, overload := function.DecodeOverloadID(fn.Func.GetObj())
	if overload != 0 {
		return false
	}
	source := fn.Args[0]
	if source == nil || !types.T(source.Typ.Id).IsDecimal() || !function.IsConstant(source, false) {
		return false
	}
	sourceFn := source.GetF()
	if sourceFn == nil || sourceFn.Func == nil || sourceFn.Func.GetObjName() != "cast" {
		return false
	}
	_, sourceOverload := function.DecodeOverloadID(sourceFn.Func.GetObj())
	return sourceOverload != 0
}

// ProvisionalPreparedExactPeerSource returns the exact input of a binder cast
// chosen while a prepared parameter had only a provisional type. Constant
// folding may still materialize the FLOAT or TEXT value, but EXECUTE needs
// this source to restore the fixed peer's numeric domain without rounding.
func ProvisionalPreparedExactPeerSource(expr *plan.Expr) (*plan.Expr, bool) {
	if expr == nil || !expr.GetPreparedNumeric().GetProvisionalResultPeer() {
		return nil, false
	}
	target := types.T(expr.Typ.Id)
	if !target.IsFloat() && !target.IsMySQLString() {
		return nil, false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
		return nil, false
	}
	_, overload := function.DecodeOverloadID(fn.Func.GetObj())
	if overload != 0 || fn.GetSyntaxExplicitCast() || fn.Args[0] == nil {
		return nil, false
	}
	source := types.T(fn.Args[0].Typ.Id)
	if source.IsInteger() || source.IsDecimal() || source == types.T_bit {
		return fn.Args[0], true
	}
	return nil, false
}

// PreserveFoldedLiteralStringDomain keeps a binder-inserted cast transparent to
// selected-value provenance when the cast itself is materialized as a literal.
// Explicit CAST uses a nonzero overload and remains a semantic boundary.
func PreserveFoldedLiteralStringDomain(expr *plan.Expr, literal *plan.Literal) {
	if expr == nil || literal == nil || literal.Isnull {
		return
	}
	if _, ok := literal.Value.(*plan.Literal_Sval); !ok {
		return
	}
	fn := expr.GetF()
	if fn == nil || len(fn.Args) == 0 {
		return
	}
	fid, overload := function.DecodeOverloadID(fn.GetFunc().GetObj())
	if fid != function.CAST || overload != 0 {
		return
	}
	source := fn.Args[0]
	if source == nil || !types.T(source.Typ.Id).IsMySQLString() ||
		!types.T(expr.Typ.Id).IsMySQLString() {
		return
	}

	sourceDomain := types.StaticStringDomain(types.MustTypeFromPlan(
		source.Typ))
	if sourceLiteral := source.GetLit(); sourceLiteral != nil {
		switch sourceLiteral.LiteralForm {
		case plan.StringLiteralForm_STRING_LITERAL_TEXT:
			sourceDomain = types.StringDomainText
		case plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER,
			plan.StringLiteralForm_STRING_LITERAL_HEX,
			plan.StringLiteralForm_STRING_LITERAL_BIT:
			sourceDomain = types.StringDomainBinary
		}
	}
	targetDomain := types.StaticStringDomain(types.MustTypeFromPlan(
		expr.Typ))
	if sourceDomain == targetDomain {
		literal.LiteralForm = plan.StringLiteralForm_STRING_LITERAL_NONE
	} else if sourceDomain == types.StringDomainBinary {
		literal.LiteralForm = plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER
	} else {
		literal.LiteralForm = plan.StringLiteralForm_STRING_LITERAL_TEXT
	}
}

// PreserveFoldedDecimalLiteralSemantics carries the planner provenance bit
// through constant folding. A folded CAST or arithmetic expression may replace
// the source literal with a new Literal, but an old CN would still rebind the
// persisted SQL using its pre-Decimal256 rules.
func PreserveFoldedDecimalLiteralSemantics(expr *plan.Expr, literal *plan.Literal) {
	if expr == nil || literal == nil || literal.DecimalLiteralRequiresV82 {
		return
	}
	_ = plan.VisitExprTree(expr, func(current *plan.Expr) error {
		if source := current.GetLit(); source != nil && source.DecimalLiteralRequiresV82 {
			literal.DecimalLiteralRequiresV82 = true
		}
		if source := current.GetVec(); source != nil && source.DecimalLiteralRequiresV82 {
			literal.DecimalLiteralRequiresV82 = true
		}
		return nil
	})
}

// MaterializeDecimal256Constant supplies the scalar representation absent from
// Literal. Callers must first establish successful, warning-free evaluation.
// LiteralVec cannot retain Literal.Src, so inputs needing recovery on a later
// execution stay executable instead of losing realtime or PREPARE provenance.
func MaterializeDecimal256Constant(proc *process.Process, expr *plan.Expr, vec *vector.Vector) (*plan.LiteralVec, error) {
	if !CanMaterializeConstantVector(expr) ||
		vec == nil || vec.GetType().Oid != types.T_decimal256 || vec.Length() != 1 || vec.IsNull(0) {
		return nil, nil
	}
	// Only explicit casts need the missing scalar representation. Implicit
	// decimal literals retain their canonical syntax for exact-peer binding.
	// Parents of an already materialized wide scalar must preserve broadcast
	// semantics too, without admitting ordinary literal lists.
	admitted := function.IsExplicitDecimalCast(expr)
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if IsDecimal256ScalarConstant(arg) {
				admitted = true
			}
		}
	}
	if !admitted {
		return nil, nil
	}
	// A constant tree carried by a decimal multiplication published wider
	// than the declared 65-digit SQL domain keeps its executable shape:
	// materializing it as a scalar vector erases exactly the multiplication
	// provenance the CTAS storage envelope keys on, and the folded width
	// alone cannot distinguish a carrier product from a genuine wide
	// literal (issue #30938 review round 9). Evaluation is unaffected: the
	// un-folded expression computes the same constant at runtime.
	if CarriesWideDecimalMultiplicationProvenance(expr) {
		return nil, nil
	}
	constant, err := vector.NewConstFixed(*vec.GetType(),
		vector.GetFixedAtNoTypeCheck[types.Decimal256](vec, 0), 1, proc.Mp())
	if constant != nil {
		defer constant.Free(proc.Mp())
	}
	if err != nil {
		return nil, err
	}
	data, err := constant.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &plan.LiteralVec{
		Len: 1, Data: data, StringSource: uint32(vec.GetStringSourceAt(0)),
		DecimalLiteralRequiresV82: true,
	}, nil
}

// CarriesWideDecimalMultiplicationProvenance reports whether the expression
// tree contains a decimal multiplication published wider than the declared
// 65-digit SQL domain. Multiplication is the only operator publishing a
// scalar wider than 65 digits (decimalAddSub and decimalDivision cap their
// published widths at 65), so such a node marks CTAS storage provenance that
// scalar-vector materialization must not erase (issue #30938 review round 9).
func CarriesWideDecimalMultiplicationProvenance(expr *plan.Expr) bool {
	carries := false
	_ = plan.VisitExprTree(expr, func(input *plan.Expr) error {
		if fn := input.GetF(); fn != nil && fn.Func.GetObjName() == "*" &&
			types.T(input.Typ.Id).IsDecimal() && input.Typ.Width > 65 {
			carries = true
		}
		return nil
	})
	return carries
}

// CanMaterializeConstantVector excludes recovery metadata that has no place in
// LiteralVec. Apply it to parents as well as the first wide scalar conversion.
func CanMaterializeConstantVector(expr *plan.Expr) bool {
	if !function.IsStatementConstantInput(expr) {
		return false
	}
	requiresSource := false
	_ = plan.VisitExprTree(expr, func(input *plan.Expr) error {
		if input.GetLit().GetSrc() != nil {
			requiresSource = true
		}
		if _, ok := ProvisionalPreparedExactPeerSource(input); ok {
			requiresSource = true
		}
		return nil
	})
	return !requiresSource
}

// IsScalarConstantVector distinguishes a broadcast scalar from a literal list.
// The payload's physical constness, not just Len, determines this contract.
func IsScalarConstantVector(expr *plan.Expr) bool {
	return scalarConstantVector(expr) != nil
}

// IsDecimal256ScalarConstant admits the non-NULL numeric scalar representation
// emitted by MaterializeDecimal256Constant into scalar-only binder consumers.
func IsDecimal256ScalarConstant(expr *plan.Expr) bool {
	if expr == nil || types.T(expr.Typ.Id) != types.T_decimal256 ||
		!expr.GetVec().GetDecimalLiteralRequiresV82() {
		return false
	}
	decoded := scalarConstantVector(expr)
	return decoded != nil && decoded.GetType().Oid == types.T_decimal256 && !decoded.IsNull(0)
}

func scalarConstantVector(expr *plan.Expr) *vector.Vector {
	literal := expr.GetVec()
	if literal == nil || literal.Len != 1 {
		return nil
	}
	// Checked decoding borrows immutable payload bytes (including ReadNoCopy
	// nulls); this fresh temporary owns no MPool allocations to release.
	decoded := vector.NewVec(types.T_any.ToType())
	if err := decoded.UnmarshalBinary(literal.Data); err != nil {
		return nil
	}
	if !decoded.IsConst() || decoded.Length() != 1 {
		return nil
	}
	return decoded
}

func GetConstantValue(vec *vector.Vector, transAll bool, row uint64) (literal *plan.Literal) {
	defer func() {
		if literal == nil {
			return
		}
		// Zero is the canonical spelling of an ordinary source literal. Encode
		// every other executable owner as StringSource + 1 so every vector to
		// scalar materialization path, including NULL, preserves provenance.
		if source := vec.GetStringSourceAt(int(row)); source != types.StringSourceLiteral {
			literal.StringSource = uint32(source) + 1
		}
	}()
	if vec.IsConstNull() || vec.GetNulls().Contains(row) {
		return &plan.Literal{Isnull: true}
	}
	switch vec.GetType().Oid {
	case types.T_bool:
		return &plan.Literal{
			Value: &plan.Literal_Bval{
				Bval: vector.MustFixedColNoTypeCheck[bool](vec)[row],
			},
		}
	case types.T_bit:
		return &plan.Literal{
			Value: &plan.Literal_U64Val{
				U64Val: vector.MustFixedColNoTypeCheck[uint64](vec)[row],
			},
		}
	case types.T_int8:
		return &plan.Literal{
			Value: &plan.Literal_I8Val{
				I8Val: int32(vector.MustFixedColNoTypeCheck[int8](vec)[row]),
			},
		}
	case types.T_int16:
		return &plan.Literal{
			Value: &plan.Literal_I16Val{
				I16Val: int32(vector.MustFixedColNoTypeCheck[int16](vec)[row]),
			},
		}
	case types.T_int32:
		return &plan.Literal{
			Value: &plan.Literal_I32Val{
				I32Val: vector.MustFixedColNoTypeCheck[int32](vec)[row],
			},
		}
	case types.T_int64:
		return &plan.Literal{
			Value: &plan.Literal_I64Val{
				I64Val: vector.MustFixedColNoTypeCheck[int64](vec)[row],
			},
		}
	case types.T_uint8:
		return &plan.Literal{
			Value: &plan.Literal_U8Val{
				U8Val: uint32(vector.MustFixedColNoTypeCheck[uint8](vec)[row]),
			},
		}
	case types.T_uint16:
		return &plan.Literal{
			Value: &plan.Literal_U16Val{
				U16Val: uint32(vector.MustFixedColNoTypeCheck[uint16](vec)[row]),
			},
		}
	case types.T_uint32:
		return &plan.Literal{
			Value: &plan.Literal_U32Val{
				U32Val: vector.MustFixedColNoTypeCheck[uint32](vec)[row],
			},
		}
	case types.T_uint64:
		return &plan.Literal{
			Value: &plan.Literal_U64Val{
				U64Val: vector.MustFixedColNoTypeCheck[uint64](vec)[row],
			},
		}
	case types.T_float32:
		return &plan.Literal{
			Value: &plan.Literal_Fval{
				Fval: vector.MustFixedColNoTypeCheck[float32](vec)[row],
			},
		}
	case types.T_float64:
		return &plan.Literal{
			Value: &plan.Literal_Dval{
				Dval: vector.MustFixedColNoTypeCheck[float64](vec)[row],
			},
		}
	case types.T_varchar, types.T_char,
		types.T_binary, types.T_varbinary, types.T_text, types.T_blob, types.T_datalink, types.T_geometry:
		literal := &plan.Literal{
			Value: &plan.Literal_Sval{
				Sval: vec.GetStringAt(int(row)),
			},
		}
		if vec.GetType().Oid.IsMySQLString() {
			switch vec.GetRuntimeStringDomainAt(int(row)) {
			case types.RuntimeStringText:
				literal.LiteralForm = plan.StringLiteralForm_STRING_LITERAL_TEXT
			case types.RuntimeStringBinary:
				literal.LiteralForm = plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER
			}
		}
		return literal
	case types.T_json:
		if !transAll {
			return nil
		}
		return &plan.Literal{
			Value: &plan.Literal_Sval{
				Sval: vec.GetStringAt(int(row)),
			},
		}
	case types.T_timestamp:
		return &plan.Literal{
			Value: &plan.Literal_Timestampval{
				Timestampval: int64(vector.MustFixedColNoTypeCheck[types.Timestamp](vec)[row]),
			},
		}
	case types.T_date:
		return &plan.Literal{
			Value: &plan.Literal_Dateval{
				Dateval: int32(vector.MustFixedColNoTypeCheck[types.Date](vec)[row]),
			},
		}
	case types.T_time:
		return &plan.Literal{
			Value: &plan.Literal_Timeval{
				Timeval: int64(vector.MustFixedColNoTypeCheck[types.Time](vec)[row]),
			},
		}
	case types.T_datetime:
		return &plan.Literal{
			Value: &plan.Literal_Datetimeval{
				Datetimeval: int64(vector.MustFixedColNoTypeCheck[types.Datetime](vec)[row]),
			},
		}
	case types.T_enum:
		if !transAll {
			return nil
		}
		return &plan.Literal{
			Value: &plan.Literal_EnumVal{
				EnumVal: uint32(vector.MustFixedColNoTypeCheck[types.Enum](vec)[row]),
			},
		}
	case types.T_decimal64:
		return &plan.Literal{
			Value: &plan.Literal_Decimal64Val{
				Decimal64Val: &plan.Decimal64{A: int64(vector.MustFixedColNoTypeCheck[types.Decimal64](vec)[row])},
			},
		}
	case types.T_decimal128:
		decimalValue := &plan.Decimal128{}
		decimalValue.A = int64(vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[row].B0_63)
		decimalValue.B = int64(vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[row].B64_127)
		return &plan.Literal{Value: &plan.Literal_Decimal128Val{Decimal128Val: decimalValue}}
	case types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
		data := vec.GetStringAt(int(row))
		return &plan.Literal{
			Value: &plan.Literal_VecVal{
				VecVal: data,
			},
		}
	default:
		return nil
	}
}

func GetConstantValue2(proc *process.Process, expr *plan.Expr, vec *vector.Vector) (get bool, err error) {
	if cExpr, ok := expr.Expr.(*plan.Expr_Lit); ok {
		source, sourceErr := colexec.DecodeLiteralStringSource(cExpr.Lit)
		if sourceErr != nil {
			return false, sourceErr
		}
		// Publish known provenance with the value, not via an ordinary append
		// followed by a row-source rewrite (quadratic for uniform VALUES).
		if cExpr.Lit.Isnull {
			err = vector.AppendBytesWithStringSource(vec, nil, true, source, proc.Mp())
			return true, err
		}
		switch vec.GetType().Oid {
		case types.T_bool:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Bval); ok {
				val := val.Bval
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_bit:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				val := val.U64Val
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_int8:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_I8Val); ok {
				val := int8(val.I8Val)
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_int16:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_I16Val); ok {
				val := int16(val.I16Val)
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_int32:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_I32Val); ok {
				val := val.I32Val
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_int64:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_I64Val); ok {
				val := val.I64Val
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_uint8:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_U8Val); ok {
				val := uint8(val.U8Val)
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_uint16:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_U16Val); ok {
				val := uint16(val.U16Val)
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_uint32:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_U32Val); ok {
				val := val.U32Val
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_uint64:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_U64Val); ok {
				val := val.U64Val
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_float32:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Fval); ok {
				val := val.Fval
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_float64:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Dval); ok {
				val := val.Dval
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_varchar, types.T_char, types.T_binary, types.T_varbinary, types.T_text,
			types.T_blob, types.T_datalink, types.T_json, types.T_geometry:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Sval); ok {
				val := val.Sval
				err = vector.AppendBytesWithStringSource(vec, []byte(val), false, source, proc.Mp())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, false, proc.Mp())
				return false, err
			}
		case types.T_array_float32, types.T_array_float64,
			types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_VecVal); ok {
				val := val.VecVal
				err = vector.AppendBytesWithStringSource(vec, []byte(val), false, source, proc.Mp())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, false, proc.Mp())
				return false, err
			}
		case types.T_timestamp:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Timestampval); ok {
				val := val.Timestampval
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_date:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Dval); ok {
				val := val.Dval
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_time:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Dval); ok {
				val := val.Dval
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_datetime:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Datetimeval); ok {
				val := val.Datetimeval
				err = vector.AppendFixedWithStringSource(vec, types.Datetime(val), false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytesWithStringSource(vec, nil, true, source, proc.Mp())
				return true, err
			}
		case types.T_enum:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_EnumVal); ok {
				val := types.Enum(val.EnumVal)
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_decimal64:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Decimal64Val); ok {
				val := val.Decimal64Val.A
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		case types.T_decimal128:
			if val, ok := cExpr.Lit.Value.(*plan.Literal_Decimal128Val); ok {
				val := types.Decimal128{B0_63: uint64(val.Decimal128Val.A), B64_127: uint64(val.Decimal128Val.B)}
				err = vector.AppendFixedWithStringSource(vec, val, false, source, proc.GetMPool())
				return true, err
			} else {
				err = vector.AppendBytes(vec, nil, true, proc.Mp())
				return false, err
			}
		default:
			return false, nil
		}
	} else {
		err = vector.AppendBytes(vec, nil, true, proc.Mp())
		return false, err
	}
}

// MarkFoldedLiteralSerialized preserves tuple-encoding provenance across
// constant folding. A direct serial result always acquires provenance. An
// outer function retains it only when its output bytes exactly match a marked
// input literal, which covers representation-preserving casts and selectors
// without incorrectly marking transformed values. IsBin remains reserved for
// SQL hex and bit literal execution semantics.
func MarkFoldedLiteralSerialized(overloadID int64, args []*plan.Expr, literal *plan.Literal) {
	if literal == nil || literal.Isnull {
		return
	}

	canonicalOverloadID := overloadID & function.DistinctMask
	if canonicalOverloadID == function.SerialFunctionEncodeID ||
		canonicalOverloadID == function.SerialFullFunctionEncodeID {
		literal.IsSerialized = true
		return
	}

	result, ok := literal.Value.(*plan.Literal_Sval)
	if !ok {
		return
	}
	for _, arg := range args {
		input := arg.GetLit()
		if input == nil || !input.IsSerialized {
			continue
		}
		if serialized, ok := input.Value.(*plan.Literal_Sval); ok &&
			serialized.Sval == result.Sval {
			literal.IsSerialized = true
			return
		}
	}
}

// ContainsSerializedLiteral reports whether folding a literal list into a
// LiteralVec must retain tuple-encoding provenance for diagnostic consumers.
func ContainsSerializedLiteral(exprs []*plan.Expr) bool {
	for _, expr := range exprs {
		if literal := expr.GetLit(); literal != nil && literal.IsSerialized {
			return true
		}
	}
	return false
}

func isSqlModeDependentTemporalCast(fn *plan.Function) bool {
	functionID, overloadID := function.DecodeOverloadID(fn.Func.GetObj())
	if functionID != function.CAST || len(fn.Args) != 2 {
		return false
	}

	switch types.T(fn.Args[0].Typ.Id) {
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_blob, types.T_text, types.T_datalink:
		target := types.T(fn.Args[1].Typ.Id)
		if target != types.T_date && target != types.T_datetime && target != types.T_timestamp {
			return false
		}
		lit := fn.Args[0].GetLit()
		if lit == nil {
			return true
		}
		if lit.Isnull {
			return false
		}
		if types.T(fn.Args[1].Typ.Id) == types.T_timestamp {
			// TIMESTAMP conversion also depends on the execution-time zone.
			return true
		}
		value, ok := lit.Value.(*plan.Literal_Sval)
		if !ok {
			return true
		}
		switch types.T(fn.Args[1].Typ.Id) {
		case types.T_date:
			parsed, err := types.ParseDateCast(value.Sval)
			return err != nil || parsed == types.ZeroDate
		case types.T_datetime:
			parsed, err := types.ParseDatetime(value.Sval, 6)
			return err != nil || parsed == types.ZeroDatetime
		default:
			return false
		}
	case types.T_date, types.T_datetime:
		if types.T(fn.Args[1].Typ.Id) != types.T_date ||
			(!fn.SyntaxExplicitCast && overloadID != 1) {
			return false
		}
		// Only the typed zero sentinel depends on NO_ZERO_DATE. Keep normal
		// literal casts foldable, including prepared interval constants.
		lit := fn.Args[0].GetLit()
		if lit == nil {
			return true
		}
		if lit.Isnull {
			return false
		}
		if types.T(fn.Args[0].Typ.Id) == types.T_date {
			return lit.GetDateval() == int32(types.ZeroDate)
		}
		return lit.GetDatetimeval() == int64(types.ZeroDatetime)
	default:
		return false
	}

}

func isSqlModeDependentTemporalFunction(fn *plan.Function) bool {
	id, overload := function.DecodeOverloadID(fn.Func.GetObj())
	if id == function.EXTRACT && overload >= 5 && len(fn.Args) == 2 && types.T(fn.Args[1].Typ.Id).IsMySQLString() {
		return true
	}
	if len(fn.Args) != 1 {
		return false
	}
	functionID, _ := function.DecodeOverloadID(fn.Func.GetObj())
	argType := types.T(fn.Args[0].Typ.Id)
	switch functionID {
	case function.DATE:
		return argType == types.T_date || argType == types.T_datetime || argType.IsMySQLString()
	case function.DAY:
		// DAY is resolved through a DATE argument. Retain its literal-to-DATE
		// conversion until EXECUTE so NO_ZERO_DATE can still null it.
		return true
	case function.YEAR, function.MONTH, function.QUARTER, function.DAYOFMONTH:
		return argType.IsMySQLString()
	default:
		return false
	}
}

// A wrapper such as IS NULL is itself constant-looking when its child is a
// literal-only DATE/YEAR expression. Preserve the entire path to that child
// so every EXECUTE evaluates it under the current session sql_mode.
func ContainsSqlModeDependentTemporalCall(expr *plan.Expr) bool {
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if isSqlModeDependentTemporalCast(fn) || isSqlModeDependentTemporalFunction(fn) {
		return true
	}
	for _, arg := range fn.Args {
		if arg != nil && ContainsSqlModeDependentTemporalCall(arg) {
			return true
		}
	}
	return false
}

// IsLegacyTimeAssignmentOutsideInternalRange identifies a CAST_STRICT literal
// produced for an older protocol's TIME assignment. The literal is valid MySQL
// TIME syntax but exceeds MatrixOne's internal representation, so it must reach
// the runtime assignment cast where strict/non-strict policy is available.
func IsLegacyTimeAssignmentOutsideInternalRange(fn *plan.Function) bool {
	functionID, _ := function.DecodeOverloadID(fn.Func.GetObj())
	if functionID != function.CAST_STRICT || len(fn.Args) != 2 ||
		types.T(fn.Args[1].Typ.Id) != types.T_time {
		return false
	}
	literal := fn.Args[0].GetLit()
	if literal == nil {
		return false
	}
	value, ok := literal.Value.(*plan.Literal_Sval)
	if !ok {
		return false
	}
	_, outOfRange := types.IsTimeStringOutOfInternalRange(value.Sval, fn.Args[1].Typ.Scale)
	return outOfRange
}

// ContainsSqlModeDependentDivision preserves the path to divisions whose
// denominator is not a proven nonzero numeric literal. Folding a parent under
// an empty sql_mode could otherwise erase warnings for a later EXECUTE.
// Children are folded first, so ordinary arithmetic with a nonzero constant
// denominator retains its constant-folding fast path.
func ContainsSqlModeDependentDivision(expr *plan.Expr) bool {
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if ContainsSqlModeDependentDivision(item) {
				return true
			}
		}
		return false
	}
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	id, _ := function.DecodeOverloadID(fn.Func.GetObj())
	if (id == function.DIV || id == function.INTEGER_DIV || id == function.MOD) && len(fn.Args) >= 2 {
		divisor := fn.Args[1].GetLit()
		if divisor == nil || divisor.GetIsnull() {
			return true
		}
		switch divisor.Value.(type) {
		case *plan.Literal_I8Val, *plan.Literal_I16Val, *plan.Literal_I32Val, *plan.Literal_I64Val,
			*plan.Literal_U8Val, *plan.Literal_U16Val, *plan.Literal_U32Val, *plan.Literal_U64Val,
			*plan.Literal_Fval, *plan.Literal_Dval, *plan.Literal_Decimal64Val, *plan.Literal_Decimal128Val:
			if isZeroLiteral(divisor) {
				return true
			}
		default:
			// Packed/cast decimal256 and other representations are evaluated
			// at execution rather than assuming their denominator is nonzero.
			return true
		}
	}
	for _, arg := range fn.Args {
		if ContainsSqlModeDependentDivision(arg) {
			return true
		}
	}
	return false
}

// isZeroLiteral checks if a literal value is zero
func isZeroLiteral(lit *plan.Literal) bool {
	switch v := lit.Value.(type) {
	case *plan.Literal_I8Val:
		return v.I8Val == 0
	case *plan.Literal_I16Val:
		return v.I16Val == 0
	case *plan.Literal_I32Val:
		return v.I32Val == 0
	case *plan.Literal_I64Val:
		return v.I64Val == 0
	case *plan.Literal_U8Val:
		return v.U8Val == 0
	case *plan.Literal_U16Val:
		return v.U16Val == 0
	case *plan.Literal_U32Val:
		return v.U32Val == 0
	case *plan.Literal_U64Val:
		return v.U64Val == 0
	case *plan.Literal_Fval:
		return v.Fval == 0
	case *plan.Literal_Dval:
		return v.Dval == 0
	case *plan.Literal_Decimal64Val:
		return v.Decimal64Val.A == 0
	case *plan.Literal_Decimal128Val:
		return v.Decimal128Val.A == 0 && v.Decimal128Val.B == 0
	}
	return false
}

// EvaluateConstantExpression is shared by binder and optimizer folding. The
// child borrows the context and memory pool; only the expression result needs
// freeing. Never mutate the statement's immutable warning destination.
func EvaluateConstantExpression(proc *process.Process, expr *plan.Expr, bat *batch.Batch) (vec *vector.Vector, free func(), warned bool, err error) {
	sink := &process.WarningProbe{}
	// A speculative kernel failure must use the same error channel as a
	// returned error, so selection can leave an inactive operand unevaluated.
	defer func() {
		if failure := recover(); failure != nil {
			vec, free, warned, err = nil, nil, sink.Warned(), moerr.ConvertPanicError(proc.Ctx, failure)
		}
	}()
	child := proc.NewNoContextChildProc(0)
	child.Ctx = proc.Ctx
	child.WarningSink = sink
	vec, free, err = colexec.GetReadonlyResultFromExpression(child, expr, []*batch.Batch{bat})
	return vec, free, sink.Warned(), err
}

// ContainsTruncatedFloatTextConversion keeps a context-dependent conversion and
// its parents executable. Safe source expressions may fold first; complete
// numeric literals and unrelated CASTs remain foldable. In particular, a strict
// assignment error must not escape speculative SQL PREPARE evaluation.
func ContainsTruncatedFloatTextConversion(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if fn := expr.GetF(); fn != nil {
		if fn.Func != nil && len(fn.Args) > 0 && fn.Args[0] != nil {
			id, _ := function.DecodeOverloadID(fn.Func.Obj)
			target := types.T(expr.Typ.Id)
			if (id == function.CAST || id == function.CAST_STRICT || id == function.CAST_ASSIGN || id == function.CAST_IGNORE) &&
				(target == types.T_float32 || target == types.T_float64) && types.T(fn.Args[0].Typ.Id).IsMySQLString() {
				source := fn.Args[0]
				if lit := source.GetLit(); lit != nil && !lit.Isnull && !lit.IsBin {
					if value, ok := lit.Value.(*plan.Literal_Sval); ok && function.FloatTextTruncated(value.Sval) {
						return true
					}
				}
				// scalarConstantVector is a borrowed view of the literal payload.
				if vec := scalarConstantVector(source); vec != nil && !vec.IsNull(0) && !vec.GetIsBin() &&
					function.FloatTextTruncated(vec.GetStringAt(0)) {
					return true
				}
			}
		}
		for _, arg := range fn.Args {
			if ContainsTruncatedFloatTextConversion(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, arg := range list.List {
			if ContainsTruncatedFloatTextConversion(arg) {
				return true
			}
		}
	}
	return false
}

// DecimalBoundaryDecisionEntry is the planner-owned payload recorded for one
// decimal coercive decision in the statement-local decision ledger (issue
// #30938, design decision log entries 49 and 52). The carrier in
// pkg/vm/process stores it opaquely; the binder's recorder authors it at the
// alignment site and the storage-boundary evaluator, the retry funnel's
// source-domain resolver and the prepared-CTAS replay read it.
type DecimalBoundaryDecisionEntry struct {
	// Overload is the bound overload identity of the decision node.
	Overload int64
	// Name is the binder-level function name of the decision.
	Name string
	// ArgCount is the decision's arity.
	ArgCount int
	// PreTypes holds each argument position's pre-coercion type exactly as
	// the merge consumed it at binding.
	PreTypes []plan.Type
	// AlignedTypes holds each position's post-merge aligned target. A
	// position the binder left uncast records its pre type verbatim (the
	// same-OID same-scale alignment skip at base_binder.go's alignment loop
	// leaves the physical argument untouched while the family still converges
	// on the aligned target).
	AlignedTypes []plan.Type
	// ResultType is the converged published result type the binding authored.
	ResultType plan.Type
	// Provenance flags a decision whose recorded lineage carries a widened
	// decimal multiplication publication: the decision itself publishes
	// through the widened rule, or a recorded operand decision does.
	Provenance bool
	// Retried marks the entry as authored by the source-domain retry funnel
	// (design decision log 56): the binding resolved over the operands'
	// parent-published domains after the widened first lookup failed or
	// silently degraded.
	Retried bool
}

// DecimalBoundaryDerivedDomain is the channel-(b) payload of the design's
// source-domain resolver (design decision log 56): at derived-producer
// binding construction each output column whose producing expression carries
// widened-multiplication provenance publishes its parent-published domain
// under the key "b#<binding tag>#<column position>" in the statement
// registry, so a consumer-side column reference to a derived table, view or
// CTE resolves its parent domain without inspecting the producer's plan.
// The planner authors and consumes the payload; the process carrier stores
// it opaquely.
type DecimalBoundaryDerivedDomain struct {
	// Parent is the column's parent-published domain: the declared type the
	// pre-change engine's own inference authored for the producing
	// expression.
	Parent plan.Type
	// Provenance flags that the column's producing expression carries a
	// widened decimal multiplication publication in its recorded lineage.
	Provenance bool
}

// DecimalBoundaryFoldSignature computes the fold-observable content
// signature of a function expression node: the overload identity, the
// argument count, and each argument's observed type — the binder's implicit
// alignment CASTs contribute their operand's observed type (they are
// transparent at this seam), an authored fixed-domain cast contributes its
// own declared type, and every other argument its own type.
// The binder's decision-ledger recorder computes the same encoding when a
// coercive decision is recorded, and both constant-fold engines recompute it
// at their recursive entries, so folding evolution between the recording and
// a later fold observes the identical signature (issue #30938, design
// decision log entries 52 and 55).
func DecimalBoundaryFoldSignature(expr *plan.Expr) string {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(strconv.FormatInt(fn.Func.GetObj(), 36))
	sb.WriteByte('#')
	sb.WriteString(strconv.Itoa(len(fn.Args)))
	for i := range fn.Args {
		sb.WriteByte('|')
		writeDecimalBoundaryObservedType(&sb, fn.Args[i])
	}
	return sb.String()
}

// DecimalBoundaryDecisionLedgerKey computes the ledger content key of a
// decision node (design decision log 52): the overload identity, the
// argument count, and per position the pre-coercion observed type (the
// binder's implicit alignment CAST operands contribute their operand's
// observed type; an authored fixed-domain cast contributes its own declared
// type) together with that position's post-merge aligned type (the argument
// node's own published type). Twin decisions whose operand syntax alone distinguishes them —
// identical observed pre types but different aligned targets — key
// differently, and a deep copy of the decision subtree resolves its origin's
// entry because every keyed component survives DeepCopyExpr.
func DecimalBoundaryDecisionLedgerKey(expr *plan.Expr) string {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(strconv.FormatInt(fn.Func.GetObj(), 36))
	sb.WriteByte('#')
	sb.WriteString(strconv.Itoa(len(fn.Args)))
	for i := range fn.Args {
		sb.WriteByte('|')
		writeDecimalBoundaryObservedType(&sb, fn.Args[i])
		sb.WriteByte('>')
		writeDecimalBoundaryType(&sb, &fn.Args[i].Typ)
	}
	return sb.String()
}

// writeDecimalBoundaryObservedType encodes one argument's observed
// pre-coercion type: the binder's implicit alignment casts unwrap to their
// operand, recursively, while an authored fixed-domain cast (a syntax-explicit
// CAST, or any non-zero overload such as a decimal-to-decimal conversion the
// user wrote) contributes its own declared type. Unwrapping an authored cast
// would key the decision on the cast's operand instead of its declared
// domain, so two merges whose defaults differ only inside authored casts
// (CAST(d AS DECIMAL(34,0)) vs CAST(d AS DECIMAL(37,0))) would collide on one
// ledger key and the registry's ownership check would keep the first-recorded
// envelope for both columns (issue #30938 review round 17, F1; design
// decision log 52's content keying and revision 40's fixed-domain barrier).
// The encoding covers the components the binder's decimal coercive merges
// consume: the type family, width and scale.
func writeDecimalBoundaryObservedType(sb *strings.Builder, arg *plan.Expr) {
	observed := arg
	for fn := observed.GetF(); fn != nil && fn.Func != nil && len(fn.Args) >= 1; {
		id, overload := function.DecodeOverloadID(fn.Func.GetObj())
		if id != function.CAST {
			break
		}
		if overload != 0 || fn.SyntaxExplicitCast {
			// An authored cast fixes its own domain: the observed type is
			// the cast's declared result, never its operand's type.
			break
		}
		observed = fn.Args[0]
		fn = observed.GetF()
	}
	writeDecimalBoundaryType(sb, &observed.Typ)
}

// writeDecimalBoundaryType encodes one plan type for the decision barrier,
// the ledger key and the ledger payload.
func writeDecimalBoundaryType(sb *strings.Builder, typ *plan.Type) {
	sb.WriteString(strconv.Itoa(int(typ.Id)))
	sb.WriteByte(':')
	sb.WriteString(strconv.Itoa(int(typ.Width)))
	sb.WriteByte(':')
	sb.WriteString(strconv.Itoa(int(typ.Scale)))
}

// DecimalBoundaryBarrierHit reports whether the active planning generation's
// decision barrier pins this node's operand structure against folding. A
// statement whose recorded decisions carry no widened-multiplication
// provenance — and every statement without an active generation — answers
// from the empty-set check, so provenance-free statements fold
// base-identically without paying the signature computation.
func DecimalBoundaryBarrierHit(proc *process.Process, expr *plan.Expr) bool {
	if !proc.DecimalBoundaryBarrierActive() {
		return false
	}
	if expr.GetF() == nil {
		return false
	}
	return proc.DecimalBoundaryBarrierContains(DecimalBoundaryFoldSignature(expr))
}
