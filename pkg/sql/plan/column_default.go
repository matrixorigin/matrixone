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

package plan

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// recordColumnDefaultProjection captures syntax before lowering erases the
// distinction between source fields, identity CASTs and typed temporal literals.
func recordColumnDefaultProjection(ctx *BindContext, binder *ProjectionBinder, ast tree.Expr, bound *plan.Expr, column int32) error {
	provenance := OutputColumnProvenance{State: ProvenanceNone}
	switch expr := unwrapParenExpr(ast).(type) {
	case *tree.UnaryExpr:
		name, columnRef := columnDefaultReference(expr)
		if !columnRef {
			return nil
		}
		source, typ, err := binder.columnDefaultMetadata(name)
		if err != nil {
			return err
		}
		provenance = source
		if source.State == ProvenanceSingleSource {
			provenance.ColumnDefaultSource = source.Source
			provenance.State, provenance.Source = ProvenanceNone, nil
			provenance.CTASDefaultPolicy = CTASDefaultNone
		} else if provenance.ResultType == nil {
			provenance.ResultType = DeepCopyType(&typ)
		}
	case *tree.DefaultColExpr:
		source, typ, err := binder.columnDefaultMetadata(expr.Column)
		if err != nil {
			return err
		}
		if source.State == ProvenanceSingleSource && source.Source != nil {
			typ = source.Source.Metadata.Typ
		} else if source.ResultType != nil {
			typ = *source.ResultType
		}
		if isEnumOrSetPlanType(&typ) {
			provenance.ResultType = DeepCopyType(&typ)
		}
		if bound.Typ.NotNullable {
			if _, supported := ctasViewTypeDefaultOrigin(typ); supported {
				provenance.CTASDefaultPolicy = CTASDefaultUseTypeDefault
			}
		}
	case *tree.CastExpr:
		if target, ok := expr.Type.(*tree.T); ok {
			switch target.InternalType.Family {
			case tree.DateFamily, tree.TimeFamily, tree.TimestampFamily:
				provenance.TypeDefaultNullability = typeDefaultNullable
			}
		}
	case *tree.FuncExpr:
		if expr.IsTemporalLiteral {
			provenance.TypeDefaultNullability = typeDefaultNotNullable
		} else if expr.FuncName != nil {
			switch strings.ToLower(expr.FuncName.Origin()) {
			case "date", "time", "timestamp":
				provenance.TypeDefaultNullability = typeDefaultNullable
			default:
				return nil
			}
		} else {
			return nil
		}
	default:
		return nil
	}
	ctx.setOutputColumnProvenance(column, provenance)
	return nil
}

// MySQL's unary plus preserves a field reference. Do not unwrap casts,
// unary minus, or arithmetic, even if they later fold to the same value.
func columnDefaultReference(ast tree.Expr) (*tree.UnresolvedName, bool) {
	for {
		ast = unwrapParenExpr(ast)
		if unary, ok := ast.(*tree.UnaryExpr); ok && unary.Op == tree.UNARY_PLUS {
			ast = unary.Expr
			continue
		}
		name, ok := ast.(*tree.UnresolvedName)
		return name, ok
	}
}

func (b *baseBinder) bindColumnDefault(ast *tree.DefaultColExpr) (*plan.Expr, error) {
	name := ast.Column
	provenance, typ, err := b.columnDefaultMetadata(name)
	if err != nil {
		return nil, err
	}
	if provenance.ResultType != nil {
		typ = *provenance.ResultType
	}
	var value *plan.Expr
	if provenance.TypeDefault != typeDefaultNone {
		switch provenance.TypeDefaultNullability {
		case typeDefaultNullable:
			typ.NotNullable = false
		case typeDefaultNotNullable:
			typ.NotNullable = true
		}
		// MySQL UNION fields lack an implicit non-NULL DATE/DATETIME default
		// in strict mode. This is fixed at PREPARE/bind time, unlike digests.
		if provenance.TypeDefault == typeDefaultUnion && typ.NotNullable && ast.StrictMode {
			switch types.T(typ.Id) {
			case types.T_date, types.T_datetime, types.T_timestamp:
				return nil, moerr.NewNoDefaultForColumn(b.GetContext(), name.ColNameOrigin())
			}
		}
		value, err = b.columnTypeDefault(typ)
	} else if provenance.State == ProvenanceSingleSource && provenance.Source != nil {
		typ = provenance.Source.Metadata.Typ
		value, err = b.sourceColumnDefault(provenance.Source.Metadata, typ, name.ColNameOrigin())
	} else {
		return nil, moerr.NewNoDefaultForColumn(b.GetContext(), name.ColNameOrigin())
	}
	if err != nil {
		return nil, err
	}
	// Match ordinary column-reference display/numeric contexts for ENUM/SET.
	preserve := b.mysqlSpecialTargetType != nil && typ.Enumvalues == b.mysqlSpecialTargetType.Enumvalues &&
		((isEnumPlanType(&typ) && isEnumPlanType(b.mysqlSpecialTargetType)) ||
			(isSetPlanType(&typ) && isSetPlanType(b.mysqlSpecialTargetType)))
	if !b.bindRawMySQLSpecialType && !preserve && isEnumOrSetPlanType(&typ) {
		function, _, _, err := mysqlSpecialTypeFuncNames(&typ)
		if err != nil {
			return nil, err
		}
		return BindFuncExprImplByPlanExpr(b.GetContext(), function,
			[]*plan.Expr{makePlan2StringConstExprWithType(typ.Enumvalues), value})
	}
	return value, nil
}

func (b *baseBinder) columnDefaultMetadata(name *tree.UnresolvedName) (OutputColumnProvenance, plan.Type, error) {
	var target *plan.TableDef
	switch binder := b.impl.(type) {
	case *DefaultBinder:
		target = binder.defaultTarget
	case *ReplaceValueBinder:
		target = binder.tableDef
	case *OndupUpdateBinder:
		target = binder.tableDef
	}
	if target != nil {
		if (name.TblName() == "" || strings.EqualFold(name.TblName(), target.Name)) &&
			(name.DbName() == "" || strings.EqualFold(name.DbName(), target.DbName)) {
			if index, ok := target.Name2ColIndex[name.ColName()]; ok {
				column := target.Cols[index]
				return OutputColumnProvenance{State: ProvenanceSingleSource,
					Source: &SourceColumn{Metadata: snapshotSourceColumnMetadata(column)}}, column.Typ, nil
			}
		}
		return OutputColumnProvenance{}, plan.Type{}, moerr.NewBadFieldErrorf(b.GetContext(), "unknown column '%s' in DEFAULT", name.ColNameOrigin())
	}

	// HAVING prefers declared source grouping keys, then SELECT-list aliases.
	// ORDER BY expressions prefer table fields and then consult aliases.
	if having, ok := b.impl.(*HavingBinder); ok && b.ctx != nil && !having.insideAgg && !having.bindingProjectedAlias {
		// DEFAULT is row-independent, but that must not widen HAVING's visible
		// namespace. Reuse its SELECT-output and direct-column identity owners
		// without binding a row reference or manufacturing an aggregate.
		_, _, local := resolveColumnIdentity(b.ctx, name)
		_, grouped := having.lookupGroupByExpr(name, windowExprAstKey(name))
		// A declared source grouping key precedes a colliding SELECT alias.
		// Use the logical grouping registry, not the active subtotal bitmap:
		// DEFAULT reads schema metadata even when this branch nulls the key.
		if !local || !grouped {
			var projected tree.Expr
			var found, ambiguous bool
			if name.NumParts == 1 {
				projected, found, ambiguous = b.ctx.havingOutputExpr(name.ColName())
			}
			if !found && !ambiguous {
				projected, found, ambiguous = b.ctx.havingProjectedColumnExpr(name)
			}
			if ambiguous {
				return OutputColumnProvenance{}, plan.Type{}, ambiguousHavingColumn(b.GetContext(), name.ColName())
			}
			if found {
				var column bool
				name, column = columnDefaultReference(projected)
				if !column {
					return OutputColumnProvenance{State: ProvenanceNone}, plan.Type{}, nil
				}
			} else if local {
				return OutputColumnProvenance{}, plan.Type{}, moerr.NewBadFieldErrorf(b.GetContext(),
					"Unknown column '%s' in 'having clause'", name.ColNameOrigin())
			}
		}
	}

	// DEFAULT is a metadata dependency, not a row/aggregate dependency. Resolve
	// through the same authoritative binding indexes without creating a ColRef,
	// recording a full-group-by dependency, or marking the query correlated.
	for ctx := b.ctx; ctx != nil; ctx = ctx.parent {
		var binding *Binding
		if name.TblName() != "" {
			binding = b.resolveTableBinding(ctx, name.DbName(), name.TblName())
		} else {
			var found bool
			binding, found = ctx.bindingByCol[name.ColName()]
			if found && binding == nil {
				return OutputColumnProvenance{}, plan.Type{}, moerr.NewInvalidInputf(b.GetContext(), "ambiguous column reference '%s'", name.ColNameOrigin())
			}
		}
		if binding != nil {
			index := binding.FindColumn(name.ColName())
			if index == AmbiguousName {
				return OutputColumnProvenance{}, plan.Type{}, moerr.NewInvalidInputf(b.GetContext(), "ambiguous column reference '%s'", name.ColNameOrigin())
			}
			if index >= 0 && int(index) < len(binding.types) {
				provenance := OutputColumnProvenance{State: ProvenanceNone}
				if int(index) < len(binding.outputColumnProvenance) {
					provenance = binding.outputColumnProvenance[index]
				}
				return provenance.forColumnDefault(), *binding.types[index], nil
			}
		}
	}
	// Aliases are not visible in the same SELECT list or WHERE clause.
	if b.defaultColumnAliases && b.ctx != nil && name.NumParts == 1 {
		alias, found, err := resolveOrderAlias(b.GetContext(), b.ctx, name.ColName())
		if err != nil {
			return OutputColumnProvenance{}, plan.Type{}, err
		}
		if found && alias.idx >= 0 && int(alias.idx) < len(b.ctx.projects) {
			return b.ctx.outputColumnProvenanceForProject(alias.idx).forColumnDefault(), b.ctx.projects[alias.idx].Typ, nil
		}
	}
	return OutputColumnProvenance{}, plan.Type{}, moerr.NewBadFieldErrorf(b.GetContext(), "unknown column '%s' in DEFAULT", name.ColNameOrigin())
}

func (b *baseBinder) sourceColumnDefault(source SourceColumnMetadata, typ plan.Type, name string) (*plan.Expr, error) {
	if source.Generated {
		return nil, moerr.NewNoDefaultForColumn(b.GetContext(), name)
	}
	if isGeneratedExpressionDefault(source.Default) {
		return nil, moerr.NewDefaultExpression(b.GetContext())
	}
	typ.AutoIncr = false
	typ.NotNullable = source.Typ.NotNullable || !source.NullAbility
	if source.Typ.AutoIncr {
		typ.NotNullable = true
		return b.columnTypeDefault(typ)
	}
	if source.Default == nil || source.Default.Expr == nil {
		if typ.NotNullable {
			return nil, moerr.NewNoDefaultForColumn(b.GetContext(), name)
		}
		return columnDefaultNull(typ), nil
	}
	origin := strings.ToLower(strings.TrimSpace(source.Default.OriginString))
	if (types.T(typ.Id) == types.T_timestamp || types.T(typ.Id) == types.T_datetime) &&
		(strings.HasPrefix(origin, "current_timestamp") || strings.HasPrefix(origin, "now(")) {
		return b.columnTypeDefault(typ)
	}
	value := DeepCopyExpr(source.Default.Expr)
	if err := replaceFuncId(b.GetContext(), value); err != nil {
		return nil, err
	}
	value.Typ.AutoIncr = false
	value.Typ.NotNullable = typ.NotNullable
	return value, nil
}

func columnDefaultNull(typ plan.Type) *plan.Expr {
	typ.NotNullable, typ.AutoIncr = false, false
	return &plan.Expr{Typ: typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
}

func (b *baseBinder) columnTypeDefault(typ plan.Type) (*plan.Expr, error) {
	typ.AutoIncr = false
	if !typ.NotNullable {
		return columnDefaultNull(typ), nil
	}
	var value *plan.Expr
	if isSetPlanType(&typ) {
		value = makePlan2Uint64ConstExprWithType(0)
	}
	switch types.T(typ.Id) {
	case types.T_enum:
		value = &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_EnumVal{EnumVal: 0}}}}
	case types.T_date:
		value = makePlan2DateConstExprWithType(int32(types.ZeroDate))
	case types.T_datetime:
		value = makePlan2DateTimeConstExprWithType(int64(types.ZeroDatetime))
	case types.T_timestamp:
		value = makePlan2TimestampConstExprWithType(int64(types.ZeroTimestamp))
	}
	if value != nil {
		value.Typ = typ
		return value, nil
	}
	origin, supported := ctasViewTypeDefaultOrigin(typ)
	if !supported {
		return columnDefaultNull(typ), nil
	}
	statement, err := mysql.ParseOne(b.GetContext(), "SELECT "+origin, 1)
	if err != nil {
		return nil, err
	}
	defer statement.Free()
	ast := statement.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	binder := NewDefaultBinder(b.GetContext(), b.builder, nil, typ, nil)
	value, err = binder.BindExpr(ast, 0, true)
	if err != nil {
		return nil, err
	}
	return makePlan2AssignmentCastExpr(b.GetContext(), value, typ)
}
