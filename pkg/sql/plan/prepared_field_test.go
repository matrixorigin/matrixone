// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldResolvedStringDomain(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		binary    bool
	}{
		{"bare", "select field(?, 'A', 'a')", false},
		{"binary peer", "select field(?, _binary 'A', _binary 'a')", true},
		{"explicit binary", "select field(cast(? as binary), 'A', 'a')", true},
		{"explicit text", "select field(cast(? as char), 'A', 'a')", false},
		{"coalesce null", "select field(coalesce(?, null), 'A', 'a')", true},
		{"coalesce binary", "select field(coalesce(?, _binary 'x'), 'A', 'a')", true},
		{"coalesce text", "select field(coalesce(?, 'x'), 'A', 'a')", false},
		{"if", "select field(if(true, ?, null), 'A', 'a')", true},
		{"case", "select field(case when true then ? else null end, 'A', 'a')", true},
		{"nullif return", "select field(nullif(?, 'x'), 'A', 'a')", false},
		{"nullif condition", "select field(nullif('a', ?), 'A', 'a')", false},
		{"derived", "select field(x, 'A', 'a') from (select ? x limit 1) d", false},
		{"scalar", "select field((select ? limit 1), 'A', 'a')", false},
		{"max", "select field(max(?), 'A', 'a')", false},
		{"window", "select field(x, 'A', 'a') from (select max(?) over () x) d", false},
		{"derived case", "select field(x, 'A', 'a') from (select case when true then ? else null end x limit 1) d", true},
		{"union text", "select field(x, 'A', 'a') from (select ? x union all select 'a') d", false},
		{"union binary", "select field(x, 'A', 'a') from (select ? x union all select _binary 'a') d", true},
	} {
		for _, sourceOID := range []types.T{types.T_varchar, types.T_varbinary} {
			t.Run(tc.name+"/"+sourceOID.String(), func(t *testing.T) {
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				proc := mock.ctxt.GetProcess()
				params := vector.NewVec(types.T_text.ToType())
				t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) })
				require.NoError(t, vector.AppendBytes(params, []byte("a"), false, proc.Mp()))
				proc.SetPrepareParams(params)
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				source := sourceOID.ToType()
				bindings := []PreparedSourceBinding{{Position: 0, Type: source}}
				bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings,
					[]any{ParamValue{Value: "a", SourceType: source, HasSourceType: true}})
				require.NoError(t, err)
				field := findPlanFunctionExpr(bound.Plan, "field")
				require.NotNil(t, field)
				got := types.StaticStringDomain(makeTypeByPlan2Expr(field.GetF().Args[0])) == types.StringDomainBinary
				require.Equal(t, tc.binary, got, field.String())
				require.Equal(t, source, bindings[0].Type, "consumer changed the marker's source domain")
			})
		}
	}
}

func TestFieldResolvedSelectorStringDomain(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		binary bool
	}{
		{"select field(lag(NULL, 1, 'a') over (), 'A', 'a')", false},
		{"select field(lag(NULL, 1, _binary 'a') over (), 'A', 'a')", true},
		{"select field(lag('x', 1, _binary 'a') over (), 'A', 'a')", true},
		{"select field(lag(_binary 'x', 1, 'a') over (), 'A', 'a')", true},
		{"select field(lead(NULL, 1, 'a') over (), 'A', 'a')", false},
		{"select field(lead(NULL, 1, _binary 'a') over (), 'A', 'a')", true},
		{"select field(lead('x', 1, _binary 'a') over (), 'A', 'a')", true},
		{"select field(lead(_binary 'x', 1, 'a') over (), 'A', 'a')", true},
		{"select field(coalesce('a', _binary 'x'), 'A', 'a')", true},
		{"select field(coalesce(_binary 'a', 'x'), 'A', 'a')", true},
		{"select field(coalesce('a', 'x'), 'A', 'a')", false},
		{"select field(nullif('a', _binary 'x'), 'A', 'a')", false},
		{"select field(x, 'A', 'a') from (select coalesce('a', _binary 'x') x limit 1) d", true},
		{"select field(x,'A','a') from (select _binary 'a' x union all select '') d where x<>''", true},
		{"select field(x,'A','a') from (select 'z' x union all select _binary 'a') d where x<>'z'", true},
		{"select field(x,'A','a') from (select _binary 'a' x union select '') d where x<>''", true},
		{"select field(x,'A','a') from (select 'a' x union all select '') d where x<>''", false},
		{"select field(x,'A','a') from (select cast(_binary 'a' as char) x union all select '') d where x<>''", false},
		{"select field(x,'A','a') from (select cast('a' as binary(1)) x union all select '') d where x<>''", true},
		{"select field(cast(x as char),'A','a') from (select _binary 'a' x union all select '') d where x<>''", false},
		{"select field(cast(x as binary(1)),'A','a') from (select 'a' x union all select '') d where x<>''", true},
		{"select field(concat_ws('',elt(1,x)),'A','a') from (select _binary 'a' x union all select '') d where x<>''", true},
		{"select field(concat_ws('',elt(1,cast(x as char))),'A','a') from (select _binary 'a' x union all select '') d where x<>''", false},
		{"select field(lower(x),'A','a') from (select _binary 'A' x union all select '') d where x<>''", true},
		{"select field(concat_ws('',lower(x)),'A','a') from (select _binary 'A' x union all select '') d where x<>''", true},
		{"select field(reverse(coalesce(_binary 'a','')),'A','a')", true},
		{"select field(lower(coalesce(_binary 'A','')),'A','a')", true},
		{"select field(upper(coalesce(_binary 'a','')),'A','a')", true},
		{"select field(reverse(cast(coalesce(_binary 'a','') as char)),'A','a')", false},
		{"select field(reverse(reverse(coalesce(_binary 'a',''))),'A','a')", true},
		{"select field(substring(coalesce(_binary 'a',''),1,1),'A','a')", true},
		{"select field(repeat(coalesce(_binary 'a',''),1),'A','a')", true},
		{"select field(trim(both ' ' from coalesce(_binary ' a ','')),'A','a')", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false)
			require.NoError(t, err)
			field := findPlanFunctionExpr(bound, "field")
			require.NotNil(t, field)
			got := types.StaticStringDomain(makeTypeByPlan2Expr(field.GetF().Args[0])) == types.StringDomainBinary
			require.Equal(t, tc.binary, got, field.String())
		})
	}
}

func TestPlainTextStringSourceProof(t *testing.T) {
	textType := types.T_varchar.ToType()
	column := GetColExpr(makePlan2Type(&textType), 10, 0)
	builder := &QueryBuilder{
		qry: &planpb.Query{Nodes: []*planpb.Node{{NodeType: planpb.Node_TABLE_SCAN,
			TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{{Typ: column.Typ}}}}}},
		tag2NodeID: map[int32]int32{10: 0},
	}
	binder := &baseBinder{sysCtx: context.Background(), builder: builder}
	require.True(t, binder.stringSourceIsPlainText(column))
	nested := column
	for range 32 {
		var err error
		nested, err = BindFuncExprImplByPlanExpr(context.Background(), "reverse", []*Expr{nested})
		require.NoError(t, err)
	}
	require.True(t, binder.stringSourceIsPlainText(nested))
	transport := DeepCopyExpr(nested)
	binaryTransport := types.T_blob.ToType()
	transport.GetF().Args[0].Typ = makePlan2Type(&binaryTransport)
	resolver := newPreparedFieldStringResolver(binder)
	resolver.readOnly = true
	require.Equal(t, fieldStringText, resolver.expr(transport).kind)
	require.True(t, binder.stringSourceIsPlainText(transport), "a forwarder's transport does not own the declaration")
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if !binder.stringSourceIsPlainText(nested) {
			panic("plain TEXT proof changed")
		}
	}), "the proof must not replace per-consumer maps with another allocation")
	literal := makePlan2StringConstExprWithType("text")
	require.True(t, binder.stringSourceIsPlainText(literal))
	for _, expr := range []*Expr{nil, makePlan2NullConstExprWithType(),
		makePlan2StringConstExprWithType("binary", true),
		{Typ: column.Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}},
		{Typ: column.Typ, Expr: &planpb.Expr_V{V: &planpb.VarRef{Name: "v"}}},
	} {
		require.False(t, binder.stringSourceIsPlainText(expr))
	}
	folded := DeepCopyExpr(literal)
	folded.GetLit().Src = makePlan2StringConstExprWithType("", true)
	require.False(t, binder.stringSourceIsPlainText(folded))
	converted, err := appendExplicitCastBeforeExpr(context.Background(), makePlan2StringConstExprWithType("binary", true), makePlan2Type(&textType))
	require.NoError(t, err)
	// A proof miss is safe: authoritative casts keep the original resolver.
	require.False(t, binder.stringSourceIsPlainText(converted))
	builder.qry.Nodes[0].NodeType = planpb.Node_PROJECT
	require.False(t, binder.stringSourceIsPlainText(nested), "physical TEXT is not an alias/materialization proof")
	builder.qry.Nodes[0].NodeType = planpb.Node_TABLE_SCAN
	binaryType := types.T_varbinary.ToType()
	builder.qry.Nodes[0].TableDef.Cols[0].Typ = makePlan2Type(&binaryType)
	require.False(t, binder.stringSourceIsPlainText(nested), "the declaration owner, not its transport, must be TEXT")
	builder.qry.Nodes[0].TableDef.Cols[0].Typ = column.Typ
	node := builder.qry.Nodes[0]
	node.ProjectList = []*Expr{column}
	require.True(t, binder.stringSourceIsPlainText(nested), "a direct scan self-reference has the same declaration")
	projected, err := appendSetOperationCastBeforeExpr(context.Background(),
		makePlan2StringConstExprWithType("binary", true), makePlan2Type(&textType))
	require.NoError(t, err)
	node.ProjectList = []*Expr{projected}
	require.False(t, binder.stringSourceIsPlainText(nested), "an existing scan projection can hide a different declaration")
	consumed, err := binder.bindStringSourceDeclaration("reverse", []*Expr{column})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_blob), consumed[0].Typ.Id)
	node.ProjectList = []*Expr{GetColExpr(column.Typ, 20, 0)}
	require.False(t, binder.stringSourceIsPlainText(nested), "an alias is not a direct scan declaration")
	node.ProjectList = nil
	builder.qry.Nodes[0] = nil
	require.False(t, binder.stringSourceIsPlainText(column))
	builder.qry.Nodes[0] = node
	node.TableDef.Cols[0] = nil
	require.False(t, binder.stringSourceIsPlainText(column))
	node.TableDef.Cols[0] = &planpb.ColDef{Typ: column.Typ}
	builder.isPrepareStatement = true
	require.False(t, binder.stringSourceIsPlainText(nested))
	builder.isPrepareStatement = false
	binder.sysCtx = withPreparedSourceBindings(context.Background(), nil)
	require.False(t, binder.stringSourceIsPlainText(nested), "prepared provenance still uses the full resolver")
	binder.sysCtx = context.Background()
	binder.builder = nil
	require.False(t, binder.stringSourceIsPlainText(nested))
}

func TestPreparedFieldStringSourceDeclaration(t *testing.T) {
	ctx := context.Background()
	source, err := BindFuncExprImplByPlanExpr(ctx, "coalesce", []*Expr{
		makePlan2StringConstExprWithType("a", true), makePlan2StringConstExprWithType(""),
	})
	require.NoError(t, err)
	binder := &baseBinder{sysCtx: ctx}
	alreadyBinary, err := binder.bindStringSourceDeclaration("reverse", []*Expr{source})
	require.NoError(t, err)
	require.Same(t, source, alreadyBinary[0])
	target := types.T_text.ToType()
	source, err = appendSetOperationCastBeforeExpr(ctx, source, makePlan2Type(&target))
	require.NoError(t, err)
	args := []*Expr{source}
	converted, err := binder.bindStringSourceDeclaration("reverse", args)
	require.NoError(t, err)
	require.Same(t, source, args[0], "do not mutate the caller's arguments")
	require.NotSame(t, source, converted[0])
	require.Equal(t, types.T_blob, makeTypeByPlan2Expr(converted[0]).Oid)
	require.True(t, isCastOverload(converted[0], 1), "declaration consumption owns the result, unlike transparent cast0")
	require.False(t, converted[0].GetF().GetSyntaxExplicitCast())
	again, err := binder.bindStringSourceDeclaration("reverse", converted)
	require.NoError(t, err)
	require.Same(t, converted[0], again[0], "do not add repeated representation casts")
	explicit, err := appendExplicitCastBeforeExpr(ctx, source, makePlan2Type(&target))
	require.NoError(t, err)
	text, err := binder.bindStringSourceDeclaration("reverse", []*Expr{explicit})
	require.NoError(t, err)
	require.Same(t, explicit, text[0], "SQL-authored text declarations remain authoritative")
	textColumn := &Expr{Typ: makePlan2Type(&target), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}}}
	binaryType := types.T_varbinary.ToType()
	binaryColumn := &Expr{Typ: makePlan2Type(&binaryType), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 1}}}
	convertedBinary, err := appendCastBeforeExpr(ctx, binaryColumn, makePlan2Type(&target))
	require.NoError(t, err)
	mixed, err := BindFuncExprImplByPlanExpr(ctx, "coalesce", []*Expr{textColumn, convertedBinary})
	require.NoError(t, err)
	preserved, err := binder.bindStringSourceDeclaration("left", []*Expr{mixed, makePlan2Int64ConstExprWithType(1)})
	require.NoError(t, err)
	require.Same(t, mixed, preserved[0], "a common binary declaration must not overwrite the selected text row")
	folded, err := binder.bindStringSourceDeclaration("upper", []*Expr{mixed})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_blob), folded[0].Typ.Id, "case folding uses declaration collation")
	require.Same(t, mixed, folded[0].GetF().Args[0])
	consumed, err := binder.bindStringSourceDeclaration("coalesce", []*Expr{source, source})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_blob), consumed[0].Typ.Id, "materialized declarations are consumed before value selection")
	require.Same(t, source, consumed[0].GetF().Args[0])
	require.Equal(t, int32(types.T_blob), consumed[1].Typ.Id)
	require.Same(t, source, consumed[1].GetF().Args[0])
	for _, name := range []string{"timestampadd", "left"} {
		// Both registry contracts own argument 1's numeric normalization.
		// A transport declaration must not wrap it in a second conversion.
		args := []*Expr{makePlan2StringConstExprWithType("SECOND"), source, makePlan2StringConstExprWithType("2024-01-01")}
		if name == "left" {
			args = args[:2]
		}
		snapshot := DeepCopyExpr(source)
		preserved, err := binder.bindStringSourceDeclaration(name, args)
		require.NoError(t, err)
		require.Same(t, source, preserved[1])
		require.Same(t, source, args[1])
		require.Equal(t, snapshot, source)
	}
}

func TestStringDeclarationBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		binary    bool
	}{
		{"mergeable projection", "select reverse(x) from (select coalesce(n_name,_binary '') x from nation) d", false},
		{"filtered projection", "select reverse(x) from (select coalesce(n_name,_binary '') x from nation where n_nationkey=1) d", false},
		{"having projection", "select reverse(x) from (select coalesce(n_name,_binary '') x from nation having x is not null) d", true},
		{"constant having projection", "select reverse(x) from (select coalesce(n_name,_binary '') x from nation having true) d", true},
		{"limited projection", "select reverse(x) from (select coalesce(n_name,_binary '') x from nation limit 1) d", true},
		{"sorted limited projection", "select reverse(x) from (select coalesce(n_name,_binary '') x from nation order by n_name limit 1) d", true},
		{"distinct projection", "select reverse(x) from (select distinct coalesce(n_name,_binary '') x from nation) d", true},
		{"window projection", "select reverse(x) from (select coalesce(n_name,_binary '') x,row_number() over () rn from nation) d", true},
		{"scalar projection", "select reverse((select coalesce(n_name,_binary '') from nation limit 1))", false},
		{"scalar grouped", "select reverse((select coalesce(n_name,_binary '') from nation group by coalesce(n_name,_binary '')))", true},
		{"scalar singleton group", "select reverse((select coalesce(n_name,_binary '') from nation where n_nationkey=1 group by coalesce(n_name,_binary '')))", false},
		{"scalar having", "select reverse((select coalesce(n_name,_binary '') x from nation where n_nationkey=1 having x is not null))", false},
		{"scalar distinct", "select reverse((select distinct coalesce(n_name,_binary '') from nation limit 1))", true},
		{"scalar window", "select reverse((select first_value(coalesce(n_name,_binary '')) over () from nation limit 1))", true},
		{"scalar set", "select reverse((select coalesce(n_name,_binary '') x from nation union all select _binary '' order by x desc limit 1))", true},
		{"scalar materialized source", "select reverse((select x from (select coalesce(n_name,_binary '') x from nation limit 1) d))", true},

		{"direct COALESCE", "select reverse(coalesce(n_name,_binary '')) from nation", false},
		{"nested COALESCE", "select reverse(coalesce(coalesce(n_name,_binary ''),'')) from nation", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false)
			require.NoError(t, err)
			reverse := findPlanFunctionExpr(bound, "reverse")
			require.NotNil(t, reverse)
			require.Equal(t, tc.binary, types.StaticStringDomain(makeTypeByPlan2Expr(reverse.GetF().Args[0])) == types.StringDomainBinary)
		})
	}
}

func TestStringCommonValueDeclarationBoundary(t *testing.T) {
	for _, tc := range []struct {
		value string
		owns  bool
	}{
		{"if(n_nationkey=1,_binary 'a',n_name)", false},
		{"case when n_nationkey=1 then _binary 'a' else n_name end", false},
		{"ifnull(n_name,_binary '')", false},
		{"if(n_nationkey=1,cast('a' as binary(1)),n_name)", true},
		{"case when n_nationkey=1 then cast('a' as binary(1)) else n_name end", true},
		{"ifnull(n_name,cast('' as binary(1)))", true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				"select reverse(coalesce("+tc.value+",'')) from nation", 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false)
			require.NoError(t, err)
			coalesce := findPlanFunctionExpr(bound, "coalesce")
			require.NotNil(t, coalesce)
			arg := coalesce.GetF().Args[0]
			for source := preparedFieldPrivateCastSource(arg, 0); source != nil; source = preparedFieldPrivateCastSource(arg, 0) {
				arg = source
			}
			require.Equal(t, tc.owns, isCastOverload(arg, 1), "only authoritative binary contributors may consume the selected row: %s", arg.String())
			if !tc.owns {
				return
			}
			require.False(t, arg.GetF().GetSyntaxExplicitCast())
			require.Equal(t, int32(types.T_varbinary), arg.Typ.Id)
			require.Equal(t, makeTypeByPlan2Expr(arg.GetF().Args[0]), makeTypeByPlan2Expr(arg),
				"declaration consumption must preserve the complete bounded binary type")
		})
	}
}

func TestStringDeclarationConsumers(t *testing.T) {
	for _, tc := range []struct {
		name, sql, function string
		argument            int
		ownsDeclaration     bool
	}{
		{"NULLIF forwards selected row", "select reverse(nullif(coalesce(n_name,_binary ''),'')) from nation", "reverse", 0, false},
		{"NULLIF merged source", "select reverse(x) from (select nullif(coalesce(n_name,_binary ''),'') x from nation) d", "reverse", 0, false},
		{"CASE retains stronger text source", "select reverse(case when coalesce(n_name,_binary '')='' then null else coalesce(n_name,_binary '') end) from nation", "reverse", 0, false},
		{"NULLIF forwards conditional text", "select reverse(nullif(if(n_nationkey=1,_binary '',coalesce(n_name,_binary '')),'')) from nation", "reverse", 0, false},
		{"NULLIF retains allocated owner", "select reverse(nullif(lpad(coalesce(n_name,_binary ''),3,'z'),'')) from nation", "reverse", 0, true},
		{"CASE retains pure text LPAD", "select reverse(case when n_nationkey=0 then lpad(n_name,3,'z') else _binary '' end) from nation", "reverse", 0, false},
		{"limited CASE retains text declaration", "select reverse(x) from (select case when n_nationkey=0 then n_name else _binary '' end x from nation limit 1) d", "reverse", 0, false},
		{"HAVING CASE retains text declaration", "select reverse(x) from (select case when n_nationkey=0 then n_name else _binary '' end x from nation having x is not null) d", "reverse", 0, false},
		{"limited LPAD CASE retains text declaration", "select reverse(x) from (select case when n_nationkey=0 then lpad(n_name,3,'z') else _binary '' end x from nation limit 1) d", "reverse", 0, false},
		{"CASE retains allocated binary owner", "select reverse(case when n_nationkey=0 then lpad(coalesce(n_name,cast('' as binary(4))),3,'z') else _binary '' end) from nation", "reverse", 0, true},
		{"BIT_COUNT direct source", "select bit_count(coalesce(n_name,_binary '')) from nation", "bit_count", 0, true},
		{"BIT_COUNT merged source", "select bit_count(x) from (select coalesce(n_name,_binary '') x from nation) d", "bit_count", 0, true},
		{"COALESCE over REVERSE", "select coalesce(reverse(coalesce(n_name,_binary '')),'') from nation", "coalesce", 0, false},
		{"COALESCE over LEFT", "select coalesce(left(coalesce(n_name,_binary ''),1),'') from nation", "coalesce", 0, false},
		{"COALESCE over TRIM", "select coalesce(trim(coalesce(n_name,_binary '')),'') from nation", "coalesce", 0, false},
		{"COALESCE over REPEAT", "select coalesce(repeat(coalesce(n_name,_binary ''),2),'') from nation", "coalesce", 0, false},
		{"COALESCE over RPAD", "select coalesce(rpad(coalesce(n_name,_binary ''),3,'z'),'') from nation", "coalesce", 0, false},
		{"COALESCE over LPAD", "select coalesce(lpad(coalesce(n_name,_binary ''),3,'z'),'') from nation", "coalesce", 0, true},
		{"IF over COALESCE", "select char_length(if(n_nationkey=1,_binary '',coalesce(n_name,_binary ''))) from nation", "char_length", 0, false},
		{"CASE over COALESCE", "select char_length(case when n_nationkey=1 then _binary '' else coalesce(n_name,_binary '') end) from nation", "char_length", 0, false},
		{"CASE over column", "select char_length(case when n_nationkey=1 then _binary '' else n_name end) from nation", "char_length", 0, false},
		{"merged conditional", "select char_length(x) from (select if(n_nationkey=1,_binary '',coalesce(n_name,_binary '')) x from nation) d", "char_length", 0, false},
		{"LOCATE merged source", "select locate('a',x) from (select coalesce(n_name,_binary '') x from nation) d", "locate", 1, true},
		{"INSTR merged source", "select instr(x,'a') from (select coalesce(n_name,_binary '') x from nation) d", "instr", 0, true},
		{"LIKE merged source", "select x like '__' from (select coalesce(n_name,_binary '') x from nation) d", "like", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false)
			require.NoError(t, err)
			consumer := findPlanFunctionExpr(bound, tc.function)
			require.NotNil(t, consumer)
			arg := consumer.GetF().Args[tc.argument]
			for source := preparedFieldPrivateCastSource(arg, 0); source != nil; source = preparedFieldPrivateCastSource(arg, 0) {
				arg = source
			}
			require.Equal(t, tc.ownsDeclaration, isCastOverload(arg, 1), "the actual consumer owns the conversion, not an arbitrary function result: %s", arg.String())
		})
	}
}

func TestSerializedJoinKeyRetainsPhysicalOwner(t *testing.T) {
	for _, name := range []string{"serial", "serial_full"} {
		t.Run(name, func(t *testing.T) {
			sql := "select p.n_nationkey from nation p join nation b on " + name +
				"(p.n_nationkey,p.n_regionkey)=" + name + "(b.n_nationkey,b.n_regionkey)"
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false)
			require.NoError(t, err)
			comparison := findPlanFunctionExpr(bound, "=")
			require.NotNil(t, comparison)
			for _, key := range comparison.GetF().Args {
				require.NotNil(t, key.GetF())
				require.Equal(t, name, key.GetF().GetFunc().GetObjName(), "no representation CAST may hide an opaque serialized key")
				_, hashType := GetHashColumn(key)
				require.Equal(t, int32(types.T_varchar), hashType)
				require.Equal(t, types.StringDomainBinary, types.StaticStringDomain(makeTypeByPlan2Expr(key)))
			}
		})
	}
}

func TestStringConditionalDeclarationOwnership(t *testing.T) {
	binaryType := types.T_blob.ToType()
	typ := makePlan2Type(&binaryType)
	variable := &Expr{Typ: typ, Expr: &planpb.Expr_V{V: &planpb.VarRef{Name: "bound_s"}}}
	parameter := &Expr{Typ: typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	column := &Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}}}
	null := &Expr{Typ: typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	binary := makePlan2VarBinaryConstExprWithType("x")
	binary.GetLit().LiteralForm = planpb.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER
	textType := types.T_varchar.ToType()
	textColumn := GetColExpr(makePlan2Type(&textType), 1, 1)
	textLiteral := makePlan2StringConstExprWithType("你a")
	scalar := &Expr{Typ: typ, Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{Typ: planpb.SubqueryRef_SCALAR}}}
	fn := func(name string, args ...*Expr) *Expr {
		return &Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: name}, Args: args,
		}}}
	}
	explicit := fn("cast", null)
	explicit.GetF().SyntaxExplicitCast = true
	for _, tc := range []struct {
		name        string
		value, peer *Expr
		owns        bool
	}{
		{"variable transport", variable, null, false},
		{"parameter transport", parameter, null, false},
		{"forwarded variable", fn("left", variable, makePlan2Int64ConstExprWithType(1)), null, false},
		{"RPAD forwards value", fn("rpad", variable, makePlan2Int64ConstExprWithType(3), makePlan2StringConstExprWithType("z")), null, false},
		{"LPAD owns result", fn("lpad", variable, makePlan2Int64ConstExprWithType(3), makePlan2StringConstExprWithType("z")), null, true},
		{"materializing result", fn("any_value", variable), null, true},
		{"genuine binary peer", variable, binary, true},
		{"genuine binary column", variable, column, true},
		{"explicit binary NULL", variable, explicit, true},
		{"text column outranks binary literal", textColumn, binary, false},
		{"operand order does not change rank", binary, textColumn, false},
		{"text literal cannot outrank binary literal", textLiteral, binary, true},
		{"equal rank binary column", textColumn, column, true},
		{"equal rank binary variable", textColumn, variable, true},
		{"equal rank explicit cast", textColumn, explicit, true},
		{"unknown scalar is not a weaker literal", textColumn, fn("coalesce", scalar, binary), true},
		{"untyped scalar is not a weaker literal", textColumn, fn("coalesce", &Expr{Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{Typ: planpb.SubqueryRef_SCALAR}}}, binary), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The binary condition contributes no result declaration.
			expr := fn("case", binary, tc.value, tc.peer)
			b := &baseBinder{}
			require.Equal(t, tc.owns, b.stringSourceHasDeclarationBoundary(expr, nil, nil))
		})
	}
}

func TestStringDomainContract(t *testing.T) {
	for _, tc := range []struct {
		name       string
		arity, pos int
		collation  bool
		result     bool
	}{
		{"lower", 1, 0, true, false},
		{"lower", 0, -1, true, false},
		{"reverse", 1, 0, false, false},
		{"reverse", 0, -1, false, false},
		{"repeat", 2, 0, false, false},
		{"rpad", 3, 0, false, false},
		{"lpad", 3, 0, false, true},
		{"lpad", 0, -1, false, true},
		{"replace", 3, 0, false, true},
		{"substring_index", 3, 0, false, true},
		{"trim", 3, 2, false, false},
		{"trim", 2, -1, false, false},
		{"coalesce", 2, -1, false, false},
		{"case", 3, -1, false, true},
	} {
		pos, collation, result := preparedStringDomainContract(tc.name, tc.arity)
		require.Equal(t, tc.pos, pos, tc.name)
		require.Equal(t, tc.collation, collation, tc.name)
		require.Equal(t, tc.result, result, tc.name)
		legacyPos, legacyCollation := preparedStringDomainSourceIndex(tc.name, tc.arity)
		require.Equal(t, pos, legacyPos, "the collation API must use the same source mapping")
		require.Equal(t, collation, legacyCollation)
	}
}

func TestStringDeclarationBoundaryAliasCycle(t *testing.T) {
	target := types.T_text.ToType()
	first := GetColExpr(makePlan2Type(&target), 10, 0)
	second := GetColExpr(makePlan2Type(&target), 20, 0)
	builder := &QueryBuilder{
		qry: &planpb.Query{Nodes: []*planpb.Node{
			{NodeType: planpb.Node_PROJECT, ProjectList: []*Expr{second}},
			{NodeType: planpb.Node_PROJECT, ProjectList: []*Expr{first}},
		}},
		tag2NodeID: map[int32]int32{10: 0, 20: 1},
	}
	binder := &baseBinder{sysCtx: context.Background(), builder: builder}
	visiting := make(map[[2]int32]bool)
	require.False(t, binder.stringSourceHasDeclarationBoundary(first, nil, visiting), "an alias cycle does not prove materialization")
	require.Empty(t, visiting, "path ownership ends on every return")
	builder.qry.Nodes[1].Limit = makePlan2Int64ConstExprWithType(1)
	require.True(t, binder.stringSourceHasDeclarationBoundary(first, nil, visiting), "a real boundary still owns its declaration")
	require.Empty(t, visiting)
}

func TestPreparedFieldSetConversionSource(t *testing.T) {
	for _, tc := range []struct {
		name             string
		explicit, folded bool
	}{
		{"set conversion", false, false},
		{"folded set conversion", false, true},
		{"SQL declaration", true, false},
		{"folded SQL declaration", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			target := types.T_text.ToType()
			converted, err := appendSetOperationCastBeforeExpr(ctx, makePlan2StringConstExprWithType("a", true), makePlan2Type(&target))
			require.NoError(t, err)
			require.True(t, isCastOverload(converted, 3))
			converted.GetF().SyntaxExplicitCast = tc.explicit
			require.True(t, isExplicitPreparedCast(converted), "do not change the shared numeric-specialization predicate")
			if tc.folded {
				folded := makePlan2StringConstExprWithType("a")
				folded.GetLit().Src = converted
				converted = folded
			}
			r := newPreparedFieldStringResolver(&baseBinder{sysCtx: ctx})
			require.Equal(t, !tc.explicit, r.expr(converted).kind == fieldStringBinary)
		})
	}
}

func TestPreparedFieldTypeHistory(t *testing.T) {
	for _, tc := range []struct {
		name           string
		binaryProtocol bool
		sequence       []types.T
		want           []types.T
	}{
		{"SQL text first", false, []types.T{types.T_varchar, types.T_any, types.T_blob, types.T_varchar}, []types.T{types.T_text, types.T_text, types.T_text, types.T_text}},
		{"SQL binary first", false, []types.T{types.T_blob, types.T_varchar, types.T_any, types.T_blob}, []types.T{types.T_blob, types.T_blob, types.T_blob, types.T_blob}},
		{"SQL null first", false, []types.T{types.T_any, types.T_varchar, types.T_blob}, []types.T{types.T_any, types.T_any, types.T_any}},
		{"COM binary first", true, []types.T{types.T_blob, types.T_varchar, types.T_any}, []types.T{types.T_text, types.T_text, types.T_text}},
		{"numeric transition", false, []types.T{types.T_blob, types.T_int64, types.T_varchar, types.T_blob}, []types.T{types.T_blob, types.T_int64, types.T_int64, types.T_int64}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select field(case when ? then ? else null end, 'Z', 'A', 'a')", 1)
			require.NoError(t, err)
			defer stmt.Free()
			var state *PreparedFieldTypes
			for i, oid := range tc.sequence {
				bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: oid.ToType()}}
				var value any = "a"
				if oid == types.T_any {
					value = nil
				}
				if oid == types.T_int64 {
					value = int64(123)
				}
				values := []any{ParamValue{Value: int64(1), SourceType: bindings[0].Type, HasSourceType: true, IsBinaryProtocol: tc.binaryProtocol},
					ParamValue{Value: value, SourceType: bindings[1].Type, HasSourceType: true, IsBinaryProtocol: tc.binaryProtocol}}
				ctx.SetContext(WithPreparedFieldTypes(context.Background(), state))
				var priorType types.Type
				if state != nil {
					priorType = state.cases[0].result
				}
				bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
				require.NoError(t, err)
				require.NotNil(t, bound.FieldTypes)
				if state != nil {
					require.Equal(t, priorType, state.cases[0].result, "reprepare must not mutate the prior snapshot")
				}
				require.Equal(t, tc.want[i], bound.FieldTypes.cases[0].result.Oid, "execution %d", i)
				if state != nil && tc.want[i] == tc.want[i-1] {
					require.Same(t, state, bound.FieldTypes, "compatible execution must retain immutable snapshot")
				}
				state = bound.FieldTypes
				require.Equal(t, oid, bindings[1].Type.Oid, "consumer must not rewrite source")
			}
		})
	}
}

func TestPreparedFieldHistoryRelations(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		binary bool
	}{
		{"select field(x, 'A', 'a') from (select case when ? then ? else null end x limit 1) d", true},
		{"select field((select case when ? then ? else null end limit 1), 'A', 'a')", true},
		{"select field(max(case when ? then ? else null end), 'A', 'a')", true},
		{"select field(x, 'A', 'a') from (select max(case when ? then ? else null end) over () x) d", true},
		{"select field(coalesce(case when ? then ? else null end, 'x'), 'A', 'a')", false},
		{"select field(case when ? then reverse(?) else null end, 'A', 'a')", false},
		{"select field(x,'A','a') from (select case when ? then ? else null end x union all select 'a') d", false},
		{"with d as (select case when ? then ? else null end x) select field(x,'A','a'),field(x,'a','A') from d", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			var state *PreparedFieldTypes
			for _, oid := range []types.T{types.T_any, types.T_varchar, types.T_blob, types.T_varchar} {
				bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: oid.ToType()}}
				var value any = "a"
				if oid == types.T_any {
					value = nil
				}
				values := []any{ParamValue{Value: int64(1), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: value, SourceType: bindings[1].Type, HasSourceType: true}}
				ctx.SetContext(WithPreparedFieldTypes(context.Background(), state))
				var priorType types.Type
				if state != nil {
					priorType = state.cases[0].result
				}
				bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
				require.NoError(t, err)
				if state != nil {
					require.Same(t, state, bound.FieldTypes, "compatible string/NULL changes cannot reset history")
					require.Equal(t, priorType, state.cases[0].result, "published snapshots must remain immutable")
				}
				state = bound.FieldTypes
				require.Len(t, state.cases, 1, "shared relational consumers own one fact")
				field := findPlanFunctionExpr(bound.Plan, "field")
				require.NotNil(t, field)
				if oid != types.T_any {
					require.Equal(t, tc.binary, types.StaticStringDomain(makeTypeByPlan2Expr(field.GetF().Args[0])) == types.StringDomainBinary)
				}
			}
		})
	}
}

func TestPreparedFieldCaseIdentity(t *testing.T) {
	sql := "with c as (select case when ? then ? else null end x) select case when true then (select case when ? then ? else null end) else x end from c where case when ? then true else false end"
	first, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer first.Free()
	second, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer second.Free()
	a, b := tree.CaseExpressions(first), tree.CaseExpressions(second)
	require.Len(t, a, 4)
	require.Len(t, b, 4)
	for i := range a {
		require.NotSame(t, a[i], b[i])
		require.Equal(t, tree.String(a[i], dialect.MYSQL), tree.String(b[i], dialect.MYSQL))
	}
	require.Equal(t, a, tree.CaseExpressions(first), "enumeration must not mutate the AST")
}

func TestPreparedFieldOverflowAdmission(t *testing.T) {
	signed := types.T_int64.ToType()
	snapshot := &PreparedFieldTypes{revision: 1, params: map[int32]preparedFieldParameter{0: {typ: signed}}}
	for _, tc := range []struct {
		value string
		want  bool
	}{
		{"9223372036854775807", false}, {"-9223372036854775808", false},
		{"9223372036854775808", true}, {"18446744073709551615", true},
		{"a", false}, {"18446744073709551615tail", false}, {"1e100000", true}, {"1e-100000", false},
	} {
		bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}}
		require.Equal(t, tc.want, snapshot.NeedsReprepare(bindings, []any{ParamValue{Value: tc.value}}), tc.value)
	}
	require.Equal(t, signed, snapshot.params[0].typ)
	require.False(t, (&PreparedFieldTypes{params: map[int32]preparedFieldParameter{0: {typ: signed, inherited: true}}}).NeedsReprepare(
		[]PreparedSourceBinding{{Position: 0, Type: types.T_text.ToType()}}, []any{"18446744073709551615"}))
}

func TestPreparedFieldUnrelatedStatementsKeepNoHistory(t *testing.T) {
	for _, sql := range []string{"select ?", "select case when ? then ? else null end", "select field(cast(case when ? then ? else null end as char),'A','a')", "select field(case when true then 'a' else 'b' end,'A','a')"} {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
			ctx.SetContext(WithPreparedFieldTypes(context.Background(), nil))
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bindings := make([]PreparedSourceBinding, tree.ParameterCount(stmt))
			values := make([]any, len(bindings))
			for i := range bindings {
				bindings[i] = PreparedSourceBinding{Position: int32(i), Type: types.T_text.ToType()}
				values[i] = ParamValue{Value: "1", SourceType: bindings[i].Type, HasSourceType: true}
			}
			bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
			require.NoError(t, err)
			require.Nil(t, bound.FieldTypes)
		})
	}
}

func TestPreparedFieldWrappedTypeHistory(t *testing.T) {
	for _, wrapper := range []string{"concat(?)", "concat_ws('', ?)", "(select ? limit 1)"} {
		for _, first := range []types.T{types.T_any, types.T_varchar, types.T_blob, types.T_int64} {
			t.Run(wrapper+"/"+first.String(), func(t *testing.T) {
				ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select field(case when ? then "+wrapper+" else null end,'A','a')", 1)
				require.NoError(t, err)
				defer stmt.Free()
				var state *PreparedFieldTypes
				want := types.T_text
				if first == types.T_blob {
					want = types.T_blob
				} else if first == types.T_int64 && wrapper == "(select ? limit 1)" {
					want = types.T_int64
				}
				for _, oid := range []types.T{first, types.T_varchar, types.T_blob} {
					bindings := []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: oid.ToType()}}
					var value any = "a"
					if oid == types.T_any {
						value = nil
					} else if oid == types.T_int64 {
						value = int64(123)
					}
					values := []any{ParamValue{Value: int64(1), SourceType: bindings[0].Type, HasSourceType: true}, ParamValue{Value: value, SourceType: bindings[1].Type, HasSourceType: true}}
					ctx.SetContext(WithPreparedFieldTypes(context.Background(), state))
					bound, err := BuildPreparedExecutionPlan(ctx, stmt, bindings, values)
					require.NoError(t, err)
					require.NotNil(t, bound.FieldTypes, "the wrapper must retain its parameter dependency")
					require.Equal(t, want, bound.FieldTypes.cases[0].result.Oid)
					if state != nil {
						require.Same(t, state, bound.FieldTypes, "string source changes must preserve resolved history")
					}
					state = bound.FieldTypes
					require.Equal(t, oid, bindings[1].Type.Oid, "return types must not rewrite marker sources")
				}
			})
		}
	}
}
