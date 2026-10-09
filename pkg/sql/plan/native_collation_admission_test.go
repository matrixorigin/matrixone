package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// Existing planner tests exercise the candidate native implementation without
// a running cluster. This switch exists only in test binaries; production has
// no way to turn the admission fence on before the durable rollout gate lands.
func init() {
	native0900TestAdmission.Store(true)
}

func native0900AdmissionDisabled(t *testing.T) {
	previous := native0900TestAdmission.Swap(false)
	t.Cleanup(func() { native0900TestAdmission.Store(previous) })
}

func TestBuildPlanAllowsUnqualifiedLegacyTextByDefault(t *testing.T) {
	native0900AdmissionDisabled(t)
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
		"create table t (name varchar(32))", 1)
	require.NoError(t, err)
	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.Zero(t, p.GetDdl().GetCreateTable().GetTableDef().Cols[0].Typ.CollationVersion)
}

func TestLegacyExplicitCollationRemainsAdmitted(t *testing.T) {
	native0900AdmissionDisabled(t)
	for _, sql := range []string{
		"create table t (name varchar(32) collate utf8mb4_general_ci)",
		"create table t (name varchar(32) collate utf8mb4_bin)",
		"create table t (name varchar(32) character set utf8mb4)",
		"create table t (name varchar(32)) collate utf8mb4_general_ci",
		"select 'a' collate utf8mb4_bin",
		"select 'a' collate utf8mb4_general_ci",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(true)
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			plan, err := BuildPlan(ctx, stmt, false)
			stmt.Free()
			require.NoError(t, err)
			if ddl := plan.GetDdl(); ddl != nil && ddl.GetCreateTable() != nil {
				require.Zero(t, ddl.GetCreateTable().GetTableDef().Cols[0].Typ.CollationVersion)
			}
		})
	}
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
		"select 'a' collate utf8mb4_0900_ai_ci", 1)
	require.NoError(t, err)
	_, err = BuildPlan(ctx, stmt, false)
	stmt.Free()
	require.ErrorContains(t, err, native0900AdmissionError)
}

func TestTableDefaultTextKeepsLegacySemanticVersion(t *testing.T) {
	typ := &planpb.Type{Id: int32(types.T_varchar)}
	applyTableDefaultCharsetToPlanType(typ, uint32(types.CharsetUTF8))
	require.Equal(t, uint32(types.CharsetUTF8), typ.Charset)
	require.Zero(t, typ.CollationVersion)
}

func TestDefaultOnlyNativeCollationIsNotAdmitted(t *testing.T) {
	native0900AdmissionDisabled(t)
	for _, collation := range []string{"utf8mb4_0900_ai_ci", "utf8mb4_0900_bin"} {
		for _, columns := range []string{
			"id int",
			"id int, name varchar(32) collate utf8mb4_general_ci",
		} {
			t.Run(collation+"/"+columns, func(t *testing.T) {
				ctx := NewMockCompilerContext(true)
				stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
					"create table t ("+columns+") collate "+collation, 1)
				require.NoError(t, err)
				_, err = BuildPlan(ctx, stmt, false)
				stmt.Free()
				require.ErrorContains(t, err, native0900AdmissionError)
			})
		}
	}
	table := &planpb.TableDef{DefaultCharset: 260}
	plan := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Nodes: []*planpb.Node{{TableDef: table}},
	}}}
	require.ErrorContains(t, requireNative0900PlanAdmission(context.Background(), nil, plan),
		"unsupported table default collation identity")
}

func TestNativeCollationRelationFormatIsRejectedByDefault(t *testing.T) {
	native0900AdmissionDisabled(t)
	p := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Nodes: []*planpb.Node{{TableDef: &planpb.TableDef{
			KeyFormat: uint32(types.PADSpaceKeyV1),
		}}},
	}}}
	require.ErrorContains(t,
		requireNative0900PlanAdmission(context.Background(), nil, p),
		native0900AdmissionError)
}

func TestCollationKeyOnlyPlanIsRejectedByDefault(t *testing.T) {
	native0900AdmissionDisabled(t)
	p := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{{
			Typ: planpb.Type{Id: int32(types.T_varbinary)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{
					Obj:     int64(581) << 32,
					ObjName: "internal_collation_key",
				},
				Args: []*planpb.Expr{{
					Typ:  planpb.Type{Id: int32(types.T_varchar)},
					Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
				}, {
					Typ: planpb.Type{Id: int32(types.T_uint64)},
					Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
						Value: &planpb.Literal_U64Val{U64Val: uint64(types.CharsetUTF8)},
					}},
				}},
			}},
		}}}},
	}}}
	err := requireNative0900PlanAdmission(context.Background(), nil, p)
	require.ErrorContains(t, err, native0900AdmissionError)
}
