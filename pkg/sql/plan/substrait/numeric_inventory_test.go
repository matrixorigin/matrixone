// Copyright 2026 Matrix Origin
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

package substrait

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestTPCHNumericEligibilityInventory(t *testing.T) {
	inventory := []struct {
		queryNumber     int
		eligibleOutput  []string
		reason          EligibilityReason
		message         string
		numericEvidence string
	}{
		{1, nil, EligibilityExpression, `substrait: not eligible (expression): aggregate overload "sum" has no declared Sirius semantic equivalence`, `sum([DECIMAL256(53,4)!])->DECIMAL256(65,4)!`},
		{2, []string{`DECIMAL64(15,2)!`, `VARCHAR(25)!`, `VARCHAR(25)!`, `INT!`, `VARCHAR(25)!`, `VARCHAR(40)!`, `VARCHAR(15)!`, `VARCHAR(101)!`}, "", "", `min([DECIMAL64(15,2)!])->DECIMAL64(15,2)!`},
		{3, nil, EligibilityExpression, `substrait: not eligible (expression): aggregate overload "sum" has no declared Sirius semantic equivalence`, `sum([DECIMAL256(53,4)!])->DECIMAL256(65,4)!`},
		{4, []string{`VARCHAR(15)!`, `BIGINT!`}, "", "", ""},
		{5, nil, EligibilityExpression, `substrait: not eligible (expression): aggregate overload "sum" has no declared Sirius semantic equivalence`, `sum([DECIMAL256(53,4)!])->DECIMAL256(65,4)!`},
		{6, nil, EligibilityType, `substrait: not eligible (type): unsupported type DECIMAL256`, `sum([DECIMAL128(38,4)!])->DECIMAL256(60,4)!`},
		{7, nil, EligibilityExpression, `substrait: not eligible (expression): aggregate overload "sum" has no declared Sirius semantic equivalence`, `sum([DECIMAL256(53,4)!])->DECIMAL256(65,4)!`},
		{8, nil, EligibilityExpression, `substrait: not eligible (expression): unsupported multiply signature`, `*([DECIMAL256(15,2)! DECIMAL256(38,2)!])->DECIMAL256(53,4)!`},
		{9, nil, EligibilityExpression, `substrait: not eligible (expression): aggregate overload "sum" has no declared Sirius semantic equivalence`, `sum([DECIMAL256(65,4)!])->DECIMAL256(65,4)!`},
		{10, nil, EligibilityExpression, `substrait: not eligible (expression): aggregate overload "sum" has no declared Sirius semantic equivalence`, `sum([DECIMAL256(53,4)!])->DECIMAL256(65,4)!`},
		{11, nil, EligibilityType, `substrait: not eligible (type): unsupported type DECIMAL256`, `sum([DECIMAL128(38,2)!])->DECIMAL256(60,2)!`},
		{12, []string{`VARCHAR(10)!`, `DECIMAL128(38,0)?`, `DECIMAL128(38,0)?`}, "", "", `sum([BIGINT!])->DECIMAL128(38,0)!`},
		{13, []string{`BIGINT!`, `BIGINT!`}, "", "", ""},
		{14, nil, EligibilityExpression, `substrait: not eligible (expression): aggregate overload "sum" has no declared Sirius semantic equivalence`, `sum([DECIMAL256(53,4)!])->DECIMAL256(65,4)!`},
		{15, nil, EligibilityExpression, `substrait: not eligible (expression): aggregate overload "sum" has no declared Sirius semantic equivalence`, `sum([DECIMAL256(53,4)!])->DECIMAL256(65,4)!`},
		{16, []string{`VARCHAR(10)!`, `VARCHAR(25)!`, `INT!`, `BIGINT!`}, "", "", ""},
		{17, nil, EligibilityExpression, `substrait: not eligible (expression): scalar overload "*" has no declared Sirius semantic equivalence`, `*([DECIMAL128(38,1)! DECIMAL128(19,6)!])->DECIMAL128(38,7)!`},
		{18, []string{`VARCHAR(25)!`, `INT!`, `BIGINT!`, `DATE!`, `DECIMAL64(15,2)!`, `DECIMAL128(37,2)?`}, "", "", `sum([DECIMAL64(15,2)!])->DECIMAL128(37,2)!`},
		{19, nil, EligibilityExpression, `substrait: not eligible (expression): aggregate overload "sum" has no declared Sirius semantic equivalence`, `sum([DECIMAL256(53,4)!])->DECIMAL256(65,4)!`},
		{20, nil, EligibilityExpression, `substrait: not eligible (expression): scalar overload "*" has no declared Sirius semantic equivalence`, `*([DECIMAL128(38,1)! DECIMAL128(37,2)!])->DECIMAL128(38,3)!`},
		{21, []string{`VARCHAR(25)!`, `BIGINT!`}, "", "", ""},
		{22, []string{`VARCHAR(2)!`, `BIGINT!`, `DECIMAL128(37,2)?`}, "", "", `avg([DECIMAL64(15,2)!])->DECIMAL128(19,6)!`},
	}
	require.Len(t, inventory, 22)

	mock := planbuilder.NewMockOptimizer(false)
	for index, tc := range inventory {
		require.Equal(t, index+1, tc.queryNumber, "inventory must cover Q1-Q22 in order")
		t.Run(fmt.Sprintf("q%d", tc.queryNumber), func(t *testing.T) {
			path := filepath.Join("..", "tpch", fmt.Sprintf("q%d.sql", tc.queryNumber))
			wire, err := os.ReadFile(path)
			require.NoError(t, err)
			statements, err := parsers.Parse(context.Background(), dialect.MYSQL, string(wire), 1)
			require.NoError(t, err)
			require.Len(t, statements, 1)
			query, err := mock.Optimize(statements[0])
			require.NoError(t, err)

			// MockOptimizer may share catalog definitions between equivalent
			// scans. Keep every alias on one harmless, internally consistent ID.
			for _, read := range query.Nodes {
				if read == nil || read.TableDef == nil || read.ObjRef == nil {
					continue
				}
				read.TableDef.DbId = 7
				read.TableDef.TblId = 42
				read.ObjRef.Obj = 42
			}

			candidate, err := Export(query)
			numericSignatures := numericFunctionSignatures(query)
			if tc.eligibleOutput == nil {
				require.Nil(t, candidate)
				require.EqualError(t, err, tc.message)
				reason, ok := NotEligibleReason(err)
				require.True(t, ok)
				require.Equal(t, tc.reason, reason)
				require.Contains(t, numericSignatures, tc.numericEvidence)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, candidate)
			require.Equal(t, tc.eligibleOutput, formatPlanTypes(candidate.OutputTypes()))
			if tc.numericEvidence != "" {
				require.Contains(t, numericSignatures, tc.numericEvidence)
			}
		})
	}
}

func numericFunctionSignatures(query *planpb.Query) []string {
	found := make(map[string]struct{})
	var visit func(*planpb.Expr)
	visit = func(expr *planpb.Expr) {
		if expr == nil {
			return
		}
		if call := expr.GetF(); call != nil {
			args := make([]planpb.Type, 0, len(call.Args))
			numeric := types.T(expr.Typ.Id).IsDecimal()
			for _, arg := range call.Args {
				visit(arg)
				if arg == nil || arg.GetT() != nil {
					continue
				}
				args = append(args, arg.Typ)
				numeric = numeric || types.T(arg.Typ.Id).IsDecimal()
			}
			if numeric {
				found[fmt.Sprintf("%s(%v)->%s", call.Func.ObjName, formatPlanTypes(args), formatPlanType(expr.Typ))] = struct{}{}
			}
		}
		if list := expr.GetList(); list != nil {
			for _, item := range list.List {
				visit(item)
			}
		}
	}
	for _, node := range query.Nodes {
		if node == nil {
			continue
		}
		expressions := [][]*planpb.Expr{
			node.ProjectList, node.OnList, node.FilterList, node.GroupBy,
			node.AggList, node.WinSpecList, node.TblFuncExprList,
		}
		for _, values := range expressions {
			for _, expr := range values {
				visit(expr)
			}
		}
		for _, order := range node.OrderBy {
			visit(order.GetExpr())
		}
		visit(node.Limit)
		visit(node.Offset)
	}
	result := make([]string, 0, len(found))
	for signature := range found {
		result = append(result, signature)
	}
	sort.Strings(result)
	return result
}

func formatPlanTypes(planTypes []planpb.Type) []string {
	result := make([]string, len(planTypes))
	for i := range planTypes {
		result[i] = formatPlanType(planTypes[i])
	}
	return result
}

func formatPlanType(planType planpb.Type) string {
	oid := types.T(planType.Id)
	typ := types.New(oid, planType.Width, planType.Scale)
	description := typ.DescString()
	if oid.IsDecimal() {
		description = fmt.Sprintf("%s(%d,%d)", oid.String(), planType.Width, planType.Scale)
	}
	nullability := "?"
	if planType.NotNullable {
		nullability = "!"
	}
	return description + nullability
}
