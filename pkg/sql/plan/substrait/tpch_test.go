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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

func TestExportCanonicalTPCHPlans(t *testing.T) {
	mock := planbuilder.NewMockOptimizer(false, newPlanTestProcess(t))
	// Exact DECIMAL arithmetic and SUM widening make these plans contain
	// Decimal256 expressions. Substrait decimal is capped at precision 38, so
	// declining Sirius offload preserves MatrixOne's wider arithmetic semantics.
	decimal256Plans := map[int]EligibilityReason{
		1: EligibilityExpression, 3: EligibilityExpression, 5: EligibilityExpression,
		6: EligibilityType, 7: EligibilityExpression, 8: EligibilityExpression,
		9: EligibilityExpression, 10: EligibilityExpression, 11: EligibilityType,
		14: EligibilityExpression, 15: EligibilityExpression, 17: EligibilityExpression,
		19: EligibilityExpression, 20: EligibilityExpression,
	}
	for queryNumber := 1; queryNumber <= 22; queryNumber++ {
		t.Run(fmt.Sprintf("q%d", queryNumber), func(t *testing.T) {
			path := filepath.Join("..", "tpch", fmt.Sprintf("q%d.sql", queryNumber))
			wire, err := os.ReadFile(path)
			require.NoError(t, err)
			statements, err := parsers.Parse(context.Background(), dialect.MYSQL, string(wire), 1)
			require.NoError(t, err)
			query, err := mock.Optimize(statements[0])
			require.NoError(t, err)

			// MockOptimizer shares catalog pointers between equivalent scans. One
			// harmless identity keeps those aliases internally consistent while the
			// test remains about the logical Substrait coverage, not catalog setup.
			for _, read := range query.Nodes {
				if read == nil || read.TableDef == nil || read.ObjRef == nil {
					continue
				}
				read.TableDef.DbId = 7
				read.TableDef.TblId = 42
				read.ObjRef.Obj = 42
			}

			candidate, err := Export(query)
			if expectedReason, expectedIneligible := decimal256Plans[queryNumber]; expectedIneligible {
				require.Error(t, err)
				require.True(t, IsNotEligible(err))
				reason, ok := NotEligibleReason(err)
				require.True(t, ok)
				require.Equal(t, expectedReason, reason)
				if expectedReason == EligibilityType {
					require.ErrorContains(t, err, "unsupported type DECIMAL256")
				}
				return
			}
			require.NoError(t, err)
			readValues := make(map[int32][]byte, len(candidate.Reads()))
			for _, read := range candidate.Reads() {
				readValues[read.NodeID] = []byte{1}
			}
			wirePlan, err := candidate.Build(readValues)
			require.NoError(t, err)
			require.LessOrEqual(t, len(wirePlan), MaxPlanBytes)
			plan := new(spb.Plan)
			require.NoError(t, proto.Unmarshal(wirePlan, plan))
			require.Len(t, plan.Relations, len(query.Steps))
			require.Equal(t, query.Headings, plan.Relations[len(plan.Relations)-1].GetRoot().Names)
		})
	}
}

func TestExportExtractSemanticBoundary(t *testing.T) {
	for _, tc := range []struct {
		expression string
		eligible   bool
	}{
		{"extract(year from l_shipdate)", true},
		{"extract(month from l_shipdate)", true},
		{"extract(day from l_shipdate)", true},
		{"extract(quarter from l_shipdate)", true},
		{"extract(week from l_shipdate)", false}, // MO mode 0, backend ISO
		{"extract(year_month from l_shipdate)", false},
		{"extract(year from l_comment)", false}, // tolerant text vs no overload
		{"extract(week from l_comment)", false},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			mock := planbuilder.NewMockOptimizer(false, newPlanTestProcess(t))
			statements, err := parsers.Parse(t.Context(), dialect.MYSQL,
				"select "+tc.expression+" from lineitem", 1)
			require.NoError(t, err)
			defer statements[0].Free()
			query, err := mock.Optimize(statements[0])
			require.NoError(t, err)
			for _, node := range query.Nodes {
				if node != nil && node.TableDef != nil && node.ObjRef != nil {
					node.TableDef.DbId, node.TableDef.TblId, node.ObjRef.Obj = 7, 42, 42
				}
			}
			candidate, err := Export(query)
			if tc.eligible {
				require.NoError(t, err)
				require.NotNil(t, candidate)
			} else {
				require.Nil(t, candidate)
				require.True(t, IsNotEligible(err), "%v", err)
				reason, ok := NotEligibleReason(err)
				require.True(t, ok)
				require.Equal(t, EligibilityExpression, reason)
			}
			embedded, embeddedErr := ExportEmbeddedMO(query, NewEmbeddedExportProfile(31))
			if tc.eligible && tc.expression != "extract(quarter from l_shipdate)" {
				require.NoError(t, embeddedErr)
				require.NotNil(t, embedded)
			} else {
				require.Nil(t, embedded)
				require.True(t, IsNotEligible(embeddedErr), "%v", embeddedErr)
			}
		})
	}
}
