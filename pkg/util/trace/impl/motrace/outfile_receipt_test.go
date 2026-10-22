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

package motrace

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/models"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/stretchr/testify/require"
)

type outfileReceiptTestPlan struct{ raw []byte }

func (p *outfileReceiptTestPlan) Marshal(context.Context) []byte { return p.raw }
func (*outfileReceiptTestPlan) Free()                            {}
func (*outfileReceiptTestPlan) Stats(context.Context) (statistic.StatsArray, Statistic) {
	return statistic.StatsArray{}, Statistic{}
}

func TestStatementInfoOutfileReceiptCompositionAndOwnership(t *testing.T) {
	s := NewStatementInfo()
	s.ExecPlan = &outfileReceiptTestPlan{raw: []byte(`{"large":18446744073709551615,"nested":{"id":9007199254740993}}`)}
	s.SetOutfileReceipt(StatementOutfileReceipt{Target: "stage://audit/out.csv", Format: "csv"})
	sqlText := s.ExecPlan2Json(context.Background())
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(sqlText, &fields))
	require.Equal(t, json.RawMessage(`18446744073709551615`), fields["large"])
	require.Equal(t, json.RawMessage(`9007199254740993`), mustJSONRawField(t, fields["nested"], "id"))

	var explain models.ExplainData
	require.NoError(t, json.Unmarshal(sqlText, &explain), "the existing mo_explain_phy JSON consumer must tolerate the additive field")

	s.UpdateOutfileReceipt("csv", 40, 1, true)
	updated := s.ExecPlan2Json(context.Background())
	require.NotEqual(t, string(sqlText), string(updated), "receipt updates invalidate the cached JSON")
	var updatedFields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(updated, &updatedFields))
	var receipt StatementOutfileReceipt
	require.NoError(t, json.Unmarshal(updatedFields["outfile"], &receipt))
	require.Equal(t, uint64(40), receipt.BytesWritten)
	require.Equal(t, uint64(1), receipt.RowsWritten)
	require.True(t, receipt.Complete)

	clone := s.CloneWithoutExecPlan()
	require.True(t, clone.disableAgg)
	require.NotNil(t, clone.outfileReceipt)
	require.Equal(t, receipt.Target, clone.outfileReceipt.Target)
	s.SetOutfileReceipt(StatementOutfileReceipt{Target: "stage://audit/other.csv", Format: "csv"})
	require.Equal(t, "stage://audit/out.csv", clone.outfileReceipt.Target)

	for _, statement := range []*StatementInfo{s, clone} {
		statement.end = true
		statement.exported = true
		statement.Free()
		require.Nil(t, statement.outfileReceipt)
		require.False(t, statement.disableAgg)
		require.Nil(t, statement.jsonByte)
	}
}

func TestExecPlanWithOutfileReceiptPreservesCapacity(t *testing.T) {
	receipt := StatementOutfileReceipt{Version: 1, Target: "stage://audit/out.csv", Format: "csv"}
	largeNumberPlan := []byte(`{"large":18446744073709551615}`)
	got := execPlanWithOutfileReceipt(largeNumberPlan, receipt)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(got, &fields))
	require.Equal(t, json.RawMessage(`18446744073709551615`), fields["large"])
	require.NotEmpty(t, fields["outfile"])

	oversizedPlan := []byte(`{"plan":"` + strings.Repeat("p", db_holder.StatementInfoTextLimit) + `"}`)
	got = execPlanWithOutfileReceipt(oversizedPlan, receipt)
	require.LessOrEqual(t, len(got), db_holder.StatementInfoTextLimit)
	require.NoError(t, json.Unmarshal(got, &fields))
	require.NotEmpty(t, fields["outfile"], "capacity fallback must retain the audit receipt")
	var summary map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(got, &summary))
	var originalBytes int
	require.NoError(t, json.Unmarshal(summary["original_bytes"], &originalBytes))
	require.Equal(t, len(oversizedPlan), originalBytes)

	got = execPlanWithOutfileReceipt([]byte("not-json"), receipt)
	require.NoError(t, json.Unmarshal(got, &fields))
	require.NotEmpty(t, fields["outfile"], "malformed plan diagnostics must not discard a valid receipt")
}

func mustJSONRawField(t *testing.T, data json.RawMessage, name string) json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fields))
	return fields[name]
}

func TestStatementInfoOutfileReceiptPreventsFastAggregation(t *testing.T) {
	s := NewStatementInfo()
	s.StatementType = "Select"
	s.SqlSourceType = "external_sql"
	s.Duration = 1
	s.SetOutfileReceipt(StatementOutfileReceipt{Target: "SHARED:/audit.csv", Format: "csv"})
	require.False(t, StatementInfoFilter(s))
	clone := s.CloneWithoutExecPlan()
	require.False(t, StatementInfoFilter(clone))
	for _, statement := range []*StatementInfo{s, clone} {
		statement.end = true
		statement.exported = true
		statement.Free()
	}
}

func TestStatementInfoOutfileReceiptEnforcesBoundAndFormat(t *testing.T) {
	s := NewStatementInfo()
	s.SetOutfileReceipt(StatementOutfileReceipt{
		Target: strings.Repeat("a", statementOutfileTargetLimit+1),
		Format: strings.Repeat("not-a-format", 100),
	})
	require.NotNil(t, s.outfileReceipt)
	require.Len(t, s.outfileReceipt.Target, statementOutfileTargetLimit)
	require.True(t, s.outfileReceipt.TargetTruncated)
	require.Equal(t, "unknown", s.outfileReceipt.Format)
	s.end = true
	s.exported = true
	s.Free()
}
