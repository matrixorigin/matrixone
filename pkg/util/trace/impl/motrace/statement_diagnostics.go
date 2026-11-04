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
	"bytes"
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
)

// StatementDiagnosticsSetter consumes terminal facts under StatementInfo.mux.
// It must not retain ctx, err or live execution state, or reacquire this mutex.
type StatementDiagnosticsSetter interface {
	SetStatementDiagnostics(context.Context, resource.StatementResourceSummary, error) bool
}

type summaryDiagnosticPlan struct{ data *models.StatementDiagnostics }

func (p *summaryDiagnosticPlan) Marshal(context.Context) []byte {
	var b bytes.Buffer
	if err := p.data.WriteJSON(&b); err != nil {
		return noExecPlan
	}
	return b.Bytes()
}
func (p *summaryDiagnosticPlan) Free() { p.data = nil }
func (p *summaryDiagnosticPlan) Stats(context.Context) (statistic.StatsArray, Statistic) {
	return statistic.DefaultStatsArray, Statistic{}
}

func (s *StatementInfo) finalizeStatementDiagnostics(ctx context.Context, err error) {
	if !UseCompactStatementDiagnostics() || s.IsMoLogger() {
		return
	}
	if setter, ok := s.ExecPlan.(StatementDiagnosticsSetter); ok {
		if setter.SetStatementDiagnostics(ctx, s.resourceSummary, err) {
			s.jsonByte = nil
			s.DisableAgg()
		}
		return
	}
	// Preserve custom serializers. Only absent plans need the scalar fallback
	// for parse/compile failures and other triggers discovered after release.
	if s.ExecPlan != nil {
		return
	}
	level, reasons := models.SelectStatementDiagnosticLevel(s.Duration, GetLongQueryTime(), &s.resourceSummary, err != nil, false)
	if level == 0 {
		return
	}
	d := &models.StatementDiagnostics{Version: models.DiagnosticsVersion, Level: level, Reasons: reasons, Outcome: models.DiagnosticOutcome(err), Detail: models.DiagnosticDetail{Capture: "no_query"}}
	d.SetSummary(s.resourceSummary, s.Duration, time.Duration(0), false)
	d.SetPhases(statistic.StatsInfoFromContext(ctx))
	s.ExecPlan = &summaryDiagnosticPlan{data: d}
	s.jsonByte = nil
	s.DisableAgg()
}
