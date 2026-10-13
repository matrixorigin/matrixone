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

package colexec

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type explainDiagnosticTestReader struct {
	diagnostics []*plan.Query
}

func (*explainDiagnosticTestReader) Close() error { return nil }
func (*explainDiagnosticTestReader) Read(context.Context, []string, *plan.Expr, *mpool.MPool, *batch.Batch) (bool, error) {
	return true, nil
}
func (*explainDiagnosticTestReader) SetOrderBy([]*plan.OrderBySpec)       {}
func (*explainDiagnosticTestReader) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (*explainDiagnosticTestReader) SetIndexParam(*plan.IndexReaderParam) {}
func (*explainDiagnosticTestReader) SetFilterZM(objectio.ZoneMap)         {}
func (r *explainDiagnosticTestReader) TakeExplainDiagnostics() []*plan.Query {
	diagnostics := r.diagnostics
	r.diagnostics = nil
	return diagnostics
}

func TestCollectReaderExplainDiagnosticsDrainsOnce(t *testing.T) {
	diagnostic := &plan.Query{Headings: []string{"round"}}
	reader := &explainDiagnosticTestReader{diagnostics: []*plan.Query{diagnostic}}
	analyzer := process.NewTempAnalyzer()

	CollectReaderExplainDiagnostics(reader, analyzer)
	require.Equal(t, []*plan.Query{diagnostic}, analyzer.GetOpStats().BackgroundQueries)

	CollectReaderExplainDiagnostics(reader, analyzer)
	require.Len(t, analyzer.GetOpStats().BackgroundQueries, 1)
	CollectReaderExplainDiagnostics(reader, nil)
}
