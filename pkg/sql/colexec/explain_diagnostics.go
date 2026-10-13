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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// CollectReaderExplainDiagnostics drains optional reader diagnostics into the
// operator statistics that already feed logical EXPLAIN ANALYZE output.
func CollectReaderExplainDiagnostics(reader engine.Reader, analyzer process.Analyzer) {
	if reader == nil || analyzer == nil {
		return
	}
	provider, ok := reader.(engine.ExplainDiagnosticReader)
	if !ok {
		return
	}
	diagnostics := provider.TakeExplainDiagnostics()
	if len(diagnostics) == 0 {
		return
	}
	opStats := analyzer.GetOpStats()
	opStats.BackgroundQueries = append(opStats.BackgroundQueries, diagnostics...)
}
