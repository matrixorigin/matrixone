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
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// testIvfScanOptions returns the ivfflat settings of spec.
func testIvfScanOptions(t *testing.T, spec *plan.IndexSearchScan) ivfflatplan.ScanOptions {
	t.Helper()
	opts, err := ivfflatplan.DecodeScanOptions(spec.GetAlgoOptions())
	require.NoError(t, err)
	return opts
}

// testAlgoExpr returns the algorithm expression of spec named name, or nil.
func testAlgoExpr(spec *plan.IndexSearchScan, name string) *plan.Expr {
	for i, n := range spec.GetAlgoExprNames() {
		if n == name {
			return spec.AlgoExprs[i]
		}
	}
	return nil
}

// testIvfAlgoOptions returns the algo_options bytes of opts.
func testIvfAlgoOptions(t *testing.T, opts ivfflatplan.ScanOptions) []byte {
	t.Helper()
	data, err := ivfflatplan.EncodeScanOptions(opts)
	require.NoError(t, err)
	return data
}
