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

package iscp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// NewSync resolves a LOCAL-fileservice spill dir before handing off to
// hnsw.NewHnswSync. No registered ISCP executor, and a registered executor with no
// LOCAL fileservice, both leave the spill dir empty (meaning $TMPDIR) and reach the
// hand-off. The hand-off reads index metadata over SQL, so these drive it with a
// malformed index def, which NewHnswSync rejects before any SQL runs.
func TestHnswSqlWriterNewSync_SpillDirResolution(t *testing.T) {
	sqlproc := &sqlexec.SqlProcess{SqlCtx: &sqlexec.SqlContext{Ctx: context.Background()}}
	service := sqlproc.GetService()

	for _, c := range []struct {
		name             string
		registerExecutor bool
	}{
		{"no executor registered", false},
		{"executor with no local fileservice", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			if c.registerExecutor {
				iscpExecutors.Store(service, &ISCPTaskExecutor{})
				t.Cleanup(func() { iscpExecutors.Delete(service) })
			}
			_, ok := GetExecutorRuntime(service)
			require.Equal(t, c.registerExecutor, ok)

			tabledef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 3)
			w, err := NewHnswSqlWriter("hnsw", newTestJobID(), newTestConsumerInfo(), tabledef, tabledef.Indexes)
			require.NoError(t, err)
			hw := w.(*HnswSqlWriter[float32])

			// HNSW indexes exactly one column; two parts is refused before any SQL.
			hw.indexdef[0].Parts = []string{"vec", "vec2"}

			_, err = hw.NewSync(sqlproc)
			require.Error(t, err)
			require.Contains(t, err.Error(), "index parts")
		})
	}
}
