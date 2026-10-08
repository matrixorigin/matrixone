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

//go:build sirius && sirius_integration && cgo && linux && amd64

package substrait

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// Preparation validates the complete native graph and output schema without
// admitting any reader work. Execution parity belongs to the public SQL test
// and the separately versioned full-data campaign.
func TestExactEmbeddedTPCHNativePreparation(t *testing.T) {
	config := os.Getenv("MO_SIRIUS_TEST_CONFIG")
	require.NotEmpty(t, config, "sirius_integration requires MO_SIRIUS_TEST_CONFIG")
	config, err := filepath.Abs(config)
	require.NoError(t, err)
	runtime, err := siriusbridge.New(siriusbridge.Config{ConfigPath: config, GPUStreams: 2})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, runtime.Close(ctx))
	})
	require.NotZero(t, runtime.Capabilities()&MOExactDecimalV1Capability)
	mock := planbuilder.NewMockOptimizer(false, newPlanTestProcess(t))
	for number := 1; number <= 22; number++ {
		t.Run(fmt.Sprintf("q%d", number), func(t *testing.T) {
			bound := exactTPCHQuery(t, mock, number)
			candidate, err := ExportEmbeddedMO(bound, NewEmbeddedExportProfile(runtime.Capabilities()))
			require.NoError(t, err)
			reads, err := candidate.EmbeddedMOReads()
			require.NoError(t, err)
			bindings := make(map[int32]EmbeddedReadBinding, len(reads))
			request := siriusbridge.Request{AccountID: 42, QueryID: []byte(fmt.Sprintf("mo-exact-q%d", number))}
			for i, typ := range candidate.OutputTypes() {
				request.Columns = append(request.Columns, siriusbridge.Column{
					OID: uint32(typ.Id), Width: typ.Width, Scale: typ.Scale, Nullable: !typ.NotNullable, Name: bound.Headings[i],
				})
			}
			var starts atomic.Int32
			for i, read := range reads {
				binding := uint64(i + 1)
				bindings[read.NodeID] = EmbeddedReadBinding{BindingID: binding, Source: EmbeddedReadMO}
				nativeRead := siriusbridge.Read{BindingID: binding, Database: read.Database, Table: read.Table, Schema: read.Schema,
					Producer: func(context.Context, *siriusbridge.Input) error { starts.Add(1); return nil }}
				for _, col := range read.Columns {
					nativeRead.Columns = append(nativeRead.Columns, siriusbridge.ReadColumn{
						Column:     siriusbridge.Column{OID: uint32(col.Type.Id), Width: col.Type.Width, Scale: col.Type.Scale, Nullable: !col.Type.NotNullable, Name: col.Name},
						PhysicalID: col.PhysicalID, Sequence: col.Sequence,
					})
				}
				request.Reads = append(request.Reads, nativeRead)
			}
			request.Plan, err = candidate.BuildEmbedded(bindings)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			query, err := runtime.Prepare(ctx, request)
			require.NoError(t, err)
			t.Cleanup(func() {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				require.NoError(t, query.Close(cleanupCtx))
			})
			require.NoError(t, query.Close(ctx))
			require.Zero(t, starts.Load(), "preparation must not start MO readers")
			stats, ready := query.Statistics()
			require.True(t, ready)
			require.Zero(t, stats.GPUTasksStarted)
			require.Zero(t, stats.MOInputUnits)
		})
	}
}
