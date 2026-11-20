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

package table_function

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	indexplugin "github.com/matrixorigin/matrixone/pkg/indexplugin"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type diagnosticTestPlugin struct {
	indexplugin.AlgoPlugin
	name string
}

func (p diagnosticTestPlugin) Algo() string           { return p.name }
func (p diagnosticTestPlugin) Plan() planplugin.Hooks { return diagnosticTestHooks{} }

type diagnosticTestHooks struct{ planplugin.Hooks }

func (diagnosticTestHooks) ConfigureIndexStats(context.Context, *plan.TableDef, []*plan.IndexDef, *indexstats.Config) error {
	return nil
}
func (diagnosticTestHooks) ReadIndexStats(_ *sqlexec.SqlProcess, cfg indexstats.Config) (indexstats.Rows, error) {
	return indexstats.Rows{IndexType: cfg.Algorithm, Count: 129, At: func(i int) indexstats.Row {
		return indexstats.Row{Kind: "partition", ObjectID: strconv.Itoa(i), Metrics: map[string]any{"position": i}}
	}}, nil
}

var diagnosticTestPluginID atomic.Uint64

func TestMoIndexStatsPluginExecution(t *testing.T) {
	name := "test_diagnostic_" + strconv.FormatUint(diagnosticTestPluginID.Add(1), 10)
	indexplugin.Register(diagnosticTestPlugin{name: name})
	proc := testutil.NewProc(t)
	tf := &TableFunction{Attrs: []string{"index_type", "row_type", "object_id", "metrics"}}
	tf.ctr.retSchema = []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_json.ToType()}
	state := indexStatsState{config: indexstats.Config{Algorithm: name}}
	defer state.free(tf, proc, false, nil)
	require.NoError(t, state.start(tf, proc, 0, nil))
	for page, n := range []int{128, 1} {
		out, err := state.call(tf, proc)
		require.NoError(t, err)
		require.Equal(t, n, out.Batch.RowCount())
		require.Equal(t, name, out.Batch.Vecs[0].GetStringAt(0))
		require.Equal(t, "partition", out.Batch.Vecs[1].GetStringAt(0))
		pos := page * 128
		require.Equal(t, strconv.Itoa(pos), out.Batch.Vecs[2].GetStringAt(0))
		require.JSONEq(t, `{"position":`+strconv.Itoa(pos)+"}", types.DecodeJson(out.Batch.Vecs[3].GetBytesAt(0)).String())
	}
	done, err := state.call(tf, proc)
	require.NoError(t, err)
	require.Nil(t, done.Batch)
	state.config.Algorithm = "unsupported_statistics"
	require.ErrorContains(t, state.start(tf, proc, 0, nil), "not supported")
}
