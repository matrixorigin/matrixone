// Copyright 2022 Matrix Origin
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

package ivfflat

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// give blob
func mock_runSql_streaming(proc *process.Process, sql string, ch chan executor.Result, err_chan chan error) (executor.Result, error) {
	// don't close channel because it may run faster than err_chan
	//defer close(ch)
	err_chan <- moerr.NewInternalErrorNoCtx("sql error")
	return executor.Result{}, nil
}

func mock_runSql_streaming_parser_error(proc *process.Process, sql string, ch chan executor.Result, err_chan chan error) (executor.Result, error) {
	return executor.Result{}, moerr.NewInternalErrorNoCtx("sql parser error")
}

func mock_runSql_streaming_cancel(proc *process.Process, sql string, ch chan executor.Result, err_chan chan error) (executor.Result, error) {
	proc.Cancel(moerr.NewInternalErrorNoCtx("user cancel"))
	return executor.Result{}, nil
}

func TestIvfSearchRace(t *testing.T) {

	runSql_streaming = mock_runSql_streaming

	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	v := []float32{0, 1, 2}
	rt := vectorindex.RuntimeConfig{}

	idx := &IvfflatSearchIndex[float32]{}

	_, _, err := idx.Search(proc, idxcfg, tblcfg, v, rt, 4)
	require.NotNil(t, err)

}

func TestIvfSearchParserError(t *testing.T) {

	runSql_streaming = mock_runSql_streaming_parser_error

	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	v := []float32{0, 1, 2}
	rt := vectorindex.RuntimeConfig{}

	idx := &IvfflatSearchIndex[float32]{}

	_, _, err := idx.Search(proc, idxcfg, tblcfg, v, rt, 4)
	require.NotNil(t, err)
}

func TestIvfSearchCancel(t *testing.T) {

	runSql_streaming = mock_runSql_streaming_cancel

	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	proc.Ctx, proc.Cancel = context.WithCancelCause(proc.Ctx)

	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	v := []float32{0, 1, 2}
	rt := vectorindex.RuntimeConfig{}

	idx := &IvfflatSearchIndex[float32]{}

	_, _, err := idx.Search(proc, idxcfg, tblcfg, v, rt, 4)
	require.NotNil(t, err)
}
