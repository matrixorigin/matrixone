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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

// give blob
func mock_runSql(
	sqlproc *sqlexec.SqlProcess,
	sql string,
) (executor.Result, error) {
	return executor.Result{}, nil
}

func mock_runSql_parser_error(
	sqlproc *sqlexec.SqlProcess,
	sql string,
) (executor.Result, error) {
	return executor.Result{}, moerr.NewInternalErrorNoCtx("sql parser error")
}

func TestIvfSearchRace(t *testing.T) {

	runSql = mock_runSql

	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	v := []float32{0, 1, 2}
	rt := vectorindex.RuntimeConfig{}

	idx := &IvfflatSearchIndex[float32]{}

	_, _, err := idx.Search(sqlproc, idxcfg, tblcfg, v, rt, 4)
	require.Nil(t, err)

}

func TestIvfSearchParserError(t *testing.T) {

	runSql = mock_runSql_parser_error

	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	v := []float32{0, 1, 2}
	rt := vectorindex.RuntimeConfig{}

	idx := &IvfflatSearchIndex[float32]{}

	_, _, err := idx.Search(sqlproc, idxcfg, tblcfg, v, rt, 4)
	require.NotNil(t, err)
}
