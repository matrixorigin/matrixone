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

package table_function

import (
	"strings"
	"testing"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func ft2IgnoredCfg(t *testing.T, ignored string, limit int64, pkey string) string {
	b, err := sonic.Marshal(fulltext2.TableConfig{ThreadsBuild: 1, DbName: "db", IndexTable: "__idx", MetadataTable: "__meta",
		Parser: "json", IgnoredTable: ignored, MaxBadRecords: limit, PKey: pkey})
	require.NoError(t, err)
	return string(b)
}

// A row whose text is not JSON is left out and recorded within the limit; one more fails the build.
func TestFulltext2CreateLeavesOutInvalidJSON(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	lookups := installOrdinaryMaintenanceRelations(t, proc)
	var sqls []string
	var policies []*executor.IndexRelationPolicy
	prev := fulltext2_runSql
	t.Cleanup(func() { fulltext2_runSql = prev })
	fulltext2_runSql = func(p *sqlexec.SqlProcess, sql string) (executor.Result, error) {
		sqls = append(sqls, sql)
		policies = append(policies, p.IndexRelationPolicy)
		return executor.Result{}, nil
	}

	cfg := ft2IgnoredCfg(t, "__ign", 1, "id")
	st := &fulltext2CreateState{}
	tf, _ := ft2CreateArgVecs(t, mp, cfg, 3, "{bad")
	t.Cleanup(func() { st.free(tf, proc, false, nil) })
	require.NoError(t, st.start(tf, proc, 0, nil))
	require.Equal(t, 3, *lookups, "the store, metadata and ignored tables are admitted")
	require.Equal(t, int64(1), st.ignored.spill.Len())
	admitted := st.policy

	require.NoError(t, st.end(tf, proc))
	joined := strings.Join(sqls, ";")
	require.Contains(t, joined, "REPLACE INTO `db`.`__ign`")
	require.Contains(t, joined, "(3, 20351, 'the text the index parses as JSON is not valid JSON: ")
	for i, sql := range sqls {
		if strings.Contains(sql, "`__ign`") {
			require.Same(t, admitted, policies[i], "the ignored rows are written under the admitted policy")
		}
	}

	tf2, _ := ft2CreateArgVecs(t, mp, cfg, 4, "[worse")
	err := st.start(tf2, proc, 0, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrIndexTooManyBadRecords), "%v", err)
	st.free(tf, proc, false, nil)
	require.Nil(t, st.ignored)
}

func TestFulltext2CreateWithoutIgnoredTable(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	installOrdinaryMaintenanceRelations(t, proc)
	st := &fulltext2CreateState{}
	tf, _ := ft2CreateArgVecs(t, mp, ft2IgnoredCfg(t, "", 0, "id"), 3, "{bad")
	err := st.start(tf, proc, 0, nil)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrIndexInvalidJSON), "%v", err)
}

// A composite primary key, the serial of its parts, is recorded in hex.
func TestFulltext2CreateLeaveOutCompositeKey(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
	st := &fulltext2CreateState{tblcfg: fulltext2.TableConfig{PKey: catalog.CPrimaryKeyColName}, ignored: newIgnoredBuild(st0Cfg(), "")}
	t.Cleanup(st.ignored.close)
	pk := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(pk, []byte{0x01, 0xff}, false, mp))
	tf := newFT2TF([]string{"status"}, ft2StatusRets())
	tf.ctr.argVecs = []*vector.Vector{nil, pk}
	require.NoError(t, st.leaveOut(tf, proc, 0, moerr.NewIndexInvalidJSONNoCtx("x")))
	require.Equal(t, "X'01FF'", st.ignored.firstPk)
	require.ErrorContains(t, st.leaveOut(tf, proc, 0, moerr.NewInternalErrorNoCtx("io")), "io", "not a reason a row cannot be stored")
}

func st0Cfg() vectorindex.IndexTableConfig {
	return vectorindex.IndexTableConfig{IgnoredTable: "__ign", MaxBadRecords: 5}
}
