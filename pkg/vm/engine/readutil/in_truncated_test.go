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

package readutil

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type truncatedInCompilerContext struct {
	*plan2.MockCompilerContext
	table *plan.TableDef
}

func (c *truncatedInCompilerContext) Resolve(db, table string, snapshot *plan.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
	return &plan.ObjectRef{SchemaName: db, ObjName: c.table.Name, Obj: int64(c.table.TblId)}, plan2.DeepCopyTableDef(c.table, true), nil
}

// 真正的 parse/bind、行求值和对象持久化：不能以摘要前缀替代精确 IN 值。
func TestNonPrimaryBinaryInTruncatedZoneMap(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     []byte
		truncated bool
	}{
		{"overflow_prefix", bytes.Repeat([]byte{0xff}, 31), true},
		{"short_binary", bytes.Repeat([]byte{0xff}, 30), false},
		{"long_ascii", bytes.Repeat([]byte{'z'}, 31), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(proc.Mp())
				require.Zero(t, proc.Mp().CurrNB())
			})
			fs, err := fileservice.NewLocalFS2(proc.Ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(context.Background()) })
			table := &plan.TableDef{
				TblId: 42050, Name: "binary_in", Name2ColIndex: map[string]int32{"id": 0, "k": 1},
				Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
				Cols: []*plan.ColDef{
					{Name: "id", Seqnum: 0, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}},
					{Name: "k", Seqnum: 1, Typ: plan.Type{Id: int32(types.T_varbinary), Width: 128}},
				},
			}
			mock := plan2.NewMockCompilerContext(false)
			mock.SetContext(proc.Ctx)
			mock.GetProcessFunc = func() *process.Process { return proc }
			ctx := &truncatedInCompilerContext{MockCompilerContext: mock, table: table}
			sql := fmt.Sprintf("select k from binary_in where k in (cast(x'%s' as varbinary(128)),cast(x'00' as varbinary(128)))", hex.EncodeToString(tc.value))
			stmt, err := mysql.ParseOne(proc.Ctx, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := plan2.BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			var scan *plan.Node
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == plan.Node_TABLE_SCAN {
					require.Nil(t, scan)
					scan = node
				}
			}
			require.NotNil(t, scan)
			require.Len(t, scan.BlockFilterList, 1)
			require.Equal(t, "in", scan.BlockFilterList[0].GetF().Func.ObjName)

			input := batch.NewWithSize(2)
			defer input.Clean(proc.Mp())
			input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
			input.Vecs[1] = vector.NewVec(types.T_varbinary.ToType())
			require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(1), false, proc.Mp()))
			require.NoError(t, vector.AppendBytes(input.Vecs[1], tc.value, false, proc.Mp()))
			input.SetRowCount(1)
			rowExpr := plan2.DeepCopyExpr(scan.FilterList[0])
			require.NotNil(t, rowExpr.GetF().Args[0].GetCol())
			rowExpr.GetF().Args[0].GetCol().RelPos = 0
			rowExpr.GetF().Args[0].GetCol().ColPos = 1
			executor, err := colexec.NewExpressionExecutor(proc, rowExpr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.False(t, result.IsNull(0))
			require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, 0), "独立行求值必须命中")

			writer := ioutil.ConstructWriter(0, []uint16{0, 1}, 0, true, false, fs)
			_, err = writer.WriteBatch(input)
			require.NoError(t, err)
			_, _, err = writer.Sync(proc.Ctx)
			require.NoError(t, err)
			stats := writer.GetObjectStats(objectio.WithSorted())
			location := stats.ObjectLocation()
			meta, err := objectio.FastLoadObjectMeta(proc.Ctx, &location, false, fs)
			require.NoError(t, err)
			require.Equal(t, tc.truncated, meta.MustDataMeta().MustGetColumn(1).ZoneMap().MaxTruncated())
			require.Equal(t, tc.truncated, meta.MustDataMeta().GetBlockMeta(0).MustGetColumn(1).ZoneMap().MaxTruncated())
			var unfiltered objectio.BlockInfoSlice
			_, _, _, _, _, _, _, _, err = FilterObjects(proc.Ctx, engine.RangesParam{}, nil, nil, nil, nil, nil,
				nil, nil, []objectio.ObjectStats{stats}, &unfiltered, false, nil, fs)
			require.NoError(t, err)
			require.Equal(t, 1, unfiltered.Len())
			// reader 接收完整关系 schema，而不是投影裁剪后的 scan schema。
			fast, load, object, block, seek, ok, hint := CompileFilterExprs(scan.BlockFilterList, table, fs)
			require.True(t, ok)
			var selected objectio.BlockInfoSlice
			_, _, _, _, _, _, _, _, err = FilterObjects(proc.Ctx, engine.RangesParam{}, fast, load, object, block, seek,
				nil, nil, []objectio.ObjectStats{stats}, &selected, hint, nil, fs)
			require.NoError(t, err)
			require.Equal(t, 1, selected.Len(), "IN 准入不能排除行求值命中的持久化块")
		})
	}
}
