// Copyright 2025 Matrix Origin
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

package compile

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestIndexMetadataType(t *testing.T) {
	require.Equal(t, INDEX_TYPE_UNIQUE, indexMetadataType(true, ""))
	require.Equal(t, INDEX_TYPE_MULTIPLE, indexMetadataType(false, ""))
	require.Equal(t, INDEX_TYPE_SPATIAL, indexMetadataType(false, catalog.MoIndexRTreeAlgo.ToString()))
	require.Equal(t, INDEX_TYPE_FULLTEXT, indexMetadataType(false, catalog.MOIndexFullTextAlgo.ToString()))
}

func TestGenInsertIndexTableSql_QuotesReservedKeywordColumn(t *testing.T) {
	originTableDef := &planpb.TableDef{
		Name: "files_related_mph",
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}
	indexDef := &planpb.IndexDef{
		IndexTableName: "__mo_index_secondary_test",
		Parts:          []string{"order", catalog.CreateAlias("id")},
		Unique:         false,
	}

	sql := genInsertIndexTableSql(originTableDef, indexDef, "test", false)

	require.Contains(t, sql, "serial_full(`order`,`id`)")
	require.Contains(t, sql, "select serial_full(`order`,`id`), `id` from `test`.`files_related_mph`;")
}

func TestGenInsertIndexTableSql_SpatialIndexUsesRawGeometryColumn(t *testing.T) {
	originTableDef := &planpb.TableDef{
		Name: "geo_t",
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "id",
			Names:       []string{"id"},
		},
	}
	indexDef := &planpb.IndexDef{
		IndexTableName: "__mo_index_secondary_spatial",
		Parts:          []string{"g", catalog.CreateAlias("id")},
		Unique:         false,
		IndexAlgo:      "rtree",
	}

	sql := genInsertIndexTableSql(originTableDef, indexDef, "test", false)

	require.Contains(t, sql, "select (`g`), `id` from `test`.`geo_t` where (`g`) is not null;")
	require.NotContains(t, sql, "serial_full")
}

func TestBuildCreateUniqueIndexDuplicateCheckSQL(t *testing.T) {
	tableDef := &planpb.TableDef{Name: "orders"}

	t.Run("single column", func(t *testing.T) {
		indexDef := &planpb.IndexDef{
			IndexName:      "u_order",
			IndexTableName: "__mo_index_unique_test",
			Parts:          []string{"order"},
			Unique:         true,
		}

		sql := buildCreateUniqueIndexDuplicateCheckSQL("test", tableDef, indexDef)

		require.Equal(t, "SELECT `order` FROM `test`.`orders` WHERE `order` IS NOT NULL GROUP BY `order` HAVING count(*) > 1 LIMIT 1", sql)
	})

	t.Run("compound column", func(t *testing.T) {
		indexDef := &planpb.IndexDef{
			IndexName:      "u_order_id",
			IndexTableName: "__mo_index_unique_test",
			Parts:          []string{"order", catalog.CreateAlias("id")},
			Unique:         true,
		}

		sql := buildCreateUniqueIndexDuplicateCheckSQL("test", tableDef, indexDef)

		require.Equal(t, "SELECT `order`,`id` FROM `test`.`orders` WHERE `order` IS NOT NULL AND `id` IS NOT NULL GROUP BY `order`,`id` HAVING count(*) > 1 LIMIT 1", sql)
	})
}

func TestPrecheckAndInsertUniqueIndexTableUsesSkipDedupAndPipelineFlush(t *testing.T) {
	proc := testutil.NewProcess(t)
	topCtx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.ReplaceTopCtx(topCtx)
	proc.Ctx = nil

	const (
		insertSQL = "insert into  `test`.`__mo_index_unique_test` select (`order`), `id` from `test`.`orders` where (`order`) is not null;"
		checkSQL  = "SELECT `order` FROM `test`.`orders` WHERE `order` IS NOT NULL GROUP BY `order` HAVING count(*) > 1 LIMIT 1"
	)
	insertErr := errors.New("stop after insert")
	spyExec := &alterCopyInsertSpyExecutor{
		insertSQL: insertSQL,
		insertErr: insertErr,
	}
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)

	c := &Compile{proc: proc, pn: &planpb.Plan{}}
	tableDef := &planpb.TableDef{Name: "orders"}
	indexDef := &planpb.IndexDef{
		IndexName:      "u_order",
		IndexTableName: "__mo_index_unique_test",
		Parts:          []string{"order"},
		Unique:         true,
	}

	err := c.precheckAndInsertUniqueIndexTable("test", tableDef, indexDef, insertSQL)
	require.ErrorIs(t, err, insertErr)
	require.Equal(t, []string{checkSQL, insertSQL}, spyExec.executedSQLs)

	require.NotNil(t, spyExec.insertCtx)
	require.Equal(t, true, spyExec.insertCtx.Value(ioutil.PipelineFlushKey) == true)
	accountID, err := defines.GetAccountId(spyExec.insertCtx)
	require.NoError(t, err)
	require.Equal(t, uint32(catalog.System_Account), accountID)

	opt := spyExec.insertOption.AlterCopyDedupOpt()
	require.NotNil(t, opt)
	require.True(t, opt.SkipPkDedup)
	require.Equal(t, indexDef.IndexTableName, opt.TargetTableName)

	require.Same(t, topCtx, proc.Ctx)
	require.NotEqual(t, true, proc.Ctx.Value(ioutil.PipelineFlushKey))
}

func TestPrecheckAndInsertUniqueIndexTableRejectsDuplicateBeforeInsert(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.ReplaceTopCtx(ctx)
	proc.Ctx = ctx

	const (
		insertSQL = "insert into  `test`.`__mo_index_unique_test` select (`order`), `id` from `test`.`orders` where (`order`) is not null;"
		checkSQL  = "SELECT `order` FROM `test`.`orders` WHERE `order` IS NOT NULL GROUP BY `order` HAVING count(*) > 1 LIMIT 1"
	)
	spyExec := &alterCopyInsertSpyExecutor{}
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)

	c := &Compile{proc: proc, pn: &planpb.Plan{}}
	spyExec.results = map[string]executor.Result{
		checkSQL: newAlterCopyFixedResult(t, c.proc.Mp(), types.T_int32.ToType(), []int32{7}),
	}

	tableDef := &planpb.TableDef{Name: "orders"}
	indexDef := &planpb.IndexDef{
		IndexName:      "u_order",
		IndexTableName: "__mo_index_unique_test",
		Parts:          []string{"order"},
		Unique:         true,
	}

	err := c.precheckAndInsertUniqueIndexTable("test", tableDef, indexDef, insertSQL)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	require.Contains(t, err.Error(), "Duplicate entry '7' for key '__mo_index_idx_col'")
	require.Equal(t, []string{checkSQL}, spyExec.executedSQLs)
	require.Nil(t, spyExec.insertCtx)
}

func TestPrecheckAndInsertUniqueIndexTableFormatsCompoundDuplicate(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.ReplaceTopCtx(ctx)
	proc.Ctx = ctx

	const (
		insertSQL = "insert into  `test`.`__mo_index_unique_test` select serial(`order`,`id`), `pk` from `test`.`orders` where serial(`order`,`id`) is not null;"
		checkSQL  = "SELECT `order`,`id` FROM `test`.`orders` WHERE `order` IS NOT NULL AND `id` IS NOT NULL GROUP BY `order`,`id` HAVING count(*) > 1 LIMIT 1"
	)
	spyExec := &alterCopyInsertSpyExecutor{}
	moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)

	c := &Compile{proc: proc, pn: &planpb.Plan{}}
	memRes := executor.NewMemResult([]types.Type{types.T_int32.ToType(), types.T_int32.ToType()}, c.proc.Mp())
	memRes.NewBatchWithRowCount(1)
	require.NoError(t, executor.AppendFixedRows(memRes, 0, []int32{1}))
	require.NoError(t, executor.AppendFixedRows(memRes, 1, []int32{2}))
	spyExec.results = map[string]executor.Result{
		checkSQL: memRes.GetResult(),
	}

	tableDef := &planpb.TableDef{Name: "orders"}
	indexDef := &planpb.IndexDef{
		IndexName:      "u_order_id",
		IndexTableName: "__mo_index_unique_test",
		Parts:          []string{"order", catalog.CreateAlias("id")},
		Unique:         true,
	}

	err := c.precheckAndInsertUniqueIndexTable("test", tableDef, indexDef, insertSQL)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	require.Contains(t, err.Error(), "Duplicate entry '(1,2)' for key '__mo_index_idx_col'")
	require.Equal(t, []string{checkSQL}, spyExec.executedSQLs)
	require.Nil(t, spyExec.insertCtx)
}
