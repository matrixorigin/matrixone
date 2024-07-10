// Copyright 2024 Matrix Origin
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

package testutil

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"testing"

	catalog2 "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func GetDefaultTestPath(module string, t *testing.T) string {
	usr, _ := user.Current()
	dirName := fmt.Sprintf("%s-ut-workspace", usr.Username)
	return filepath.Join("/tmp", dirName, module, t.Name())
}

func MakeDefaultTestPath(module string, t *testing.T) string {
	path := GetDefaultTestPath(module, t)
	err := os.MkdirAll(path, os.FileMode(0755))
	assert.Nil(t, err)
	return path
}

func RemoveDefaultTestPath(module string, t *testing.T) {
	path := GetDefaultTestPath(module, t)
	os.RemoveAll(path)
}

func InitTestEnv(module string, t *testing.T) string {
	RemoveDefaultTestPath(module, t)
	return MakeDefaultTestPath(module, t)
}

func CreateEngines(ctx context.Context, opts TestOptions,
	t *testing.T) (disttaeEngine *TestDisttaeEngine, taeEngine *TestTxnStorage,
	rpcAgent *MockRPCAgent, mp *mpool.MPool) {

	if v := ctx.Value(defines.TenantIDKey{}); v == nil {
		panic("cannot find account id in ctx")
	}

	var err error

	mp, err = mpool.NewMPool("test", 0, mpool.NoFixed)
	require.Nil(t, err)

	rpcAgent = NewMockLogtailAgent()

	taeEngine, err = NewTestTAEEngine(ctx, "partition_state", t, rpcAgent, opts.TaeEngineOptions)
	require.Nil(t, err)

	disttaeEngine, err = NewTestDisttaeEngine(ctx, mp, taeEngine.GetDB().Runtime.Fs.Service, rpcAgent, taeEngine)
	require.Nil(t, err)

	return
}

//func MockObjectStatsBatBySchema(tblDef *plan.TableDef, schema *catalog2.Schema,
//	eachRowCnt, batCnt int, mp *mpool.MPool, t *testing.T) (statsBat *batch.Batch) {
//
//	offset := 0
//	bats := make([]*batch.Batch, batCnt)
//	for idx := range bats {
//		ret := containers.MockBatchWithAttrsAndOffset(schema.Types(), schema.Attrs(), eachRowCnt, offset)
//
//		bat := containers.ToCNBatch(ret)
//		bats[idx] = bat
//
//		offset += eachRowCnt
//	}
//
//	proc := testutil2.NewProcessWithMPool(mp)
//	s3Writer, err := colexec.AllocS3Writer(proc, tblDef)
//	require.Nil(t, err)
//
//	s3Writer.InitBuffers(proc, bats[0])
//
//	for idx := range bats {
//		s3Writer.Put(bats[idx], proc)
//	}
//
//	err = s3Writer.SortAndFlush(proc)
//	require.Nil(t, err)
//
//	statsBat = s3Writer.GetBlockInfoBat()
//	require.Equal(t, statsBat.Attrs, []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats})
//	require.Equal(t, statsBat.Vecs[0].Length(), 100)
//	require.Equal(t, statsBat.Vecs[1].Length(), 100)
//	require.Equal(t, statsBat.Vecs[2].Length(), 1)
//
//	return
//}

func GetDefaultTNShard() metadata.TNShard {
	return metadata.TNShard{
		TNShardRecord: metadata.TNShardRecord{
			ShardID:    0,
			LogShardID: 1,
		},
		ReplicaID: 0x2f,
		Address:   "echo to test",
	}
}

func TableDefBySchema(schema *catalog.Schema) ([]engine.TableDef, error) {
	var defs = make([]engine.TableDef, 0)
	for idx := range schema.ColDefs {
		if schema.ColDefs[idx].Name == catalog2.Row_ID {
			continue
		}

		defs = append(defs, &engine.AttributeDef{
			Attr: engine.Attribute{
				Type:          schema.ColDefs[idx].Type,
				IsRowId:       schema.ColDefs[idx].Name == catalog2.Row_ID,
				Name:          schema.ColDefs[idx].Name,
				ID:            uint64(schema.ColDefs[idx].Idx),
				Primary:       schema.ColDefs[idx].IsPrimary(),
				IsHidden:      schema.ColDefs[idx].IsHidden(),
				Seqnum:        schema.ColDefs[idx].SeqNum,
				ClusterBy:     schema.ColDefs[idx].ClusterBy,
				AutoIncrement: schema.ColDefs[idx].AutoIncrement,
			},
		})
	}

	if schema.Constraint != nil {
		var con engine.ConstraintDef
		err := con.UnmarshalBinary(schema.Constraint)
		if err != nil {
			return nil, err
		}

		defs = append(defs, &con)
	}

	return defs, nil
}
