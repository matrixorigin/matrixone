// Copyright 2021 Matrix Origin
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

package frontend

import (
	"context"
	"math"
	"os"
	"path"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_getSqlForAccountInfo(t *testing.T) {
	type arg struct {
		s    string
		want string
	}
	args := []arg{
		{
			s:    "show accounts;",
			want: "select account_id as `account_id`, account_name as `account_name`, created_time as `created`, status as `status`, suspended_time as `suspended_time`, comments as `comment` from mo_catalog.mo_account ;",
		},
		{
			s:    "show accounts like '%abc';",
			want: "select account_id as `account_id`, account_name as `account_name`, created_time as `created`, status as `status`, suspended_time as `suspended_time`, comments as `comment` from mo_catalog.mo_account where account_name like '%abc';",
		},
	}

	for _, a := range args {
		one, err := parsers.ParseOne(context.Background(), dialect.MYSQL, a.s, 1)
		assert.NoError(t, err)
		sa1 := one.(*tree.ShowAccounts)
		r1 := getSqlForAllAccountInfo(sa1.Like)
		assert.Equal(t, a.want, r1)
	}
}

func newAccountInfo(mp *mpool.MPool) (*batch.Batch, error) {
	var err error
	ret := batch.NewWithSize(idxOfComment + 1)
	ret.Vecs[idxOfAccountId] = vector.NewVec(types.New(types.T_int32, 32, -1))
	err = vector.AppendAny(ret.Vecs[idxOfAccountId], int32(0), false, mp)
	if err != nil {
		return nil, err
	}
	ret.Vecs[idxOfAccountName] = vector.NewVec(types.New(types.T_varchar, 300, 0))
	err = vector.AppendAny(ret.Vecs[idxOfAccountName], []byte("acc"), false, mp)
	if err != nil {
		return nil, err
	}
	ret.Vecs[idxOfCreated] = vector.NewVec(types.New(types.T_timestamp, 8, 0))
	err = vector.AppendAny(ret.Vecs[idxOfCreated], types.Timestamp(0), false, mp)
	if err != nil {
		return nil, err
	}
	ret.Vecs[idxOfStatus] = vector.NewVec(types.New(types.T_varchar, 300, 0))
	err = vector.AppendAny(ret.Vecs[idxOfStatus], []byte("status"), false, mp)
	if err != nil {
		return nil, err
	}
	ret.Vecs[idxOfSuspendedTime] = vector.NewVec(types.New(types.T_timestamp, 8, 0))
	err = vector.AppendAny(ret.Vecs[idxOfSuspendedTime], types.Timestamp(0), false, mp)
	if err != nil {
		return nil, err
	}
	ret.Vecs[idxOfComment] = vector.NewVec(types.New(types.T_varchar, 256, 0))
	err = vector.AppendAny(ret.Vecs[idxOfComment], []byte("comment"), false, mp)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func newTableStatsResult(mp *mpool.MPool) (*batch.Batch, error) {
	var err error
	ret := batch.NewWithSize(idxOfComment + 1)
	ret.Vecs[idxOfAdminName] = vector.NewVec(types.New(types.T_varchar, 300, 0))
	err = vector.AppendAny(ret.Vecs[idxOfAdminName], []byte("name"), false, mp)
	if err != nil {
		return nil, err
	}
	ret.Vecs[idxOfDBCount] = vector.NewVec(types.New(types.T_int64, 8, 0))
	err = vector.AppendAny(ret.Vecs[idxOfDBCount], int64(0), false, mp)
	if err != nil {
		return nil, err
	}
	ret.Vecs[idxOfTableCount] = vector.NewVec(types.New(types.T_int64, 8, 0))
	err = vector.AppendAny(ret.Vecs[idxOfTableCount], int64(0), false, mp)
	if err != nil {
		return nil, err
	}
	ret.Vecs[idxOfSize] = vector.NewVec(types.New(types.T_decimal128, 29, 3))
	err = vector.AppendAny(ret.Vecs[idxOfSize], types.Decimal128{}, false, mp)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func Test_mergeResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	outputBatch := batch.NewWithSize(finalColumnCount)
	accountInfo, err := newAccountInfo(ses.mp)
	assert.Nil(t, err)
	tableStatsResult, err := newTableStatsResult(ses.mp)
	assert.Nil(t, err)

	err = mergeOutputResult(ses, outputBatch, accountInfo, []*batch.Batch{tableStatsResult})
	assert.Nil(t, err)
}

func Test_embeddingSizeToBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ses := newTestSession(t, ctrl)
	defer ses.Close()

	bat := &batch.Batch{}
	for i := 0; i <= finalColumnCount; i++ {
		bat.Vecs = append(bat.Vecs, vector.NewVec(types.T_float64.ToType()))
		vector.AppendFixed(bat.Vecs[i], float64(99), false, ses.mp)
	}

	size := uint64(1024 * 1024 * 11235)
	embeddingSizeToBatch(bat, size, ses.mp)

	require.Equal(t, math.Round(float64(size)/1048576.0*1e6)/1e6, vector.GetFixedAt[float64](bat.Vecs[idxOfSize], 0))
}

func mockCheckpointData(t *testing.T, accIds []uint64, sizes []uint64) *logtail.CheckpointData {
	ckpData := logtail.NewCheckpointData(common.CheckpointAllocator)
	storageUsageBat := ckpData.GetBatches()[logtail.SEGStorageUsageIDX]

	accVec := storageUsageBat.GetVectorByName(catalog.SystemColAttr_AccID).GetDownstreamVector()
	sizeVec := storageUsageBat.GetVectorByName(logtail.CheckpointMetaAttr_ObjectSize).GetDownstreamVector()

	for idx := range accIds {
		vector.AppendFixed(accVec, accIds[idx], false, ckpData.Allocator())
		vector.AppendFixed(sizeVec, sizes[idx], false, ckpData.Allocator())
	}

	return ckpData
}

func mockObjectFileService(ctx context.Context, dirName string) *objectio.ObjectFS {
	fs := objectio.TmpNewFileservice(ctx, path.Join(dirName, "data"))
	serviceDir := path.Join(dirName, "data")
	return objectio.NewObjectFS(fs, serviceDir)
}

// test plan
// 1. mock a checkpoint and write it down
// 2. mock a storage usage response
// 3. handle this response
func Test_ShowAccounts(t *testing.T) {
	ctx := context.Background()
	accIds := []uint64{0, 1, 2, 3, 4}
	sizes := []uint64{0, 1024, 2048, 4096, 8192}

	ckpData := mockCheckpointData(t, accIds, sizes)
	defer ckpData.Close()

	dirName := "show_account_test"
	objFs := mockObjectFileService(ctx, dirName)
	cnLocation, _, _, err := ckpData.WriteTo(objFs.Service, logtail.DefaultCheckpointBlockRows, logtail.DefaultCheckpointSize)
	require.Nil(t, err)

	defer func() {
		err = os.RemoveAll(dirName)
		require.Nil(t, err)
	}()

	resp := &db.StorageUsageResp{}
	resp.CkpEntries = append(resp.CkpEntries, &db.CkpMetaInfo{
		Location: cnLocation,
		Version:  9,
	})

	usage, err := handleStorageUsageResponse(ctx, resp, objFs.Service)
	require.Nil(t, err)

	require.Equal(t, len(usage), len(accIds))

	for idx := range accIds {
		require.Equal(t, sizes[idx], usage[int32(accIds[idx])])
	}

}
