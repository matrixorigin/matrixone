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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
		one, err := parsers.ParseOne(context.Background(), dialect.MYSQL, a.s, 1, 0)
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
	accountInfo, err := newAccountInfo(ses.pool)
	assert.Nil(t, err)
	tableStatsResult, err := newTableStatsResult(ses.pool)
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
		vector.AppendFixed(bat.Vecs[i], float64(99), false, ses.pool)
	}

	size := uint64(1024 * 1024 * 11235)
	embeddingSizeToBatch(bat, size, ses.pool)

	require.Equal(t, math.Round(float64(size)/1048576.0*1e6)/1e6, vector.GetFixedAt[float64](bat.Vecs[idxOfSize], 0))
}
