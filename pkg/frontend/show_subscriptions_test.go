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
	"go/constant"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type mockedBackgroundHandler struct {
	currentSql            string
	accountIdNamesBatches []*batch.Batch
	pubBatches            map[int32][]*batch.Batch
	subBatches            []*batch.Batch
	accountId             int32
}

func (m *mockedBackgroundHandler) Close() {}

func (m *mockedBackgroundHandler) Exec(ctx context.Context, s string) error {
	m.currentSql = s
	m.accountId = int32(ctx.Value(defines.TenantIDKey{}).(uint32))
	return nil
}

func (m *mockedBackgroundHandler) ExecStmt(ctx context.Context, statement tree.Statement) error {
	return nil
}

func (m *mockedBackgroundHandler) ExecRestore(context.Context, string, uint32, uint32) error {
	panic("unimplement")
}

func (m *mockedBackgroundHandler) GetExecResultSet() []interface{} {
	return nil
}

func (m *mockedBackgroundHandler) ClearExecResultSet() {}

func (m *mockedBackgroundHandler) GetExecResultBatches() []*batch.Batch {
	if m.currentSql == getAccountIdNamesSql {
		return m.accountIdNamesBatches
	} else if strings.HasPrefix(m.currentSql, getPubsSql) {
		return m.pubBatches[m.accountId]
	} else {
		return m.subBatches
	}
}

func (m *mockedBackgroundHandler) ClearExecResultBatches() {}

func (m *mockedBackgroundHandler) init(mp *mpool.MPool) {
	m.pubBatches = make(map[int32][]*batch.Batch)

	var b *batch.Batch
	v1 := vector.NewVec(types.T_int32.ToType())
	_ = vector.AppendFixedList(v1, []int32{0, 1}, nil, mp)
	v2 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v2, []string{"sys", "account1"}, nil, mp)
	b = &batch.Batch{Vecs: []*vector.Vector{v1, v2}}
	b.SetRowCount(2)
	m.accountIdNamesBatches = []*batch.Batch{b}

	v3 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v3, []string{"pub1", "pub2"}, nil, mp)
	v4 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v4, []string{"db1", "db2"}, nil, mp)
	v12 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v12, []string{"all", "all"}, nil, mp)
	v5 := vector.NewVec(types.T_timestamp.ToType())
	_ = vector.AppendFixedList(v5, []types.Timestamp{1234567, 2234567}, nil, mp)
	b = &batch.Batch{Vecs: []*vector.Vector{v3, v4, v12, v5}}
	b.SetRowCount(2)
	m.pubBatches[0] = []*batch.Batch{b}

	v6 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v6, []string{"pub3", "pub4"}, nil, mp)
	v7 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v7, []string{"db3", "db4"}, nil, mp)
	v13 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v13, []string{"account2", "all"}, nil, mp)
	v8 := vector.NewVec(types.T_timestamp.ToType())
	_ = vector.AppendFixedList(v8, []types.Timestamp{3234567, 4234567}, nil, mp)
	b = &batch.Batch{Vecs: []*vector.Vector{v6, v7, v13, v8}}
	b.SetRowCount(2)
	m.pubBatches[1] = []*batch.Batch{b}

	v9 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v9, []string{"sub1", "sub4"}, nil, mp)
	v10 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v10, []string{
		"create database sub1 from sys publication pub1",
		"create database sub4 from account1 publication pub4",
	}, nil, mp)
	v11 := vector.NewVec(types.T_timestamp.ToType())
	_ = vector.AppendFixedList(v11, []types.Timestamp{1234568, 4234568}, nil, mp)
	b = &batch.Batch{Vecs: []*vector.Vector{v9, v10, v11}}
	b.SetRowCount(2)
	m.subBatches = []*batch.Batch{b}
}

func (m *mockedBackgroundHandler) initLike(mp *mpool.MPool) {
	m.pubBatches = make(map[int32][]*batch.Batch)

	var b *batch.Batch
	v1 := vector.NewVec(types.T_int32.ToType())
	_ = vector.AppendFixedList(v1, []int32{0, 1}, nil, mp)
	v2 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v2, []string{"sys", "account1"}, nil, mp)
	b = &batch.Batch{Vecs: []*vector.Vector{v1, v2}}
	b.SetRowCount(2)
	m.accountIdNamesBatches = []*batch.Batch{b}

	v3 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v3, []string{"pub1"}, nil, mp)
	v4 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v4, []string{"db1"}, nil, mp)
	v12 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v12, []string{"all"}, nil, mp)
	v5 := vector.NewVec(types.T_timestamp.ToType())
	_ = vector.AppendFixedList(v5, []types.Timestamp{1234567}, nil, mp)
	b = &batch.Batch{Vecs: []*vector.Vector{v3, v4, v12, v5}}
	b.SetRowCount(1)
	m.pubBatches[0] = []*batch.Batch{b}

	v6 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v6, []string{}, nil, mp)
	v7 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v7, []string{}, nil, mp)
	v13 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v13, []string{}, nil, mp)
	v8 := vector.NewVec(types.T_timestamp.ToType())
	_ = vector.AppendFixedList(v8, []types.Timestamp{}, nil, mp)
	b = &batch.Batch{Vecs: []*vector.Vector{v6, v7, v13, v8}}
	b.SetRowCount(0)
	m.pubBatches[1] = []*batch.Batch{b}

	v9 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v9, []string{"sub1", "sub4"}, nil, mp)
	v10 := vector.NewVec(types.T_varchar.ToType())
	_ = vector.AppendStringList(v10, []string{
		"create database sub1 from sys publication pub1",
		"create database sub4 from account1 publication pub4",
	}, nil, mp)
	v11 := vector.NewVec(types.T_timestamp.ToType())
	_ = vector.AppendFixedList(v11, []types.Timestamp{1234568, 4234568}, nil, mp)
	b = &batch.Batch{Vecs: []*vector.Vector{v9, v10, v11}}
	b.SetRowCount(2)
	m.subBatches = []*batch.Batch{b}
}

func (m *mockedBackgroundHandler) Clear() {}

var _ BackgroundExec = &mockedBackgroundHandler{}

var bh = &mockedBackgroundHandler{}

var tenant = &TenantInfo{
	Tenant:        sysAccountName,
	User:          rootName,
	DefaultRole:   moAdminRoleName,
	TenantID:      sysAccountID,
	UserID:        rootID,
	DefaultRoleID: moAdminRoleID,
}

func TestDoShowSubscriptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

	ses := newTestSession(t, ctrl)
	ses.SetTimeZone(time.UTC)
	ses.SetTenantInfo(tenant)
	ses.SetMysqlResultSet(&MysqlResultSet{})
	defer ses.Close()

	mp := ses.GetMemPool()
	bh.init(mp)
	sa := &tree.ShowSubscriptions{}

	bhStub := gostub.StubFunc(&GetRawBatchBackgroundExec, bh)
	defer bhStub.Reset()

	err := doShowSubscriptions(ctx, ses, sa)
	require.NoError(t, err)

	rs := ses.GetMysqlResultSet()
	require.Equal(t, uint64(len(showSubscriptionOutputColumns)), rs.GetColumnCount())
	require.Equal(t, uint64(2), rs.GetRowCount())

	var actual, expected []interface{}
	// sort by sub_time, pub_time desc
	expected = []interface{}{"pub4", "account1", "db4", "0001-01-01 00:00:04", "sub4", "0001-01-01 00:00:04"}
	actual, err = rs.GetRow(ctx, uint64(0))
	require.NoError(t, err)
	require.Equal(t, expected, actual)

	expected = []interface{}{"pub1", "sys", "db1", "0001-01-01 00:00:01", "sub1", "0001-01-01 00:00:01"}
	actual, err = rs.GetRow(ctx, uint64(1))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestDoShowSubscriptionsAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

	ses := newTestSession(t, ctrl)
	ses.SetTimeZone(time.UTC)
	ses.SetTenantInfo(tenant)
	ses.SetMysqlResultSet(&MysqlResultSet{})
	defer ses.Close()

	mp := ses.GetMemPool()
	bh.init(mp)
	sa := &tree.ShowSubscriptions{All: true}

	bhStub := gostub.StubFunc(&GetRawBatchBackgroundExec, bh)
	defer bhStub.Reset()

	err := doShowSubscriptions(ctx, ses, sa)
	require.NoError(t, err)

	rs := ses.GetMysqlResultSet()
	require.Equal(t, uint64(len(showSubscriptionOutputColumns)), rs.GetColumnCount())
	require.Equal(t, uint64(3), rs.GetRowCount())

	var actual, expected []interface{}
	// sort by sub_time, pub_time desc
	expected = []interface{}{"pub4", "account1", "db4", "0001-01-01 00:00:04", "sub4", "0001-01-01 00:00:04"}
	actual, err = rs.GetRow(ctx, uint64(0))
	require.NoError(t, err)
	require.Equal(t, expected, actual)

	expected = []interface{}{"pub1", "sys", "db1", "0001-01-01 00:00:01", "sub1", "0001-01-01 00:00:01"}
	actual, err = rs.GetRow(ctx, uint64(1))
	require.NoError(t, err)
	require.Equal(t, expected, actual)

	expected = []interface{}{"pub2", "sys", "db2", "0001-01-01 00:00:02", nil, nil}
	actual, err = rs.GetRow(ctx, uint64(2))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestDoShowSubscriptionsAllLike(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))

	ses := newTestSession(t, ctrl)
	ses.SetTimeZone(time.UTC)
	ses.SetTenantInfo(tenant)
	ses.SetMysqlResultSet(&MysqlResultSet{})
	defer ses.Close()

	mp := ses.GetMemPool()
	bh.initLike(mp)
	sa := &tree.ShowSubscriptions{
		All: true,
		Like: &tree.ComparisonExpr{
			Op:    tree.LIKE,
			Right: &tree.NumVal{Value: constant.MakeString("%1")},
		},
	}

	bhStub := gostub.StubFunc(&GetRawBatchBackgroundExec, bh)
	defer bhStub.Reset()

	err := doShowSubscriptions(ctx, ses, sa)
	require.NoError(t, err)

	rs := ses.GetMysqlResultSet()
	require.Equal(t, uint64(len(showSubscriptionOutputColumns)), rs.GetColumnCount())
	require.Equal(t, uint64(1), rs.GetRowCount())

	var actual, expected []interface{}
	expected = []interface{}{"pub1", "sys", "db1", "0001-01-01 00:00:01", "sub1", "0001-01-01 00:00:01"}
	actual, err = rs.GetRow(ctx, uint64(0))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
