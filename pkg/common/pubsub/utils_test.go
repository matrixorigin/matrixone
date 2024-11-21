// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
)

type MockTxnExecutor struct{}

func (MockTxnExecutor) Use(db string) {
	//TODO implement me
	panic("implement me")
}

func (MockTxnExecutor) LockTable(table string) error {
	//TODO implement me
	panic("implement me")
}

func (MockTxnExecutor) Exec(sql string, options executor.StatementOption) (executor.Result, error) {
	bat := batch.New([]string{"a", "b", "c", "d", "e"})
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil)
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"Name"}, nil)
	bat.Vecs[2] = testutil.MakeVarcharVector([]string{"Status"}, nil)
	bat.Vecs[3] = testutil.MakeUint64Vector([]uint64{1}, nil)
	bat.Vecs[4] = testutil.MakeScalarNull(types.T_timestamp, 1)
	bat.SetRowCount(1)
	return executor.Result{
		Batches: []*batch.Batch{bat},
		Mp:      testutil.TestUtilMp,
	}, nil
}

func (MockTxnExecutor) Txn() client.TxnOperator {
	//TODO implement me
	panic("implement me")
}

func TestAddSingleQuotesJoin(t *testing.T) {
	type args struct {
		s []string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			args: args{[]string{}},
			want: "",
		},
		{
			args: args{[]string{"acc1"}},
			want: "'acc1'",
		},
		{
			args: args{[]string{"acc1", "acc2", "acc3"}},
			want: "'acc1','acc2','acc3'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AddSingleQuotesJoin(tt.args.s); got != tt.want {
				t.Errorf("AddSingleQuotesJoin() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInSubMetaTables(t *testing.T) {
	type args struct {
		m         *plan.SubscriptionMeta
		tableName string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				m:         &plan.SubscriptionMeta{Tables: TableAll},
				tableName: "t1",
			},
			want: true,
		},
		{
			args: args{
				m:         &plan.SubscriptionMeta{Tables: "t1,t2,t3"},
				tableName: "t1",
			},
			want: true,
		},
		{
			args: args{
				m:         &plan.SubscriptionMeta{Tables: "t2,t3"},
				tableName: "t1",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InSubMetaTables(tt.args.m, tt.args.tableName); got != tt.want {
				t.Errorf("InSubMetaTables() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSplitAccounts(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name         string
		args         args
		wantAccounts []string
	}{
		{
			args:         args{""},
			wantAccounts: nil,
		},
		{
			args:         args{"acc1"},
			wantAccounts: []string{"acc1"},
		},
		{
			args:         args{"acc1,acc2,acc3"},
			wantAccounts: []string{"acc1", "acc2", "acc3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotAccounts := SplitAccounts(tt.args.s); !reflect.DeepEqual(gotAccounts, tt.wantAccounts) {
				t.Errorf("SplitAccounts() = %v, want %v", gotAccounts, tt.wantAccounts)
			}
		})
	}
}

func TestJoinAccounts(t *testing.T) {
	type args struct {
		accountMap map[int32]*AccountInfo
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			args: args{accountMap: map[int32]*AccountInfo{}},
			want: "",
		},
		{
			args: args{accountMap: map[int32]*AccountInfo{1: {Name: "acc1"}}},
			want: "acc1",
		},
		{
			args: args{accountMap: map[int32]*AccountInfo{1: {Name: "acc1"}, 2: {Name: "acc2"}, 3: {Name: "acc3"}}},
			want: "acc1,acc2,acc3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := JoinAccounts(tt.args.accountMap); got != tt.want {
				t.Errorf("JoinAccounts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCanPubToAll(t *testing.T) {
	type args struct {
		accountName    string
		pubAllAccounts string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				accountName:    "acc1",
				pubAllAccounts: "*",
			},
			want: true,
		},
		{
			args: args{
				accountName:    "acc1",
				pubAllAccounts: "acc1,acc2",
			},
			want: true,
		},
		{
			args: args{
				accountName:    "acc1",
				pubAllAccounts: "acc2",
			},
			want: false,
		},
		{
			args: args{
				accountName:    "acc1",
				pubAllAccounts: "",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CanPubToAll(tt.args.accountName, tt.args.pubAllAccounts); got != tt.want {
				t.Errorf("CanPubToAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRemoveTable(t *testing.T) {
	type args struct {
		oldTableListStr string
		tblName         string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			args: args{
				oldTableListStr: "*",
				tblName:         "t1",
			},
			want: "*",
		},
		{
			args: args{
				oldTableListStr: "t1,t2,t3",
				tblName:         "t1",
			},
			want: "t2,t3",
		},
		{
			args: args{
				oldTableListStr: "t1,t3,t2",
				tblName:         "t1",
			},
			want: "t2,t3",
		},
		{
			args: args{
				oldTableListStr: "t1",
				tblName:         "t1",
			},
			want: "",
		},
		{
			args: args{
				oldTableListStr: "t2",
				tblName:         "t1",
			},
			want: "t2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RemoveTable(tt.args.oldTableListStr, tt.args.tblName); got != tt.want {
				t.Errorf("RemoveTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJoinAccountIds(t *testing.T) {
	type args struct {
		accIds []int32
	}
	tests := []struct {
		name  string
		args  args
		wantS string
	}{
		{
			args: args{
				accIds: []int32{},
			},
			wantS: "",
		},
		{
			args: args{
				accIds: []int32{1},
			},
			wantS: "1",
		},

		{
			args: args{
				accIds: []int32{1, 2, 3},
			},
			wantS: "1,2,3",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotS := JoinAccountIds(tt.args.accIds); gotS != tt.wantS {
				t.Errorf("JoinAccountIds() = %v, want %v", gotS, tt.wantS)
			}
		})
	}
}

func TestGetAccounts(t *testing.T) {
	nameInfoMap, idInfoMap, err := GetAccounts(&MockTxnExecutor{})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(nameInfoMap))
	assert.Equal(t, 1, len(idInfoMap))
}
