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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

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
