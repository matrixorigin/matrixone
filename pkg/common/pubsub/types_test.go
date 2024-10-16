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
	"testing"
)

func TestPubInfo_InSubAccounts(t *testing.T) {
	type fields struct {
		PubName        string
		DbName         string
		DbId           uint64
		TablesStr      string
		SubAccountsStr string
		CreateTime     string
		UpdateTime     string
		Comment        string
	}
	type args struct {
		accountName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			fields: fields{SubAccountsStr: AccountAll},
			args:   args{accountName: "acc1"},
			want:   true,
		},
		{
			fields: fields{SubAccountsStr: "acc1,acc2,acc3"},
			args:   args{accountName: "acc1"},
			want:   true,
		},
		{
			fields: fields{SubAccountsStr: "acc2"},
			args:   args{accountName: "acc1"},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pubInfo := &PubInfo{
				PubName:        tt.fields.PubName,
				DbName:         tt.fields.DbName,
				DbId:           tt.fields.DbId,
				TablesStr:      tt.fields.TablesStr,
				SubAccountsStr: tt.fields.SubAccountsStr,
				CreateTime:     tt.fields.CreateTime,
				UpdateTime:     tt.fields.UpdateTime,
				Comment:        tt.fields.Comment,
			}
			if got := pubInfo.InSubAccounts(tt.args.accountName); got != tt.want {
				t.Errorf("InSubAccounts() = %v, want %v", got, tt.want)
			}
		})
	}
}
