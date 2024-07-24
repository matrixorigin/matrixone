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
