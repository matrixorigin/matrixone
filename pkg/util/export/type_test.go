// Copyright 2022 Matrix Origin
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

package export

import (
	"context"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
)

type dummyContextKey int

var dummyCKKey = dummyContextKey(0)

func TestDefaultContext(t *testing.T) {
	tests := []struct {
		name string
		f    getContextFunc
		want context.Context
	}{
		{
			name: "normal",
			want: context.Background(),
		},
		{
			name: "set",
			f: func() context.Context {
				return context.WithValue(context.Background(), dummyCKKey, "val")
			},
			want: context.WithValue(context.Background(), dummyCKKey, "val"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.f != nil {
				SetDefaultContextFunc(tt.f)
			}
			if got := DefaultContext(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DefaultContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetGlobalBatchProcessor(t *testing.T) {
	tests := []struct {
		name string
		want BatchProcessor
	}{
		{
			name: "normal",
			want: &noopBatchProcessor{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetGlobalBatchProcessor(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetGlobalBatchProcessor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegister(t *testing.T) {
	type args struct {
		name batchpipe.HasName
		impl batchpipe.PipeImpl[batchpipe.HasName, any]
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{
				name: newDummy(1),
				impl: &dummyPipeImpl{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Register(tt.args.name, tt.args.impl)
		})
	}
}

func TestString2Bytes(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		wantRet []byte
	}{
		{
			name: "normal",
			args: args{
				s: "12345a",
			},
			wantRet: []byte{'1', '2', '3', '4', '5', 'a'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotRet := String2Bytes(tt.args.s); !reflect.DeepEqual(gotRet, tt.wantRet) {
				t.Errorf("String2Bytes() = %v, want %v", gotRet, tt.wantRet)
			}
		})
	}
}

func TestPathBuilder(t *testing.T) {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
	type field struct {
		builder PathBuilder
	}
	type args struct {
		account  string
		typ      MergeLogType
		ts       time.Time
		db       string
		name     string
		nodeUUID string
		nodeType string
	}
	tests := []struct {
		name        string
		field       field
		args        args
		wantDir     string
		wantETLPath string
		wantLogFN   string
	}{
		{
			name:  "db_tbl",
			field: field{builder: NewDBTablePathBuilder()},
			args: args{
				account:  "user",
				typ:      MergeLogTypeLogs,
				ts:       time.Unix(0, 0),
				db:       "db",
				name:     "table",
				nodeUUID: "123456",
				nodeType: "node",
			},
			wantDir:     `db`,
			wantETLPath: `db/table_*.csv`,
			wantLogFN:   `table_123456_node_19700101.000000.000000.csv`,
		},
		{
			name:  "metric_log",
			field: field{builder: NewAccountDatePathBuilder()},
			args: args{
				account:  "user",
				typ:      MergeLogTypeLogs,
				ts:       time.Unix(0, 0),
				db:       "db",
				name:     "table",
				nodeUUID: "123456",
				nodeType: "node",
			},
			wantDir:     `user/logs` + `/1970/01/01` + `/table`,
			wantETLPath: `/*/*` + `/*/*/*` + `/table/*.csv`,
			wantLogFN:   `0_123456_node.csv`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := tt.field.builder
			gotDir := m.Build(tt.args.account, tt.args.typ, tt.args.ts, tt.args.db, tt.args.name)
			require.Equal(t, tt.wantDir, gotDir)
			gotETLPath := m.BuildETLPath(tt.args.db, tt.args.name, ETLParamAccountAll)
			require.Equal(t, tt.wantETLPath, gotETLPath)
			gotLogFN := m.NewLogFilename(tt.args.name, tt.args.nodeUUID, tt.args.nodeType, tt.args.ts)
			require.Equal(t, tt.wantLogFN, gotLogFN)
		})
	}
}
