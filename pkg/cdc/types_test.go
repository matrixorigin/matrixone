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

package cdc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestNewAtomicBatch(t *testing.T) {
	fromTs := types.BuildTS(1, 1)
	toTs := types.BuildTS(2, 1)
	wanted := &AtomicBatch{
		Mp:   nil,
		From: fromTs,
		To:   toTs,
		Rows: btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
	}
	actual := NewAtomicBatch(nil, fromTs, toTs)
	assert.Equal(t, wanted.From, actual.From)
	assert.Equal(t, wanted.To, actual.To)
	assert.NotNil(t, actual.Rows)
	assert.Equal(t, 0, actual.Rows.Len())
}

func TestAtomicBatch_Append(t *testing.T) {
	atomicBat := &AtomicBatch{
		From:    types.BuildTS(1, 1),
		To:      types.BuildTS(2, 1),
		Batches: []*batch.Batch{},
		Rows:    btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
	}
	bat := batch.New(true, []string{"pk", "ts"})
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil)
	bat.Vecs[1] = testutil.MakeTSVector([]types.TS{types.BuildTS(1, 1)}, nil)

	atomicBat.Append(types.NewPacker(), bat, 1, 0)
	assert.Equal(t, 1, len(atomicBat.Batches))
	assert.Equal(t, 1, atomicBat.Rows.Len())
}

func TestAtomicBatch_Close(t *testing.T) {
	type fields struct {
		Mp      *mpool.MPool
		From    types.TS
		To      types.TS
		Batches []*batch.Batch
		Rows    *btree.BTreeG[AtomicBatchRow]
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			fields: fields{
				Batches: []*batch.Batch{batch.New(false, []string{"attr1"})},
				Rows:    btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bat := &AtomicBatch{
				Mp:      tt.fields.Mp,
				From:    tt.fields.From,
				To:      tt.fields.To,
				Batches: tt.fields.Batches,
				Rows:    tt.fields.Rows,
			}
			bat.Close()
		})
	}
}

func TestAtomicBatch_GetRowIterator(t *testing.T) {
	type fields struct {
		Mp      *mpool.MPool
		From    types.TS
		To      types.TS
		Batches []*batch.Batch
		Rows    *btree.BTreeG[AtomicBatchRow]
	}
	tests := []struct {
		name   string
		fields fields
		want   RowIterator
	}{
		{
			fields: fields{
				Rows: btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
			},
			want: &atomicBatchRowIter{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bat := &AtomicBatch{
				Mp:      tt.fields.Mp,
				From:    tt.fields.From,
				To:      tt.fields.To,
				Batches: tt.fields.Batches,
				Rows:    tt.fields.Rows,
			}
			bat.GetRowIterator()
		})
	}
}

func TestAtomicBatchRow_Less(t *testing.T) {
	t1 := types.BuildTS(1, 1)
	t2 := types.BuildTS(2, 1)

	type fields struct {
		Ts     types.TS
		Pk     []byte
		Offset int
		Src    *batch.Batch
	}
	type args struct {
		other AtomicBatchRow
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			fields: fields{Ts: t1},
			args:   args{other: AtomicBatchRow{Ts: t2}},
			want:   true,
		},
		{
			fields: fields{Ts: t2},
			args:   args{other: AtomicBatchRow{Ts: t1}},
			want:   false,
		},
		{
			fields: fields{Ts: t1, Pk: []byte{1}},
			args:   args{other: AtomicBatchRow{Ts: t1, Pk: []byte{2}}},
			want:   true,
		},
		{
			fields: fields{Ts: t1, Pk: []byte{2}},
			args:   args{other: AtomicBatchRow{Ts: t1, Pk: []byte{1}}},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := AtomicBatchRow{
				Ts:     tt.fields.Ts,
				Pk:     tt.fields.Pk,
				Offset: tt.fields.Offset,
				Src:    tt.fields.Src,
			}
			assert.Equalf(t, tt.want, row.Less(tt.args.other), "Less(%v)", tt.args.other)
		})
	}
}

func Test_atomicBatchRowIter(t *testing.T) {
	rows := btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64})
	row1 := AtomicBatchRow{Ts: types.BuildTS(1, 1), Pk: []byte{1}}
	row2 := AtomicBatchRow{Ts: types.BuildTS(2, 1), Pk: []byte{2}}
	row3 := AtomicBatchRow{Ts: types.BuildTS(3, 1), Pk: []byte{3}}
	rows.Set(row1)
	rows.Set(row2)
	rows.Set(row3)

	// at init position (before the first row)
	iter := &atomicBatchRowIter{
		iter:     rows.Iter(),
		initIter: rows.Iter(),
	}

	// first row
	iter.Next()
	item := iter.Item()
	assert.Equal(t, row1, item)
	err := iter.Row(context.Background(), []any{})
	assert.NoError(t, err)

	// first row can't go back
	ok := iter.Prev()
	assert.Equal(t, false, ok)

	// first row -> second row -> first row
	iter.Next()
	item = iter.Item()
	assert.Equal(t, row2, item)
	iter.Prev()
	item = iter.Item()
	assert.Equal(t, row1, item)

	// reset position
	iter.Reset()
	iter.Next()
	item = iter.Item()
	assert.Equal(t, row1, item)

	err = iter.Close()
	assert.NoError(t, err)
}

func TestDbTableInfo_String(t *testing.T) {
	type fields struct {
		SourceAccountName string
		SourceDbName      string
		SourceTblName     string
		SourceAccountId   uint64
		SourceDbId        uint64
		SourceTblId       uint64
		SinkAccountName   string
		SinkDbName        string
		SinkTblName       string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				SourceDbName:  "source_db",
				SourceDbId:    1,
				SourceTblName: "source_tbl",
				SourceTblId:   1,
				SinkDbName:    "sink_db",
				SinkTblName:   "sink_tbl",
			},
			want: "source_db(1).source_tbl(1) -> sink_db.sink_tbl",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := DbTableInfo{
				SourceAccountName: tt.fields.SourceAccountName,
				SourceDbName:      tt.fields.SourceDbName,
				SourceTblName:     tt.fields.SourceTblName,
				SourceAccountId:   tt.fields.SourceAccountId,
				SourceDbId:        tt.fields.SourceDbId,
				SourceTblId:       tt.fields.SourceTblId,
				SinkAccountName:   tt.fields.SinkAccountName,
				SinkDbName:        tt.fields.SinkDbName,
				SinkTblName:       tt.fields.SinkTblName,
			}
			assert.Equalf(t, tt.want, info.String(), "String()")
		})
	}
}

func TestJsonDecode(t *testing.T) {
	// TODO
	//type args struct {
	//	jbytes string
	//	value  any
	//}
	//tests := []struct {
	//	name      string
	//	args      args
	//	wantValue any
	//	wantErr   assert.ErrorAssertionFunc
	//}{
	//	{
	//		args:      args{jbytes: "7b2261223a317d"},
	//		wantValue: map[string]int{"a": 1},
	//		wantErr:   assert.NoError,
	//	},
	//}
	//for _, tt := range tests {
	//	t.Run(tt.name, func(t *testing.T) {
	//		tt.wantErr(t, JsonDecode(tt.args.jbytes, tt.args.value), fmt.Sprintf("JsonDecode(%v, %v)", tt.args.jbytes, tt.args.value))
	//		assert.Equal(t, tt.wantValue, tt.args.value)
	//	})
	//}
}

func TestJsonEncode(t *testing.T) {
	type args struct {
		value any
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			args:    args{value: map[string]int{"a": 1}},
			want:    "7b2261223a317d",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JsonEncode(tt.args.value)
			if !tt.wantErr(t, err, fmt.Sprintf("JsonEncode(%v)", tt.args.value)) {
				return
			}
			assert.Equalf(t, tt.want, got, "JsonEncode(%v)", tt.args.value)
		})
	}
}

func TestOutputType_String(t *testing.T) {
	tests := []struct {
		name string
		t    OutputType
		want string
	}{
		{
			t:    OutputTypeCheckpoint,
			want: "Checkpoint",
		},
		{
			t:    OutputTypeTailDone,
			want: "TailDone",
		},

		{
			t:    OutputTypeUnfinishedTailWIP,
			want: "UnfinishedTailWIP",
		},
		{
			t:    100,
			want: "usp output type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.t.String(), "String()")
		})
	}
}

func TestPatternTable_String(t *testing.T) {
	type fields struct {
		AccountId     uint64
		Account       string
		Database      string
		Table         string
		TableIsRegexp bool
		Reserved      bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				AccountId: 123,
				Account:   "account",
				Database:  "database",
				Table:     "table",
			},
			want: "(account,123,database,table)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := PatternTable{
				AccountId:     tt.fields.AccountId,
				Account:       tt.fields.Account,
				Database:      tt.fields.Database,
				Table:         tt.fields.Table,
				TableIsRegexp: tt.fields.TableIsRegexp,
				Reserved:      tt.fields.Reserved,
			}
			assert.Equalf(t, tt.want, table.String(), "String()")
		})
	}
}

func TestPatternTuple_String(t *testing.T) {
	type fields struct {
		Source       PatternTable
		Sink         PatternTable
		OriginString string
		Reserved     string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				Source: PatternTable{
					AccountId: 123,
					Account:   "account1",
					Database:  "database1",
					Table:     "table1",
				},
				Sink: PatternTable{
					AccountId: 456,
					Account:   "account2",
					Database:  "database2",
					Table:     "table2",
				},
			},
			want: "(account1,123,database1,table1),(account2,456,database2,table2)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuple := &PatternTuple{
				Source:       tt.fields.Source,
				Sink:         tt.fields.Sink,
				OriginString: tt.fields.OriginString,
				Reserved:     tt.fields.Reserved,
			}
			assert.Equalf(t, tt.want, tuple.String(), "String()")
		})
	}

	var tuple *PatternTuple
	assert.Equalf(t, "", tuple.String(), "String()")
}

func TestPatternTuples_Append(t *testing.T) {
	type fields struct {
		Pts      []*PatternTuple
		Reserved string
	}
	type args struct {
		pt *PatternTuple
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			fields: fields{
				Pts: []*PatternTuple{},
			},
			args: args{
				pt: &PatternTuple{
					Source: PatternTable{
						AccountId: 123,
						Account:   "account1",
						Database:  "database1",
						Table:     "table1",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pts := &PatternTuples{
				Pts:      tt.fields.Pts,
				Reserved: tt.fields.Reserved,
			}
			pts.Append(tt.args.pt)
		})
	}
}

func TestPatternTuples_String(t *testing.T) {
	type fields struct {
		Pts      []*PatternTuple
		Reserved string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				Pts: nil,
			},
			want: "",
		},
		{
			fields: fields{
				Pts: []*PatternTuple{
					{
						Source: PatternTable{
							AccountId: 123,
							Account:   "account1",
							Database:  "database1",
							Table:     "table1",
						},
						Sink: PatternTable{
							AccountId: 456,
							Account:   "account2",
							Database:  "database2",
							Table:     "table2",
						},
					},
				},
			},
			want: "(account1,123,database1,table1),(account2,456,database2,table2)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pts := &PatternTuples{
				Pts:      tt.fields.Pts,
				Reserved: tt.fields.Reserved,
			}
			assert.Equalf(t, tt.want, pts.String(), "String()")
		})
	}
}

func TestUriInfo_GetEncodedPassword(t *testing.T) {
	AesKey = "test-aes-key-not-use-it-in-cloud"

	type fields struct {
		SinkTyp       string
		User          string
		Password      string
		Ip            string
		Port          int
		PasswordStart int
		PasswordEnd   int
		Reserved      string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			fields:  fields{Password: "password"},
			want:    "6b66312c27142b570457556a5b11050bb2105255b3d96050",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &UriInfo{
				SinkTyp:       tt.fields.SinkTyp,
				User:          tt.fields.User,
				Password:      tt.fields.Password,
				Ip:            tt.fields.Ip,
				Port:          tt.fields.Port,
				PasswordStart: tt.fields.PasswordStart,
				PasswordEnd:   tt.fields.PasswordEnd,
				Reserved:      tt.fields.Reserved,
			}
			_, err := info.GetEncodedPassword()
			if !tt.wantErr(t, err, "GetEncodedPassword()") {
				return
			}
			// TODO assert equal
			//assert.Equalf(t, tt.want, got, "GetEncodedPassword()")
		})
	}
}

func TestUriInfo_String(t *testing.T) {
	type fields struct {
		SinkTyp       string
		User          string
		Password      string
		Ip            string
		Port          int
		PasswordStart int
		PasswordEnd   int
		Reserved      string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				User:     "user",
				Password: "password",
				Ip:       "127.0.0.1",
				Port:     3306,
			},
			want: "mysql://user:******@127.0.0.1:3306",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &UriInfo{
				SinkTyp:       tt.fields.SinkTyp,
				User:          tt.fields.User,
				Password:      tt.fields.Password,
				Ip:            tt.fields.Ip,
				Port:          tt.fields.Port,
				PasswordStart: tt.fields.PasswordStart,
				PasswordEnd:   tt.fields.PasswordEnd,
				Reserved:      tt.fields.Reserved,
			}
			assert.Equalf(t, tt.want, info.String(), "String()")
		})
	}
}
