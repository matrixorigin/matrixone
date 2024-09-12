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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/btree"
)

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
		// TODO: Add test cases.
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
			assert.Equalf(t, tt.want, bat.GetRowIterator(), "GetRowIterator()")
		})
	}
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
		// TODO: Add test cases.
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
	type args struct {
		jbytes string
		value  any
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, JsonDecode(tt.args.jbytes, tt.args.value), fmt.Sprintf("JsonDecode(%v, %v)", tt.args.jbytes, tt.args.value))
		})
	}
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
		// TODO: Add test cases.
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

func TestNewAtomicBatch(t *testing.T) {
	type args struct {
		mp   *mpool.MPool
		from types.TS
		to   types.TS
	}
	tests := []struct {
		name string
		args args
		want *AtomicBatch
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewAtomicBatch(tt.args.mp, tt.args.from, tt.args.to), "NewAtomicBatch(%v, %v, %v)", tt.args.mp, tt.args.from, tt.args.to)
		})
	}
}

func TestNewCdcActiveRoutine(t *testing.T) {
	tests := []struct {
		name string
		want *ActiveRoutine
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, NewCdcActiveRoutine(), "NewCdcActiveRoutine()")
		})
	}
}

func TestOutputType_String(t *testing.T) {
	tests := []struct {
		name string
		t    OutputType
		want string
	}{
		// TODO: Add test cases.
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
		// TODO: Add test cases.
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
		// TODO: Add test cases.
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
		// TODO: Add test cases.
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
		// TODO: Add test cases.
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
		// TODO: Add test cases.
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
			got, err := info.GetEncodedPassword()
			if !tt.wantErr(t, err, fmt.Sprintf("GetEncodedPassword()")) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetEncodedPassword()")
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
		// TODO: Add test cases.
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

//func Test_atomicBatchRowIter_Close(t *testing.T) {
//	type fields struct {
//		iter     btree.IterG
//		initIter btree.IterG
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		wantErr assert.ErrorAssertionFunc
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			iter := &atomicBatchRowIter{
//				iter:     tt.fields.iter,
//				initIter: tt.fields.initIter,
//			}
//			tt.wantErr(t, iter.Close(), fmt.Sprintf("Close()"))
//		})
//	}
//}
//
//func Test_atomicBatchRowIter_Item(t *testing.T) {
//	type fields struct {
//		iter     btree.IterG
//		initIter btree.IterG
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   AtomicBatchRow
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			iter := &atomicBatchRowIter{
//				iter:     tt.fields.iter,
//				initIter: tt.fields.initIter,
//			}
//			assert.Equalf(t, tt.want, iter.Item(), "Item()")
//		})
//	}
//}
//
//func Test_atomicBatchRowIter_Next(t *testing.T) {
//	type fields struct {
//		iter     btree.IterG
//		initIter btree.IterG
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			iter := &atomicBatchRowIter{
//				iter:     tt.fields.iter,
//				initIter: tt.fields.initIter,
//			}
//			assert.Equalf(t, tt.want, iter.Next(), "Next()")
//		})
//	}
//}
//
//func Test_atomicBatchRowIter_Prev(t *testing.T) {
//	type fields struct {
//		iter     btree.IterG
//		initIter btree.IterG
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			iter := &atomicBatchRowIter{
//				iter:     tt.fields.iter,
//				initIter: tt.fields.initIter,
//			}
//			assert.Equalf(t, tt.want, iter.Prev(), "Prev()")
//		})
//	}
//}
//
//func Test_atomicBatchRowIter_Reset(t *testing.T) {
//	type fields struct {
//		iter     btree.IterG
//		initIter btree.IterG
//	}
//	tests := []struct {
//		name   string
//		fields fields
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			iter := &atomicBatchRowIter{
//				iter:     tt.fields.iter,
//				initIter: tt.fields.initIter,
//			}
//			iter.Reset()
//		})
//	}
//}
//
//func Test_atomicBatchRowIter_Row(t *testing.T) {
//	type fields struct {
//		iter     btree.IterG
//		initIter btree.IterG
//	}
//	type args struct {
//		ctx context.Context
//		row []any
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		wantErr assert.ErrorAssertionFunc
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			iter := &atomicBatchRowIter{
//				iter:     tt.fields.iter,
//				initIter: tt.fields.initIter,
//			}
//			tt.wantErr(t, iter.Row(tt.args.ctx, tt.args.row), fmt.Sprintf("Row(%v, %v)", tt.args.ctx, tt.args.row))
//		})
//	}
//}
