package tpEngine

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_encodeBytes(t *testing.T) {
	type args struct {
		data []byte
		val  []byte
	}

	t1 := []byte{}
	t1_want := []byte{0,0,0,0,0,0,0,0,255 - 8}

	t2 := []byte{1}
	t2_want := []byte{1,0,0,0,0,0,0,0,255 - 7}

	t3 := []byte{1,2,3,4,5,6,0}
	t3_want := []byte{1,2,3,4,5,6,0,0,255 - 1}

	t4 := []byte{1,2,3,4,5,6,7,8}
	t4_want := []byte{1,2,3,4,5,6,7,8,255 - 0, 0,0,0,0,0,0,0,0,255 - 8}

	tests := []struct {
		name string
		args args
		want []byte
	}{
		{"t1",args{nil,t1},t1_want},
		{"t2",args{nil,t2},t2_want},
		{"t3",args{nil,t3},t3_want},
		{"t4",args{nil,t4},t4_want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := encodeComparableBytes(tt.args.data, tt.args.val);
			!reflect.DeepEqual(got, tt.want) {
				t.Errorf("encodeComparableBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_encode_bytes_sort(t *testing.T)  {
	type args struct {
		a,b string
		f func(a,b string,aEnc,bEnc []byte) bool
	}

	f1 := func(a, b string, aEnc, bEnc []byte) bool {
		return a < b && bytes.Compare(aEnc,bEnc) < 0 && b > a && bytes.Compare(bEnc,aEnc) > 0
	}

	f2 := func(a, b string, aEnc, bEnc []byte) bool {
		return a == b && bytes.Compare(aEnc,bEnc) == 0
	}

	tests := []struct {
		name string
		args args
		want bool
	}{
		{"t1",
			args{
				a:"abc",
				b:"bcd",
				f:f1,
			},
			true,
		},
		{"t2",
			args{
				a:"ABC",
				b:"abc",
				f:f1,
			},
			true,
		},
		{"t3",
			args{
				a:"abc",
				b:"abc",
				f:f2,
			},
			true,
		},
		{"t4",
			args{
				a:"我",
				b:"我",
				f:f2,
			},
			true,
		},
		{"t5",
			args{
				a:"我们",
				b:"我们不再是我们，我们依然是我们",
				f:f1,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aEnc := encodeComparableBytes(nil, []byte(tt.args.a))
			bEnc := encodeComparableBytes(nil, []byte(tt.args.b))
			if got := tt.args.f(tt.args.a,tt.args.b,aEnc,bEnc);
			got != tt.want {
				t.Errorf("got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_decodeComparableBytes(t *testing.T) {
	type args struct {
		data []byte
		enc  []byte
	}

	t1 := []byte{}

	t2 := []byte{1}

	t3 := []byte{1,2,3,4,5,6,0}

	t4 := []byte{1,2,3,4,5,6,7,8}

	t5 := []byte("我们不再是我们，我们依然是我们")

	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{"t1",args{nil,t1},t1,false},
		{"t2",args{nil,t2},t2,false},
		{"t3",args{nil,t3},t3,false},
		{"t4",args{nil,t4},t4,false},
		{"t5",args{nil,t5},t5,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := encodeComparableBytes(nil,tt.args.enc)
			got, err := decodeComparableBytes(tt.args.data, enc)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeComparableBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if len(got) == 0 && len(tt.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeComparableBytes() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTableDatabasesRowCode(t *testing.T) {
	row := NewTableDatabasesRow("abc",10,"xxx")
	enc := row.encode(nil)

	row2 := &tableDatabasesRow{}

	_,err := row2.decode(enc)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(row,row2) {
		t.Errorf("not equal. %v %v",row,row2)
		return
	}
}

func Test_decodeUint64(t *testing.T) {
	cases := []uint64{
		0,1,2,3,4,5,6,7,8,9,10,
		11,12,13,14,15,16,17,18,19,20,
	}

	for _,s := range cases{
		enc := encodeUint64(nil,s)
		_,d,_ := decodeUint64(enc)
		if s != d {
			t.Error("not equal")
		}
	}
}

func Test_decodeString(t *testing.T) {
	cases := []string{
		"abc","def","dec","xxx","ppppzdaf",
	}

	for _,s := range cases{
		enc := encodeString(nil,s)
		_,d,_ := decodeString(enc)
		if s != d {
			t.Error("not equal")
		}
	}
}

func Test_decodeValue(t *testing.T) {
	cases := []interface{}{
		"abc","bcd","xxx",uint64(0xabcdef),uint64(19),
	}

	for _,s := range cases{
		enc := encodeValues(nil,s)
		_,d,err := decodeValue(enc)
		if err != nil {
			t.Error(err)
		}
		var ddd interface{}
		switch dd := s.(type) {
		case uint64:
			ddd = dd
		case string:
			ddd = dd
		default:
			t.Errorf("unsupported type")
		}

		if !reflect.DeepEqual(s,ddd) {
			t.Error(fmt.Errorf("not equal %v %v",s,d))
		}
	}
}

func Test_tpTupleImpl_decode(t *testing.T) {
	cases := [][]interface{}{
		{"abc","bcd","xxx",uint64(0xabcdef),uint64(19),},
		{uint64(0),uint64(10000),"我们"},
	}

	for _,s := range cases{
		tuple := &tpTupleImpl{
			fields:      s,
		}
		enc := tuple.encode(nil)
		d,err := tuple.decode(enc)
		if err != nil {
			t.Error(err)
		}

		if !reflect.DeepEqual(s,tuple.fields) {
			t.Error(fmt.Errorf("not equal %v %v",s,d))
		}
	}
}

func Test_encodeUint64Key(t *testing.T) {
	cases :=[][]uint64 {
		{0,1},
		{1,2},
		{1,0xffffffffffffffff},
	}

	f1 := func(a, b uint64, aEnc, bEnc []byte) bool {
		return a < b && bytes.Compare(aEnc,bEnc) < 0 && b > a && bytes.Compare(bEnc,aEnc) > 0
	}

	for _,c := range cases{
		e1 := encodeUint64Key(nil,c[0])
		e2 := encodeUint64Key(nil,c[1])
		assert.True(t, f1(c[0],c[1],e1,e2))
	}
}

func Test_encodeInt64Key(t *testing.T) {
	cases :=[][]int64 {
		{-1,1},
		{-2,-1},
		{0,1},
		{1,2},
	}

	f1 := func(a, b int64, aEnc, bEnc []byte) bool {
		return a < b && bytes.Compare(aEnc,bEnc) < 0 && b > a && bytes.Compare(bEnc,aEnc) > 0
	}

	for _,c := range cases{
		e1 := encodeInt64Key(nil,c[0])
		e2 := encodeInt64Key(nil,c[1])
		assert.True(t, f1(c[0],c[1],e1,e2))
	}
}

func Test_decodeStringKey(t *testing.T) {
	type args struct {
		data []byte
	}

	t1_want := "abc"
	t1 := encodeStringKey(nil,t1_want)

	t2_want := "abcdef"
	t2 := encodeStringKey(nil,t2_want)

	t3_want := "abcdefxswddddddffffffffffggggggggg\a0000g"
	t3 := encodeStringKey(nil,t3_want)

	tests := []struct {
		name    string
		args    args
		want   string
		wantErr bool
	}{
		{"t1",args{t1},t1_want,false},
		{"t2",args{t2},t2_want,false},
		{"t3",args{t3},t3_want,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got1, err := decodeStringKey(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeStringKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if got1 != tt.want {
				t.Errorf("decodeStringKey() got1 = %v, want %v", got1, tt.want)
			}
		})
	}
}

func Test_decodeKey(t *testing.T) {
	type args struct {
		data    []byte
		keyType byte
	}
	t1 := "abc"
	t2 := uint64(0x768)


	tests := []struct {
		name    string
		args    args
		want1   interface{}
		wantErr bool
	}{
		{"t1",args{encodeKeyWithType(nil,t1,TP_ENCODE_TYPE_STRING),TP_ENCODE_TYPE_STRING},t1,false},
		{"t2",args{encodeKeyWithType(nil,t2,TP_ENCODE_TYPE_UINT64),TP_ENCODE_TYPE_UINT64},t2,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got1, err := decodeKey(tt.args.data, tt.args.keyType)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("decodeKey() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_decodeKeys(t *testing.T) {
	type args struct {
		data     []byte
		keyTypes *tpSchema
	}

	t1 := []interface{}{"abc",uint64(0x789),"def",uint64(0x906)}
	t1_sch := NewTpSchema(TP_ENCODE_TYPE_STRING,TP_ENCODE_TYPE_UINT64,TP_ENCODE_TYPE_STRING,TP_ENCODE_TYPE_UINT64)

	tests := []struct {
		name    string
		args    args
		want   []interface{}
		wantErr bool
	}{
		{"t1",args{encodeKeysWithSchema(nil,t1_sch,t1...),t1_sch},t1,false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got1, err := decodeKeys(tt.args.data, tt.args.keyTypes)
			if (err != nil) != tt.wantErr {
				t.Errorf("decodeKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got1, tt.want) {
				t.Errorf("decodeKeys() got1 = %v, want %v", got1, tt.want)
			}
		})
	}
}