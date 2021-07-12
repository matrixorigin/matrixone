package tpEngine

import (
	"bytes"
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