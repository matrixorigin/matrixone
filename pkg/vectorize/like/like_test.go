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

package like

import (
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

func Test_sliceLikePure(t *testing.T) {
	type args struct {
		s    []string
		expr []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []int64
		wantErr bool
	}{
		// 1. %
		{
			name: "%",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte("%"),
			},
			want:    []int64{0, 1},
			wantErr: false,
		},
		// 2. _
		{
			name: "_",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte("_"),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 3. nil
		{
			name: "nil",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte(""),
			},
			want:    []int64{},
			wantErr: false,
		},
		// 4. no _ and %
		{
			name: "bc",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte("bc"),
			},
			want:    []int64{1},
			wantErr: false,
		},
		// 5. _XXX
		{
			name: "_XXX",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte("_c"),
			},
			want:    []int64{1},
			wantErr: false,
		},
		// 6. %XXX
		{
			name: "%XXX",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte("%c"),
			},
			want:    []int64{1},
			wantErr: false,
		},
		// 7. _XXX%
		{
			name: "_XXX%",
			args: args{
				s:    []string{"a", "bc", "aca"},
				expr: []byte("_c%"),
			},
			want:    []int64{1, 2},
			wantErr: false,
		},
		// 8. %XXX_
		{
			name: "%XXX_",
			args: args{
				s:    []string{"a", "bc", "aca"},
				expr: []byte("%c_"),
			},
			want:    []int64{2},
			wantErr: false,
		},
		// 9. _XXX_
		{
			name: "_XXX_",
			args: args{
				s:    []string{"a", "bc", "aca"},
				expr: []byte("_c_"),
			},
			want:    []int64{2},
			wantErr: false,
		},
		// 10. %XXX%
		{
			name: "%XXX%",
			args: args{
				s:    []string{"a", "bc", "aca"},
				expr: []byte("%c%"),
			},
			want:    []int64{1, 2},
			wantErr: false,
		},
		// 11. XXX%YYY
		{
			name: "XXX%YYY",
			args: args{
				s:    []string{"abc", "ac", "aca"},
				expr: []byte("a%c"),
			},
			want:    []int64{0, 1},
			wantErr: false,
		},
		// 12. XXX_YYY
		{
			name: "XXX_YYY",
			args: args{
				s:    []string{"abc", "ac", "aca"},
				expr: []byte("a_c"),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 13. single char none wild card
		{
			name: "*",
			args: args{
				s:    []string{"abc", "*", "aca"},
				expr: []byte("*"),
			},
			want:    []int64{1},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := make([]bool, len(tt.args.s))
			_, err := BtSliceAndConst(tt.args.s, tt.args.expr, rs)
			if (err != nil) != tt.wantErr {
				t.Errorf("sliceLikeScalar() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			got := []int64{}
			for i, b := range rs {
				if b {
					got = append(got, int64(i))
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sliceLikeScalar() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_pureLikePure(t *testing.T) {
	type args struct {
		p    []byte
		expr []byte
		rs   []int64
	}
	tests := []struct {
		name    string
		args    args
		want    []int64
		wantErr bool
	}{
		// 1. %
		{
			name: "%",
			args: args{
				p:    []byte("123"),
				expr: []byte("%"),
				rs:   make([]int64, 1),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 2. _
		{
			name: "_",
			args: args{
				p:    []byte("123"),
				expr: []byte("_"),
				rs:   make([]int64, 1),
			},
			want:    nil,
			wantErr: false,
		},
		// 3. nil
		{
			name: "",
			args: args{
				p:    []byte("123"),
				expr: []byte(""),
				rs:   make([]int64, 1),
			},
			want:    nil,
			wantErr: false,
		},
		// 4. %X%
		{
			name: "%X%",
			args: args{
				p:    []byte("123"),
				expr: []byte("%23%"),
				rs:   make([]int64, 1),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 5. %X_
		{
			name: "%X_",
			args: args{
				p:    []byte("123"),
				expr: []byte("%12_"),
				rs:   make([]int64, 1),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 6. _X_
		{
			name: "_X_",
			args: args{
				p:    []byte("323"),
				expr: []byte("_2_"),
				rs:   make([]int64, 1),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 7. _X%
		{
			name: "_X%",
			args: args{
				p:    []byte("12323"),
				expr: []byte("_1%"),
				rs:   make([]int64, 1),
			},
			want:    nil,
			wantErr: false,
		},
		// 8. _XX
		{
			name: "_XX",
			args: args{
				p:    []byte("k123"),
				expr: []byte("_123"),
				rs:   make([]int64, 1),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 9. %XX
		{
			name: "%XX",
			args: args{
				p:    []byte("k123"),
				expr: []byte("%23"),
				rs:   make([]int64, 1),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 10. XX_
		{
			name: "XX_",
			args: args{
				p:    []byte("k123"),
				expr: []byte("k123_"),
				rs:   make([]int64, 1),
			},
			want:    nil,
			wantErr: false,
		},
		// 11. XX%
		{
			name: "XX%",
			args: args{
				p:    []byte("wop23"),
				expr: []byte("w%"),
				rs:   make([]int64, 1),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 12. XX_XX
		{
			name: "XX_XX",
			args: args{
				p:    []byte("wop23"),
				expr: []byte("wo_23"),
				rs:   make([]int64, 1),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 13. XX%XX
		{
			name: "XX%XX",
			args: args{
				p:    []byte("wop23"),
				expr: []byte("wop%23"),
				rs:   make([]int64, 1),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 14. single char none wild card
		{
			name: "*",
			args: args{
				p:    []byte("*"),
				expr: []byte("*"),
				rs:   make([]int64, 1),
			},
			want:    []int64{0},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := BtConstAndConst(string(tt.args.p), tt.args.expr)
			if (err != nil) != tt.wantErr {
				t.Errorf("scalarLikeScalar() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if ok != (len(tt.want) == 1) {
				t.Errorf("scalarLikeScalar() got = %v, want %v", ok, tt.want)
			}
		})
	}
}

func Test_sliceNullLikePure(t *testing.T) {
	type args struct {
		s    []string
		expr []byte
		ns   *nulls.Nulls
	}
	tests := []struct {
		name    string
		args    args
		want    []int64
		wantErr bool
	}{
		// 1. %
		{
			name: "%",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte("%"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{0, 1},
			wantErr: false,
		},
		// 2. _
		{
			name: "_",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte("_"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 3. nil
		{
			name: "nil",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte(""),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{},
			wantErr: false,
		},
		// 4. no _ and %
		{
			name: "bc",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte("bc"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{1},
			wantErr: false,
		},
		// 5. _XXX
		{
			name: "_XXX",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte("_c"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{1},
			wantErr: false,
		},
		// 6. %XXX
		{
			name: "%XXX",
			args: args{
				s:    []string{"a", "bc"},
				expr: []byte("%c"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{1},
			wantErr: false,
		},
		// 7. _XXX%
		{
			name: "_XXX%",
			args: args{
				s:    []string{"a", "bc", "aca"},
				expr: []byte("_c%"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{1, 2},
			wantErr: false,
		},
		// 8. %XXX_
		{
			name: "%XXX_",
			args: args{
				s:    []string{"a", "bc", "aca"},
				expr: []byte("%c_"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{2},
			wantErr: false,
		},
		// 9. _XXX_
		{
			name: "_XXX_",
			args: args{
				s:    []string{"a", "bc", "aca"},
				expr: []byte("_c_"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{2},
			wantErr: false,
		},
		// 10. %XXX%
		{
			name: "%XXX%",
			args: args{
				s:    []string{"a", "bc", "aca"},
				expr: []byte("%c%"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{1, 2},
			wantErr: false,
		},
		// 11. XXX%YYY
		{
			name: "XXX%YYY",
			args: args{
				s:    []string{"abc", "ac", "aca"},
				expr: []byte("a%c"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{0, 1},
			wantErr: false,
		},
		// 12. XXX_YYY
		{
			name: "XXX_YYY",
			args: args{
				s:    []string{"abc", "ac", "aca"},
				expr: []byte("a_c"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{0},
			wantErr: false,
		},
		// 13. XXX_
		{
			name: "XXX_",
			args: args{
				s:    []string{"abc", "ac", "aca"},
				expr: []byte("a_"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{1},
			wantErr: false,
		},
		// 14. XXX%
		{
			name: "XXX%",
			args: args{
				s:    []string{"abc", "ac", "aca"},
				expr: []byte("a%"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{0, 1, 2},
			wantErr: false,
		},
		// 15. single char none wild card
		{
			name: "*",
			args: args{
				s:    []string{"abc", "*", "*a"},
				expr: []byte("*"),
				ns:   nulls.NewWithSize(0),
			},
			want:    []int64{1},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := make([]bool, len(tt.args.s))
			_, err := BtSliceNullAndConst(tt.args.s, tt.args.expr, tt.args.ns, rs)
			if (err != nil) != tt.wantErr {
				t.Errorf("sliceContainsNullLikeScalar() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			got := []int64{}
			for i, b := range rs {
				if b {
					got = append(got, int64(i))
				}
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sliceContainsNullLikeScalar() got = %v, want %v", got, tt.want)
			}
		})
	}
}
