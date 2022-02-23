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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/sql/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"reflect"
	"testing"
)

func Test_build_pruneExtend(t *testing.T) {
	type fields struct {
		flg bool
		db  string
		sql string
		e   engine.Engine
	}
	type args struct {
		e            extend.Extend
		isProjection bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    extend.Extend
		wantErr bool
	}{
		// 1. unaryExtend
		{
			name:   "not_1",
			fields: fields{},
			args: args{
				e: &extend.UnaryExtend{
					Op: overload.Not,
					E: &extend.ValueExtend{
						V: testutil.MakeInt64Vector([]int64{1}, 0),
					},
				},
				isProjection: true,
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt8Vector([]int8{0}, 0),
			},
		},
		{
			name:   "not_2",
			fields: fields{},
			args: args{
				e: &extend.UnaryExtend{
					Op: overload.Not,
					E: &extend.ValueExtend{
						V: testutil.MakeInt64Vector([]int64{1}, 0),
					},
				},
				isProjection: false,
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{1}, 0),
			},
		},
		{
			name:   "unary_minus_1",
			fields: fields{},
			args: args{
				e: &extend.UnaryExtend{
					Op: overload.UnaryMinus,
					E: &extend.ValueExtend{
						V: testutil.MakeInt64Vector([]int64{1}, 0),
					},
				},
				isProjection: false,
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{-1}, 0),
			},
		},
		{
			name:   "unary_minus_2",
			fields: fields{},
			args: args{
				e: &extend.UnaryExtend{
					Op: overload.UnaryMinus,
					E: &extend.ValueExtend{
						V: testutil.MakeInt64Vector([]int64{-1}, 0),
					},
				},
				isProjection: false,
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{1}, 0),
			},
		},
		// 2. parenExtend
		{
			name:   "paren",
			fields: fields{},
			args: args{
				e: &extend.ParenExtend{E: &extend.ValueExtend{
					V: testutil.MakeInt8Vector([]int8{1}, 0),
				}},
			},
			want: &extend.ParenExtend{E: &extend.ValueExtend{
				V: testutil.MakeInt8Vector([]int8{1}, 0),
			}},
		},
		// 3. binaryExtend
		{
			name:   "binary_or",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Or,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{1}, 0),
			},
		},
		{
			name:   "binary_or_1",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Or,
					Left:  &extend.Attribute{Name: "a", Type: 0},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
		},
		{
			name:   "binary_or_2",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Or,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.Attribute{Name: "a", Type: 0},
				},
			},
			want: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
		},
		{
			name:   "binary_or_3",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Or,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{0}, 0)},
					Right: &extend.Attribute{Name: "a", Type: 0},
				},
			},
			want: &extend.Attribute{Name: "a", Type: 0},
		},
		{
			name:   "binary_or_4",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Or,
					Left:  &extend.Attribute{Name: "a", Type: 0},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{0}, 0)},
				},
			},
			want: &extend.Attribute{Name: "a", Type: 0},
		},

		{
			name:   "binary_and",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.And,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{1}, 0),
			},
		},
		{
			name:   "binary_and_1",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.And,
					Left:  &extend.Attribute{Name: "a", Type: 0},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.Attribute{Name: "a", Type: 0},
		},
		{
			name:   "binary_and_2",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.And,
					Left:  &extend.Attribute{Name: "a", Type: 0},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{0}, 0)},
				},
			},
			want: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{0}, 0)},
		},
		{
			name:   "binary_and_3",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.And,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{0}, 0)},
					Right: &extend.Attribute{Name: "a", Type: 0},
				},
			},
			want: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{0}, 0)},
		},
		{
			name:   "binary_and_4",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.And,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.Attribute{Name: "a", Type: 0},
				},
			},
			want: &extend.Attribute{Name: "a", Type: 0},
		},
		{
			name:   "binary_and_5",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.And,
					Left:  &extend.ValueExtend{V: testutil.MakeFloat64Vector([]float64{1}, 0)},
					Right: &extend.Attribute{Name: "a", Type: 0},
				},
			},
			want: &extend.Attribute{Name: "a", Type: 0},
		},
		{
			name:   "binary_and_6",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.And,
					Left:  &extend.Attribute{Name: "a", Type: 0},
					Right: &extend.ValueExtend{V: testutil.MakeFloat64Vector([]float64{1}, 0)},
				},
			},
			want: &extend.Attribute{Name: "a", Type: 0},
		},
		{
			name:   "binary_and_7",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.And,
					Left:  &extend.Attribute{Name: "a", Type: 0},
					Right: &extend.ValueExtend{V: testutil.MakeFloat64Vector([]float64{0}, 0)},
				},
			},
			want: &extend.ValueExtend{V: testutil.MakeFloat64Vector([]float64{0}, 0)},
		},
		{
			name:   "binary_and_8",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.And,
					Left:  &extend.ValueExtend{V: testutil.MakeFloat64Vector([]float64{0}, 0)},
					Right: &extend.Attribute{Name: "a", Type: 0},
				},
			},
			want: &extend.ValueExtend{V: testutil.MakeFloat64Vector([]float64{0}, 0)},
		},

		{
			name:   "binary_eq",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.EQ,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{1}, 1),
			},
		},
		{
			name:   "binary_eq_1",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.EQ,
					Left:  &extend.Attribute{Name: "a", Type: types.T_int8},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.EQ,
				Left:  &extend.Attribute{Name: "a", Type: types.T_int8},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 1)},
			},
		},
		{
			name:   "binary_eq_2",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.EQ,
					Left:  &extend.Attribute{Name: "a", Type: types.T_int16},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.EQ,
				Left:  &extend.Attribute{Name: "a", Type: types.T_int16},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 1)},
			},
		},
		{
			name:   "binary_eq_3",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.EQ,
					Left:  &extend.Attribute{Name: "a", Type: types.T_int32},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.EQ,
				Left:  &extend.Attribute{Name: "a", Type: types.T_int32},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 1)},
			},
		},
		{
			name:   "binary_eq_4",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.EQ,
					Left:  &extend.Attribute{Name: "a", Type: types.T_int64},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.EQ,
				Left:  &extend.Attribute{Name: "a", Type: types.T_int64},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 1)},
			},
		},
		{
			name:   "binary_eq_5",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.EQ,
					Left:  &extend.Attribute{Name: "a", Type: types.T_uint8},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.EQ,
				Left:  &extend.Attribute{Name: "a", Type: types.T_uint8},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 1)},
			},
		},

		{
			name:   "binary_lt",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.LT,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{0}, 1),
			},
		},
		{
			name:   "binary_le",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.LE,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{2}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{0}, 1),
			},
		},
		{
			name:   "binary_gt",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.GT,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{2}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{1}, 1),
			},
		},
		{
			name:   "binary_ge",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.GE,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{1}, 1),
			},
		},
		{
			name:   "binary_ne",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.NE,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{0}, 1),
			},
		},
		{
			name:   "binary_div",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Div,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{4}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{2}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeFloat64Vector([]float64{2}, 1),
			},
		},
		{
			name:   "binary_mod",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Mod,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{5}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{3}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{2}, 1),
			},
		},
		{
			name:   "binary_mult",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Mult,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{2}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{2}, 1),
			},
		},
		{
			name:   "binary_plus",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Plus,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{2}, 1),
			},
		},
		{
			name:   "binary_minus",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Minus,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{0}, 1),
			},
		},
		{
			name:   "binary_like",
			fields: fields{},
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.Like,
					Left:  &extend.ValueExtend{V: testutil.MakeStringVector([]string{"123"}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeStringVector([]string{"123"}, 0)},
				},
			},
			want: &extend.ValueExtend{
				V: testutil.MakeInt64Vector([]int64{1}, 1),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &build{
				flg: tt.fields.flg,
				db:  tt.fields.db,
				sql: tt.fields.sql,
				e:   tt.fields.e,
			}
			got, err := b.pruneExtend(tt.args.e, tt.args.isProjection)
			if (err != nil) != tt.wantErr {
				t.Errorf("pruneExtend() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil && tt.wantErr {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("pruneExtend() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_logicInverse(t *testing.T) {
	type args struct {
		e extend.Extend
	}
	tests := []struct {
		name string
		args args
		want extend.Extend
	}{
		// 1. parenExtend
		{
			name: "parenExtend",
			args: args{
				e: &extend.ParenExtend{E: nil},
			},
			want: &extend.ParenExtend{E: nil},
		},
		// 2. binaryExtend
		{
			name: "eq",
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.EQ,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.NE,
				Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
			},
		},
		{
			name: "ne",
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.NE,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.EQ,
				Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
			},
		},
		{
			name: "ge",
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.GE,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.LT,
				Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
			},
		},
		{
			name: "lt",
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.LT,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.GE,
				Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
			},
		},
		{
			name: "le",
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.LE,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.GT,
				Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
			},
		},
		{
			name: "gt",
			args: args{
				e: &extend.BinaryExtend{
					Op:    overload.GT,
					Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
					Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.BinaryExtend{
				Op:    overload.LE,
				Left:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				Right: &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
			},
		},
		// 3. unaryExtend
		{
			name: "unary_minus",
			args: args{
				e: &extend.UnaryExtend{
					Op: overload.UnaryMinus,
					E:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
				},
			},
			want: &extend.UnaryExtend{
				Op: overload.UnaryMinus,
				E:  &extend.ValueExtend{V: testutil.MakeInt64Vector([]int64{1}, 0)},
			},
		},
		{
			name: "unary_not",
			args: args{
				e: &extend.UnaryExtend{
					Op: overload.Not,
					E:  nil,
				},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := logicInverse(tt.args.e); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("logicInverse() = %v, want %v", got, tt.want)
			}
		})
	}
}
