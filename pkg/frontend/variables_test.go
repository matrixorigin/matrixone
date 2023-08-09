// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestScope(t *testing.T) {
	convey.Convey("test scope", t, func() {
		wanted := make(map[Scope]string)
		for i := ScopeGlobal; i <= ScopeResetPersist; i++ {
			wanted[i] = i.String()
		}

		convey.So(wanted[ScopeBoth], convey.ShouldEqual, ScopeBoth.String())
	})
}

func TestSystemVariable(t *testing.T) {
	convey.Convey("all", t, func() {
		bt := SystemVariableBoolType{}
		it := SystemVariableIntType{}
		ut := SystemVariableUintType{}
		dt := SystemVariableDoubleType{}
		et := SystemVariableEnumType{}
		sett := SystemVariableSetType{}
		st := SystemVariableStringType{}
		nt := SystemVariableNullType{}
		svs := []SystemVariableType{
			bt,
			it,
			ut,
			dt,
			et,
			sett,
			st,
			nt,
		}

		for _, sv := range svs {
			fmt.Sprintln(sv.String(), sv.Type(), sv.MysqlType(), sv.Zero())
		}

		btrt, err := nt.Convert(nil)
		convey.So(err, convey.ShouldBeNil)
		convey.So(btrt, convey.ShouldBeNil)

		_, err = nt.Convert("string")
		convey.So(err, convey.ShouldNotBeNil)

		_, err = bt.Convert(0)
		convey.So(err, convey.ShouldBeNil)

		_, err = bt.Convert(1)
		convey.So(err, convey.ShouldBeNil)

	})
}

func Test_valueIsBoolTrue(t *testing.T) {
	type args struct {
		value interface{}
	}
	dumpWantErr := func(t assert.TestingT, err error, msgAndArgs ...interface{}) bool {
		return false
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "bool/true",
			args: args{
				value: true,
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "bool/false",
			args: args{
				value: false,
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "int/1",
			args: args{
				value: 1,
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "int/0",
			args: args{
				value: 0,
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "int8/1",
			args: args{
				value: int8(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "int8/0",
			args: args{
				value: int8(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "int16/1",
			args: args{
				value: int16(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "int16/0",
			args: args{
				value: int16(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "int32/1",
			args: args{
				value: int32(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "int32/0",
			args: args{
				value: int32(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "int64/1",
			args: args{
				value: int64(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "int64/0",
			args: args{
				value: int64(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "uint8/1",
			args: args{
				value: uint8(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "uint8/0",
			args: args{
				value: uint8(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "uint16/1",
			args: args{
				value: uint16(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "uint16/0",
			args: args{
				value: uint16(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "uint32/1",
			args: args{
				value: uint32(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "uint32/0",
			args: args{
				value: uint32(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "uint64/1",
			args: args{
				value: uint64(1),
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "uint64/0",
			args: args{
				value: uint64(0),
			},
			want:    false,
			wantErr: dumpWantErr,
		},
		{
			name: "string/on",
			args: args{
				value: "ON",
			},
			want:    true,
			wantErr: dumpWantErr,
		},
		{
			name: "string/off",
			args: args{
				value: "OFF",
			},
			want:    false,
			wantErr: dumpWantErr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := valueIsBoolTrue(tt.args.value)
			if !tt.wantErr(t, err, fmt.Sprintf("valueIsBoolTrue(%v)", tt.args.value)) {
				return
			}
			assert.Equalf(t, tt.want, got, "valueIsBoolTrue(%v)", tt.args.value)
		})
	}
}
