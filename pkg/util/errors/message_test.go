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

package errors

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWithMessage(t *testing.T) {
	type args struct {
		err     error
		message string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name:    "normal",
			args:    args{testErr, "message"},
			wantErr: true,
		},
		{
			name:    "nil",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := WithMessage(tt.args.err, tt.args.message); (err != nil) != tt.wantErr {
				t.Errorf("WithMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWithMessagef(t *testing.T) {
	type args struct {
		err    error
		format string
		args   []any
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "normal",
			args:    args{testErr, "message", []any{}},
			wantErr: true,
		},
		{
			name:    "nil",
			args:    args{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := WithMessagef(tt.args.err, tt.args.format, tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("WithMessagef() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_withMessage_Format(t *testing.T) {
	type fields struct {
		cause error
		msg   string
	}
	tests := []struct {
		name     string
		fields   fields
		wantMsg  string
		wantMsgQ string
		wantMsgV string
	}{
		{
			name:     "normal",
			fields:   fields{testErr, "prefix"},
			wantMsg:  "prefix: test error",
			wantMsgQ: `"prefix: test error"`,
			wantMsgV: "test error\nprefix",
		},
		{
			name:     "empty",
			fields:   fields{testErr, ""},
			wantMsg:  "test error",
			wantMsgQ: `"test error"`,
			wantMsgV: "test error\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &withMessage{
				cause: tt.fields.cause,
				msg:   tt.fields.msg,
			}
			require.Equal(t, tt.wantMsg, w.Error())
			require.Equal(t, tt.wantMsg, fmt.Sprintf("%s", w))
			require.Equal(t, tt.wantMsgQ, fmt.Sprintf("%q", w))
			require.Equal(t, tt.wantMsgV, fmt.Sprintf("%+v", w))
		})
	}
}
