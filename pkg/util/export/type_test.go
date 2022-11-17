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
	"reflect"
	"testing"
	"time"
)

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

func init() {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
}
