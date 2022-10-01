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

package logutil

import (
	"github.com/aws/smithy-go/logging"
	"testing"
)

func TestS3Logger_Logf(t *testing.T) {
	type args struct {
		classification logging.Classification
		format         string
		v              []any
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "warn",
			args: args{
				classification: logging.Warn,
				format:         "hello %s!",
				v:              []any{"world"},
			},
		},
		{
			name: "debug",
			args: args{
				classification: logging.Debug,
				format:         "hello %s!",
				v:              []any{"world"},
			},
		},
		{
			name: "info",
			args: args{
				classification: "",
				format:         "hello %s!",
				v:              []any{"world"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := GetS3Logger()
			logger.Logf(tt.args.classification, tt.args.format, tt.args.v...)
		})
	}
}
