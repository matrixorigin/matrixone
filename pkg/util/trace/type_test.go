// Copyright The OpenTelemetry Authors
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

// Portions of this file are additionally subject to the following
// copyright.
//
// Copyright (C) 2022 Matrix Origin.
//
// Modified the behavior and the interface of the step.

package trace

import (
	"reflect"
	"testing"
)

func TestGetGlobalBatchProcessor(t *testing.T) {
	tests := []struct {
		name string
		want BatchProcessor
	}{
		{
			name: "normal",
			want: noopBatchProcessor{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetGlobalBatchProcessor(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetGlobalBatchProcessor() = %v, want %v", got, tt.want)
			}
		})
	}
}
