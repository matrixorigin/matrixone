// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package motrace

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContentBuffer_isEmpty(t *testing.T) {
	type fields struct {
		options []BufferOption
	}
	tests := []struct {
		name   string
		fields fields
		run    func(buf *ContentBuffer)
		want   bool
	}{
		{
			name: "empty",
			fields: fields{
				options: []BufferOption{BufferWithGenBatchFunc(noopGenBatchSQL), BufferWithType("test")},
			},
			run:  func(buf *ContentBuffer) { /*none op*/ },
			want: true,
		},
		{
			name: "not_empty",
			fields: fields{
				options: []BufferOption{BufferWithGenBatchFunc(noopGenBatchSQL), BufferWithType("test")},
			},
			run:  func(buf *ContentBuffer) { buf.Add(&StatementInfo{}) },
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewContentBuffer(tt.fields.options...)
			tt.run(b)
			_ = b.ShouldFlush()
			_ = b.Size()
			assert.Equalf(t, tt.want, b.isEmpty(), "isEmpty()")
		})
	}
}
