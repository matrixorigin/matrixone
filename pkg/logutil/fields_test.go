// Copyright 2026 Matrix Origin
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
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestErrorField(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		fieldType zapcore.FieldType
	}{
		{name: "nil", fieldType: zapcore.SkipType},
		{name: "EOF", err: io.EOF, fieldType: zapcore.SkipType},
		{
			name:      "wrapped EOF",
			err:       fmt.Errorf("client stream closed: %w", io.EOF),
			fieldType: zapcore.SkipType,
		},
		{
			name:      "unexpected EOF",
			err:       io.ErrUnexpectedEOF,
			fieldType: zapcore.ErrorType,
		},
		{
			name:      "ordinary error",
			err:       errors.New("statement failed"),
			fieldType: zapcore.ErrorType,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoder := zapcore.NewMapObjectEncoder()
			require.NotPanics(t, func() {
				field := ErrorField(test.err)
				require.Equal(t, test.fieldType, field.Type)
				field.AddTo(encoder)
			})
			encodedError, encoded := encoder.Fields["error"]
			require.Equal(t, test.fieldType == zapcore.ErrorType, encoded)
			if encoded {
				require.Equal(t, test.err.Error(), encodedError)
			}
		})
	}
}
