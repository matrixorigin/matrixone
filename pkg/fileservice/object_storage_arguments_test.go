// Copyright 2023 Matrix Origin
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

package fileservice

import (
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestObjectStorageArguments(t *testing.T) {

	t.Run("no api key", func(t *testing.T) {
		args := ObjectStorageArguments{
			KeyID: "foo",
		}
		field := zap.Any("arguments", args)

		{
			encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
			buf, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{field})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(buf.String(), "foo") {
				t.Fatal()
			}
		}

		{
			encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{})
			buf, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{field})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(buf.String(), "foo") {
				t.Fatal()
			}
		}

	})
}
