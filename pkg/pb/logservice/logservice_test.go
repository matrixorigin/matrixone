// Copyright 2021 - 2022 Matrix Origin
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

package logservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogRecord(t *testing.T) {
	r := LogRecord{
		Data: make([]byte, 32),
	}
	assert.Equal(t, 32-HeaderSize-8, len(r.Payload()))
	r.ResizePayload(2)
	assert.Equal(t, HeaderSize+8+2, len(r.Data))
	assert.Equal(t, 2, len(r.Payload()))
}
