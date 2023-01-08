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

package fileservice

import (
	"bytes"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"
)

func TestIOEntriesReader(t *testing.T) {

	entries := []IOEntry{
		{
			Offset: 0,
			Size:   1,
			Data:   []byte("a"),
		},
	}
	assert.Nil(t, iotest.TestReader(newIOEntriesReader(entries), []byte("a")))

	entries = []IOEntry{
		{
			Offset: 1,
			Size:   1,
			Data:   []byte("a"),
		},
	}
	assert.Nil(t, iotest.TestReader(newIOEntriesReader(entries), []byte("\x00a")))

	entries = []IOEntry{
		{
			Offset: 0,
			Size:   1024,
			Data:   bytes.Repeat([]byte("a"), 1024),
		},
	}
	assert.Nil(t, iotest.TestReader(newIOEntriesReader(entries), bytes.Repeat([]byte("a"), 1024)))

	entries = []IOEntry{
		{
			Size:           -1,
			ReaderForWrite: bytes.NewReader([]byte("abc")),
		},
	}
	assert.Nil(t, iotest.TestReader(newIOEntriesReader(entries), []byte("abc")))

}
