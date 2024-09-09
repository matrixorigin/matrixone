// Copyright 2024 Matrix Origin
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
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkObjectStorageSemaphore(b *testing.B) {
	diskStorage, err := newDiskObjectStorage(
		context.Background(),
		ObjectStorageArguments{
			Endpoint: "disk",
			Bucket:   b.TempDir(),
		},
		nil,
	)
	assert.Nil(b, err)

	storage := newObjectStorageSemaphore(
		diskStorage,
		1024,
	)

	err = storage.Write(context.Background(), "foo", bytes.NewReader([]byte("foo")), 3, nil)
	assert.Nil(b, err)

	b.ResetTimer()
	for range b.N {
		r, err := storage.Read(context.Background(), "foo", nil, nil)
		assert.Nil(b, err)
		_, err = io.Copy(io.Discard, r)
		assert.Nil(b, err)
		r.Close()
	}
}
