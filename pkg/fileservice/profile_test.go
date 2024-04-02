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
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProfile(t *testing.T) {
	write, stop := FSProfileHandler.StartProfile()
	defer stop()
	defer write(io.Discard)
	testFileService(t, 0, func(name string) FileService {
		ctx := context.Background()
		dir := t.TempDir()
		fs, err := NewLocalFS(ctx, name, dir, DisabledCacheConfig, nil)
		assert.Nil(t, err)
		return fs
	})
}

func BenchmarkNoProfileAddSample(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FSProfileHandler.AddSample(1)
	}
}

func BenchmarkProfileAddSample(b *testing.B) {
	write, stop := FSProfileHandler.StartProfile()
	defer stop()
	defer write(io.Discard)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FSProfileHandler.AddSample(1)
	}
}

func BenchmarkProfileWrite(b *testing.B) {
	write, stop := FSProfileHandler.StartProfile()
	defer stop()
	FSProfileHandler.AddSample(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		write(io.Discard)
	}
}
