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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProfile(t *testing.T) {
	stop := FSProfileHandler.StartProfile(io.Discard)
	defer stop()
	testFileService(t, func(name string) FileService {
		dir := t.TempDir()
		fs, err := NewLocalFS(name, dir, -1)
		assert.Nil(t, err)
		return fs
	})
}

func BenchmarkNoProfileAddSample(b *testing.B) {
	for i := 0; i < b.N; i++ {
		FSProfileHandler.AddSample()
	}
}

func BenchmarkProfileAddSample(b *testing.B) {
	stop := FSProfileHandler.StartProfile(io.Discard)
	defer stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FSProfileHandler.AddSample()
	}
}
