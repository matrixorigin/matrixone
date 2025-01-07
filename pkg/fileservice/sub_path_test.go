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
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSubPathFS(t *testing.T) {

	t.Run("file service", func(t *testing.T) {
		testFileService(t, 0, func(name string) FileService {
			upstream, err := NewMemoryFS(name, DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return SubPath(upstream, "foo")
		})
	})

	t.Run("mutable file service", func(t *testing.T) {
		testMutableFileService(t, func() MutableFileService {
			upstream, err := NewLocalFS(context.Background(), "test", t.TempDir(), DisabledCacheConfig, nil)
			assert.Nil(t, err)
			return SubPath(upstream, "foo").(MutableFileService)
		})
	})

	t.Run("bad path", func(t *testing.T) {
		ctx := context.Background()
		fs, err := NewLocalFS(ctx, "test", t.TempDir(), DisabledCacheConfig, nil)
		assert.Nil(t, err)
		subFS := SubPath(fs, "foo").(MutableFileService)
		_, err = subFS.NewMutator(ctx, "~~")
		assert.NotNil(t, err)
	})

	t.Run("not mutable", func(t *testing.T) {
		fs, err := NewMemoryFS("test", DisabledCacheConfig, nil)
		assert.Nil(t, err)
		subFS := SubPath(fs, "foo").(MutableFileService)
		func() {
			defer func() {
				p := recover()
				if p == nil {
					t.Fatal("should panic")
				}
				msg := fmt.Sprintf("%v", p)
				if !strings.Contains(msg, "does not implement MutableFileService") {
					t.Fatalf("got %v", msg)
				}
			}()
			subFS.NewMutator(context.Background(), "foo")
		}()
	})

}
