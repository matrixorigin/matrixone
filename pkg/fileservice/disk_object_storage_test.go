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
	"context"
	"testing"
)

func TestDiskObjectStorage(t *testing.T) {
	ctx := context.Background()

	testFileService(t, 0, func(name string) FileService {
		dir := t.TempDir()
		fs, err := NewS3FS(
			ctx,
			ObjectStorageArguments{
				Name:     name,
				Endpoint: "disk",
				Bucket:   dir,
			},
			DisabledCacheConfig,
			//CacheConfig{
			//	MemoryCapacity: ptrTo(toml.ByteSize(1 << 30)),
			//	DiskPath:       ptrTo(diskCacheDir),
			//	DiskCapacity:   ptrTo(toml.ByteSize(1 << 30)),
			//},
			nil,
			false,
			true,
		)
		if err != nil {
			t.Fatal(err)
		}
		return fs
	})

}
