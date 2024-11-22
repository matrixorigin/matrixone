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
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
)

func TestQCloudSDK(t *testing.T) {

	t.Run("object storage", func(t *testing.T) {
		for _, args := range objectStorageArgumentsForTest("test", t) {
			if !strings.Contains(args.Endpoint, "myqcloud") {
				continue
			}

			t.Run(fmt.Sprintf("%s %s", args.Name, args.Bucket), func(t *testing.T) {

				testObjectStorage(t, "qcloud", func(t *testing.T) *QCloudSDK {
					args.KeyPrefix = fmt.Sprintf("%v", rand.Int64())
					ret, err := NewQCloudSDK(
						context.Background(),
						args,
						nil,
					)
					if err != nil {
						t.Fatal(err)
					}
					return ret
				})

			})
		}
	})

	t.Run("file service", func(t *testing.T) {
		for _, args := range objectStorageArgumentsForTest("test", t) {
			if !strings.Contains(args.Endpoint, "myqcloud") {
				continue
			}

			t.Run(fmt.Sprintf("%s %s", args.Name, args.Bucket), func(t *testing.T) {

				t.Run("file service", func(t *testing.T) {
					testFileService(t, 0, func(name string) FileService {
						args.Name = name
						args.KeyPrefix = fmt.Sprintf("%v", rand.Int64())
						ret, err := NewS3FS(
							context.Background(),
							args,
							DisabledCacheConfig,
							nil,
							true,
							true,
						)
						if err != nil {
							t.Fatal(err)
						}
						return ret
					})
				})

			})
		}
	})

}
