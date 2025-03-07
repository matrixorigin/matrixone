// Copyright 2025 Matrix Origin
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
	"testing"
)

func TestHDFS(t *testing.T) {
	type Case struct {
		Addr string
		User string
		Path string
	}

	cases := []Case{
		{
			Addr: "hdfs://hadoop-namenode.hadoop.svc.cluster.local:9820",
			User: "root",
			Path: "/user/root/mo-test/",
		},
		{
			Addr: "hdfs://hadoop-namenode.hadoop.svc.cluster.local:9820",
			User: "runner",
			Path: "/user/runner/mo-test/",
		},
	}

	for _, kase := range cases {
		t.Run(fmt.Sprintf("%+v", kase), func(t *testing.T) {
			_, err := NewHDFS(
				context.Background(),
				ObjectStorageArguments{
					Endpoint: kase.Addr,
					User:     kase.User,
					Bucket:   kase.Path,
				},
				nil,
			)
			if err != nil {
				t.Skip(err.Error())
			}

			// test object storage
			testObjectStorage(
				t,
				"HDFS",
				func(t *testing.T) *HDFS {
					fs, err := NewHDFS(
						context.Background(),
						ObjectStorageArguments{
							Endpoint: kase.Addr,
							User:     kase.User,
							Bucket:   kase.Path,
						},
						nil,
					)
					if err != nil {
						t.Fatal(err)
					}
					return fs
				},
			)

			// test fileservice
			testFileService(t, 0, func(name string) FileService {
				fs, err := NewS3FS(
					context.Background(),
					ObjectStorageArguments{
						Name:      name,
						Endpoint:  kase.Addr,
						User:      kase.User,
						Bucket:    kase.Path,
						KeyPrefix: fmt.Sprintf("%v", rand.Int64()),
					},
					DisabledCacheConfig,
					nil,
					false,
					true,
				)
				if err != nil {
					t.Fatal(err)
				}
				return fs
			})

		})
	}

}
