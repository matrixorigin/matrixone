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
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	hdfsDefaultNamenodeAddr = "hdfs://hadoop-namenode.hadoop.svc.cluster.local:9820"
	hdfsNamenodeAddrEnv     = "HDFS_NAMENODE_ADDR"
	hdfsNamenodeDialTimeout = 5 * time.Second
)

func TestHDFS(t *testing.T) {
	type Case struct {
		Addr string
		User string
		Path string
	}

	addr := getHDFSTestAddr(t)

	cases := []Case{
		{
			Addr: addr,
			User: "root",
			Path: "user/root/mo-test/",
		},
		{
			Addr: addr,
			User: "runner",
			Path: "user/runner/mo-test/",
		},
	}

	for _, kase := range cases {
		t.Run(kase.Addr, func(t *testing.T) {
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
							Endpoint: strings.TrimPrefix(kase.Addr, "hdfs://"),
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

			// test GetForETL
			endpoint := kase.Addr + "?user=" + kase.User
			_, _, err = GetForETL(
				context.Background(),
				nil,
				"hdfs,is-hdfs=true,endpoint="+endpoint+",bucket="+kase.Path+":/foo/bar/baz",
			)
			if err != nil {
				t.Fatal(err)
			}

			// bad path
			_, err = NewHDFS(
				context.Background(),
				ObjectStorageArguments{
					Endpoint: kase.Addr,
					User:     kase.User,
					Bucket:   "..:..",
				},
				nil,
			)
			if err == nil {
				t.Fatal("should fail")
			}

		})
	}

}

func getHDFSTestAddr(t *testing.T) string {
	t.Helper()

	addr := os.Getenv(hdfsNamenodeAddrEnv)
	if addr == "" {
		addr = hdfsDefaultNamenodeAddr
	}

	if !strings.HasPrefix(addr, "hdfs://") {
		addr = "hdfs://" + addr
	}

	u, err := url.Parse(addr)
	if err != nil {
		t.Fatalf("invalid %s: %v", hdfsNamenodeAddrEnv, err)
	}
	if u.Host == "" {
		t.Fatalf("invalid %s: missing namenode host", hdfsNamenodeAddrEnv)
	}

	conn, err := net.DialTimeout("tcp", u.Host, hdfsNamenodeDialTimeout)
	if err != nil {
		t.Skipf("HDFS namenode %q is unavailable: %v", u.Host, err)
	}
	if conn != nil {
		_ = conn.Close()
	}

	return addr
}
