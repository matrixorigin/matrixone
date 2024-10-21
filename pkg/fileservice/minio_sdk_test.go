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
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestMinioSDK(t *testing.T) {

	t.Run("object storage", func(t *testing.T) {
		testObjectStorage(t, "minio", func(t *testing.T) *MinioSDK {
			cmd, err := startMinio(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if cmd == nil {
				t.SkipNow()
			}
			defer cmd.Process.Kill()

			ret, err := NewMinioSDK(
				context.Background(),
				ObjectStorageArguments{
					Endpoint:  "http://localhost:9007",
					Bucket:    "test",
					KeyID:     "minioadmin",
					KeySecret: "minioadmin",
				},
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			return ret
		})
	})

	t.Run("file service", func(t *testing.T) {
		cmd, err := startMinio(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if cmd == nil {
			t.SkipNow()
		}
		defer cmd.Process.Kill()

		testFileService(t, 0, func(name string) FileService {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				ObjectStorageArguments{
					Name:      name,
					Endpoint:  "http://localhost:9007",
					Bucket:    "test",
					KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
					IsMinio:   true,
				},
				CacheConfig{
					DiskPath: ptrTo(t.TempDir()),
				},
				nil,
				true,
				false,
			)
			assert.Nil(t, err)
			return fs
		})
	})

}

func startMinio(dir string) (*exec.Cmd, error) {
	// find minio executable
	exePath, err := exec.LookPath("minio")
	if errors.Is(err, exec.ErrNotFound) {
		// minio not found in machine
		return nil, nil
	}

	// start minio
	cmd := exec.Command(
		exePath,
		"server",
		dir,
		"--address", "localhost:9007",
	)
	cmd.Env = append(os.Environ(),
		"MINIO_SITE_NAME=test",
	)
	//cmd.Stderr = os.Stderr
	//cmd.Stdout = os.Stdout
	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	// create bucket
	for {
		endpoint := "localhost:9007"
		client, err := minio.New(endpoint, &minio.Options{
			Creds: credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		})
		if err != nil {
			return nil, err
		}
		err = client.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
		if err != nil {
			logutil.Warn("minio error", zap.Any("error", err))
			time.Sleep(time.Second)
			continue
		}
		break
	}

	return cmd, nil
}
