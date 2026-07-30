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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			t.Cleanup(func() {
				cmd.Process.Kill()
			})

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
			if err != nil {
				t.Fatal(err)
			}
			return fs
		})
	})

}

func TestMinioPutObjectPhysicalAccounting(t *testing.T) {
	var fail bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = w.Write([]byte(`<LocationConstraint>us-east-1</LocationConstraint>`))
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		if fail {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`<Error><Code>InvalidRequest</Code><Message>rejected</Message></Error>`))
			return
		}
		w.Header().Set("ETag", "etag")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	client, err := minio.New(endpoint.Host, &minio.Options{
		Creds:     credentials.NewStaticV4("id", "secret", ""),
		Secure:    false,
		Transport: server.Client().Transport,
	})
	require.NoError(t, err)
	sdk := &MinioSDK{bucket: "bucket", client: client}

	data := []byte("accepted")
	size := int64(len(data))
	counter := new(perfcounter.CounterSet)
	ctx := perfcounter.WithCounterSet(context.Background(), counter)
	_, err = sdk.putObject(ctx, "object", bytes.NewReader(data), &size, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), counter.FileService.S3.Put.Load())
	require.Equal(t, size, counter.FileService.S3WriteSize.Load())

	fail = true
	failed := new(perfcounter.CounterSet)
	ctx = perfcounter.WithCounterSet(context.Background(), failed)
	_, err = sdk.putObject(ctx, "object", bytes.NewReader(data), &size, nil)
	require.Error(t, err)
	require.Equal(t, int64(1), failed.FileService.S3.Put.Load())
	require.Zero(t, failed.FileService.S3WriteSize.Load())
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
	var lastErr error
	for i := 0; i < 30; i++ {
		endpoint := "localhost:9007"
		client, err := minio.New(endpoint, &minio.Options{
			Creds: credentials.NewStaticV4("minioadmin", "minioadmin", ""),
		})
		if err != nil {
			return nil, err
		}
		lastErr = client.MakeBucket(context.Background(), "test", minio.MakeBucketOptions{})
		if lastErr != nil {
			logutil.Warn("minio error", zap.Any("error", lastErr))
			time.Sleep(time.Second)
			continue
		}
		break
	}
	if lastErr != nil {
		cmd.Process.Kill()
		return nil, lastErr
	}

	return cmd, nil
}

func TestMinioSDKRoleARN(t *testing.T) {
	_, err := NewMinioSDK(
		context.Background(),
		ObjectStorageArguments{
			Endpoint:           "http://localhost",
			RoleARN:            "abc",
			NoBucketValidation: true,
		},
		nil,
	)
	assert.Nil(t, err)
}

func TestMinioSDKTianYiYun(t *testing.T) {
	_, err := NewMinioSDK(
		context.Background(),
		ObjectStorageArguments{
			Endpoint:           "http://ctyunapi.cn",
			NoBucketValidation: true,
		},
		nil,
	)
	assert.Nil(t, err)
}
