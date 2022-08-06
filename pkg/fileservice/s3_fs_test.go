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
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
)

type _TestS3Config struct {
	Endpoint  string
	Region    string
	APIKey    string
	APISecret string
	Bucket    string
	KeyPrefix string
}

func TestS3FS(t *testing.T) {

	var config _TestS3Config
	content, err := os.ReadFile("s3.json")
	if os.IsNotExist(err) {
		// no s3.json, skip tests
		return
	}
	assert.Nil(t, err)
	err = json.Unmarshal(content, &config)
	assert.Nil(t, err)

	os.Setenv("AWS_REGION", config.Region)
	os.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)

	t.Run("file service", func(t *testing.T) {
		testFileService(t, func() FileService {

			fs, err := NewS3FS(
				config.Endpoint,
				config.Bucket,
				time.Now().Format("2006-01-02.15:04:05.000000"),
				128*1024,
			)
			assert.Nil(t, err)

			return fs
		})
	})

	t.Run("list root", func(t *testing.T) {
		fs, err := NewS3FS(
			config.Endpoint,
			config.Bucket,
			"",
			128*1024,
		)
		assert.Nil(t, err)
		ctx := context.Background()
		entries, err := fs.List(ctx, "")
		assert.Nil(t, err)
		assert.True(t, len(entries) > 0)
	})

	t.Run("caching file service", func(t *testing.T) {
		testCachingFileService(t, func() CachingFileService {
			fs, err := NewS3FS(
				config.Endpoint,
				config.Bucket,
				time.Now().Format("2006-01-02.15:04:05.000000"),
				128*1024,
			)
			assert.Nil(t, err)
			return fs
		})
	})

}

func TestS3FSMinioServer(t *testing.T) {

	// find minio executable
	exePath, err := exec.LookPath("minio")
	if errors.Is(err, exec.ErrNotFound) {
		// minio not found in machine
		return
	}

	// start minio
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(ctx,
		exePath,
		"server",
		t.TempDir(),
		//"--certs-dir", filepath.Join("testdata", "minio-certs"),
	)
	cmd.Env = append(os.Environ(),
		"MINIO_SITE_NAME=test",
		"MINIO_SITE_REGION=test",
	)
	//cmd.Stderr = os.Stderr
	//cmd.Stdout = os.Stdout
	err = cmd.Start()
	assert.Nil(t, err)

	// set s3 credentials
	os.Setenv("AWS_REGION", "test")
	os.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

	endpoint := "http://localhost:9000"

	// create bucket
	ctx, cancel = context.WithTimeout(ctx, time.Second*59)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	assert.Nil(t, err)
	client := s3.NewFromConfig(cfg,
		s3.WithEndpointResolver(
			s3.EndpointResolverFunc(
				func(
					region string,
					options s3.EndpointResolverOptions,
				) (
					ep aws.Endpoint,
					err error,
				) {
					ep.URL = endpoint
					ep.Source = aws.EndpointSourceCustom
					ep.HostnameImmutable = true
					ep.SigningRegion = region
					return
				},
			),
		),
	)
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: ptrTo("test"),
	})
	assert.Nil(t, err)

	// run test
	t.Run("file service", func(t *testing.T) {
		testFileService(t, func() FileService {

			fs, err := NewS3FSOnMinio(
				endpoint,
				"test",
				time.Now().Format("2006-01-02.15:04:05.000000"),
				128*1024,
			)
			assert.Nil(t, err)

			return fs
		})
	})

}

func BenchmarkS3FS(b *testing.B) {

	var config _TestS3Config
	content, err := os.ReadFile("s3.json")
	if os.IsNotExist(err) {
		// no s3.json, skip
		return
	}
	assert.Nil(b, err)
	err = json.Unmarshal(content, &config)
	assert.Nil(b, err)

	os.Setenv("AWS_REGION", config.Region)
	os.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	os.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)

	b.ResetTimer()

	benchmarkFileService(b, func() FileService {
		fs, err := NewS3FS(
			config.Endpoint,
			config.Bucket,
			time.Now().Format("2006-01-02.15:04:05.000000"),
			128*1024,
		)
		assert.Nil(b, err)
		return fs
	})
}
