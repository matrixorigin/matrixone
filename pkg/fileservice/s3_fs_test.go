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
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
)

type _TestS3Config struct {
	Endpoint  string `json:"s3-test-endpoint"`
	Region    string `json:"s3-test-region"`
	APIKey    string `json:"s3-test-key"`
	APISecret string `json:"s3-test-secret"`
	Bucket    string `json:"s3-test-bucket"`
}

func loadS3TestConfig() (config _TestS3Config, err error) {

	// load from s3.json
	content, err := os.ReadFile("s3.json")
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		} else {
			return config, err
		}
	}
	if len(content) > 0 {
		err := json.Unmarshal(content, &config)
		if err != nil {
			return config, err
		}
	}

	// load from env
	loadEnv := func(name string, ptr *string) {
		if *ptr != "" {
			return
		}
		if value := os.Getenv(name); value != "" {
			*ptr = value
		}
	}
	loadEnv("endpoint", &config.Endpoint)
	loadEnv("region", &config.Region)
	loadEnv("apikey", &config.APIKey)
	loadEnv("apisecret", &config.APISecret)
	loadEnv("bucket", &config.Bucket)

	return
}

func TestS3FS(t *testing.T) {
	config, err := loadS3TestConfig()
	assert.Nil(t, err)
	if config.Endpoint == "" {
		// no config
		t.Skip()
	}

	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)

	t.Run("file service", func(t *testing.T) {
		cacheDir := t.TempDir()
		testFileService(t, func(name string) FileService {

			fs, err := NewS3FS(
				"",
				name,
				config.Endpoint,
				config.Bucket,
				time.Now().Format("2006-01-02.15:04:05.000000"),
				128*1024,
				128*1024,
				cacheDir,
			)
			assert.Nil(t, err)

			return fs
		})
	})

	t.Run("list root", func(t *testing.T) {
		cacheDir := t.TempDir()
		fs, err := NewS3FS(
			"",
			"s3",
			config.Endpoint,
			config.Bucket,
			"",
			128*1024,
			128*1024,
			cacheDir,
		)
		assert.Nil(t, err)
		ctx := context.Background()
		entries, err := fs.List(ctx, "")
		assert.Nil(t, err)
		assert.True(t, len(entries) > 0)
	})

	t.Run("caching file service", func(t *testing.T) {
		cacheDir := t.TempDir()
		testCachingFileService(t, func() CachingFileService {
			fs, err := NewS3FS(
				"",
				"s3",
				config.Endpoint,
				config.Bucket,
				time.Now().Format("2006-01-02.15:04:05.000000"),
				128*1024,
				128*1024,
				cacheDir,
			)
			assert.Nil(t, err)
			return fs
		})
	})

}

func TestDynamicS3(t *testing.T) {
	config, err := loadS3TestConfig()
	assert.Nil(t, err)
	if config.Endpoint == "" {
		// no config
		t.Skip()
	}
	testFileService(t, func(name string) FileService {
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		err := w.Write([]string{
			"s3",
			config.Endpoint,
			config.Region,
			config.Bucket,
			config.APIKey,
			config.APISecret,
			time.Now().Format("2006-01-02.15:04:05.000000"),
			name,
		})
		assert.Nil(t, err)
		w.Flush()
		fs, path, err := GetForETL(nil, JoinPath(
			buf.String(),
			"foo/bar/baz",
		))
		assert.Nil(t, err)
		assert.Equal(t, path, "foo/bar/baz")
		return fs
	})
}

func TestDynamicS3NoKey(t *testing.T) {
	config, err := loadS3TestConfig()
	assert.Nil(t, err)
	if config.Endpoint == "" {
		// no config
		t.Skip()
	}
	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)
	testFileService(t, func(name string) FileService {
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		err := w.Write([]string{
			"s3-no-key",
			config.Endpoint,
			config.Region,
			config.Bucket,
			time.Now().Format("2006-01-02.15:04:05.000000"),
			name,
		})
		assert.Nil(t, err)
		w.Flush()
		fs, path, err := GetForETL(nil, JoinPath(
			buf.String(),
			"foo/bar/baz",
		))
		assert.Nil(t, err)
		assert.Equal(t, path, "foo/bar/baz")
		return fs
	})
}

func TestDynamicS3Opts(t *testing.T) {
	config, err := loadS3TestConfig()
	assert.Nil(t, err)
	if config.Endpoint == "" {
		// no config
		t.Skip()
	}
	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)
	testFileService(t, func(name string) FileService {
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		err := w.Write([]string{
			"s3-opts",
			"endpoint=" + config.Endpoint,
			"region=" + config.Region,
			"bucket=" + config.Bucket,
			"prefix=" + time.Now().Format("2006-01-02.15:04:05.000000"),
			"name=" + name,
		})
		assert.Nil(t, err)
		w.Flush()
		fs, path, err := GetForETL(nil, JoinPath(
			buf.String(),
			"foo/bar/baz",
		))
		assert.Nil(t, err)
		assert.Equal(t, path, "foo/bar/baz")
		return fs
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
	t.Setenv("AWS_REGION", "test")
	t.Setenv("AWS_ACCESS_KEY_ID", "minioadmin")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

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
					_ = options
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
		cacheDir := t.TempDir()
		testFileService(t, func(name string) FileService {

			fs, err := NewS3FSOnMinio(
				"",
				name,
				endpoint,
				"test",
				time.Now().Format("2006-01-02.15:04:05.000000"),
				128*1024,
				128*1024,
				cacheDir,
			)
			assert.Nil(t, err)

			return fs
		})
	})

}

func BenchmarkS3FS(b *testing.B) {
	config, err := loadS3TestConfig()
	assert.Nil(b, err)
	if config.Endpoint == "" {
		// no config
		b.Skip()
	}

	b.Setenv("AWS_REGION", config.Region)
	b.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	b.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)

	cacheDir := b.TempDir()

	b.ResetTimer()

	benchmarkFileService(b, func() FileService {
		fs, err := NewS3FS(
			"",
			"s3",
			config.Endpoint,
			config.Bucket,
			time.Now().Format("2006-01-02.15:04:05.000000"),
			128*1024,
			128*1024,
			cacheDir,
		)
		assert.Nil(b, err)
		return fs
	})
}
