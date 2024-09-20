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
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http/httptrace"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type _TestS3Config struct {
	Endpoint  string `json:"s3-test-endpoint"`
	Region    string `json:"s3-test-region"`
	APIKey    string `json:"s3-test-key"`
	APISecret string `json:"s3-test-secret"`
	Bucket    string `json:"s3-test-bucket"`
	RoleARN   string `json:"role-arn"`
}

func loadS3TestConfig(t testing.TB) (config _TestS3Config, err error) {

	defer func() {
		// default to disk S3
		if config.Endpoint == "" {
			config.Endpoint = "disk"
			config.Bucket = t.TempDir()
		}
	}()

	// load from s3.json
	content, err := os.ReadFile("s3.json")
	if err == nil {
		if len(content) > 0 {
			if err := json.Unmarshal(content, &config); err == nil {
				return config, nil
			}
		}
	}
	err = nil // ignore errors

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

func TestS3FS(
	t *testing.T,
) {
	t.Run("default policy", func(t *testing.T) {
		testS3FS(t, 0)
	})
	t.Run("skip full file preloads", func(t *testing.T) {
		testS3FS(t, SkipFullFilePreloads)
	})
}

func testS3FS(
	t *testing.T,
	policy Policy,
) {
	config, err := loadS3TestConfig(t)
	assert.Nil(t, err)

	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)

	t.Run("file service", func(t *testing.T) {
		testFileService(t, policy, func(name string) FileService {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				ObjectStorageArguments{
					Name:      name,
					Endpoint:  config.Endpoint,
					Bucket:    config.Bucket,
					KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
					RoleARN:   config.RoleARN,
				},
				DisabledCacheConfig,
				nil,
				true,
				false,
			)
			assert.Nil(t, err)

			// to test continuation
			switch storage := fs.storage.(type) {
			case *AwsSDKv2:
				storage.listMaxKeys = 5
			case *AwsSDKv1:
				storage.listMaxKeys = 5
			}

			return fs
		})
	})

	t.Run("list root", func(t *testing.T) {
		ctx := context.Background()
		fs, err := NewS3FS(
			ctx,
			ObjectStorageArguments{
				Name:      "s3",
				Endpoint:  config.Endpoint,
				Bucket:    config.Bucket,
				RoleARN:   config.RoleARN,
				KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
			},
			DisabledCacheConfig,
			nil,
			true,
			false,
		)
		assert.Nil(t, err)
		var counterSet, counterSet2 perfcounter.CounterSet
		ctx = perfcounter.WithCounterSet(ctx, &counterSet)
		ctx = perfcounter.WithCounterSet(ctx, &counterSet2)

		err = fs.Write(ctx, IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Data: []byte("foo"),
					Size: 3,
				},
			},
		})
		assert.Nil(t, err)

		entries, err := fs.List(ctx, "")
		assert.Nil(t, err)
		assert.True(t, len(entries) > 0)
		assert.True(t, counterSet.FileService.S3.List.Load() > 0)
		assert.True(t, counterSet2.FileService.S3.List.Load() > 0)
	})

	t.Run("mem caching file service", func(t *testing.T) {
		testCachingFileService(t, func() CachingFileService {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				ObjectStorageArguments{
					Name:      "s3",
					Endpoint:  config.Endpoint,
					Bucket:    config.Bucket,
					KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
					RoleARN:   config.RoleARN,
				},
				CacheConfig{
					MemoryCapacity: ptrTo[toml.ByteSize](128 * 1024),
				},
				nil,
				false,
				false,
			)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("disk caching file service", func(t *testing.T) {
		testCachingFileService(t, func() CachingFileService {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				ObjectStorageArguments{
					Name:      "s3",
					Endpoint:  config.Endpoint,
					Bucket:    config.Bucket,
					KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
					RoleARN:   config.RoleARN,
				},
				CacheConfig{
					MemoryCapacity: ptrTo[toml.ByteSize](1),
					DiskCapacity:   ptrTo[toml.ByteSize](128 * 1024),
					DiskPath:       ptrTo(t.TempDir()),
				},
				nil,
				false,
				false,
			)
			assert.Nil(t, err)
			return fs
		})
	})

	t.Run("mem and disk caching file service", func(t *testing.T) {
		testCachingFileService(t, func() CachingFileService {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				ObjectStorageArguments{
					Name:      "s3",
					Endpoint:  config.Endpoint,
					Bucket:    config.Bucket,
					KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
					RoleARN:   config.RoleARN,
				},
				CacheConfig{
					MemoryCapacity: ptrTo[toml.ByteSize](128 * 1024),
					DiskCapacity:   ptrTo[toml.ByteSize](128 * 1024),
					DiskPath:       ptrTo(t.TempDir()),
				},
				nil,
				false,
				false,
			)
			assert.Nil(t, err)
			return fs
		})
	})

}

func TestDynamicS3(t *testing.T) {
	ctx := context.Background()
	config, err := loadS3TestConfig(t)
	assert.Nil(t, err)

	testFileService(t, 0, func(name string) FileService {
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
		fs, path, err := GetForETL(ctx, nil, JoinPath(
			buf.String(),
			"foo/bar/baz",
		))
		assert.Nil(t, err)
		assert.Equal(t, path, "foo/bar/baz")
		return fs
	})
}

func TestDynamicS3NoKey(t *testing.T) {
	ctx := context.Background()
	config, err := loadS3TestConfig(t)
	assert.Nil(t, err)

	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)
	testFileService(t, 0, func(name string) FileService {
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
		fs, path, err := GetForETL(ctx, nil, JoinPath(
			buf.String(),
			"foo/bar/baz",
		))
		assert.Nil(t, err)
		assert.Equal(t, path, "foo/bar/baz")
		return fs
	})
}

func TestDynamicS3Opts(t *testing.T) {
	ctx := context.Background()
	config, err := loadS3TestConfig(t)
	assert.Nil(t, err)

	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)
	testFileService(t, 0, func(name string) FileService {
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
		fs, path, err := GetForETL(ctx, nil, JoinPath(
			buf.String(),
			"foo/bar/baz",
		))
		assert.Nil(t, err)
		assert.Equal(t, path, "foo/bar/baz")
		return fs
	})
}

func TestDynamicS3OptsRoleARN(t *testing.T) {
	ctx := context.Background()
	config, err := loadS3TestConfig(t)
	assert.Nil(t, err)

	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)
	testFileService(t, 0, func(name string) FileService {
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		err := w.Write([]string{
			"s3-opts",
			"endpoint=" + config.Endpoint,
			"bucket=" + config.Bucket,
			"prefix=" + time.Now().Format("2006-01-02.15:04:05.000000"),
			"name=" + name,
			"role-arn=" + config.RoleARN,
		})
		assert.Nil(t, err)
		w.Flush()
		fs, path, err := GetForETL(ctx, nil, JoinPath(
			buf.String(),
			"foo/bar/baz",
		))
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, path, "foo/bar/baz")
		return fs
	})
}

func TestDynamicS3OptsNoRegion(t *testing.T) {
	ctx := context.Background()
	config, err := loadS3TestConfig(t)
	assert.Nil(t, err)

	// only AWS supports this
	if !strings.Contains(config.Endpoint, "amazonaws") {
		t.Skip()
	}

	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)
	testFileService(t, 0, func(name string) FileService {
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		err := w.Write([]string{
			"s3-opts",
			"bucket=" + config.Bucket,
			"prefix=" + time.Now().Format("2006-01-02.15:04:05.000000"),
			"name=" + name,
			"role-arn=" + config.RoleARN,
		})
		assert.Nil(t, err)
		w.Flush()
		fs, path, err := GetForETL(ctx, nil, JoinPath(
			buf.String(),
			"foo/bar/baz",
		))
		if err != nil {
			t.Fatal(err)
		}
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
		testFileService(t, 0, func(name string) FileService {
			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				ObjectStorageArguments{
					Name:      name,
					Endpoint:  endpoint,
					Bucket:    "test",
					KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
					IsMinio:   true,
				},
				CacheConfig{
					DiskPath: ptrTo(cacheDir),
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

func BenchmarkS3FS(b *testing.B) {
	config, err := loadS3TestConfig(b)
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

	ctx := context.Background()
	benchmarkFileService(ctx, b, func() FileService {
		fs, err := NewS3FS(
			ctx,
			ObjectStorageArguments{
				Name:      "s3",
				Endpoint:  config.Endpoint,
				Bucket:    config.Bucket,
				KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
				RoleARN:   config.RoleARN,
			},
			CacheConfig{
				DiskPath: ptrTo(cacheDir),
			},
			nil,
			true,
			false,
		)
		assert.Nil(b, err)
		return fs
	})
}

func TestS3FSWithSubPath(t *testing.T) {
	config, err := loadS3TestConfig(t)
	assert.Nil(t, err)

	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)

	testFileService(t, 0, func(name string) FileService {
		ctx := context.Background()
		fs, err := NewS3FS(
			ctx,
			ObjectStorageArguments{
				Name:      name,
				Endpoint:  config.Endpoint,
				Bucket:    config.Bucket,
				KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
				RoleARN:   config.RoleARN,
			},
			DisabledCacheConfig,
			nil,
			true,
			false,
		)
		assert.Nil(t, err)
		return SubPath(fs, "foo/")
	})

}

func BenchmarkS3ConcurrentRead(b *testing.B) {
	config, err := loadS3TestConfig(b)
	if err != nil {
		b.Fatal(err)
	}
	if config.Endpoint == "" {
		// no config
		b.Skip()
	}
	b.Setenv("AWS_REGION", config.Region)
	b.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	b.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)

	var numRead atomic.Int64
	var numGotConn, numReuse, numConnect atomic.Int64
	var numTLSHandshake atomic.Int64
	ctx := context.Background()
	trace := &httptrace.ClientTrace{

		GetConn: func(hostPort string) {
			//fmt.Printf("get conn: %s\n", hostPort)
		},

		GotConn: func(info httptrace.GotConnInfo) {
			numGotConn.Add(1)
			if info.Reused {
				numReuse.Add(1)
			}
			//fmt.Printf("got conn: %+v\n", info)
		},

		PutIdleConn: func(err error) {
			//if err != nil {
			//	fmt.Printf("put idle conn failed: %v\n", err)
			//}
		},

		ConnectStart: func(network, addr string) {
			numConnect.Add(1)
			//fmt.Printf("connect %v %v\n", network, addr)
		},

		TLSHandshakeStart: func() {
			numTLSHandshake.Add(1)
		},
	}

	ctx = httptrace.WithClientTrace(ctx, trace)
	defer func() {
		fmt.Printf("read %v, got %v conns, reuse %v, connect %v, tls handshake %v\n",
			numRead.Load(),
			numGotConn.Load(),
			numReuse.Load(),
			numConnect.Load(),
			numTLSHandshake.Load(),
		)
	}()

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "bench",
			Endpoint:  config.Endpoint,
			Bucket:    config.Bucket,
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
			RoleARN:   config.RoleARN,
		},
		DisabledCacheConfig,
		nil,
		true,
		false,
	)
	if err != nil {
		b.Fatal(err)
	}
	if fs == nil {
		b.Fatal(err)
	}

	vector := IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	}
	err = fs.Write(ctx, vector)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		sem := make(chan struct{}, 128)
		for pb.Next() {
			sem <- struct{}{}
			go func() {
				defer func() {
					<-sem
				}()
				err := fs.Read(ctx, &IOVector{
					FilePath: "foo",
					Entries: []IOEntry{
						{
							Size: 3,
						},
					},
				})
				if err != nil {
					panic(err)
				}
				numRead.Add(1)
			}()
		}
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}
	})

}

func TestSequentialS3Read(t *testing.T) {
	config, err := loadS3TestConfig(t)
	if err != nil {
		t.Fatal(err)
	}

	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)

	var numRead atomic.Int64
	var numGotConn, numReuse, numConnect atomic.Int64
	var numTLSHandshake atomic.Int64
	ctx := context.Background()
	trace := &httptrace.ClientTrace{

		GetConn: func(hostPort string) {
			fmt.Printf("get conn: %s\n", hostPort)
		},

		GotConn: func(info httptrace.GotConnInfo) {
			numGotConn.Add(1)
			if info.Reused {
				numReuse.Add(1)
			} else {
				fmt.Printf("got conn not reuse: %+v\n", info)
			}
		},

		PutIdleConn: func(err error) {
			if err != nil {
				fmt.Printf("put idle conn failed: %v\n", err)
			}
		},

		ConnectDone: func(network string, addr string, err error) {
			numConnect.Add(1)
			fmt.Printf("connect done: %v %v\n", network, addr)
			if err != nil {
				fmt.Printf("connect error: %v\n", err)
			}
		},

		TLSHandshakeStart: func() {
			numTLSHandshake.Add(1)
		},
	}

	ctx = httptrace.WithClientTrace(ctx, trace)
	defer func() {
		fmt.Printf("read %v, got %v conns, reuse %v, connect %v, tls handshake %v\n",
			numRead.Load(),
			numGotConn.Load(),
			numReuse.Load(),
			numConnect.Load(),
			numTLSHandshake.Load(),
		)
	}()

	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "bench",
			Endpoint:  config.Endpoint,
			Bucket:    config.Bucket,
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
			RoleARN:   config.RoleARN,
		},
		DisabledCacheConfig,
		nil,
		true,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if fs == nil {
		t.Fatal(err)
	}

	vector := IOVector{
		FilePath: "foo",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	}
	err = fs.Write(ctx, vector)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 128; i++ {
		err := fs.Read(ctx, &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: 3,
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		numRead.Add(1)
	}

}

func TestS3RestoreFromCache(t *testing.T) {
	t.Skip("no longer valid since we delete cache files when calling Delete")
	ctx := context.Background()

	config, err := loadS3TestConfig(t)
	assert.Nil(t, err)

	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)

	cacheDir := t.TempDir()
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  config.Endpoint,
			Bucket:    config.Bucket,
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
			RoleARN:   config.RoleARN,
		},
		CacheConfig{
			DiskPath: ptrTo(cacheDir),
		},
		nil,
		false,
		false,
	)
	assert.Nil(t, err)

	// write file
	err = fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
	})
	assert.Nil(t, err)

	// write file without full file cache
	err = fs.Write(ctx, IOVector{
		FilePath: "quux",
		Entries: []IOEntry{
			{
				Size: 3,
				Data: []byte("foo"),
			},
		},
		Policy: SkipFullFilePreloads,
	})
	assert.Nil(t, err)
	err = fs.Read(ctx, &IOVector{
		FilePath: "quux",
		Entries: []IOEntry{
			{
				Size: 3,
			},
		},
	})
	assert.Nil(t, err)

	err = fs.Delete(ctx, "foo/bar")
	assert.Nil(t, err)

	logutil.Info("cache dir", zap.Any("dir", cacheDir))

	counterSet := new(perfcounter.CounterSet)
	ctx = perfcounter.WithCounterSet(ctx, counterSet)
	fs.restoreFromDiskCache(ctx)

	if n := counterSet.FileService.S3.Put.Load(); n != 1 {
		t.Fatalf("got %v", n)
	}

	vec := &IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: -1,
			},
		},
	}
	err = fs.Read(ctx, vec)
	assert.Nil(t, err)
	assert.Equal(t, []byte("foo"), vec.Entries[0].Data)

}

func TestS3PrefetchFile(t *testing.T) {
	ctx := context.Background()
	var pcSet perfcounter.CounterSet
	ctx = perfcounter.WithCounterSet(ctx, &pcSet)

	config, err := loadS3TestConfig(t)
	assert.Nil(t, err)

	t.Setenv("AWS_REGION", config.Region)
	t.Setenv("AWS_ACCESS_KEY_ID", config.APIKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", config.APISecret)

	cacheDir := t.TempDir()
	fs, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Name:      "s3",
			Endpoint:  config.Endpoint,
			Bucket:    config.Bucket,
			KeyPrefix: time.Now().Format("2006-01-02.15:04:05.000000"),
			RoleARN:   config.RoleARN,
		},
		CacheConfig{
			DiskPath:     ptrTo(cacheDir),
			DiskCapacity: ptrTo[toml.ByteSize](1 << 30),
		},
		nil,
		false,
		false,
	)
	assert.Nil(t, err)

	data := bytes.Repeat([]byte("abcd"), 2<<20)

	// write file
	err = fs.Write(ctx, IOVector{
		FilePath: "foo/bar",
		Entries: []IOEntry{
			{
				Size: int64(len(data)),
				Data: data,
			},
		},
		Policy: SkipDiskCache | SkipMemoryCache,
	})
	assert.Nil(t, err)
	assert.Equal(t, int64(0), pcSet.FileService.Cache.Disk.WriteFile.Load())

	// preload
	err = fs.PrefetchFile(ctx, "foo/bar")
	assert.Nil(t, err)
	assert.Equal(t, int64(1), pcSet.FileService.Cache.Disk.WriteFile.Load())
	err = fs.PrefetchFile(ctx, "foo/bar")
	assert.Nil(t, err)
	assert.Equal(t, int64(1), pcSet.FileService.Cache.Disk.WriteFile.Load())

	// read
	lastHit := int64(0)
	for i := 1; i < len(data); i += len(data) / 1000 {
		vec := &IOVector{
			FilePath: "foo/bar",
			Entries: []IOEntry{
				{
					Size: int64(i),
				},
			},
		}
		err = fs.Read(ctx, vec)
		assert.Nil(t, err)
		assert.Equal(t, data[:i], vec.Entries[0].Data)
		assert.Equal(t, lastHit+1, pcSet.FileService.Cache.Disk.Hit.Load())
		vec.Release()
		lastHit++
	}

}

type S3CredentialTestCase struct {
	Skip bool
	ObjectStorageArguments
}

var s3CredentialTestCases = func() []S3CredentialTestCase {
	content, err := os.ReadFile("s3_fs_test_new.xml")
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		panic(err)
	}
	var spec struct {
		XMLName xml.Name               `xml:"Spec"`
		Cases   []S3CredentialTestCase `xml:"Case"`
	}
	err = xml.Unmarshal(content, &spec)
	if err != nil {
		panic(err)
	}
	return spec.Cases
}()

func TestNewS3FSFromSpec(t *testing.T) {
	if len(s3CredentialTestCases) == 0 {
		t.Skip("no case")
	}

	for _, kase := range s3CredentialTestCases {
		if kase.Skip {
			continue
		}

		t.Run(kase.Name, func(t *testing.T) {

			ctx := context.Background()
			fs, err := NewS3FS(
				ctx,
				kase.ObjectStorageArguments,
				DisabledCacheConfig,
				nil,
				true,
				false,
			)
			assert.Nil(t, err)
			_ = fs

		})

		t.Run(kase.Name+" bad bucket", func(t *testing.T) {

			args := kase.ObjectStorageArguments
			args.Bucket = args.Bucket + "foobarbaz"
			ctx := context.Background()
			_, err := NewS3FS(
				ctx,
				args,
				DisabledCacheConfig,
				nil,
				true,
				false,
			)
			if err == nil {
				t.Fatal("should fail")
			}

		})
	}

}

func TestNewS3NoDefaultCredential(t *testing.T) {
	ctx := context.Background()
	_, err := NewS3FS(
		ctx,
		ObjectStorageArguments{
			Endpoint: "aliyuncs.com",
		},
		DisabledCacheConfig,
		nil,
		true,
		true,
	)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	assert.True(t, strings.Contains(err.Error(), "no valid credentials"))
}
