// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/stretchr/testify/require"
)

func TestRunProxyAfterFileServiceInitialization(t *testing.T) {
	permanent := errors.New("storage unavailable")
	started := false
	err := runProxyAfterFileServiceInitialization(
		context.Background(),
		func(context.Context) (*fileservice.FileServices, error) {
			return nil, permanent
		},
		func(context.Context, *fileservice.FileServices) {
			started = true
		},
	)
	require.ErrorIs(t, err, permanent)
	require.False(t, started)

	initialized := false
	err = runProxyAfterFileServiceInitialization(
		context.Background(),
		func(context.Context) (*fileservice.FileServices, error) {
			initialized = true
			return new(fileservice.FileServices), nil
		},
		func(context.Context, *fileservice.FileServices) {
			require.True(t, initialized)
			started = true
		},
	)
	require.NoError(t, err)
	require.True(t, started)
}

func TestRunObservabilityTaskTreatsProxyStopAsCancellation(t *testing.T) {
	for _, stage := range []string{"trace", "metric"} {
		t.Run(stage, func(t *testing.T) {
			serviceStopper := stopper.NewStopper("proxy-observability-" + stage)
			outerStarted := make(chan struct{})
			taskErrC := make(chan error, 1)
			require.NoError(t, serviceStopper.RunNamedTask("proxy-service", func(ctx context.Context) {
				close(outerStarted)
				<-ctx.Done()
				taskErrC <- runObservabilityTask(
					metadata.ServiceType_PROXY,
					serviceStopper,
					stage,
					func(context.Context) {},
				)
			}))
			<-outerStarted

			stopped := make(chan struct{})
			go func() {
				serviceStopper.Stop()
				close(stopped)
			}()

			require.ErrorIs(t, <-taskErrC, context.Canceled)
			<-stopped
		})
	}
}

func TestRunObservabilityTaskPreservesUnavailableForNonProxy(t *testing.T) {
	serviceStopper := stopper.NewStopper("non-proxy-observability")
	serviceStopper.Stop()
	err := runObservabilityTask(
		metadata.ServiceType_CN,
		serviceStopper,
		"trace",
		func(context.Context) {},
	)
	require.ErrorIs(t, err, stopper.ErrUnavailable)
}

func TestClearSpillFilesReturnsCanceledListError(t *testing.T) {
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.CacheConfig{},
		nil,
	)
	require.NoError(t, err)
	fs, err := fileservice.NewFileServices(defines.LocalFileServiceName, localFS)
	require.NoError(t, err)
	defer fs.Close(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, clearSpillFiles(ctx, fs), context.Canceled)
}

func TestCreateProxyFileServiceWithRetryRecovers(t *testing.T) {
	cfg := fileservice.Config{Name: "SHARED", Backend: "MEM"}
	attempts := 0
	var waits []time.Duration
	create := func(
		ctx context.Context,
		cfg fileservice.Config,
		counters []*perfcounter.CounterSet,
	) (fileservice.FileService, error) {
		attempts++
		if attempts < 3 {
			return nil, &net.DNSError{Err: "no such host", Name: "minio"}
		}
		return fileservice.NewFileService(ctx, cfg, counters)
	}
	wait := func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}

	fs, err := createProxyFileServiceWithRetry(context.Background(), cfg, nil, create, wait)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	require.Equal(t, 3, attempts)
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second}, waits)
}

func TestCreateProxyFileServiceWithRetryUsesMinioBucketValidationError(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Has("location") {
			attempts++
			w.Header().Set("Content-Type", "application/xml")
			if attempts < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`<Error><Code>ServiceUnavailable</Code><Message>retry later</Message></Error>`))
				return
			}
			_, _ = w.Write([]byte(`<LocationConstraint>us-east-1</LocationConstraint>`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := fileservice.Config{
		Name:    "SHARED",
		Backend: "MINIO",
		S3: fileservice.ObjectStorageArguments{
			Endpoint:             server.URL,
			Bucket:               "test",
			KeyID:                "id",
			KeySecret:            "secret",
			NoDefaultCredentials: true,
		},
	}
	var waits []time.Duration
	fs, err := createProxyFileServiceWithRetry(
		context.Background(),
		cfg,
		nil,
		fileservice.NewFileService,
		func(_ context.Context, delay time.Duration) error {
			waits = append(waits, delay)
			return nil
		},
	)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	require.Equal(t, 3, attempts)
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second}, waits)
}

func TestCreateProxyFileServiceWithRetryRejectsMinioProtocolMismatch(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.Start()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waits := 0
	_, err := createProxyFileServiceWithRetry(
		ctx,
		fileservice.Config{
			Name:    "SHARED",
			Backend: "MINIO",
			S3: fileservice.ObjectStorageArguments{
				Endpoint:             "https://" + strings.TrimPrefix(server.URL, "http://"),
				Bucket:               "test",
				KeyID:                "id",
				KeySecret:            "secret",
				NoDefaultCredentials: true,
			},
		},
		nil,
		fileservice.NewFileService,
		func(context.Context, time.Duration) error {
			waits++
			return errors.New("protocol mismatch must not retry")
		},
	)
	require.Error(t, err)
	require.Zero(t, waits)
}

func TestCreateProxyFileServiceWithRetryCapsBackoff(t *testing.T) {
	cfg := fileservice.Config{Name: "SHARED", Backend: "MEM"}
	attempts := 0
	var waits []time.Duration
	create := func(
		ctx context.Context,
		cfg fileservice.Config,
		counters []*perfcounter.CounterSet,
	) (fileservice.FileService, error) {
		attempts++
		if attempts <= 7 {
			return nil, &net.DNSError{Err: "no such host", Name: "minio"}
		}
		return fileservice.NewFileService(ctx, cfg, counters)
	}
	wait := func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}

	fs, err := createProxyFileServiceWithRetry(context.Background(), cfg, nil, create, wait)
	require.NoError(t, err)
	defer fs.Close(context.Background())
	require.Equal(t, []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}, waits)
}

func TestCreateProxyFileServiceWithRetryRejectsPermanentError(t *testing.T) {
	permanent := errors.New("bad credentials")
	attempts := 0
	waits := 0
	_, err := createProxyFileServiceWithRetry(
		context.Background(),
		fileservice.Config{Name: "SHARED", Backend: "MEM"},
		nil,
		func(context.Context, fileservice.Config, []*perfcounter.CounterSet) (fileservice.FileService, error) {
			attempts++
			return nil, permanent
		},
		func(context.Context, time.Duration) error {
			waits++
			return nil
		},
	)
	require.ErrorIs(t, err, permanent)
	require.Equal(t, 1, attempts)
	require.Zero(t, waits)
}

func TestCreateProxyFileServiceWithRetryStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	_, err := createProxyFileServiceWithRetry(
		ctx,
		fileservice.Config{Name: "SHARED", Backend: "MEM"},
		nil,
		func(context.Context, fileservice.Config, []*perfcounter.CounterSet) (fileservice.FileService, error) {
			attempts++
			return nil, &net.DNSError{Err: "no such host", Name: "minio"}
		},
		func(ctx context.Context, _ time.Duration) error {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
}

func TestWaitProxyFileServiceRetryStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := waitProxyFileServiceRetry(ctx, time.Hour)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), time.Second)
}

func TestProxyFileServiceRetryBackoffSequence(t *testing.T) {
	got := []time.Duration{
		nextProxyFileServiceRetryDelay(time.Second),
		nextProxyFileServiceRetryDelay(2 * time.Second),
		nextProxyFileServiceRetryDelay(4 * time.Second),
		nextProxyFileServiceRetryDelay(8 * time.Second),
		nextProxyFileServiceRetryDelay(16 * time.Second),
		nextProxyFileServiceRetryDelay(30 * time.Second),
	}
	want := []time.Duration{
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
		30 * time.Second,
	}
	require.True(t, reflect.DeepEqual(want, got), "backoff = %v, want %v", got, want)
}
