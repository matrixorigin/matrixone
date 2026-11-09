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
	"iter"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/tnservice"
	"github.com/stretchr/testify/require"
)

type testProxyServerLifecycle struct {
	startErr   error
	closeErr   error
	started    chan struct{}
	closeCalls int
}

func (s *testProxyServerLifecycle) Start() error {
	if s.started != nil {
		close(s.started)
	}
	return s.startErr
}

func (s *testProxyServerLifecycle) Close() error {
	s.closeCalls++
	return s.closeErr
}

func TestRunProxyServerUntilCanceled(t *testing.T) {
	t.Run("running server closes after cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		server := &testProxyServerLifecycle{started: make(chan struct{})}
		done := make(chan error, 1)
		go func() { done <- runProxyServerUntilCanceled(ctx, server) }()
		select {
		case <-server.started:
		case <-time.After(time.Second):
			t.Fatal("Proxy server did not enter Start")
		}
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("Proxy server did not close after cancellation")
		}
		require.Equal(t, 1, server.closeCalls)
	})

	t.Run("startup cancellation closes without error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		server := &testProxyServerLifecycle{startErr: context.Canceled}
		require.NoError(t, runProxyServerUntilCanceled(ctx, server))
		require.Equal(t, 1, server.closeCalls)
	})

	t.Run("custom cancellation cause closes without error", func(t *testing.T) {
		cause := errors.New("stopper shutdown")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)
		server := &testProxyServerLifecycle{startErr: cause}
		require.NoError(t, runProxyServerUntilCanceled(ctx, server))
		require.Equal(t, 1, server.closeCalls)
	})

	t.Run("permanent startup error is preserved after close", func(t *testing.T) {
		startErr := errors.New("listener failed")
		server := &testProxyServerLifecycle{startErr: startErr}
		require.ErrorIs(t, runProxyServerUntilCanceled(context.Background(), server), startErr)
		require.Equal(t, 1, server.closeCalls)
	})

	t.Run("cancellation does not hide unrelated startup error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		startErr := errors.New("listener failed")
		server := &testProxyServerLifecycle{startErr: startErr}
		require.ErrorIs(t, runProxyServerUntilCanceled(ctx, server), startErr)
		require.Equal(t, 1, server.closeCalls)
	})

	t.Run("close error does not replace startup error", func(t *testing.T) {
		startErr := errors.New("listener failed")
		closeErr := errors.New("close failed")
		server := &testProxyServerLifecycle{startErr: startErr, closeErr: closeErr}
		err := runProxyServerUntilCanceled(context.Background(), server)
		require.ErrorIs(t, err, startErr)
		require.ErrorIs(t, err, closeErr)
		require.Equal(t, 1, server.closeCalls)
	})

	t.Run("close error after cancellation is propagated", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		server := &testProxyServerLifecycle{closeErr: errors.New("close failed")}
		done := make(chan error, 1)
		go func() { done <- runProxyServerUntilCanceled(ctx, server) }()
		cancel()
		require.ErrorIs(t, <-done, server.closeErr)
		require.Equal(t, 1, server.closeCalls)
	})
}

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

func TestInitServiceFileServicesPropagatesCreatorError(t *testing.T) {
	permanent := errors.New("storage unavailable")
	cfg := &Config{
		ServiceType: metadata.ServiceType_PROXY.String(),
		FileServices: []fileservice.Config{{
			Name:    defines.LocalFileServiceName,
			Backend: "MEM",
		}},
	}

	_, err := initServiceFileServices(
		context.Background(),
		metadata.ServiceType_PROXY,
		cfg,
		stopper.NewStopper("proxy-file-service-error"),
		func(context.Context, fileservice.Config, []*perfcounter.CounterSet) (fileservice.FileService, error) {
			return nil, permanent
		},
	)
	require.ErrorIs(t, err, permanent)
}

type listErrorFileService struct {
	*fileservice.LocalETLFS
	err error
}

func (f *listErrorFileService) List(context.Context, string) iter.Seq2[*fileservice.DirEntry, error] {
	return func(yield func(*fileservice.DirEntry, error) bool) {
		yield(nil, f.err)
	}
}

func TestInitCNFileServicesPublishesETLAfterSpillCleanup(t *testing.T) {
	configs := []fileservice.Config{
		{Name: defines.LocalFileServiceName, Backend: "MEM"},
		{Name: defines.SharedFileServiceName, Backend: "MEM"},
		{Name: defines.ETLFileServiceName, Backend: "MEM"},
		{Name: defines.TmpFileServiceName, Backend: "MEM"},
	}
	cfg := NewConfig()
	cfg.ServiceType = metadata.ServiceType_CN.String()
	cfg.CN.UUID = "cn-spill-list-error"
	cfg.FileServices = configs
	cfg.Observability.DisableMetric = true
	cfg.Observability.DisableTrace = true
	previousEtlFS := globalEtlFS
	previousServiceType := globalServiceType
	previousNodeID := globalNodeId
	globalEtlFS = nil
	globalServiceType = ""
	globalNodeId = ""
	t.Cleanup(func() {
		globalEtlFS = previousEtlFS
		globalServiceType = previousServiceType
		globalNodeId = previousNodeID
	})

	listErr := errors.New("spill listing failed")
	_, err := initServiceFileServices(
		context.Background(),
		metadata.ServiceType_CN,
		cfg,
		stopper.NewStopper("cn-spill-list-error"),
		func(ctx context.Context, fsCfg fileservice.Config, counters []*perfcounter.CounterSet) (fileservice.FileService, error) {
			if fsCfg.Name == defines.LocalFileServiceName {
				local, err := fileservice.NewLocalETLFS(fsCfg.Name, t.TempDir())
				if err != nil {
					return nil, err
				}
				return &listErrorFileService{LocalETLFS: local, err: listErr}, nil
			}
			return fileservice.NewFileService(ctx, fsCfg, counters)
		},
	)
	require.ErrorIs(t, err, listErr)
	require.Nil(t, globalEtlFS)
	for _, fsCfg := range configs {
		_, published := perfcounter.Named.Load(perfcounter.NameForFileService(
			metadata.ServiceType_CN.String(),
			cfg.CN.UUID,
			fsCfg.Name,
		))
		require.Falsef(t, published, "counter %s remains published", fsCfg.Name)
	}
	_, published := perfcounter.Named.Load(perfcounter.NameForNode(
		metadata.ServiceType_CN.String(), cfg.CN.UUID,
	))
	require.False(t, published)
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

func TestInitServiceFileServicesOnlyCNOwnsSpillRoot(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	spillDir := filepath.Join(root, defines.SpillFileServiceName)
	require.NoError(t, os.MkdirAll(spillDir, 0700))
	orphan := filepath.Join(spillDir, "previous-process")
	require.NoError(t, os.WriteFile(orphan, []byte("orphan"), 0600))
	previousEtlFS, previousServiceType, previousNodeID := globalEtlFS, globalServiceType, globalNodeId
	t.Cleanup(func() {
		globalEtlFS, globalServiceType, globalNodeId = previousEtlFS, previousServiceType, previousNodeID
	})
	initRole := func(role metadata.ServiceType) *fileservice.FileServices {
		t.Helper()
		cfg := NewConfig()
		cfg.ServiceType = role.String()
		cfg.CN.UUID = "spill-startup-cn"
		cfg.TN_please_use_getTNServiceConfig = &tnservice.Config{UUID: "spill-startup-tn"}
		cfg.LogService.UUID = "spill-startup-log"
		cfg.Observability.DisableMetric = true
		cfg.Observability.DisableTrace = true
		rt := runtime.DefaultRuntime()
		runtime.SetupServiceBasedRuntime(cfg.mustGetServiceUUID(), rt)
		t.Cleanup(func() { runtime.CompareAndDeleteServiceBasedRuntime(cfg.mustGetServiceUUID(), rt) })
		cfg.FileServices = []fileservice.Config{
			{Name: defines.LocalFileServiceName, Backend: "DISK", DataDir: root},
			{Name: defines.SharedFileServiceName, Backend: "MEM"},
			{Name: defines.ETLFileServiceName, Backend: "MEM"},
			{Name: defines.TmpFileServiceName, Backend: "MEM"},
		}
		serviceStopper := stopper.NewStopper("spill-startup-" + role.String())
		t.Cleanup(serviceStopper.Stop)
		fs, err := initServiceFileServices(ctx, role, cfg, serviceStopper, fileservice.NewFileService)
		require.NoError(t, err)
		t.Cleanup(func() {
			fs.Close(ctx)
			for _, config := range cfg.FileServices {
				perfcounter.Named.Delete(perfcounter.NameForFileService(role.String(), cfg.mustGetServiceUUID(), config.Name))
			}
			perfcounter.Named.Delete(perfcounter.NameForNode(role.String(), cfg.mustGetServiceUUID()))
		})
		return fs
	}

	// Launch's TN and CN configurations can share LOCAL. A non-spilling role
	// must neither consume CN's lease nor delete its previous-process files.
	initRole(metadata.ServiceType_TN)
	_, err := os.Stat(orphan)
	require.NoError(t, err)
	cn := initRole(metadata.ServiceType_CN)
	_, err = os.Stat(orphan)
	require.ErrorIs(t, err, os.ErrNotExist)
	local, err := fileservice.Get[fileservice.MutableFileService](cn, defines.LocalFileServiceName)
	require.NoError(t, err)
	spill, err := fileservice.GetSpillFileService(ctx, local, defines.SpillFileServiceName)
	require.NoError(t, err)
	fd, err := spill.CreateFile(ctx, "live-cn")
	require.NoError(t, err)
	t.Cleanup(func() { _ = fd.Close() })
	_, err = fd.WriteString("still owned by CN")
	require.NoError(t, err)

	initRole(metadata.ServiceType_LOG)
	contents, err := os.ReadFile(filepath.Join(spillDir, "live-cn"))
	require.NoError(t, err)
	require.Equal(t, "still owned by CN", string(contents))
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
