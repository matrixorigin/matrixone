// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

type testProxy struct {
	address   string
	upstreams []string
	startErr  error
	started   bool
}

func (p *testProxy) Start() error {
	p.started = true
	return p.startErr
}

func (p *testProxy) Stop() error {
	return nil
}

func (p *testProxy) AddUpStream(address string, _ time.Duration) {
	p.upstreams = append(p.upstreams, address)
}

type testHAKeeperClient struct {
	logservice.CNHAKeeperClient
	getState   func(context.Context) (logpb.CheckerState, error)
	getDetails func(context.Context) (logpb.ClusterDetails, error)
	closeErr   error
	closed     bool
}

func (c *testHAKeeperClient) GetClusterState(ctx context.Context) (logpb.CheckerState, error) {
	return c.getState(ctx)
}

func (c *testHAKeeperClient) GetClusterDetails(ctx context.Context) (logpb.ClusterDetails, error) {
	return c.getDetails(ctx)
}

func (c *testHAKeeperClient) Close() error {
	c.closed = true
	return c.closeErr
}

func writeLaunchTestFile(t *testing.T, name, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func setLaunchTestHooks(t *testing.T) {
	t.Helper()
	oldStartService := launchStartService
	oldStartDynamic := launchStartDynamic
	oldNewProxy := launchNewProxy
	oldNewClient := launchNewHAKeeperClient
	oldSleep := launchSleep
	oldLaunchFile := *launchFile
	oldWithProxy := *withProxy
	oldCNProxy := cnProxy
	t.Cleanup(func() {
		launchStartService = oldStartService
		launchStartDynamic = oldStartDynamic
		launchNewProxy = oldNewProxy
		launchNewHAKeeperClient = oldNewClient
		launchSleep = oldSleep
		*launchFile = oldLaunchFile
		*withProxy = oldWithProxy
		cnProxy = oldCNProxy
	})
}

func TestStartClusterUsesConfiguredProxy(t *testing.T) {
	setLaunchTestHooks(t)
	config := writeLaunchTestFile(t, "service.toml", "")
	launch := fmt.Sprintf(
		"logservices=[%q]\ntnservices=[%q]\ncnservices=[%q,%q]\nproxy-services=[%q]\npython-udf-services=[%q]\n",
		config, config, config, config, config, config,
	)
	*launchFile = writeLaunchTestFile(t, "launch.toml", launch)
	*withProxy = true

	var started []*Config
	launchStartService = func(
		_ context.Context,
		cfg *Config,
		_ *stopper.Stopper,
		_ chan struct{},
	) error {
		started = append(started, cfg)
		return nil
	}
	launchNewProxy = func(string, *zap.Logger) goetty.Proxy {
		t.Fatal("builtin proxy must not start with -with-proxy")
		return nil
	}

	if err := startCluster(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	if len(started) != 6 {
		t.Fatalf("started %d services, want 6", len(started))
	}
	if !started[1].IsStandalone {
		t.Fatal("TN service was not marked standalone")
	}
}

func TestStartClusterStartsBuiltinProxyWithoutProxyService(t *testing.T) {
	setLaunchTestHooks(t)
	cn1 := writeLaunchTestFile(t, "cn1.toml", "[cn.frontend]\nport=16001\n")
	cn2 := writeLaunchTestFile(t, "cn2.toml", "[cn.frontend]\nport=16002\n")
	config := writeLaunchTestFile(t, "service.toml", "")
	launch := fmt.Sprintf(
		"logservices=[%q]\ntnservices=[%q]\ncnservices=[%q,%q]\n",
		config, config, cn1, cn2,
	)
	*launchFile = writeLaunchTestFile(t, "launch.toml", launch)
	*withProxy = false
	launchStartService = func(context.Context, *Config, *stopper.Stopper, chan struct{}) error {
		return nil
	}
	proxy := &testProxy{}
	launchNewProxy = func(address string, _ *zap.Logger) goetty.Proxy {
		proxy.address = address
		return proxy
	}

	if err := startCluster(context.Background(), nil, nil); err != nil {
		t.Fatal(err)
	}
	if !proxy.started || proxy.address != "0.0.0.0:6001" {
		t.Fatalf("builtin proxy state = %+v", proxy)
	}
	want := []string{"127.0.0.1:16001", "127.0.0.1:16002"}
	if fmt.Sprint(proxy.upstreams) != fmt.Sprint(want) {
		t.Fatalf("upstreams = %v, want %v", proxy.upstreams, want)
	}
}

func TestStartClusterDynamicAndInvalidLaunch(t *testing.T) {
	t.Run("dynamic", func(t *testing.T) {
		setLaunchTestHooks(t)
		*launchFile = writeLaunchTestFile(t, "launch.toml", "[dynamic]\nenable=true\n")
		called := false
		launchStartDynamic = func(
			context.Context,
			*LaunchConfig,
			*stopper.Stopper,
			chan struct{},
		) error {
			called = true
			return errors.New("dynamic")
		}
		if err := startCluster(context.Background(), nil, nil); err == nil || !called {
			t.Fatalf("startCluster() = %v, dynamic called = %t", err, called)
		}
	})
	t.Run("missing launch flag", func(t *testing.T) {
		setLaunchTestHooks(t)
		*launchFile = ""
		defer func() {
			if recover() == nil {
				t.Fatal("startCluster did not panic")
			}
		}()
		_ = startCluster(context.Background(), nil, nil)
	})
	t.Run("unreadable launch", func(t *testing.T) {
		setLaunchTestHooks(t)
		*launchFile = filepath.Join(t.TempDir(), "missing.toml")
		if err := startCluster(context.Background(), nil, nil); err == nil {
			t.Fatal("startCluster unexpectedly succeeded")
		}
	})
}

func TestServiceClusterValidationAndErrors(t *testing.T) {
	setLaunchTestHooks(t)
	ctx := context.Background()
	startError := errors.New("start failed")
	invalid := filepath.Join(t.TempDir(), "missing.toml")
	valid := writeLaunchTestFile(t, "service.toml", "")

	tests := []struct {
		name    string
		start   func([]string) error
		emptyOK bool
	}{
		{"log", func(files []string) error { return startLogServiceCluster(ctx, files, nil, nil) }, false},
		{"tn", func(files []string) error { return startTNServiceCluster(ctx, files, nil, nil) }, false},
		{"cn", func(files []string) error { return startCNServiceCluster(ctx, files, nil, nil) }, false},
		{"proxy", func(files []string) error { return startProxyServiceCluster(ctx, files, nil, nil) }, false},
		{"python", func(files []string) error { return startPythonUdfServiceCluster(ctx, files, nil, nil) }, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.start(nil)
			if test.emptyOK && err != nil {
				t.Fatalf("empty configs: %v", err)
			}
			if !test.emptyOK && err == nil {
				t.Fatal("empty configs unexpectedly succeeded")
			}
			if err := test.start([]string{invalid}); err == nil {
				t.Fatal("invalid config unexpectedly succeeded")
			}
			launchStartService = func(context.Context, *Config, *stopper.Stopper, chan struct{}) error {
				return startError
			}
			if err := test.start([]string{valid}); !errors.Is(err, startError) {
				t.Fatalf("start error = %v, want %v", err, startError)
			}
		})
	}
}

func TestCNProxyStartErrorAndSingleCN(t *testing.T) {
	setLaunchTestHooks(t)
	valid := writeLaunchTestFile(t, "cn.toml", "")
	launchStartService = func(context.Context, *Config, *stopper.Stopper, chan struct{}) error {
		return nil
	}
	*withProxy = false
	if err := startCNServiceCluster(context.Background(), []string{valid}, nil, nil); err != nil {
		t.Fatal(err)
	}

	startError := errors.New("proxy start failed")
	launchNewProxy = func(string, *zap.Logger) goetty.Proxy {
		return &testProxy{startErr: startError}
	}
	if err := startCNServiceCluster(context.Background(), []string{valid, valid}, nil, nil); !errors.Is(err, startError) {
		t.Fatalf("startCNServiceCluster() = %v, want %v", err, startError)
	}
}

func TestShouldStartBuiltinCNProxy(t *testing.T) {
	tests := []struct {
		name                string
		upstreamCount       int
		proxyServiceEnabled bool
		want                bool
	}{
		{"multiple CNs without proxy service", 2, false, true},
		{"real proxy service owns SQL entrypoint", 2, true, false},
		{"single CN needs no builtin proxy", 1, false, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldStartBuiltinCNProxy(test.upstreamCount, test.proxyServiceEnabled); got != test.want {
				t.Fatalf("shouldStartBuiltinCNProxy(%d, %t) = %t, want %t",
					test.upstreamCount, test.proxyServiceEnabled, got, test.want)
			}
		})
	}
}

func TestWaitHAKeeperReadyRetries(t *testing.T) {
	setLaunchTestHooks(t)
	client := &testHAKeeperClient{}
	attempts := 0
	launchNewHAKeeperClient = func(
		context.Context,
		string,
		logservice.HAKeeperClientConfig,
	) (logservice.CNHAKeeperClient, error) {
		attempts++
		if attempts == 1 {
			return nil, errors.New("not ready")
		}
		return client, nil
	}
	sleeps := 0
	launchSleep = func(time.Duration) { sleeps++ }

	got, err := waitHAKeeperReady("service", logservice.HAKeeperClientConfig{})
	if err != nil || got != client || attempts != 2 || sleeps != 1 {
		t.Fatalf("client=%v err=%v attempts=%d sleeps=%d", got, err, attempts, sleeps)
	}
}

func TestWaitHAKeeperRunning(t *testing.T) {
	setLaunchTestHooks(t)
	launchSleep = func(time.Duration) {}
	t.Run("retries until running", func(t *testing.T) {
		calls := 0
		client := &testHAKeeperClient{
			getState: func(context.Context) (logpb.CheckerState, error) {
				calls++
				if calls == 1 {
					return logpb.CheckerState{}, moerr.NewNoHAKeeper(context.Background())
				}
				return logpb.CheckerState{State: logpb.HAKeeperRunning}, nil
			},
		}
		if err := waitHAKeeperRunning(client); err != nil || calls != 2 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		client := &testHAKeeperClient{
			getState: func(context.Context) (logpb.CheckerState, error) {
				return logpb.CheckerState{}, context.DeadlineExceeded
			},
		}
		if err := waitHAKeeperRunning(client); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waitHAKeeperRunning() = %v", err)
		}
	})
	t.Run("running returns client error", func(t *testing.T) {
		want := errors.New("state error")
		client := &testHAKeeperClient{
			getState: func(context.Context) (logpb.CheckerState, error) {
				return logpb.CheckerState{State: logpb.HAKeeperRunning}, want
			},
		}
		if err := waitHAKeeperRunning(client); !errors.Is(err, want) {
			t.Fatalf("waitHAKeeperRunning() = %v, want %v", err, want)
		}
	})
}

func TestWaitAnyShardReady(t *testing.T) {
	setLaunchTestHooks(t)
	launchSleep = func(time.Duration) {}
	t.Run("retries errors and empty stores", func(t *testing.T) {
		calls := 0
		client := &testHAKeeperClient{
			getDetails: func(context.Context) (logpb.ClusterDetails, error) {
				calls++
				switch calls {
				case 1:
					return logpb.ClusterDetails{}, errors.New("temporary")
				case 2:
					return logpb.ClusterDetails{TNStores: []logpb.TNStore{{}}}, nil
				default:
					return logpb.ClusterDetails{
						TNStores: []logpb.TNStore{{Shards: []logpb.TNShardInfo{{ShardID: 1}}}},
					}, nil
				}
			},
		}
		if err := waitAnyShardReady(client); err != nil || calls != 3 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		client := &testHAKeeperClient{
			getDetails: func(context.Context) (logpb.ClusterDetails, error) {
				return logpb.ClusterDetails{}, context.DeadlineExceeded
			},
		}
		if err := waitAnyShardReady(client); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("waitAnyShardReady() = %v", err)
		}
	})
}

func TestWaitClusterCondition(t *testing.T) {
	setLaunchTestHooks(t)
	client := &testHAKeeperClient{closeErr: errors.New("ignored close error")}
	launchNewHAKeeperClient = func(
		context.Context,
		string,
		logservice.HAKeeperClientConfig,
	) (logservice.CNHAKeeperClient, error) {
		return client, nil
	}
	if err := waitClusterCondition(
		"service",
		logservice.HAKeeperClientConfig{},
		func(logservice.CNHAKeeperClient) error { return nil },
	); err != nil {
		t.Fatal(err)
	}
	if !client.closed {
		t.Fatal("HAKeeper client was not closed")
	}

	waitErr := errors.New("wait failed")
	client.closed = false
	if err := waitClusterCondition(
		"service",
		logservice.HAKeeperClientConfig{},
		func(logservice.CNHAKeeperClient) error { return waitErr },
	); !errors.Is(err, waitErr) {
		t.Fatalf("waitClusterCondition() = %v, want %v", err, waitErr)
	}
	if client.closed {
		t.Fatal("client unexpectedly closed after wait error")
	}
}
