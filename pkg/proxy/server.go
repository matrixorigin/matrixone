// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"
	"net"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/goetty/v2/codec"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"github.com/matrixorigin/matrixone/pkg/version"
)

var statsFamilyName = "proxy counter"

const (
	// These values pin the application-facing Goetty session contract used by
	// calculateProtocolMemoryBudget. Goetty allocates the two copy buffers when
	// the session is created; the read input is phase-owned and released after
	// authentication, while the other three buffers live until session close.
	proxyApplicationReadBufferSize         = 256
	proxyApplicationWriteBufferSize        = 256
	proxyApplicationReadCopyBufferSize     = 1024
	proxyApplicationWriteCopyBufferSize    = 1024
	proxyApplicationReadChunkSize          = 4 << 10
	proxyApplicationSessionPersistentBytes = proxyApplicationWriteBufferSize +
		proxyApplicationReadCopyBufferSize +
		proxyApplicationWriteCopyBufferSize
)

type Server struct {
	runtime runtime.Runtime
	stopper *stopper.Stopper
	config  Config
	app     goetty.NetApplication

	// handler handles the client connection.
	handler *handler
	// counterSet counts the events in proxy.
	counterSet     *counterSet
	haKeeperClient logservice.ProxyHAKeeperClient
	// configData will be sent to HAKeeper.
	configData *util.ConfigData
	test       bool
}

// NewServer creates the proxy server.
//
// NB: runtime must be included in opts.
func NewServer(ctx context.Context, config Config, opts ...Option) (*Server, error) {
	config.FillDefault()
	if err := config.Validate(); err != nil {
		return nil, err
	}

	frontend.InitServerVersion(version.Version)

	configKVMap, _ := dumpProxyConfig(config)
	opts = append(opts, WithConfigData(configKVMap))

	s := &Server{
		config:     config,
		counterSet: newCounterSet(),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.runtime == nil {
		panic("runtime of proxy is not set")
	}

	frontend.InitServerLevelVars(config.UUID)
	var err error
	if s.haKeeperClient == nil {
		ctx, cancel := context.WithTimeoutCause(ctx, time.Second*3, moerr.CauseNewServer)
		defer cancel()
		s.haKeeperClient, err = logservice.NewProxyHAKeeperClient(ctx, config.UUID, config.HAKeeper.ClientConfig)
		if err != nil {
			return nil, err
		}
	}

	logExporter := newCounterLogExporter(s.counterSet)
	// Unregister first to handle the case where a previous registration might still exist
	// (e.g., from a previous test run or failed initialization).
	stats.Unregister(statsFamilyName)
	stats.Register(statsFamilyName, stats.WithLogExporter(logExporter))

	s.stopper = stopper.NewStopper("mo-proxy", stopper.WithLogger(s.runtime.Logger().RawLogger()))
	h, err := newProxyHandler(
		ctx,
		s.runtime,
		s.config,
		s.stopper,
		s.counterSet,
		s.haKeeperClient,
		s.test,
	)
	if err != nil {
		return nil, err
	}

	if err := runBootstrapTask(ctx, s.stopper, h); err != nil {
		return nil, err
	}

	if err := s.stopper.RunNamedTask("proxy heartbeat", s.heartbeat); err != nil {
		return nil, err
	}

	s.handler = h
	listener, err := newProxyListener(config.ListenAddress)
	if err != nil {
		return nil, err
	}
	listener = newConnectionAdmissionListener(
		listener,
		s.handler.connectionLimiter,
		s.handler.rejectBeforeSession,
	)
	app, err := goetty.NewApplicationWithListeners([]net.Listener{listener}, nil,
		goetty.WithAppLogger(s.runtime.Logger().RawLogger()),
		goetty.WithAppHandleSessionFunc(s.handler.handle),
		goetty.WithAppSessionOptions(
			goetty.WithSessionCodec(newProxySessionCodec(config)),
			goetty.WithSessionLogger(s.runtime.Logger().RawLogger()),
			goetty.WithSessionAllocator(newProxyApplicationAllocator(
				s.handler.sessionAllocator,
			)),
			// Keep the dependency defaults explicit because their concrete
			// allocations are part of the configured protocol-memory budget.
			goetty.WithSessionRWBUfferSize(
				int(proxyApplicationReadBufferSize),
				int(proxyApplicationWriteBufferSize),
			),
		),
	)
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	s.app = app
	return s, nil
}

func newProxySessionCodec(config Config) codec.Codec {
	return WithProxyProtocolCodec(frontend.NewSqlCodec(
		frontend.WithSQLCodecMaxPayloadSize(int(config.ClientHandshakePacketLimit)),
	), WithProxyProtocolMaxBodySize(int(config.ProxyProtocolBodyLimit)))
}

// proxyApplicationAllocator keeps Goetty's small bootstrap buffers infallible,
// then routes every grown protocol buffer through the Proxy's shared bounded
// allocator. Goetty constructs an application session after listener admission
// but before the handler takes the lease, and panics if that initial allocation
// returns an error. The listener bounds these bootstrap allocations; putting
// them behind a fallible allocator would turn overload into a process crash.
//
// The first socket read always requests a 4 KiB writable region, which moves the
// input above the inline threshold after the handshake lease has been acquired.
// From then on growth and handoff Close have deterministic allocate/free edges.
type proxyApplicationAllocator struct {
	managed frontend.Allocator
}

func newProxyApplicationAllocator(managed frontend.Allocator) *proxyApplicationAllocator {
	return &proxyApplicationAllocator{managed: managed}
}

func (a *proxyApplicationAllocator) Alloc(capacity int) ([]byte, error) {
	if capacity < 0 {
		return nil, moerr.NewInternalErrorNoCtx("negative proxy application buffer capacity")
	}
	if capacity <= proxyApplicationReadBufferSize {
		return make([]byte, capacity), nil
	}
	if a == nil || a.managed == nil {
		return nil, moerr.NewInternalErrorNoCtx("proxy application allocator is unavailable")
	}
	return a.managed.Alloc(capacity)
}

func (a *proxyApplicationAllocator) Free(data []byte) {
	if len(data) <= proxyApplicationReadBufferSize {
		return
	}
	if a == nil || a.managed == nil {
		// A large buffer cannot be produced by Alloc without a managed owner.
		// Keep cleanup non-panicking if a future caller violates that invariant.
		return
	}
	a.managed.Free(data)
}

func runBootstrapTask(ctx context.Context, st *stopper.Stopper, h *handler) error {
	return st.RunNamedTask("proxy bootstrap", func(taskCtx context.Context) {
		bootstrapCtx, cancel := context.WithCancelCause(ctx)
		stopCancelPropagation := context.AfterFunc(taskCtx, func() {
			cancel(context.Cause(taskCtx))
		})
		defer func() {
			stopCancelPropagation()
			cancel(nil)
		}()

		if err := h.bootstrap(bootstrapCtx); err != nil && ctx.Err() == nil && taskCtx.Err() == nil {
			h.logger.Error("proxy bootstrap failed", zap.Error(err))
		}
	})
}

// Start starts the proxy server.
func (s *Server) Start() error {
	err := s.app.Start()
	if err != nil {
		s.runtime.Logger().Error("proxy server start failed", zap.Error(err))
	} else {
		s.runtime.Logger().Info("proxy server started")
	}
	return err
}

// Close closes the proxy server.
func (s *Server) Close() error {
	_ = s.handler.Close()
	s.stopper.Stop()
	stats.Unregister(statsFamilyName)
	return s.app.Stop()
}
