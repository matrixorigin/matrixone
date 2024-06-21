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
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
	"github.com/matrixorigin/matrixone/pkg/version"
	"go.uber.org/zap"
)

var statsFamilyName = "proxy counter"

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

	var err error
	if s.haKeeperClient == nil {
		ctx, cancel := context.WithTimeout(ctx, time.Second*3)
		defer cancel()
		s.haKeeperClient, err = logservice.NewProxyHAKeeperClient(ctx, config.HAKeeper.ClientConfig)
		if err != nil {
			return nil, err
		}
	}

	logExporter := newCounterLogExporter(s.counterSet)
	stats.Register(statsFamilyName, stats.WithLogExporter(logExporter))

	s.stopper = stopper.NewStopper("mo-proxy", stopper.WithLogger(s.runtime.Logger().RawLogger()))
	h, err := newProxyHandler(ctx, s.runtime, s.config, s.stopper, s.counterSet, s.haKeeperClient)
	if err != nil {
		return nil, err
	}

	go h.bootstrap(ctx)

	if err := s.stopper.RunNamedTask("proxy heartbeat", s.heartbeat); err != nil {
		return nil, err
	}

	s.handler = h
	app, err := goetty.NewApplication(config.ListenAddress, nil,
		goetty.WithAppLogger(s.runtime.Logger().RawLogger()),
		goetty.WithAppHandleSessionFunc(s.handler.handle),
		goetty.WithAppSessionOptions(
			goetty.WithSessionCodec(WithProxyProtocolCodec(frontend.NewSqlCodec())),
			goetty.WithSessionLogger(s.runtime.Logger().RawLogger()),
		),
	)
	if err != nil {
		return nil, err
	}
	s.app = app
	return s, nil
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
