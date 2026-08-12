// Copyright 2026 Matrix Origin
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

package substrait

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ResolverServer is an opt-in CN lifecycle component. A disabled deployment
// creates no listener and changes no native query path.
type ResolverServer struct {
	server   *http.Server
	mu       sync.Mutex
	listener net.Listener
	done     chan error
	closed   bool
}

func NewResolverServer(address string, tlsConfig *tls.Config, leases *LeaseManager, auditor ResolveAuditRecorder) (*ResolverServer, error) {
	if address == "" || leases == nil || !leases.Ready() || !leases.Protected() || auditor == nil {
		return nil, moerr.NewInternalErrorNoCtx("substrait: resolver requires an address, a replayed lease manager, and an audit recorder")
	}
	if tlsConfig == nil || tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert || tlsConfig.ClientCAs == nil || len(tlsConfig.Certificates) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("substrait: resolver requires a server certificate and verified client CA")
	}
	config := tlsConfig.Clone()
	if config.MinVersion < tls.VersionTLS12 {
		config.MinVersion = tls.VersionTLS12
	}
	return &ResolverServer{server: &http.Server{Addr: address, Handler: ResolveHandler(leases, nil, auditor), TLSConfig: config, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 8 << 10}, done: make(chan error, 1)}, nil
}

func (s *ResolverServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil || s.closed {
		return moerr.NewInternalErrorNoCtx("substrait: resolver already started or closed")
	}
	ln, err := net.Listen("tcp", s.server.Addr)
	if err != nil {
		return err
	}
	s.listener = tls.NewListener(ln, s.server.TLSConfig)
	go func() {
		err := s.server.Serve(s.listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		s.done <- err
	}()
	return nil
}

func (s *ResolverServer) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	started := s.listener != nil
	if !started {
		s.closed = true
		return nil
	}
	if err := s.server.Shutdown(ctx); err != nil {
		return err
	}
	err := <-s.done
	s.closed = true
	s.listener = nil
	return err
}
