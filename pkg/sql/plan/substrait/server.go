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
	server    *http.Server
	mu        sync.Mutex
	listener  net.Listener
	serveDone chan error
	closeDone chan struct{}
	closeErr  error
	closing   bool
	closed    bool
}

func NewResolverServer(address string, tlsConfig *tls.Config, leases *LeaseManager, auditor ResolveAuditRecorder) (*ResolverServer, error) {
	if address == "" || leases == nil || !leases.DurableReady() || auditor == nil {
		return nil, moerr.NewInternalErrorNoCtx("substrait: resolver requires an address, a replayed lease manager, and an audit recorder")
	}
	if tlsConfig == nil || tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert || tlsConfig.ClientCAs == nil || len(tlsConfig.Certificates) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("substrait: resolver requires a server certificate and verified client CA")
	}
	config := tlsConfig.Clone()
	if config.MinVersion < tls.VersionTLS12 {
		config.MinVersion = tls.VersionTLS12
	}
	return &ResolverServer{server: &http.Server{Addr: address, Handler: ResolveHandler(leases, nil, auditor), TLSConfig: config, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: time.Minute, MaxHeaderBytes: 8 << 10}, serveDone: make(chan error, 1)}, nil
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
		s.serveDone <- err
	}()
	return nil
}

func (s *ResolverServer) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed {
		err := s.closeErr
		s.mu.Unlock()
		return err
	}
	if s.closing {
		done := s.closeDone
		s.mu.Unlock()
		select {
		case <-done:
			s.mu.Lock()
			err := s.closeErr
			s.mu.Unlock()
			return err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	s.closing = true
	s.closeDone = make(chan struct{})
	started := s.listener != nil
	listener := s.listener
	server := s.server
	done := s.closeDone
	s.mu.Unlock()
	var result error
	if started {
		shutdownErr := context.Cause(ctx)
		if shutdownErr == nil {
			shutdownErr = server.Shutdown(ctx)
		}
		if shutdownErr != nil {
			result = errors.Join(result, shutdownErr)
			result = errors.Join(result, server.Close())
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				result = errors.Join(result, err)
			}
		}
		result = errors.Join(result, <-s.serveDone)
	}
	s.mu.Lock()
	s.closed = true
	s.closing = false
	s.listener = nil
	s.closeErr = result
	close(done)
	s.mu.Unlock()
	return result
}
