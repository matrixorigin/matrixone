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

package cacheservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/cache"
)

// CacheService is used to send GET cache request to another file-service.
type CacheService interface {
	// Start starts the service.
	Start() error
	// AddHandleFunc add message handler.
	AddHandleFunc(method pb.CmdMethod, h func(context.Context, *pb.Request, *pb.CacheResponse) error, async bool)
	// Close closes the service.
	Close() error
}

// cacheService is a cache server started in file-service.
type cacheService struct {
	log     *log.MOLogger
	handler morpc.MessageHandler[*pb.Request, *pb.CacheResponse]
}

// NewCacheServer creates a new cache server instance.
func NewCacheServer(address string, cfg morpc.Config) (CacheService, error) {
	if len(address) == 0 {
		return nil, nil
	}
	serviceName := "cache-server"
	rt := runtime.ProcessLevelRuntime()
	if rt == nil {
		rt = runtime.DefaultRuntime()
	}
	logger := rt.Logger().Named(serviceName)

	pool := morpc.NewMessagePool(
		func() *pb.Request { return &pb.Request{} },
		func() *pb.CacheResponse { return &pb.CacheResponse{} })

	h, err := morpc.NewMessageHandler(serviceName, address, cfg, pool)
	if err != nil {
		return nil, err
	}
	cs := &cacheService{
		log:     logger,
		handler: h,
	}
	return cs, nil
}

// Start implements the CacheService interface.
func (s *cacheService) Start() error {
	return s.handler.Start()
}

// AddHandleFunc implements the CacheService interface.
func (s *cacheService) AddHandleFunc(
	method pb.CmdMethod, h func(context.Context, *pb.Request, *pb.CacheResponse) error, async bool,
) {
	s.handler.RegisterHandleFunc(uint32(method), h, async)
}

// Close implements the CacheService interface.
func (s *cacheService) Close() error {
	return s.handler.Close()
}
