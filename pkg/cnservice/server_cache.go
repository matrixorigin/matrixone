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

package cnservice

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/cacheservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/cache"
)

func (s *service) initCacheServer() error {
	s.gossipNode.SetListenAddrFn(s.gossipListenAddr)
	s.gossipNode.SetServiceAddrFn(s.gossipServiceAddr)
	s.gossipNode.SetCacheServerAddrFn(s.cacheServiceServiceAddr)

	if err := s.gossipNode.Create(); err != nil {
		return err
	}

	svr, err := cacheservice.NewCacheServer(s.cacheServiceListenAddr(), s.cfg.RPC)
	if err != nil {
		return err
	}
	if svr == nil {
		s.logger.Error("cache server not started, maybe port-base is not configured")
		return nil
	}
	svr.AddHandleFunc(pb.CmdMethod_RemoteRead, s.handleRemoteRead, false)
	s.cacheServer = svr
	return nil
}

// handleRemoteRead reads the cache data from the local data cache.
func (s *service) handleRemoteRead(ctx context.Context, req *pb.Request, resp *pb.CacheResponse) error {
	sharedFS, err := fileservice.Get[fileservice.FileService](s.fileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}
	return fileservice.HandleRemoteRead(ctx, sharedFS, req, resp)
}
