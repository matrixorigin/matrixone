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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
)

// routeErrRouter is shared by migration-related tests to ensure route failure
// path is reached before any backend connection is created.
type routeErrRouter struct {
	errMsg string
}

func (router *routeErrRouter) Route(context.Context, string, clientInfo, func(string) bool) (*CNServer, error) {
	msg := router.errMsg
	if msg == "" {
		msg = "route failed"
	}
	return nil, moerr.NewInternalErrorNoCtx(msg)
}

func (router *routeErrRouter) SelectByConnID(uint32) (*CNServer, error) {
	return nil, nil
}

func (router *routeErrRouter) AllServers(string) ([]*CNServer, error) {
	return nil, nil
}

func (router *routeErrRouter) Connect(*CNServer, *frontend.Packet, *tunnel) (ServerConn, []byte, error) {
	return nil, nil, nil
}

type popCountConnCache struct {
	popCount int
}

func (c *popCountConnCache) Push(cacheKey, ServerConn) bool {
	return false
}

func (c *popCountConnCache) Pop(cacheKey, uint32, []byte, []byte) ServerConn {
	c.popCount++
	return nil
}

func (c *popCountConnCache) Count() int {
	return 0
}

func (c *popCountConnCache) Close() error {
	return nil
}
