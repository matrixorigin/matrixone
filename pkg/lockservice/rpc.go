// Copyright 2022 Matrix Origin
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

package lockservice

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

var (
	requestPool = sync.Pool{
		New: func() any {
			return &pb.Request{}
		},
	}
	responsePool = &sync.Pool{
		New: func() any {
			return &pb.Response{}
		},
	}
)

type client struct {
	cfg    *morpc.Config
	client morpc.RPCClient
}

func NewClient(cfg morpc.Config) (Client, error) {
	c := &client{cfg: &cfg}
	c.cfg.Adjust()

	client, err := c.cfg.NewClient("lockservice-client",
		runtime.ProcessLevelRuntime().Logger().RawLogger(),
		func() morpc.Message { return acquireResponse() })
	if err != nil {
		return nil, err
	}
	c.client = client
	return c, nil
}

func (c *client) Send(ctx context.Context, request *pb.Request) (*pb.Response, error) {
	return nil, nil
}

func (c *client) Close() error {
	return c.client.Close()
}

func acquireRequest() *pb.Request {
	return requestPool.Get().(*pb.Request)
}

func releaseRequest(request *pb.Request) {
	request.Reset()
	requestPool.Put(request)
}

func acquireResponse() *pb.Response {
	return responsePool.Get().(*pb.Response)
}

func releaseResponse(response *pb.Response) {
	response.Reset()
	responsePool.Put(response)
}
