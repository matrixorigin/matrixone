// Copyright 2023 Matrix Origin
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

package pythonservice

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/udf"
)

type Client struct {
	cfg   ClientConfig
	sc    udf.ServiceClient
	mutex sync.Mutex
}

func NewClient(cfg ClientConfig) (*Client, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	return &Client{cfg: cfg}, nil
}

func (c *Client) init() error {
	if c.sc == nil {
		err := func() error {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			if c.sc == nil {
				conn, err := grpc.Dial(c.cfg.ServerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					return err
				}
				c.sc = udf.NewServiceClient(conn)
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) Run(ctx context.Context, request *udf.Request) (*udf.Response, error) {
	var language string
	if request != nil {
		language = request.Language
	}
	if language != udf.LanguagePython {
		return nil, moerr.NewInvalidArg(ctx, "udf language", language)
	}

	err := c.init()
	if err != nil {
		return nil, err
	}

	return c.sc.Run(ctx, request)
}

func (c *Client) Language() string {
	return udf.LanguagePython
}
