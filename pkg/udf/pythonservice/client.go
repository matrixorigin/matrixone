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
	"google.golang.org/grpc"
	"sync"
)

type client struct {
	cfg   ClientConfig
	sc    PythonUdfServiceClient
	mutex sync.Mutex
}

func NewClient(cfg ClientConfig) (PythonUdfClient, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	return &client{cfg: cfg}, nil
}

func (c *client) init() error {
	if c.sc == nil {
		err := func() error {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			if c.sc == nil {
				conn, err := grpc.Dial(c.cfg.ServerAddress, grpc.WithInsecure())
				if err != nil {
					return err
				}
				c.sc = NewPythonUdfServiceClient(conn)
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *client) Run(ctx context.Context, request *PythonUdfRequest) (*PythonUdfResponse, error) {
	err := c.init()
	if err != nil {
		return nil, err
	}
	return c.sc.Run(ctx, request)
}
