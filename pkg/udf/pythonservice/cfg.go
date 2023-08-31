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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type Config struct {
	Address string `toml:"address"`
	// Path code path
	Path string `toml:"path"`
}

func (c *Config) Validate() error {
	if c.Address == "" {
		return moerr.NewInternalError(context.Background(), "missing python udf server address")
	}
	if c.Path == "" {
		return moerr.NewInternalError(context.Background(), "missing python udf server path")
	}
	return nil
}

type ClientConfig struct {
	ServerAddress string `toml:"server-address"`
}

func (c *ClientConfig) Validate() error {
	if c.ServerAddress == "" {
		return moerr.NewInternalError(context.Background(), "missing python udf server address")
	}
	return nil
}
