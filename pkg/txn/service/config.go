// Copyright 2021 - 2022 Matrix Origin
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

package service

import (
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"go.uber.org/zap"
)

// Config txn service config
type Config struct {
	Logger   *zap.Logger      `toml:"-"`
	Metadata metadata.DNShard `toml:"-"`
	Storage  TxnStorage       `toml:"-"`

	// PayloadCopyBufferBytes is the buffer size used to copy payload data to socket. Default is 16KB
	PayloadCopyBufferBytes int `toml:"-"`
	// MaxConnectionsPerDN for the coordinator node of a 2pc transaction that needs to communicate with
	// other DNs, this parameter is used to limit the maximum number of connections that can be created
	// for each DN node. Default is 100.
	MaxConnectionsPerDN int `toml:"max-conns-per-dn"`
}
