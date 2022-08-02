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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/dnservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// DNService describes expected behavior for dn service.
type DNService interface {
	// Start sends heartbeat and start to handle command.
	Start() error
	// Close stops store
	Close() error

	// StartDNReplica start the DNShard replica
	StartDNReplica(metadata.DNShard) error
	// CloseDNReplica close the DNShard replica.
	CloseDNReplica(shard metadata.DNShard) error
}

// dnOptions is options for a dn service.
type dnOptions []dnservice.Option

// newDNService initialize a DNService instance.
func newDNService(
	cfg *dnservice.Config,
	factory fileservice.FileServiceFactory,
	opts dnOptions,
) (DNService, error) {
	return dnservice.NewService(cfg, factory, opts...)
}

// buildDnConfig builds configuration for a dn service.
func buildDnConfig(
	index int, opt Options, address serviceAddress,
) (*dnservice.Config, []dnservice.Option) {
	cfg := &dnservice.Config{
		UUID:          uuid.New().String(),
		ListenAddress: address.getDnListenAddress(index),
	}
	cfg.HAKeeper.ClientConfig.ServiceAddresses = address.listHAKeeperListenAddresses()
	// FIXME: support different storage, consult @reusee
	cfg.Txn.Storage.Backend = opt.dn.txnStorageBackend

	return cfg, nil
}
