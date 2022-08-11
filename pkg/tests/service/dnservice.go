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
	"sync"

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
	// Status returns the status of service.
	Status() ServiceStatus

	// ID returns uuid of store
	ID() string

	// StartDNReplica start the DNShard replica
	StartDNReplica(shard metadata.DNShard) error
	// CloseDNReplica close the DNShard replica.
	CloseDNReplica(shard metadata.DNShard) error
}

// dnService wraps dnservice.Service.
//
// The main purpose of this structure is to maintain status.
type dnService struct {
	sync.Mutex
	status ServiceStatus
	uuid   string
	svc    dnservice.Service
}

func (ds *dnService) Start() error {
	ds.Lock()
	defer ds.Unlock()

	if ds.status == ServiceInitialized {
		err := ds.svc.Start()
		if err != nil {
			return err
		}
		ds.status = ServiceStarted
	}

	return nil
}

func (ds *dnService) Close() error {
	ds.Lock()
	defer ds.Unlock()

	if ds.status == ServiceStarted {
		err := ds.svc.Close()
		if err != nil {
			return err
		}
		ds.status = ServiceClosed
	}

	return nil
}

func (ds *dnService) Status() ServiceStatus {
	ds.Lock()
	defer ds.Unlock()
	return ds.status
}

func (ds *dnService) ID() string {
	ds.Lock()
	defer ds.Unlock()
	return ds.uuid
}

func (ds *dnService) StartDNReplica(shard metadata.DNShard) error {
	ds.Lock()
	defer ds.Unlock()

	if ds.status != ServiceStarted {
		return ErrServiceNotStarted
	}

	return ds.svc.StartDNReplica(shard)
}

func (ds *dnService) CloseDNReplica(shard metadata.DNShard) error {
	ds.Lock()
	defer ds.Unlock()

	if ds.status != ServiceStarted {
		return ErrServiceNotStarted
	}

	return ds.svc.CloseDNReplica(shard)
}

// dnOptions is options for a dn service.
type dnOptions []dnservice.Option

// newDNService initializes an instance of `DNService`.
func newDNService(
	cfg *dnservice.Config,
	factory fileservice.FileServiceFactory,
	opts dnOptions,
) (DNService, error) {
	svc, err := dnservice.NewService(cfg, factory, opts...)
	if err != nil {
		return nil, err
	}
	return &dnService{
		status: ServiceInitialized,
		uuid:   cfg.UUID,
		svc:    svc,
	}, nil
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
	cfg.HAKeeper.HeatbeatDuration.Duration = opt.dn.heartbeatInterval
	// FIXME: support different storage, consult @reusee
	cfg.Txn.Storage.Backend = opt.dn.txnStorageBackend

	return cfg, nil
}
