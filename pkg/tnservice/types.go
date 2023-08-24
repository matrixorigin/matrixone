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

package tnservice

import (
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
)

// StorageType txn storage type
type StorageType string

const (
	// StorageTAE TAE txn storage backend
	StorageTAE = StorageType("TAE")
	// StorageMEMKV MEMKV txn storage backend
	StorageMEMKV = StorageType("MEMKV")
	// StorageMEMKV MEM txn storage backend
	StorageMEM = StorageType("MEM")
)

// Option store option
type Option func(*store)

// Service TN Service
type Service interface {
	// Start start tn store. Start all DNShards currently managed by the Store and listen
	// to and process requests from CN and other DNs.
	Start() error
	// Close close tn store
	Close() error

	// StartTNReplica start the DNShard replica
	StartTNReplica(metadata.TNShard) error
	// CloseTNReplica close the DNShard replica.
	CloseTNReplica(shard metadata.TNShard) error

	// GetTaskService returns taskservice
	GetTaskService() (taskservice.TaskService, bool)
}
