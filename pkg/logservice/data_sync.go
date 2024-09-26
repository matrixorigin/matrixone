// Copyright 2021 - 2024 Matrix Origin
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

package logservice

import "context"

type ChangeType int

const (
	AddReplica ChangeType = 0
	DelReplica ChangeType = 1
)

type DataSync interface {
	// Append appends data to the datasync module, which will sync data
	// to another storage.
	Append(ctx context.Context, lsn uint64, data []byte)

	// NotifyReplicaID notifies the replicaID of shard on this store.
	// DataSync is an interface that sync data to another storage.
	NotifyReplicaID(shardID uint64, replicaID uint64, typ ChangeType)

	// Close closes the datasync module and release the resource.
	Close() error
}
