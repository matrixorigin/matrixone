// Copyright 2026 Matrix Origin
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

import (
	"context"
	"fmt"
	"maps"
	"testing"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// waitForRecovery runs each probe synchronously so a timeout cannot leave a
// callback using a service after test cleanup has started.
func waitForRecovery(t *testing.T, ctx context.Context, what string, timeout, attemptTimeout time.Duration, probe func(context.Context) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		attempt, stop := context.WithTimeout(ctx, attemptTimeout)
		ready := probe(attempt)
		stop()
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", what, ctx.Err())
		case <-ticker.C:
		}
	}
}

// Raft resolves NodeHost IDs through gossip. Start elections only after each
// test host has received the metadata for the other hosts.
func waitForRecoveryPeers(t *testing.T, ctx context.Context, services []*Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		missing := ""
		for _, service := range services {
			registry, ok := service.store.nh.GetNodeHostRegistry()
			if !ok {
				t.Fatal("gossip registry is disabled")
			}
			for _, peer := range services {
				if peer == service {
					continue
				}
				if _, ok := registry.GetMeta(peer.ID()); !ok {
					missing = fmt.Sprintf("%s cannot resolve %s", service.ID(), peer.ID())
					break
				}
			}
			if missing != "" {
				break
			}
		}
		if missing == "" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for recovery peers: %s: %v", missing, ctx.Err())
		case <-ticker.C:
		}
	}
}

// A pending local replica is omitted from heartbeats. Wait for the exact
// reporter and membership before using a heartbeat as the setup inventory.
func recoveryHeartbeat(t *testing.T, ctx context.Context, service *Service, shardID, replicaID uint64, members map[uint64]string) pb.LogStoreHeartbeat {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()
	for {
		hb := service.store.getHeartbeatMessage()
		for _, replica := range hb.Replicas {
			if replica.ShardID == shardID && replica.ReplicaID == replicaID && !replica.IsNonVoting && maps.Equal(replica.Replicas, members) {
				return hb
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s to report shard %d replica %d: %v; last heartbeat: %+v", service.ID(), shardID, replicaID, ctx.Err(), hb.Replicas)
		case <-ticker.C:
		}
	}
}
