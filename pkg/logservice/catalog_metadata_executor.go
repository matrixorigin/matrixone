// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logservice

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/lni/dragonboat/v4"
	"github.com/matrixorigin/matrixone/pkg/hakeeper"
	pb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

const catalogExecutorFilename = "mo-catalog-executor.metadata"

// This version requires durable maintenance, complete membership reservations,
// and the serialized start/revocation supervisor. It does not advertise any
// catalog View or recovery implementation.
const catalogExecutorProtocol uint64 = 1

func (l *store) catalogExecutorCapabilities() *pb.CatalogMetadataCapabilities {
	l.catalogExecutor.Lock()
	defer l.catalogExecutor.Unlock()
	incarnation := l.getStoreIncarnation()
	if !l.catalogExecutor.enabled || l.catalogExecutor.failed || incarnation == "" ||
		(l.catalogExecutor.record.Permit.Token != 0 && l.catalogExecutor.record.Permit.StoreIncarnation != incarnation) {
		return nil
	}
	return &pb.CatalogMetadataCapabilities{HAKeeperBarrierProtocol: catalogExecutorProtocol}
}

// One HAKeeper replica per store: the latch and permit high-water occupy one
// durable slot, independent of the number of delivered/retried commands.
type catalogExecutorRecord struct {
	Version uint32
	UUID    string
	State   string
	Permit  pb.CatalogMetadataStartPermit
}

func (r *catalogExecutorRecord) Marshal() ([]byte, error) { return json.Marshal(r) }
func (r *catalogExecutorRecord) Unmarshal(b []byte) error { return json.Unmarshal(b, r) }

type catalogExecutor struct {
	sync.Mutex
	enabled bool // immutable after store construction
	failed  bool // uncertain persistence is terminal until disk recovery
	record  catalogExecutorRecord
}

func catalogExecutionError() error {
	return fmt.Errorf("catalog metadata executor rejected stale or unauthorized HAKeeper operation")
}

func (l *store) loadCatalogExecutor() error {
	c := &l.catalogExecutor
	found, err := exist(l.cfg.FS.PathJoin(l.cfg.DataDir, catalogExecutorFilename), l.cfg.FS)
	if err != nil {
		return err
	}
	if found {
		if err := readMetadataFile(l.cfg.DataDir, catalogExecutorFilename, &c.record, l.cfg.FS); err != nil {
			return err
		}
		r := &c.record
		if r.Version != 1 || r.UUID != l.cfg.UUID {
			return catalogExecutionError()
		}
		switch r.State {
		case "", "STARTING", "STARTED", "REVOKING", "REVOKED":
		default:
			return catalogExecutionError()
		}
		if (r.State == "") != (r.Permit.Token == 0) || (r.Permit.Token != 0 && (r.Permit.UUID != r.UUID || r.Permit.ReplicaID == 0 || r.Permit.StoreIncarnation == "")) {
			return catalogExecutionError()
		}
	} else if l.cfg.CatalogMetadataMaintenance {
		r := catalogExecutorRecord{Version: 1, UUID: l.cfg.UUID}
		if err := l.saveCatalogExecutor(r); err != nil {
			return err
		}
	}
	c.enabled = found || l.cfg.CatalogMetadataMaintenance
	return nil
}

// Called under the supervisor lock (or before publication at construction).
func (l *store) saveCatalogExecutor(r catalogExecutorRecord) error {
	if err := createMetadataFile(l.cfg.DataDir, catalogExecutorFilename, &r, l.cfg.FS); err != nil {
		l.catalogExecutor.failed = true
		return err
	}
	l.catalogExecutor.record = r
	return nil
}

func sameCatalogPermit(a, b pb.CatalogMetadataStartPermit) bool {
	return a.Token == b.Token && a.ReplicaID == b.ReplicaID && a.UUID == b.UUID && a.StoreIncarnation == b.StoreIncarnation && a.NonVoting == b.NonVoting
}

func (l *store) catalogReplicaPresent(p pb.CatalogMetadataStartPermit, ready bool) bool {
	for _, s := range l.nh.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: true}).ShardInfoList {
		if s.ShardID == hakeeper.DefaultHAKeeperShardID && s.ReplicaID == p.ReplicaID && (!ready || (s.IsNonVoting == p.NonVoting && !s.Pending && s.ConfigChangeIndex != 0)) {
			return true
		}
	}
	return false
}

// Serializes actual Start/Stop with the durable high-water. Transport timeout
// never changes the permit. REVOKING is persisted before Stop so a crash cannot
// restore a canceled STARTING record into a running replica.
func (l *store) executeCatalogStart(p pb.CatalogMetadataStartPermit) error {
	return l.executeCatalogStartWith(p, l)
}

// The owner accepts a replica controller so persistence and execution failures
// can be exercised without network timing; production always supplies the store.
type catalogReplicaController interface {
	catalogReplicaPresent(pb.CatalogMetadataStartPermit, bool) bool
	catalogStartReplica(pb.CatalogMetadataStartPermit) error
	catalogStopReplica(pb.CatalogMetadataStartPermit) error
}

func (l *store) catalogStartReplica(p pb.CatalogMetadataStartPermit) error {
	join := !l.nh.HasNodeInfo(hakeeper.DefaultHAKeeperShardID, p.ReplicaID)
	if p.NonVoting {
		return l.startHAKeeperNonVotingReplicaRaw(p.ReplicaID, nil, join)
	}
	return l.startHAKeeperReplicaRaw(p.ReplicaID, nil, join)
}
func (l *store) catalogStopReplica(p pb.CatalogMetadataStartPermit) error {
	if l.catalogReplicaPresent(p, false) {
		if err := l.stopReplicaRaw(hakeeper.DefaultHAKeeperShardID, p.ReplicaID); err != nil {
			return err
		}
	}
	// REVOKING is already durable. Remove stale local recovery metadata even
	// after a crash between StopReplica and this cleanup; retain the tombstone.
	l.removeMetadata(hakeeper.DefaultHAKeeperShardID, p.ReplicaID)
	return nil
}

func (l *store) executeCatalogStartWith(p pb.CatalogMetadataStartPermit, host catalogReplicaController) error {
	c := &l.catalogExecutor
	c.Lock()
	defer c.Unlock()
	return l.executeCatalogStartLocked(p, host)
}

func (l *store) executeCatalogStartLocked(p pb.CatalogMetadataStartPermit, host catalogReplicaController) error {
	c := &l.catalogExecutor
	if !c.enabled || c.failed || p.Token == 0 || p.ReplicaID == 0 || p.UUID != l.cfg.UUID || p.StoreIncarnation == "" || p.StoreIncarnation != l.getStoreIncarnation() || p.Completed {
		return catalogExecutionError()
	}
	r := c.record
	if p.RevocationPending {
		p.Revoked = true
		p.RevocationPending = false
	}
	if p.Token < r.Permit.Token {
		return catalogExecutionError()
	}
	if p.Token == r.Permit.Token {
		if !sameCatalogPermit(p, r.Permit) {
			return catalogExecutionError()
		}
		if (r.State == "REVOKED" || r.State == "REVOKING") && !p.Revoked {
			return catalogExecutionError()
		}
	} else {
		// Retirement of the same identity must not depend on successfully
		// finishing (or restarting) the older start operation.
		if (r.State == "STARTING" || r.State == "REVOKING") &&
			!(p.Revoked && r.Permit.ReplicaID == p.ReplicaID && r.Permit.NonVoting == p.NonVoting) {
			return catalogExecutionError()
		}
		// A new incarnation cannot retire or acknowledge the old supervisor.
		if r.Permit.Token != 0 && r.Permit.StoreIncarnation != p.StoreIncarnation {
			return catalogExecutionError()
		}
		if r.Permit.Token != 0 && r.State != "REVOKED" && (r.Permit.ReplicaID != p.ReplicaID || r.Permit.NonVoting != p.NonVoting) {
			return catalogExecutionError()
		}
		if r.Permit.Token != 0 && r.State == "REVOKED" && r.Permit.ReplicaID != p.ReplicaID {
			// The retired ID is not an admitted restart candidate for the new
			// permit. Do not accumulate old HAKeeper metadata across replacements.
			l.removeMetadata(hakeeper.DefaultHAKeeperShardID, r.Permit.ReplicaID)
		}
		r.Permit = p
		r.Permit.Completed, r.Permit.Revoked, r.Permit.RevocationPending = false, false, false
		r.State = "STARTING"
		if p.Revoked {
			r.State = "REVOKING"
		}
		if err := l.saveCatalogExecutor(r); err != nil {
			return err
		}
	}
	if p.Revoked {
		if r.State == "REVOKED" {
			return nil
		}
		if r.State != "REVOKING" {
			r.State = "REVOKING"
			if err := l.saveCatalogExecutor(r); err != nil {
				return err
			}
		}
		if err := host.catalogStopReplica(p); err != nil {
			return err
		}
		if host.catalogReplicaPresent(p, false) {
			return catalogExecutionError()
		}
		r.State = "REVOKED"
		return l.saveCatalogExecutor(r)
	}
	if !host.catalogReplicaPresent(p, false) {
		// STARTED is an observation of the previous process, not proof that this
		// process has restarted the replica. Keep the same token during recovery.
		if r.State != "STARTING" {
			r.State = "STARTING"
			if err := l.saveCatalogExecutor(r); err != nil {
				return err
			}
		}
		if err := host.catalogStartReplica(p); err != nil {
			return err
		}
	}
	if host.catalogReplicaPresent(p, true) && r.State != "STARTED" {
		r.State = "STARTED"
		return l.saveCatalogExecutor(r)
	}
	return nil
}

// Recovery is restricted to the local admitted metadata captured by the
// controlled maintenance cutover. It is not a schedule-command bypass.
func (l *store) recoverCatalogReplica(replicaID uint64, nonVoting bool) error {
	c := &l.catalogExecutor
	c.Lock()
	defer c.Unlock()
	if c.failed {
		return catalogExecutionError()
	}
	r := c.record
	if r.Permit.Token != 0 {
		if r.Permit.ReplicaID != replicaID {
			// Once a durable permit exists, legacy metadata for another replica
			// ID cannot authorize startup. The supervised ID is restored by its
			// own metadata entry, without querying an unavailable quorum.
			return nil
		}
		if r.Permit.NonVoting != nonVoting {
			return catalogExecutionError()
		}
		if r.State == "REVOKED" || r.State == "REVOKING" {
			return nil
		}
		return l.executeCatalogStartLocked(r.Permit, l)
	}
	if nonVoting {
		return l.startHAKeeperNonVotingReplicaRaw(replicaID, nil, false)
	}
	return l.startHAKeeperReplicaRaw(replicaID, nil, false)
}

func (l *store) catalogStartHeartbeat() *pb.CatalogMetadataStartPermit {
	c := &l.catalogExecutor
	c.Lock()
	defer c.Unlock()
	if !c.enabled || c.failed || c.record.Permit.Token == 0 {
		return nil
	}
	p := c.record.Permit
	if p.StoreIncarnation != l.getStoreIncarnation() {
		return nil
	}
	if c.record.State == "REVOKING" {
		p.Revoked = true
	}
	if c.record.State != "REVOKED" {
		if err := l.executeCatalogStartLocked(p, l); err != nil {
			return nil
		}
	}
	switch c.record.State {
	case "STARTED":
		if !l.catalogReplicaPresent(p, true) {
			return nil
		}
		p.Completed, p.Revoked, p.RevocationPending = true, false, false
	case "REVOKED":
		p.Completed, p.Revoked, p.RevocationPending = false, true, false
	default:
		return nil
	}
	return &p
}

func sameCatalogMembership(a, b *pb.CatalogMetadataMembershipOperation) bool {
	return a != nil && b != nil && a.Token != 0 && a.Token == b.Token && a.ConfigChangeIndex == b.ConfigChangeIndex && a.ReplicaID == b.ReplicaID && a.UUID == b.UUID && a.StoreIncarnation == b.StoreIncarnation && a.ChangeType == b.ChangeType && maps.Equal(a.ExpectedVoting, b.ExpectedVoting) && maps.Equal(a.ExpectedNonVoting, b.ExpectedNonVoting)
}

func catalogMembershipMatches(p *pb.CatalogMetadataMembershipOperation, m *dragonboat.Membership) bool {
	if len(p.ExpectedVoting) == 0 || !maps.Equal(p.ExpectedVoting, m.Nodes) || !maps.Equal(p.ExpectedNonVoting, m.NonVotings) {
		return false
	}
	v, vok := m.Nodes[p.ReplicaID]
	n, nok := m.NonVotings[p.ReplicaID]
	switch p.ChangeType {
	case pb.AddReplica:
		return vok && v == p.UUID && !nok
	case pb.AddNonVotingReplica:
		return nok && n == p.UUID && !vok
	case pb.RemoveReplica, pb.RemoveNonVotingReplica:
		return !vok && !nok
	default:
		return false
	}
}

// At the reserved index, validate the entire projected membership before
// submitting anything. A stale/inconsistent checker image cannot authorize a
// mutation of another member even when the requested target itself matches.
func catalogMembershipCanApply(p *pb.CatalogMetadataMembershipOperation, m *dragonboat.Membership) bool {
	projected := &dragonboat.Membership{Nodes: maps.Clone(m.Nodes), NonVotings: maps.Clone(m.NonVotings)}
	if projected.Nodes == nil {
		projected.Nodes = make(map[uint64]string)
	}
	if projected.NonVotings == nil {
		projected.NonVotings = make(map[uint64]string)
	}
	_, voting := projected.Nodes[p.ReplicaID]
	_, nonVoting := projected.NonVotings[p.ReplicaID]
	switch p.ChangeType {
	case pb.AddReplica, pb.AddNonVotingReplica:
		if voting || nonVoting {
			return false
		}
		if p.ChangeType == pb.AddReplica {
			projected.Nodes[p.ReplicaID] = p.UUID
		} else {
			projected.NonVotings[p.ReplicaID] = p.UUID
		}
	case pb.RemoveReplica:
		if projected.Nodes[p.ReplicaID] != p.UUID || nonVoting {
			return false
		}
		delete(projected.Nodes, p.ReplicaID)
	case pb.RemoveNonVotingReplica:
		if projected.NonVotings[p.ReplicaID] != p.UUID || voting {
			return false
		}
		delete(projected.NonVotings, p.ReplicaID)
	default:
		return false
	}
	return catalogMembershipMatches(p, projected)
}

// Reads replicated ownership before every attempt. Once admitted, an in-flight
// mutation remains fenced by Dragonboat's ordered config index, even if its
// caller times out and another executor completes the reservation.
type catalogMembershipController interface {
	catalogBarrierState(context.Context) (*pb.CatalogMetadataBarrierState, error)
	catalogMembership(context.Context) (*dragonboat.Membership, error)
	catalogMutateMembership(context.Context, *pb.CatalogMetadataMembershipOperation) error
	catalogCompleteMembership(context.Context, *pb.CatalogMetadataMembershipOperation, *dragonboat.Membership) error
}

func (l *store) catalogBarrierState(ctx context.Context) (*pb.CatalogMetadataBarrierState, error) {
	v, err := l.read(ctx, hakeeper.DefaultHAKeeperShardID, &hakeeper.CatalogMetadataStateQuery{})
	if err != nil {
		return nil, err
	}
	b, ok := v.(*pb.CatalogMetadataBarrierState)
	if !ok {
		return nil, catalogExecutionError()
	}
	return b, nil
}
func (l *store) catalogMembership(ctx context.Context) (*dragonboat.Membership, error) {
	return l.nh.SyncGetShardMembership(ctx, hakeeper.DefaultHAKeeperShardID)
}
func (l *store) catalogMutateMembership(ctx context.Context, p *pb.CatalogMetadataMembershipOperation) error {
	switch p.ChangeType {
	case pb.AddReplica:
		return l.nh.SyncRequestAddReplica(ctx, hakeeper.DefaultHAKeeperShardID, p.ReplicaID, p.UUID, p.ConfigChangeIndex)
	case pb.AddNonVotingReplica:
		return l.nh.SyncRequestAddNonVoting(ctx, hakeeper.DefaultHAKeeperShardID, p.ReplicaID, p.UUID, p.ConfigChangeIndex)
	case pb.RemoveReplica, pb.RemoveNonVotingReplica:
		return l.nh.SyncRequestDeleteReplica(ctx, hakeeper.DefaultHAKeeperShardID, p.ReplicaID, p.ConfigChangeIndex)
	default:
		return catalogExecutionError()
	}
}
func (l *store) catalogCompleteMembership(ctx context.Context, p *pb.CatalogMetadataMembershipOperation, m *dragonboat.Membership) error {
	cmd, err := hakeeper.GetCatalogMetadataRequestCmd(pb.CatalogMetadataRequest{Version: 1, Action: pb.CATALOG_ACTION_COMPLETE_MEMBERSHIP, Token: p.Token, ObservedConfigChangeIndex: m.ConfigChangeID, ObservedVoting: m.Nodes, ObservedNonVoting: m.NonVotings})
	if err != nil {
		return err
	}
	result, err := l.propose(ctx, l.nh.GetNoOPSession(hakeeper.DefaultHAKeeperShardID), cmd)
	if err != nil {
		return err
	}
	if result.Value != hakeeper.CatalogMetadataApplied {
		return catalogExecutionError()
	}
	return nil
}

func (l *store) reconcileCatalogMembership(ctx context.Context, expected *pb.CatalogMetadataMembershipOperation) error {
	if !l.catalogExecutor.enabled {
		return catalogExecutionError()
	}
	return reconcileCatalogMembership(ctx, expected, l)
}

func reconcileCatalogMembership(ctx context.Context, expected *pb.CatalogMetadataMembershipOperation, owner catalogMembershipController) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b, err := owner.catalogBarrierState(ctx)
	if err != nil {
		return err
	}
	if b == nil || b.Arbitration == nil || !b.Arbitration.MaintenanceEnabled {
		return catalogExecutionError()
	}
	p := b.Arbitration.Reservation
	if p == nil {
		if expected == nil {
			return nil
		}
		return catalogExecutionError()
	}
	if expected != nil && !sameCatalogMembership(expected, p) {
		return catalogExecutionError()
	}
	if p.Token == 0 || p.ConfigChangeIndex == 0 || p.ReplicaID == 0 || p.UUID == "" || p.StoreIncarnation == "" {
		return catalogExecutionError()
	}
	m, err := owner.catalogMembership(ctx)
	if err != nil {
		return err
	}
	if m == nil || m.ConfigChangeID < p.ConfigChangeIndex {
		return catalogExecutionError()
	}
	if m.ConfigChangeID == p.ConfigChangeIndex {
		if !catalogMembershipCanApply(p, m) {
			return catalogExecutionError()
		}
		if err := owner.catalogMutateMembership(ctx, p); err != nil {
			return err
		}
		m, err = owner.catalogMembership(ctx)
		if err != nil {
			return err
		}
		if m == nil {
			return catalogExecutionError()
		}
	}
	if m.ConfigChangeID <= p.ConfigChangeIndex {
		return nil
	}
	if !catalogMembershipMatches(p, m) {
		return catalogExecutionError()
	}
	return owner.catalogCompleteMembership(ctx, p, m)
}

// The schedule command is an internal HAKeeper delivery. New members cannot
// perform a local SyncRead before StartReplica, so start authorization travels
// in the durable permit and is fenced locally by the supervisor.
func (l *store) executeCatalogCommand(cmd pb.ScheduleCommand) error {
	cfg := cmd.ConfigChange
	if cfg == nil || cfg.Replica.ShardID != hakeeper.DefaultHAKeeperShardID || !l.catalogExecutor.enabled {
		return catalogExecutionError()
	}
	switch cfg.ChangeType {
	case pb.AddReplica, pb.AddNonVotingReplica, pb.RemoveReplica, pb.RemoveNonVotingReplica:
		p := cmd.CatalogMetadataMembership
		if p == nil || p.ReplicaID != cfg.Replica.ReplicaID || p.UUID != cfg.Replica.UUID || p.ConfigChangeIndex != cfg.Replica.Epoch || p.ChangeType != cfg.ChangeType {
			return catalogExecutionError()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return l.reconcileCatalogMembership(ctx, p)
	case pb.StartReplica, pb.StartNonVotingReplica, pb.StopReplica, pb.StopNonVotingReplica, pb.KillZombie:
		p := cmd.CatalogMetadataStart
		if p == nil || p.ReplicaID != cfg.Replica.ReplicaID || p.UUID != cfg.Replica.UUID {
			return catalogExecutionError()
		}
		stop := cfg.ChangeType == pb.StopReplica || cfg.ChangeType == pb.StopNonVotingReplica || cfg.ChangeType == pb.KillZombie
		if (p.Revoked || p.RevocationPending) != stop || (!stop && (p.NonVoting != (cfg.ChangeType == pb.StartNonVotingReplica) || len(cfg.InitialMembers) != 0)) {
			return catalogExecutionError()
		}
		if err := l.executeCatalogStart(*p); err != nil {
			return err
		}
		if cfg.ChangeType == pb.KillZombie {
			l.removeMetadata(hakeeper.DefaultHAKeeperShardID, p.ReplicaID)
		}
		return nil
	default:
		return catalogExecutionError()
	}
}
