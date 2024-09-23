// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Portions of this file are additionally subject to the following
// copyright.
//
// Copyright (C) 2021 Matrix Origin.
//
// Modified the behavior of the builder.

package operator

import (
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
)

// Builder is used to create operators. Usage:
//
//	op, err := NewBuilder(desc, cluster, shard).
//	            RemovePeer(store1).
//	            AddPeer(peer1).
//	            Build(kind)
//
// The generated Operator will choose the most appropriate execution order
// according to various constraints.
type Builder struct {
	// basic info
	desc    string
	shardID uint64
	epoch   uint64

	// operation record
	originPeers peersMap
	targetPeers peersMap

	// for non-voting
	originNonVotingPeers peersMap
	targetNonVotingPeers peersMap

	err error

	// intermediate states
	toAdd, toRemove peersMap // pending tasks.
	// non-voting replicas
	toAddNonVoting, toRemoveNonVoting peersMap // pending tasks.

	steps []OpStep // generated steps.
}

// NewBuilder creates a Builder.
func NewBuilder(desc string, shardInfo logservice.LogShardInfo) *Builder {
	b := &Builder{
		desc:    desc,
		shardID: shardInfo.ShardID,
		epoch:   shardInfo.Epoch,
	}

	// origin peers
	err := b.err
	originPeers := newPeersMap()
	originNonVotingPeers := newPeersMap()

	for replicaID, uuid := range shardInfo.Replicas {
		if uuid == "" {
			err = moerr.NewInternalErrorNoCtx("cannot build operator for shard with nil peer")
			break
		}
		originPeers.Set(uuid, replicaID)
	}
	for replicaID, uuid := range shardInfo.NonVotingReplicas {
		if uuid == "" {
			err = moerr.NewInternalErrorNoCtx("cannot build operator for shard with nil peer")
			break
		}
		originNonVotingPeers.Set(uuid, replicaID)
	}

	b.originPeers = originPeers
	b.targetPeers = originPeers.Copy()
	b.originNonVotingPeers = originNonVotingPeers
	b.targetNonVotingPeers = originNonVotingPeers.Copy()
	b.err = err
	return b
}

// AddPeer records an add Peer operation in Builder.
func (b *Builder) AddPeer(uuid string, peer uint64) *Builder {
	if b.err != nil {
		return b
	}
	if uuid == "" {
		b.err = moerr.NewInternalErrorNoCtx("cannot add peer to nil store")
		return b
	}
	if old, ok := b.targetPeers[uuid]; ok {
		b.err = moerr.NewInternalErrorNoCtxf("cannot add peer %+v to %s: already have peer %+v on %s", peer, uuid, old, uuid)
		return b
	}
	for oldUuid, old := range b.targetPeers {
		if old == peer {
			b.err = moerr.NewInternalErrorNoCtxf("cannot add peer %+v to %s: already have peer %+v on %s", peer, uuid, old, oldUuid)
			return b
		}
	}

	b.targetPeers.Set(uuid, peer)
	return b
}

// AddNonVotingPeer records an add non-voting Peer operation in Builder.
func (b *Builder) AddNonVotingPeer(uuid string, peer uint64) *Builder {
	if b.err != nil {
		return b
	}
	if uuid == "" {
		b.err = moerr.NewInternalErrorNoCtx("cannot add non-voting peer to nil store")
		return b
	}
	if old, ok := b.targetNonVotingPeers[uuid]; ok {
		b.err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("cannot add non-voting peer %+v to %s: "+
			"already have non-voting peer %+v on %s", peer, uuid, old, uuid))
		return b
	}
	for oldUuid, old := range b.targetNonVotingPeers {
		if old == peer {
			b.err = moerr.NewInternalErrorNoCtx(fmt.Sprintf("cannot add non-voting peer %+v to %s: "+
				"already have non-voting peer %+v on %s", peer, uuid, old, oldUuid))
			return b
		}
	}
	b.targetNonVotingPeers.Set(uuid, peer)
	return b
}

// RemovePeer records a remove peer operation in Builder.
func (b *Builder) RemovePeer(uuid string) *Builder {
	if b.err != nil {
		return b
	}
	if _, ok := b.targetPeers[uuid]; !ok {
		b.err = moerr.NewInternalErrorNoCtxf("cannot remove peer from %s: not found", uuid)
	} else {
		delete(b.targetPeers, uuid)
	}
	return b
}

// RemoveNonVotingPeer records a remove non-voting peer operation in Builder.
func (b *Builder) RemoveNonVotingPeer(uuid string) *Builder {
	if b.err != nil {
		return b
	}
	if _, ok := b.targetNonVotingPeers[uuid]; !ok {
		b.err = moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"cannot remove non-voting peer from %s: not found", uuid))
	} else {
		delete(b.targetNonVotingPeers, uuid)
	}
	return b
}

// Build creates the Operator.
func (b *Builder) Build() (*Operator, error) {
	if b.err != nil {
		return nil, b.err
	}

	brief := b.prepareBuild()

	if b.err = b.buildSteps(); b.err != nil {
		return nil, b.err
	}

	return NewOperator(brief, b.shardID, b.epoch, b.steps...), nil
}

func (b *Builder) prepareBuild() string {
	b.toAdd = newPeersMap()
	b.toRemove = newPeersMap()
	b.toAddNonVoting = newPeersMap()
	b.toRemoveNonVoting = newPeersMap()

	for uuid, replicaID := range b.originPeers {
		// new peer not exists
		if _, ok := b.targetPeers[uuid]; !ok {
			b.toRemove.Set(uuid, replicaID)
		}
	}

	for uuid, replicaID := range b.originNonVotingPeers {
		// new peer not exists
		if _, ok := b.targetNonVotingPeers[uuid]; !ok {
			b.toRemoveNonVoting.Set(uuid, replicaID)
		}
	}

	for uuid, replicaID := range b.targetPeers {
		// old peer not exists.
		if _, ok := b.originPeers[uuid]; !ok {
			b.toAdd.Set(uuid, replicaID)
		}
	}

	for uuid, replicaID := range b.targetNonVotingPeers {
		// old peer not exists.
		if _, ok := b.originNonVotingPeers[uuid]; !ok {
			b.toAddNonVoting.Set(uuid, replicaID)
		}
	}

	return b.brief()
}

func (b *Builder) buildToRemoveSteps() *Builder {
	for len(b.toRemove) > 0 {
		var targets []string
		for target := range b.targetPeers {
			targets = append(targets, target)
		}
		sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })

		uuid, replicaID := b.toRemove.Get()
		b.steps = append(b.steps, RemoveLogService{
			Target:  targets[0],
			Replica: Replica{uuid, b.shardID, replicaID, b.epoch},
		})
		delete(b.toRemove, uuid)
	}
	return b
}

func (b *Builder) buildToRemoveNonVotingSteps() *Builder {
	for len(b.toRemoveNonVoting) > 0 {
		var targets []string
		for target := range b.targetPeers {
			targets = append(targets, target)
		}
		sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })

		uuid, replicaID := b.toRemoveNonVoting.Get()
		b.steps = append(b.steps, RemoveNonVotingLogService{
			Target:  targets[0],
			Replica: Replica{uuid, b.shardID, replicaID, b.epoch},
		})
		delete(b.toRemoveNonVoting, uuid)
	}
	return b
}

func (b *Builder) buildToAddSteps() *Builder {
	for len(b.toAdd) > 0 {
		var targets []string
		for target := range b.originPeers {
			targets = append(targets, target)
		}
		sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })

		uuid, replicaID := b.toAdd.Get()
		b.steps = append(b.steps, AddLogService{
			Target: targets[0],
			Replica: Replica{
				UUID:      uuid,
				ShardID:   b.shardID,
				ReplicaID: replicaID,
				Epoch:     b.epoch,
			},
		})
		delete(b.toAdd, uuid)
	}
	return b
}

func (b *Builder) buildToAddNonVotingSteps() *Builder {
	for len(b.toAddNonVoting) > 0 {
		var targets []string
		for target := range b.originPeers {
			targets = append(targets, target)
		}
		sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })

		uuid, replicaID := b.toAddNonVoting.Get()
		b.steps = append(b.steps, AddNonVotingLogService{
			Target: targets[0],
			Replica: Replica{
				UUID:      uuid,
				ShardID:   b.shardID,
				ReplicaID: replicaID,
				Epoch:     b.epoch,
			},
		})
		delete(b.toAddNonVoting, uuid)
	}
	return b
}

func (b *Builder) buildSteps() error {
	b.buildToRemoveSteps().
		buildToAddSteps().
		buildToRemoveNonVotingSteps().
		buildToAddNonVotingSteps()
	if len(b.steps) == 0 {
		return moerr.NewInternalErrorNoCtx("no operator step is built")
	}
	return nil
}

// generate brief description of the operator.
func (b *Builder) brief() string {
	switch {
	case len(b.toAdd) > 0:
		return fmt.Sprintf("add peer: store %s", b.toAdd)
	case len(b.toRemove) > 0:
		return fmt.Sprintf("rm peer: store %s", b.toRemove)
	case len(b.toAddNonVoting) > 0:
		return fmt.Sprintf("add non-voting peer: store %s", b.toAddNonVoting)
	case len(b.toRemoveNonVoting) > 0:
		return fmt.Sprintf("rm non-voting peer: store %s", b.toRemoveNonVoting)
	default:
		return ""
	}
}

// Replicas indexed by store's uuid.
type peersMap map[string]uint64

func newPeersMap() peersMap {
	return make(map[string]uint64)
}

func (pm peersMap) Get() (string, uint64) {
	for uuid, replicaID := range pm {
		return uuid, replicaID
	}
	return "", 0
}

func (pm peersMap) Set(uuid string, replicaID uint64) {
	pm[uuid] = replicaID
}

func (pm peersMap) String() string {
	uuids := make([]string, 0, len(pm))
	for uuid := range pm {
		uuids = append(uuids, uuid)
	}
	return fmt.Sprintf("%v", uuids)
}

func (pm peersMap) Copy() peersMap {
	var pm2 peersMap = make(map[string]uint64, len(pm))
	for uuid, replicaID := range pm {
		pm2.Set(uuid, replicaID)
	}
	return pm2
}
