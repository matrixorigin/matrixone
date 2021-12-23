// Copyright 2021 Matrix Origin
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

package main

import (
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/pb/meta"
)

type shardStateAware struct {
	created         func(meta.Shard)
	updated         func(meta.Shard)
	splited         func(meta.Shard)
	destroyed       func(meta.Shard)
	becomeLeader    func(meta.Shard)
	becomeFollower  func(meta.Shard)
	snapshotApplied func(meta.Shard)
}

var _ aware.ShardStateAware = new(shardStateAware)

func (c *shardStateAware) Created(shard meta.Shard) {
	if c.created != nil {
		c.created(shard)
	}
}

func (c *shardStateAware) Updated(shard meta.Shard) {
	if c.updated != nil {
		c.updated(shard)
	}
}

func (c *shardStateAware) Splited(shard meta.Shard) {
	if c.splited != nil {
		c.splited(shard)
	}
}

func (c *shardStateAware) Destroyed(shard meta.Shard) {
	if c.destroyed != nil {
		c.destroyed(shard)
	}
}

func (c *shardStateAware) BecomeLeader(shard meta.Shard) {
	if c.becomeLeader != nil {
		c.becomeLeader(shard)
	}
}

func (c *shardStateAware) BecomeFollower(shard meta.Shard) {
	if c.becomeFollower != nil {
		c.becomeFollower(shard)
	}
}

func (c *shardStateAware) SnapshotApplied(shard meta.Shard) {
	if c.snapshotApplied != nil {
		c.snapshotApplied(shard)
	}
}
