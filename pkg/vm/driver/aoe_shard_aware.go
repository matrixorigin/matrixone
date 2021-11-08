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

package driver

import (
	"github.com/matrixorigin/matrixcube/pb/meta"
)

func (h *driver) addShardCallback(shard meta.Shard) error {
	return nil
}

func (h *driver) Created(shard meta.Shard) {

}

func (h *driver) Splited(shard meta.Shard) {

}

func (h *driver) Destroyed(shard meta.Shard) {
}

func (h *driver) BecomeLeader(shard meta.Shard) {

}

func (h *driver) BecomeFollower(shard meta.Shard) {

}

func (h *driver) SnapshotApplied(shard meta.Shard) {

}
