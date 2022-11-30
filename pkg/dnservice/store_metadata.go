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

package dnservice

import (
	"path/filepath"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/file"
	"go.uber.org/zap"
)

const (
	metadataDir = "dnservice"
)

func getMetadataFile(uuid string) string {
	return filepath.Join(metadataDir, uuid)
}

func (s *store) initMetadata() error {
	data, err := file.ReadFile(s.metadataFileService, getMetadataFile(s.cfg.UUID))
	if err != nil {
		return err
	}

	if len(data) == 0 {
		s.mustUpdateMetadataLocked()
		return nil
	}

	v := &metadata.DNStore{}
	protoc.MustUnmarshal(v, data)
	if v.UUID != s.mu.metadata.UUID {
		s.rt.Logger().Fatal("BUG: disk DNStore and start DNStore not match",
			zap.String("disk-store", v.UUID))
	}
	s.mu.metadata = *v

	s.rt.Logger().Info("local DNShard loaded",
		zap.String("metadata", s.mu.metadata.DebugString()))
	return nil
}

func (s *store) addDNShardLocked(shard metadata.DNShard) {
	for _, dn := range s.mu.metadata.Shards {
		if dn.ShardID == shard.ShardID {
			return
		}
	}
	s.mu.metadata.Shards = append(s.mu.metadata.Shards, shard)
	s.mustUpdateMetadataLocked()
}

func (s *store) removeDNShard(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var newShards []metadata.DNShard
	for _, dn := range s.mu.metadata.Shards {
		if dn.ShardID != id {
			newShards = append(newShards, dn)
		}
	}
	s.mu.metadata.Shards = newShards
	s.mustUpdateMetadataLocked()
}

func (s *store) mustUpdateMetadataLocked() {
	if err := file.WriteFile(s.metadataFileService,
		getMetadataFile(s.cfg.UUID),
		protoc.MustMarshal(&s.mu.metadata)); err != nil {
		s.rt.Logger().Fatal("update metadata to local file failed",
			zap.Error(err))
	}
}
