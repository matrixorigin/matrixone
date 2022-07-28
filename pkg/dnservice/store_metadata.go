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
	"context"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"go.uber.org/zap"
)

const (
	metadataFile = "dnstore/metadata.data"
)

func (s *store) initMetadata() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	vec := &fileservice.IOVector{
		FilePath: metadataFile,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   -1,
			},
		},
	}
	if err := s.localFS.Read(ctx, vec); err != nil {
		if err == fileservice.ErrFileNotFound {
			return nil
		}
		return err
	}

	if len(vec.Entries[0].Data) == 0 {
		return nil
	}

	v := &metadata.DNStore{}
	protoc.MustUnmarshal(v, vec.Entries[0].Data)
	if v.UUID != s.mu.metadata.UUID {
		s.logger.Fatal("BUG: disk DNStore and start DNStore not match",
			zap.String("disk-store", v.UUID))
	}
	s.mu.metadata = *v
	return nil
}

func (s *store) addDNShard(shard metadata.DNShard) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	vec := fileservice.IOVector{
		FilePath: metadataFile,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   s.mu.metadata.Size(),
				Data:   protoc.MustMarshal(&s.mu.metadata),
			},
		},
	}
	if err := s.localFS.Replace(ctx, vec); err != nil {
		s.logger.Fatal("update metadata to local file failed",
			zap.Error(err))
	}
}
