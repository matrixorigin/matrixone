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
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
)

func TestInitMetadata(t *testing.T) {
	fs, err := fileservice.NewMemoryFS()
	assert.NoError(t, err)

	s := &store{logger: logutil.GetPanicLogger(), localFS: fs}
	assert.NoError(t, s.initMetadata())
}

func TestInitMetadataWithExistData(t *testing.T) {
	fs, err := fileservice.NewMemoryFS()
	assert.NoError(t, err)
	value := metadata.DNStore{
		UUID: "dn1",
		Shards: []metadata.DNShard{
			{
				DNShardRecord: metadata.DNShardRecord{ShardID: 1},
			},
			{
				DNShardRecord: metadata.DNShardRecord{ShardID: 2},
			},
		},
	}
	assert.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: metadataFile,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   value.Size(),
				Data:   protoc.MustMarshal(&value),
			},
		},
	}))

	s := &store{logger: logutil.GetPanicLogger(), localFS: fs}
	s.mu.metadata.UUID = "dn1"
	assert.NoError(t, s.initMetadata())
	assert.Equal(t, value, s.mu.metadata)
}

func TestInitMetadataWithInvalidUUIDWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()

	fs, err := fileservice.NewMemoryFS()
	assert.NoError(t, err)
	value := metadata.DNStore{
		UUID: "dn1",
	}
	assert.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: metadataFile,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
				Size:   value.Size(),
				Data:   protoc.MustMarshal(&value),
			},
		},
	}))

	s := &store{logger: logutil.GetPanicLogger(), localFS: fs}
	assert.NoError(t, s.initMetadata())
}
