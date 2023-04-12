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

package cnservice

import (
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/file"
	"github.com/stretchr/testify/assert"
)

func TestInitMetadata(t *testing.T) {
	fs, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	assert.NoError(t, err)

	s := &service{logger: logutil.GetPanicLogger(), metadataFS: fs}
	s.cfg = &Config{UUID: "1"}
	s.metadata.UUID = s.cfg.UUID
	assert.NoError(t, s.initMetadata())

	v, err := file.ReadFile(s.metadataFS, getMetadataFile("1"))
	assert.NoError(t, err)
	assert.NotEmpty(t, v)
}

func TestInitMetadataWithExistData(t *testing.T) {
	fs, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	assert.NoError(t, err)
	value := metadata.CNStore{
		UUID: "cn1",
	}
	assert.NoError(t, file.WriteFile(fs, getMetadataFile(value.UUID), protoc.MustMarshal(&value)))

	s := &service{logger: logutil.GetPanicLogger(), metadataFS: fs}
	s.cfg = &Config{UUID: "cn1"}
	s.metadata.UUID = s.cfg.UUID
	assert.NoError(t, s.initMetadata())
	assert.Equal(t, value, s.metadata)
}

func TestInitMetadataWithInvalidUUIDWillPanic(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			return
		}
		assert.Fail(t, "must panic")
	}()

	fs, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	assert.NoError(t, err)
	value := metadata.CNStore{
		UUID: "cn1",
	}
	assert.NoError(t, file.WriteFile(fs, getMetadataFile(value.UUID), protoc.MustMarshal(&value)))

	s := &service{logger: logutil.GetPanicLogger(), metadataFS: fs}
	assert.NoError(t, s.initMetadata())
}
