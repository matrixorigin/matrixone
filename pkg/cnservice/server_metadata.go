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
	"path/filepath"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/util/file"
	"go.uber.org/zap"
)

const (
	metadataDir = "cnservice"
)

func getMetadataFile(uuid string) string {
	return filepath.Join(metadataDir, uuid)
}

func (s *service) initMetadata() error {
	data, err := file.ReadFile(s.metadataFS, getMetadataFile(s.cfg.UUID))
	if err != nil {
		return err
	}

	if len(data) == 0 {
		s.mustUpdateMetadata()
		return nil
	}

	v := &metadata.CNStore{}
	protoc.MustUnmarshal(v, data)
	if v.UUID != s.metadata.UUID {
		s.logger.Fatal("BUG: disk CNStore and start CNStore not match",
			zap.String("disk-store", v.UUID))
	}
	s.metadata = *v
	s.logger.Info("local CNStore loaded",
		zap.String("metadata", s.metadata.DebugString()))
	return nil
}

func (s *service) mustUpdateMetadata() {
	if err := file.WriteFile(s.metadataFS,
		getMetadataFile(s.cfg.UUID),
		protoc.MustMarshal(&s.metadata)); err != nil {
		s.logger.Fatal("update metadata to local file failed",
			zap.Error(err))
	}
}
