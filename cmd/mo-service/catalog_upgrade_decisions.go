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

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

func publishCatalogUpgradeDecisions(ctx context.Context, configPath, inputPath string) error {
	input, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer input.Close()
	cfg := NewConfig()
	if err := parseConfigFromFile(configPath, cfg); err != nil {
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}
	if !strings.EqualFold(cfg.ServiceType, metadata.ServiceType_CN.String()) {
		return fmt.Errorf("decision publication requires a CN configuration")
	}
	for _, fsConfig := range cfg.FileServices {
		if !strings.EqualFold(fsConfig.Name, defines.SharedFileServiceName) {
			continue
		}
		// The utility has no running CN cache or remote-cache service.
		fsConfig.Cache = fileservice.DisabledCacheConfig
		fs, err := fileservice.NewFileService(ctx, fsConfig, nil)
		if err != nil {
			return err
		}
		defer fs.Close(ctx)
		return bootstrap.PublishSnapshotUpgradeDecisions(ctx, fs, input)
	}
	return fmt.Errorf("CN configuration has no shared file service")
}
