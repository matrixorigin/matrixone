// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/m0admission"
)

func validateM0BirthLaunch(cfg *LaunchConfig) error {
	if !m0admission.Enabled() {
		return nil
	}
	if cfg.Dynamic.Enable || len(cfg.ProxyServiceConfigsFiles) > 0 || len(cfg.PythonUdfServiceConfigsFiles) > 0 ||
		len(cfg.LogServiceConfigFiles) != 1 || len(cfg.TNServiceConfigsFiles) != 1 || len(cfg.CNServiceConfigsFiles) == 0 {
		return fmt.Errorf("M0 birth deployment requires static in-process LOG/TN/CN roles only")
	}
	files := []string{*launchFile}
	for _, group := range []struct {
		role  string
		files []string
	}{
		{"LOG", cfg.LogServiceConfigFiles}, {"TN", cfg.TNServiceConfigsFiles}, {"CN", cfg.CNServiceConfigsFiles},
	} {
		for _, path := range group.files {
			parsed := NewConfig()
			if err := parseConfigFromFile(path, parsed); err != nil {
				return err
			}
			if parsed.ServiceType != group.role {
				return fmt.Errorf("M0 birth deployment role mismatch")
			}
			if err := parsed.validate(); err != nil {
				return err
			}
			if err := validateM0BirthConfig(parsed); err != nil {
				return err
			}
			files = append(files, path)
		}
	}
	return m0admission.CheckConfigFiles(files)
}

func validateM0BirthConfig(cfg *Config) error {
	if err := m0admission.CheckDataDirectory(cfg.DataDir); err != nil {
		return err
	}
	for _, fs := range cfg.FileServices {
		if err := m0admission.CheckDataDirectory(fs.DataDir); err != nil {
			return err
		}
		if fs.Cache.DiskPath != nil && *fs.Cache.DiskPath != "" {
			if err := m0admission.CheckDataDirectory(*fs.Cache.DiskPath); err != nil {
				return err
			}
		}
		if m0admission.Enabled() {
			switch fs.Backend {
			case "DISK", "DISK-V2", "DISK-ETL", "DISK-TMP":
			default:
				return fmt.Errorf("M0 birth deployment requires managed local disk services")
			}
		}
	}
	return nil
}
