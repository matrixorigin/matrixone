// Copyright 2022 Matrix Origin
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
	"fmt"
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/dnservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

const (
	cnServiceType         = "CN"
	dnServiceType         = "DN"
	logServiceType        = "LOG"
	standaloneServiceType = "STANDALONE"
)

var (
	supportServiceTypes = map[string]any{
		cnServiceType:         cnServiceType,
		dnServiceType:         dnServiceType,
		logServiceType:        logServiceType,
		standaloneServiceType: standaloneServiceType,
	}
)

// Config mo-service configuration
type Config struct {
	// Log log config
	Log logutil.LogConfig `toml:"log"`
	// ServiceType service type, select the corresponding configuration to start the
	// service according to the service type. [CN|DN|Log|Standalone]
	ServiceType string `toml:"service-type"`
	// FileServices the config for file services
	FileServices []fileservice.Config `toml:"fileservice"`

	HAKeeperClient logservice.HAKeeperClientConfig `toml:"hakeeper-client"`
	// DN dn service config
	DN dnservice.Config `toml:"dn"`
	// LogService is the config for log service
	LogService logservice.Config `toml:"logservice"`
}

func parseConfigFromFile(file string) (*Config, error) {
	if file == "" {
		return nil, fmt.Errorf("toml config file not set")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	return parseFromString(string(data))
}

func parseFromString(data string) (*Config, error) {
	cfg := &Config{}
	if _, err := toml.Decode(data, cfg); err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if _, ok := supportServiceTypes[strings.ToUpper(c.ServiceType)]; !ok {
		return fmt.Errorf("service type %s not support", c.ServiceType)
	}
	return nil
}

// FIXME: fileservice created by a config instance is very strange at best
func (c *Config) createFileService(name string) (fileservice.FileService, error) {
	for _, cfg := range c.FileServices {
		if strings.EqualFold(cfg.Name, name) {
			return fileservice.NewFileService(cfg)
		}
	}
	return nil, fmt.Errorf("file service named %s not set", name)
}

func (c *Config) getLogServiceConfig() logservice.Config {
	cfg := c.LogService
	fmt.Printf("hakeeper client cfg: %v", c.HAKeeperClient)
	cfg.HAKeeperClientConfig = c.HAKeeperClient
	return cfg
}
