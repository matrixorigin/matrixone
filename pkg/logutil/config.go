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

package logutil

const (
	defaultLogMaxSize = 512
	port = ":9090"
	pattern = "/change/level"
)

// Config serializes log related config in toml.
type Config struct {
	// Log level.
	Level string `toml:"level"`
	// Log format. one of json, text, or console.
	Format string `toml:"format"`
	// File log config.
	File FileLogConfig `toml:"file"`
}

// FileLogConfig serializes file log related config in toml.
type FileLogConfig struct {
	// Log filename, leave empty to disable file log.
	Filename string `toml:"filename"`
	// Max size for a single file, in MB.
	MaxSize int `toml:"max-size"`
	// Max log keep days, default is never deleting.
	MaxDays int `toml:"max-days"`
	// Maximum number of old log files to retain.
	MaxBackups int `toml:"max-backups"`
}

