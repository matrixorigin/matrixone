// Copyright 2023 Matrix Origin
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

package incrservice

const (
	defaultCountPerAllocate = 10000
)

// Config auto increment config
type Config struct {
	// CountPerAllocate how many ids are cached in the current cn node for each assignment
	CountPerAllocate int `toml:"count-per-allocate"`
	// LowCapacity when the remaining number of ids is less than this value, the current cn
	// node will initiate asynchronous task assignment in advance
	LowCapacity int `toml:"low-capacity"`
}

func (c *Config) adjust() {
	if c.CountPerAllocate == 0 {
		c.CountPerAllocate = defaultCountPerAllocate
	}
	if c.LowCapacity == 0 ||
		c.LowCapacity > c.CountPerAllocate {
		c.LowCapacity = c.CountPerAllocate / 2
	}
}
