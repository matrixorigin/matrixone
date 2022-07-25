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

package toml

import (
	"time"

	"github.com/docker/go-units"
)

// Duration is a wrapper of time.Duration for TOML
type Duration struct {
	time.Duration
}

// UnmarshalText parses a TOML string into the duration.
func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

// MarshalText returns the duration as a JSON string.
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.String()), nil
}

// ByteSize is a retype uint64 for TOML.
type ByteSize uint64

// UnmarshalText parses a Toml string into the byte size.
func (b *ByteSize) UnmarshalText(text []byte) error {
	v, err := units.RAMInBytes(string(text))
	if err != nil {
		return err
	}
	*b = ByteSize(v)
	return nil
}
