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
	"testing"
	"time"

	tp "github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
)

type durationValue struct {
	Zero Duration `toml:"z"`
	Ms   Duration `toml:"ms"`
	Sec  Duration `toml:"sec"`
}

func TestUnmarshalDuration(t *testing.T) {
	cfg := `
		ms = "1ms"
		sec = "1s"
	`
	v := &durationValue{}
	assert.NoError(t, tp.Unmarshal([]byte(cfg), v))
	assert.Equal(t, time.Millisecond, v.Ms.Duration)
	assert.Equal(t, time.Second, v.Sec.Duration)
	assert.Equal(t, time.Duration(0), v.Zero.Duration)
}

func TestMarshalDuration(t *testing.T) {
	v := &durationValue{}
	v.Ms.Duration = time.Millisecond
	v.Sec.Duration = time.Second

	data, err := v.Zero.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "0s", string(data))

	data, err = v.Ms.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "1ms", string(data))

	data, err = v.Sec.MarshalText()
	assert.NoError(t, err)
	assert.Equal(t, "1s", string(data))
}

func TestByteSize(t *testing.T) {
	var v ByteSize

	assert.NoError(t, v.UnmarshalText([]byte("1K")))
	assert.Equal(t, 1024, int(v))

	assert.NoError(t, v.UnmarshalText([]byte("1k")))
	assert.Equal(t, 1024, int(v))

	assert.NoError(t, v.UnmarshalText([]byte("1KB")))
	assert.Equal(t, 1024, int(v))

	assert.NoError(t, v.UnmarshalText([]byte("1Kb")))
	assert.Equal(t, 1024, int(v))

	assert.NoError(t, v.UnmarshalText([]byte("1kB")))
	assert.Equal(t, 1024, int(v))

}
